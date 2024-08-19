package gobitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrNoNeedToMerge = errors.New("no need to merge")
)

type DB struct {
	rw sync.RWMutex
	kd *keyDir
	s  *Storage
}

func NewDB(opt *Options) (*DB, error) {
	db := &DB{
		rw: sync.RWMutex{},
		kd: &keyDir{index: make(map[string]*Index)},
	}
	if isExist, _ := isDirExist(opt.Dir); isExist {
		if err := db.recovery(opt); err != nil {
			return nil, err
		}
		return db, nil
	}
	fileSize := getSegmentSize(opt.SegmentSize)
	s, err := NewStorage(opt.Dir, fileSize)
	if err != nil {
		return nil, err
	}
	db.s = s
	return db, nil
}

func (db *DB) Set(key []byte, value []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	entry := NewEntryWithData(key, value)
	buf := entry.Encode()
	index, err := db.s.writeAt(buf)
	if err != nil {
		return err
	}
	index.keySize = len(key)
	index.valueSize = len(value)
	db.kd.update(string(key), index)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.rw.Lock()
	defer db.rw.Unlock()
	index := db.kd.find(string(key))
	if index == nil {
		return nil, ErrKeyNotFound
	}
	dataSize := MetaSize + index.keySize + index.valueSize
	buf := make([]byte, dataSize)
	entry, err := db.s.readFullEntry(index.fid, index.off, buf)
	if err != nil {
		return nil, err
	}
	return entry.value, nil
}

func (db *DB) Delete(key []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	index := db.kd.find(string(key))
	if index == nil {
		return ErrKeyNotFound
	}
	entry := NewEntry()
	entry.meta.flag = DeleteFlag
	_, err := db.s.writeAt(entry.Encode())
	if err != nil {
		return err
	}
	delete(db.kd.index, string(key))
	return nil
}

func (db *DB) Merge() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	fids, err := getFids(db.s.dir)
	if err != nil {
		return err
	}
	if len(fids) < 2 {
		return ErrNoNeedToMerge
	}
	slices.Sort(fids)
	for _, fid := range fids[:len(fids)-1] {
		var off int64
		for {
			entry, err := db.s.readEntry(fid, off)
			if err != nil {
				if errors.Is(err, deleteEntryErr) {
					off += int64(entry.Size())
					continue
				}
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			off += int64(entry.Size())
			oldIndex := db.kd.find(string(entry.key))
			if oldIndex == nil {
				continue
			}
			if oldIndex.fid == fid && off == oldIndex.off {
				newIndex, err := db.s.writeAt(entry.Encode())
				if err != nil {
					return err
				}
				db.kd.update(string(entry.key), newIndex)
			}
		}
		path := fmt.Sprintf("%s/%d%s", db.s.dir, fid, fileSuffix)
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) recovery(opt *Options) error {
	fileSize := getSegmentSize(opt.SegmentSize)
	db.s = &Storage{
		dir:      opt.Dir,
		fileSize: fileSize,
		fds:      make(map[int]*os.File),
	}
	fids, err := getFids(opt.Dir)
	if err != nil {
		return err
	}
	slices.Sort(fids)
	for _, fid := range fids {
		var off int64
		path := fmt.Sprintf("%s/%d%s", opt.Dir, fid, fileSuffix)
		fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		db.s.fds[fid] = fd
		for {
			entry, err := db.s.readEntry(fid, off)
			if err != nil {
				if errors.Is(err, deleteEntryErr) {
					off += int64(entry.Size())
					continue
				}
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			db.kd.index[string(entry.key)] = &Index{
				fid:       fid,
				off:       off,
				timestamp: entry.meta.timestamp,
			}
			off += int64(entry.Size())
		}
		if fid == fids[len(fids)-1] {
			db.s.af = &ActiveFile{
				fid: fid,
				off: off,
				f:   fd,
			}
		}
	}

	return nil
}
