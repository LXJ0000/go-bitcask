package gobitcask

import (
	"errors"
	"fmt"
	"os"
)

var (
	readMissDataErr  = errors.New("miss data during read")
	writeMissDataErr = errors.New("miss data during write")
	crcErr           = errors.New("crc error")
	deleteEntryErr   = errors.New("read an entry which had deleted")
)

const (
	fileSuffix = ".dat"
	B          = 1
	KB         = 1024 * B
	MB         = 1024 * KB
	GB         = 1024 * MB
)

type Storage struct {
	dir      string
	fileSize int64
	fds      map[int]*os.File // 打开的文件描述符
	af       *ActiveFile
}

func NewStorage(dir string, fileSize int64) (*Storage, error) {
	if err := os.Mkdir(dir, os.ModePerm); err != nil {
		return nil, err
	}
	s := &Storage{
		dir:      dir,
		fileSize: fileSize,
		fds:      make(map[int]*os.File),
	}
	s.af = &ActiveFile{
		fid: 0,
		off: 0,
	}
	path := s.getPath()
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	s.af.f = fd
	s.fds[0] = fd
	return s, nil
}

type ActiveFile struct {
	fid int
	off int64
	f   *os.File
}

func (s *Storage) readEntry(fid int, off int64) (*Entry, error) {
	buf := make([]byte, MetaSize)
	if err := s.readAt(fid, off, buf); err != nil {
		return nil, err
	}
	entry := NewEntry()
	entry.DecodeMeta(buf)
	if entry.meta.flag == DeleteFlag {
		return entry, deleteEntryErr
	}
	off += MetaSize
	payloadSize := entry.meta.keySize + entry.meta.valueSize
	payload := make([]byte, payloadSize)
	if err := s.readAt(fid, off, payload); err != nil {
		return nil, err
	}
	entry.DecodePayload(payload)
	crc := entry.getCrc(buf)
	if entry.meta.crc != crc {
		return nil, crcErr
	}
	return entry, nil
}

func (s *Storage) readFullEntry(fid int, off int64, buf []byte) (e *Entry, err error) {
	err = s.readAt(fid, off, buf)
	if err != nil {
		return nil, err
	}
	e = NewEntry()
	e.DecodeMeta(buf[0:MetaSize])
	payloadSize := e.meta.keySize + e.meta.keySize
	e.DecodePayload(buf[MetaSize : MetaSize+payloadSize])
	crc := e.getCrc(buf[:MetaSize])
	if e.meta.crc != crc {
		return nil, crcErr
	}
	return e, nil
}

func (s *Storage) readAt(fid int, off int64, bytes []byte) error {
	if fd, ok := s.fds[fid]; ok {
		n, err := fd.ReadAt(bytes, off)
		if err != nil {
			return err
		}
		if n != len(bytes) {
			return readMissDataErr
		}
		return nil
	}
	path := fmt.Sprintf("%s/%d%s", s.dir, fid, fileSuffix)
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	s.fds[fid] = fd
	n, err := fd.ReadAt(bytes, off)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return readMissDataErr
	}
	return nil
}

func (af *ActiveFile) writeAt(bytes []byte) error {
	n, err := af.f.WriteAt(bytes, af.off)
	if n != len(bytes) {
		return writeMissDataErr
	}
	return err
}

func (s *Storage) writeAt(bytes []byte) (*Index, error) {
	if err := s.af.writeAt(bytes); err != nil {
		return nil, err
	}
	i := &Index{
		fid: s.af.fid,
		off: s.af.off,
	}
	s.af.off += int64(len(bytes))
	if s.af.off >= s.fileSize {
		if err := s.rotate(); err != nil {
			return nil, err
		}
	}
	return i, nil
}

func (s *Storage) rotate() error { // TODO check
	path := fmt.Sprintf("%s/%d%s", s.dir, s.af.fid+1, fileSuffix)
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	af := &ActiveFile{
		fid: s.af.fid + 1,
		f:   fd,
	}
	s.fds[af.fid] = fd
	s.af = af
	return nil
}

func (s *Storage) getPath() string {
	path := fmt.Sprintf("%s/%d%s", s.dir, s.af.fid, fileSuffix)
	return path
}
