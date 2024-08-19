package gobitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	MetaSize   = 29 // 4 + 8 + 8 + 4 + 4 + 1
	DeleteFlag = 1
)

type Entry struct {
	key   []byte
	value []byte
	meta  *Meta
}

func NewEntry() *Entry {
	return &Entry{
		meta: &Meta{},
	}
}

func NewEntryWithData(key, value []byte) *Entry {
	entry := &Entry{
		key:   key,
		value: value,
	}
	entry.meta = &Meta{
		timestamp: uint64(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
	}
	return entry
}

type Meta struct {
	crc       uint32
	position  uint64
	timestamp uint64
	keySize   uint32
	valueSize uint32
	flag      uint8
}

func (e *Entry) Encode() []byte {
	size := e.Size()
	buf := make([]byte, size)
	binary.LittleEndian.PutUint64(buf[4:12], e.meta.position)
	binary.LittleEndian.PutUint64(buf[12:20], e.meta.timestamp)
	binary.LittleEndian.PutUint32(buf[20:24], e.meta.keySize)
	binary.LittleEndian.PutUint32(buf[24:28], e.meta.valueSize)
	buf[28] = e.meta.flag
	if e.meta.flag != DeleteFlag {
		copy(buf[MetaSize:MetaSize+e.meta.keySize], e.key)
		copy(buf[MetaSize+e.meta.keySize:MetaSize+e.meta.keySize+e.meta.valueSize], e.value)
	}
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)
	return buf
}

func (e *Entry) DecodePayload(preload []byte) {
	keyHightBound := e.meta.keySize
	valueHightBound := e.meta.keySize + e.meta.valueSize
	e.key = preload[:keyHightBound]
	e.value = preload[keyHightBound:valueHightBound]
}

func (e *Entry) DecodeMeta(meta []byte) {
	e.meta = &Meta{
		crc:       binary.LittleEndian.Uint32(meta[0:4]),
		position:  binary.LittleEndian.Uint64(meta[4:12]),
		timestamp: binary.LittleEndian.Uint64(meta[12:20]),
		keySize:   binary.LittleEndian.Uint32(meta[20:24]),
		valueSize: binary.LittleEndian.Uint32(meta[24:28]),
		flag:      meta[28],
	}
}

func (e *Entry) Size() int {
	return int(e.meta.keySize + e.meta.valueSize + MetaSize)
}

func (e *Entry) getCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.key)
	crc = crc32.Update(crc, crc32.IEEETable, e.value)
	return crc
}
