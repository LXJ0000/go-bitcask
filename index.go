package gobitcask

type keyDir struct {
	index map[string]*Index
}

type Index struct {
	fid       int
	off       int64
	timestamp uint64
	keySize   int
	valueSize int
}

func (kd *keyDir) find(key string) *Index {
	i := kd.index[key]
	return i
}

func (kd *keyDir) update(key string, i *Index) {
	kd.index[key] = i
}
