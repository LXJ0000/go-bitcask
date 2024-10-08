package gobitcask

import (
	"os"
	"path"
	"strconv"
	"strings"
)

func isDirExist(dir string) (bool, error) {
	_, err := os.Stat(dir)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func getSegmentSize(size int64) int64 {
	var fileSize int64
	if size <= 0 {
		fileSize = DefaultSegmentSize
	} else {
		fileSize = size
	}
	return fileSize
}

func getFids(dir string) (fids []int, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		fileName := f.Name()
		filePath := path.Base(fileName)
		if path.Ext(filePath) == fileSuffix {
			filePrefix := strings.TrimSuffix(filePath, fileSuffix)
			fid, err := strconv.Atoi(filePrefix)
			if err != nil {
				return nil, err
			}
			fids = append(fids, fid)
		}
	}
	return fids, nil
}
