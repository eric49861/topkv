package utils

import (
	"io/fs"
	"path/filepath"
)

// DirSize 递归地计算一个目录下所有文件的大小总和
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
