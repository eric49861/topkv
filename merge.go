package kvdb

import (
	"encoding/binary"
	"fmt"
	"github.com/eric49861/kvdb/index"
	"github.com/eric49861/kvdb/wal"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/valyala/bytebufferpool"
)

const (
	mergeDirSuffixName   = "-merge"
	mergeFinishedBatchID = 0
)

// Merge 合并数据库所有的数据文件
// 遍历数据文件中所有的数据日志记录，然后合并数据
// 该操作是耗时的，应该在数据空闲的时候调用
func (db *DB) Merge(reopenAfterDone bool) error {
	if err := db.doMerge(); err != nil {
		return err
	}
	if !reopenAfterDone {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	_ = db.closeFiles()

	// 加载新的合并文件
	err := loadMergeFiles(db.options.DirPath)
	if err != nil {
		return err
	}

	// 打开数据文件
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return err
	}

	// 创建一个新的索引器
	db.index = index.NewIndexer()
	// 重建索引
	if err = db.loadIndex(); err != nil {
		return err
	}

	return nil
}

// doMerge 执行合并操作
func (db *DB) doMerge() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	// 检查数据库的文件是否为空
	if db.dataFiles.IsEmpty() {
		db.mu.Unlock()
		return nil
	}
	// 检查是否正在进行合并操作
	if atomic.LoadUint32(&db.mergeRunning) == 1 {
		db.mu.Unlock()
		return ErrMergeRunning
	}
	// 修改合并操作正在运行的标志
	atomic.StoreUint32(&db.mergeRunning, 1)
	// 当合并操作执行结束时，复位标志
	defer atomic.StoreUint32(&db.mergeRunning, 0)

	prevActiveSegId := db.dataFiles.ActiveSegmentID()
	// 创建一个新的激活的数据文件，将合并的后数据写入新的数据文件中
	if err := db.dataFiles.OpenNewActiveSegment(); err != nil {
		db.mu.Unlock()
		return err
	}

	db.mu.Unlock()

	// 打开一个merge db
	mergeDB, err := db.openMergeDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()

	buf := bytebufferpool.Get()
	now := time.Now().UnixNano()
	defer bytebufferpool.Put(buf)

	//迭代所有的数据文件
	reader := db.dataFiles.NewReaderWithMax(prevActiveSegId)
	for {
		buf.Reset()
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLogRecord(chunk)
		// Only handle the normal log record, LogRecordDeleted and LogRecordBatchFinished
		// will be ignored, because they are not valid data.
		if record.Type == LogRecordNormal && (record.Expire == 0 || record.Expire > now) {
			db.mu.RLock()
			indexPos := db.index.Get(record.Key)
			db.mu.RUnlock()
			if indexPos != nil && positionEquals(indexPos, position) {
				record.BatchId = mergeFinishedBatchID
				newPosition, err := mergeDB.dataFiles.Write(encodeLogRecord(record, mergeDB.encodeHeader, buf))
				if err != nil {
					return err
				}
				_, err = mergeDB.hintFile.Write(encodeHintRecord(record.Key, newPosition))
				if err != nil {
					return err
				}
			}
		}
	}

	// After rewrite all the data, we should add a file to indicate that the merge operation is completed.
	// So when we restart the database, we can know that the merge is completed if the file exists,
	// otherwise, we will delete the merge directory and redo the merge operation again.
	mergeFinFile, err := mergeDB.openMergeFinishedFile()
	if err != nil {
		return err
	}
	_, err = mergeFinFile.Write(encodeMergeFinRecord(prevActiveSegId))
	if err != nil {
		return err
	}
	// close the merge finished file
	if err := mergeFinFile.Close(); err != nil {
		return err
	}

	// all done successfully, return nil
	return nil
}

func (db *DB) openMergeDB() (*DB, error) {
	mergePath := mergeDirPath(db.options.DirPath)
	// 删除merge目录如果存在
	if err := os.RemoveAll(mergePath); err != nil {
		return nil, err
	}
	options := db.options
	options.Sync, options.BytesPerSync = false, 0
	options.DirPath = mergePath
	mergeDB, err := Open(options)
	if err != nil {
		return nil, err
	}

	// 打开线索文件，用于存储索引数据
	hintFile, err := wal.Open(wal.Options{
		DirPath:        options.DirPath,
		SegmentSize:    math.MaxInt64,
		SegmentFileExt: hintFileNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
		BlockCache:     0,
	})
	if err != nil {
		return nil, err
	}
	mergeDB.hintFile = hintFile
	return mergeDB, nil
}

func mergeDirPath(dirPath string) string {
	dir := filepath.Dir(filepath.Clean(dirPath))
	base := filepath.Base(dirPath)
	return filepath.Join(dir, base+mergeDirSuffixName)
}

func (db *DB) openMergeFinishedFile() (*wal.WAL, error) {
	return wal.Open(wal.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    GB,
		SegmentFileExt: mergeFinNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
		BlockCache:     0,
	})
}

func positionEquals(a, b *wal.ChunkPosition) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}

func encodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	// SegmentId BlockNumber ChunkOffset ChunkSize
	//    5          5           10          5      =    25
	// see binary.MaxVarintLen64 and binary.MaxVarintLen32
	buf := make([]byte, 25)
	var idx = 0

	// SegmentId
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentId))
	// BlockNumber
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))

	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
}

func decodeHintRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	var idx = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]

	return key, &wal.ChunkPosition{
		SegmentId:   wal.SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

func encodeMergeFinRecord(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}

// loadMergeFiles 加载合并文件
func loadMergeFiles(dirPath string) error {
	// check if there is a merge directory
	mergeDirPath := mergeDirPath(dirPath)
	if _, err := os.Stat(mergeDirPath); err != nil {
		// does not exist, just return.
		return nil
	}

	// remove the merge directory at last
	defer func() {
		_ = os.RemoveAll(mergeDirPath)
	}()

	copyFile := func(suffix string, fileId uint32, force bool) {
		srcFile := wal.SegmentFileName(mergeDirPath, suffix, fileId)
		stat, err := os.Stat(srcFile)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			panic(fmt.Sprintf("loadMergeFiles: failed to get src file stat %v", err))
		}
		if !force && stat.Size() == 0 {
			return
		}
		destFile := wal.SegmentFileName(dirPath, suffix, fileId)
		_ = os.Rename(srcFile, destFile)
	}

	// get the merge finished segment id
	mergeFinSegmentId, err := getMergeFinSegmentId(mergeDirPath)
	if err != nil {
		return err
	}
	// now we get the merge finished segment id, so all the segment id less than the merge finished segment id
	// should be moved to the original data directory, and the original data files should be deleted.
	for fileId := uint32(1); fileId <= mergeFinSegmentId; fileId++ {
		destFile := wal.SegmentFileName(dirPath, dataFileNameSuffix, fileId)
		// will have bug here if continue, check it later.todo

		// If we call Merge multiple times, some segment files will be deleted earlier, so just skip them.
		// if _, err = os.Stat(destFile); os.IsNotExist(err) {
		// 	continue
		// } else if err != nil {
		// 	return err
		// }

		// remove the original data file
		if _, err = os.Stat(destFile); err == nil {
			if err = os.Remove(destFile); err != nil {
				return err
			}
		}
		// move the merge data file to the original data directory
		copyFile(dataFileNameSuffix, fileId, false)
	}

	// copy MERGEFINISHED and HINT files to the original data directory
	// there is only one merge finished file, so the file id is always 1,
	// the same as the hint file.
	copyFile(mergeFinNameSuffix, 1, true)
	copyFile(hintFileNameSuffix, 1, true)

	return nil
}

func getMergeFinSegmentId(mergePath string) (wal.SegmentID, error) {
	// check if the merge operation is completed
	mergeFinFile, err := os.Open(wal.SegmentFileName(mergePath, mergeFinNameSuffix, 1))
	if err != nil {
		// if the merge finished file does not exist, it means that the merge operation is not completed.
		// so we should remove the merge directory and return nil.
		return 0, nil
	}
	defer func() {
		_ = mergeFinFile.Close()
	}()

	// Only 4 bytes are needed to store the segment id.
	// And the first 7 bytes are chunk header.
	mergeFinBuf := make([]byte, 4)
	if _, err := mergeFinFile.ReadAt(mergeFinBuf, 7); err != nil {
		return 0, err
	}
	mergeFinSegmentId := binary.LittleEndian.Uint32(mergeFinBuf)
	return mergeFinSegmentId, nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFile, err := wal.Open(wal.Options{
		DirPath: db.options.DirPath,
		// we don't need to rotate the hint file, just write all data to the same file.
		SegmentSize:    math.MaxInt64,
		SegmentFileExt: hintFileNameSuffix,
		BlockCache:     32 * KB * 10,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = hintFile.Close()
	}()

	// read all the hint records from the hint file
	reader := hintFile.NewReader()
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, position := decodeHintRecord(chunk)
		// All the hint records are valid because it is generated by the merge operation.
		// So just put them into the index without checking.
		db.index.Put(key, position)
	}
	return nil
}
