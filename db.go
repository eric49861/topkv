package kvdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/eric49861/kvdb/index"
	"github.com/eric49861/kvdb/utils"
	"github.com/eric49861/kvdb/wal"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

const (
	fileLockName       = "FLOCK"
	dataFileNameSuffix = ".SEG"
	hintFileNameSuffix = ".HINT"
	mergeFinNameSuffix = ".MERGEFIN"
)

type DB struct {
	dataFiles        *wal.WAL
	hintFile         *wal.WAL
	index            index.Indexer
	options          Options
	fileLock         *flock.Flock
	mu               sync.RWMutex
	closed           bool
	mergeRunning     uint32
	batchPool        sync.Pool
	recordPool       sync.Pool
	encodeHeader     []byte
	watchCh          chan *Event
	watcher          *Watcher
	expiredCursorKey []byte
}

// Stat 用于统计数据库的数据
type Stat struct {
	KeysNum  int
	DiskSize int64
}

// checkOptions 检查数据库配置是否合法
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.SegmentSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

// Open 打开一个数据库实例
// 主要做以下工作：
// 1. 打开数据库目录, 如果不存在就新建
// 2. 给目录进行加锁
// 3. 创建数据库实例
// 4. 加载数据文件
// 5. 启动watch监听事件(如果配置启动)
func Open(options Options) (*DB, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 如果目录不存在则创建一个新的目录
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 创建一个文件锁，防止并发的数据库进程对相同的数据库目录进行操作
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 如果merge file存在，则加载 merge file
	if err = loadMergeFiles(options.DirPath); err != nil {
		return nil, err
	}

	// 初始化数据库的实例
	db := &DB{
		index:        index.NewIndexer(),
		options:      options,
		fileLock:     fileLock,
		batchPool:    sync.Pool{New: newBatch},
		recordPool:   sync.Pool{New: newRecord},
		encodeHeader: make([]byte, maxLogRecordHeaderSize),
	}

	// 打开数据库文件
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}

	// 加载索引
	if err = db.loadIndex(); err != nil {
		return nil, err
	}

	// 启动监视器
	if options.WatchQueueSize > 0 {
		db.watchCh = make(chan *Event, 100)
		db.watcher = NewWatcher(options.WatchQueueSize)
		// 启动一个协程来
		go db.watcher.sendEvent(db.watchCh)
	}

	return db, nil
}

func (db *DB) openWalFiles() (*wal.WAL, error) {
	walFiles, err := wal.Open(wal.Options{
		DirPath:        db.options.DirPath,
		SegmentSize:    db.options.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		BlockCache:     db.options.BlockCache,
		Sync:           db.options.Sync,
		BytesPerSync:   db.options.BytesPerSync,
	})
	if err != nil {
		return nil, err
	}
	return walFiles, nil
}

// 加载索引文件
func (db *DB) loadIndex() error {
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}
	if err := db.loadIndexFromWAL(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.closeFiles(); err != nil {
		return err
	}

	// 释放数据库的文件锁
	if err := db.fileLock.Unlock(); err != nil {
		return err
	}
	// 关闭监视器管道
	if db.options.WatchQueueSize > 0 {
		close(db.watchCh)
	}
	// 更新数据库的状态
	db.closed = true
	return nil
}

// 关闭数据库持有的文件
func (db *DB) closeFiles() error {
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 刷新缓冲中的数据到磁盘中
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.dataFiles.Sync()
}

// Stat 数据库的数据统计
func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	diskSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("get database directory size error: %v", err))
	}

	return &Stat{
		KeysNum:  db.index.Size(),
		DiskSize: diskSize,
	}
}

// Put 将key/value pair存入数据库
func (db *DB) Put(key []byte, value []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.Put(key, value); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// PutWithTTL 带过期时间
func (db *DB) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.PutWithTTL(key, value, ttl); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Get 从数据库中获取key值所对应的value
func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

// Delete 删除key
func (db *DB) Delete(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()

	batch.init(false, false, db)
	if err := batch.Delete(key); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// Exist 判断key是否存在
func (db *DB) Exist(key []byte) (bool, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Exist(key)
}

// Expire 设置key的过期时间
func (db *DB) Expire(key []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()

	batch.init(false, false, db)
	if err := batch.Expire(key, ttl); err != nil {
		batch.Rollback()
		return err
	}
	return batch.Commit()
}

// TTL 获取key的过期时间
func (db *DB) TTL(key []byte) (time.Duration, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.TTL(key)
}

// Persist 持久化key的存储，即移除key的过期时间
func (db *DB) Persist(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.Persist(key); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

func (db *DB) Watch() (chan *Event, error) {
	if db.options.WatchQueueSize <= 0 {
		return nil, ErrWatchDisabled
	}
	return db.watchCh, nil
}

// Ascend 升序遍历key/value, 本质是遍历B树的索引节点
func (db *DB) Ascend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, err
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendRange 升序遍历某个范围的key/value
func (db *DB) AscendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendGreaterOrEqual 升序遍历大于等于某个key的所有pair
func (db *DB) AscendGreaterOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.AscendGreaterOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// AscendKeys 升序遍历所有的keys，支持使用正则模式匹配
// filterExpired 表示是否过滤掉过期的key
func (db *DB) AscendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		reg = regexp.MustCompile(string(pattern))
	}

	db.index.Ascend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg == nil || reg.Match(key) {
			var invalid bool
			if filterExpired {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					return false, err
				}
				if value := db.checkValue(chunk); value == nil {
					invalid = true
				}
			}
			if invalid {
				return true, nil
			}
			return handleFn(key)
		}
		return true, nil
	})
}

// Descend 降序遍历所有的key/value, 对于每一个key/value pair,将调用handleFn
func (db *DB) Descend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendRange 降序遍历某个范围内的key/value pair, 并调用回调函数
func (db *DB) DescendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendLessOrEqual 降序遍历 <= key 的值
func (db *DB) DescendLessOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.index.DescendLessOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

// DescendKeys 和AscendKeys功能相反
func (db *DB) DescendKeys(pattern []byte, filterExpired bool, handleFn func(k []byte) (bool, error)) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var reg *regexp.Regexp
	if len(pattern) > 0 {
		reg = regexp.MustCompile(string(pattern))
	}

	db.index.Descend(func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		if reg == nil || reg.Match(key) {
			var invalid bool
			if filterExpired {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					return false, err
				}
				if value := db.checkValue(chunk); value == nil {
					invalid = true
				}
			}
			if invalid {
				return true, nil
			}
			return handleFn(key)
		}
		return true, nil
	})
}

// checkValue 如果key没有被删除并且没有过期，返回key所对应的value,
// 否则返回nil
func (db *DB) checkValue(chunk []byte) []byte {
	record := decodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if record.Type != LogRecordDeleted && !record.IsExpired(now) {
		return record.Value
	}
	return nil
}

// loadIndexFromWAL 加载索引从WAL文件
// 逻辑很简单，遍历所有的wal日志文件，读出所有的key/value，根据key和所在文件的offset然后重建索引
func (db *DB) loadIndexFromWAL() error {
	mergeFinSegmentId, err := getMergeFinSegmentId(db.options.DirPath)
	if err != nil {
		return err
	}
	indexRecords := make(map[uint64][]*IndexRecord)
	now := time.Now().UnixNano()

	reader := db.dataFiles.NewReader()
	for {
		if reader.CurrentSegmentId() <= mergeFinSegmentId {
			reader.SkipCurrentSegment()
			continue
		}

		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := decodeLogRecord(chunk)
		if record.Type == LogRecordBatchFinished {
			batchId, err := snowflake.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(batchId)] {
				if idxRecord.recordType == LogRecordNormal {
					db.index.Put(idxRecord.key, idxRecord.position)
				}
				if idxRecord.recordType == LogRecordDeleted {
					db.index.Delete(idxRecord.key)
				}
			}
			delete(indexRecords, uint64(batchId))
		} else if record.Type == LogRecordNormal && record.BatchId == mergeFinishedBatchID {
			db.index.Put(record.Key, position)
		} else {
			if record.IsExpired(now) {
				db.index.Delete(record.Key)
				continue
			}
			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&IndexRecord{
					key:        record.Key,
					recordType: record.Type,
					position:   position,
				})
		}
	}
	return nil
}

func (db *DB) DeleteExpiredKeys(timeout time.Duration) error {
	// set timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{}, 1)

	var innerErr error
	now := time.Now().UnixNano()
	go func(ctx context.Context) {
		db.mu.Lock()
		defer db.mu.Unlock()
		for {
			positions := make([]*wal.ChunkPosition, 0, 100)
			db.index.AscendGreaterOrEqual(db.expiredCursorKey, func(k []byte, pos *wal.ChunkPosition) (bool, error) {
				positions = append(positions, pos)
				if len(positions) >= 100 {
					return false, nil
				}
				return true, nil
			})

			if len(positions) == 0 {
				db.expiredCursorKey = nil
				done <- struct{}{}
				return
			}
			for _, pos := range positions {
				chunk, err := db.dataFiles.Read(pos)
				if err != nil {
					innerErr = err
					done <- struct{}{}
					return
				}
				record := decodeLogRecord(chunk)
				if record.IsExpired(now) {
					db.index.Delete(record.Key)
				}
				db.expiredCursorKey = record.Key
			}
		}
	}(ctx)

	select {
	case <-ctx.Done():
		return innerErr
	case <-done:
		return nil
	}
}
