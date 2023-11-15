package kvdb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/valyala/bytebufferpool"
)

// Batch 表示数据库的批量操作
// 根据options，如果是只读的，则只能调用GET，否则将报错
// 如果是可写的，则能调用GET/PUT/DELETE
// 不能当做事务，因为不保证隔离性，保证原子性/一致性/持久性(主要开启sync，每次写入都会刷写到磁盘)
type Batch struct {
	db            *DB
	pendingWrites []*LogRecord // save the data to be written
	options       BatchOptions
	mu            sync.RWMutex
	committed     bool // batch是否被提交
	rollbacked    bool // batch是否被回滚
	batchId       *snowflake.Node
	buffers       []*bytebufferpool.ByteBuffer
}

// NewBatch 创建一个新的batch实例
func (db *DB) NewBatch(options BatchOptions) *Batch {
	batch := &Batch{
		db:         db,
		options:    options,
		committed:  false,
		rollbacked: false,
	}
	if !options.ReadOnly {
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func newBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

func newRecord() interface{} {
	return &LogRecord{}
}

func (b *Batch) init(rdonly, sync bool, db *DB) *Batch {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
	return b
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWrites = b.pendingWrites[:0]
	b.committed = false
	b.rollbacked = false
	// put all buffers back to the pool
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put adds a key-value pair to the batch for writing.
func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// write to pendingWrites
	var record *LogRecord
	// 先检查预写日志中是否存在相同的key
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	if record == nil {
		// 说明不存在相同的key，写入一条新的日志
		record = b.db.recordPool.Get().(*LogRecord)
		b.pendingWrites = append(b.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, 0
	b.mu.Unlock()

	return nil
}

// PutWithTTL 增加一个key-value pair
func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// 写到日志文件中
	var record *LogRecord
	// 如果key存在，则直接更新
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	if record == nil {
		// 如果key不存在，则将record写入预写日志缓存中
		record = b.db.recordPool.Get().(*LogRecord)
		b.pendingWrites = append(b.pendingWrites, record)
	}

	record.Key, record.Value = key, value
	record.Type, record.Expire = LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.mu.Unlock()

	return nil
}

// Get 获取和key相关的value
func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// 先检查预写日志中是否存在该key值
	b.mu.RLock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	b.mu.RUnlock()

	// 如果记录在预写日志缓存中，根据日志的类型返回
	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			return nil, ErrKeyNotFound
		}
		return record.Value, nil
	}

	// 从索引中查询，查不到说明不存在
	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}

	// 检查数据是否是过期的或者是被删除的
	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	// 如果key过期了，将其从索引中删除
	if record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// Delete 删除key
func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	// only need key and type when deleting a value.
	var exist bool
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			b.pendingWrites[i].Type = LogRecordDeleted
			b.pendingWrites[i].Value = nil
			b.pendingWrites[i].Expire = 0
			exist = true
			break
		}
	}
	if !exist {
		b.pendingWrites = append(b.pendingWrites, &LogRecord{
			Key:  key,
			Type: LogRecordDeleted,
		})
	}
	b.mu.Unlock()

	return nil
}

// Exist 检查是否存在数据库中，和Get方法逻辑相同
func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}

	now := time.Now().UnixNano()
	// 检查是否存在于预写日志缓存中
	b.mu.RLock()
	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}
	b.mu.RUnlock()

	if record != nil {
		return record.Type != LogRecordDeleted && !record.IsExpired(now), nil
	}

	//
	position := b.db.index.Get(key)
	if position == nil {
		return false, nil
	}

	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}

	record = decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return false, nil
	}
	return true, nil
}

// Expire 设置key的过期时间
func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}

	if record != nil {
		if record.Type == LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
	} else {
		position := b.db.index.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := b.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		now := time.Now()
		record = decodeLogRecord(chunk)
		if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			b.db.index.Delete(key)
			return ErrKeyNotFound
		}
		record.Expire = now.Add(ttl).UnixNano()
		b.pendingWrites = append(b.pendingWrites, record)
	}

	return nil
}

// TTL 获取key的过期时间，和Get的逻辑相同
func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}

	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pendingWrites) > 0 {
		var record *LogRecord
		for i := len(b.pendingWrites) - 1; i >= 0; i-- {
			if bytes.Equal(key, b.pendingWrites[i].Key) {
				record = b.pendingWrites[i]
				break
			}
		}
		if record != nil {
			if record.Expire == 0 {
				return -1, nil
			}
			if record.Type == LogRecordDeleted || record.IsExpired(now.UnixNano()) {
				return -1, ErrKeyNotFound
			}
			return time.Duration(record.Expire - now.UnixNano()), nil
		}
	}

	position := b.db.index.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}

	record := decodeLogRecord(chunk)
	if record.Type == LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return -1, ErrKeyNotFound
	}

	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	return -1, nil
}

// Persist 删除key的过期时间
func (b *Batch) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var record *LogRecord
	for i := len(b.pendingWrites) - 1; i >= 0; i-- {
		if bytes.Equal(key, b.pendingWrites[i].Key) {
			record = b.pendingWrites[i]
			break
		}
	}

	if record != nil {
		if record.Type == LogRecordDeleted && record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = 0
	} else {
		position := b.db.index.Get(key)
		if position == nil {
			return ErrKeyNotFound
		}
		chunk, err := b.db.dataFiles.Read(position)
		if err != nil {
			return err
		}

		record := decodeLogRecord(chunk)
		now := time.Now().UnixNano()
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
			return ErrKeyNotFound
		}
		if record.Expire == 0 {
			return nil
		}

		record.Expire = 0
		b.pendingWrites = append(b.pendingWrites, record)
	}

	return nil
}

// Commit 当ronly的batch调用时，会返回错误
func (b *Batch) Commit() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// 检查batch是否已经被提交或者回滚，防止重复提交
	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRolledBack
	}

	batchId := b.batchId.Generate()
	now := time.Now().UnixNano()
	// write to wal buffer
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := encodeLogRecord(record, b.db.encodeHeader, buf)
		b.db.dataFiles.PendingWrites(encRecord)
	}

	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	endRecord := encodeLogRecord(&LogRecord{
		Key:  batchId.Bytes(),
		Type: LogRecordBatchFinished,
	}, b.db.encodeHeader, buf)
	b.db.dataFiles.PendingWrites(endRecord)

	chunkPositions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	if len(chunkPositions) != len(b.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}

	// 根据options中的配置选择是否立即刷新到磁盘
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}

	// 写到索引中
	for i, record := range b.pendingWrites {
		if record.Type == LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, chunkPositions[i])
		}

		if b.db.options.WatchQueueSize > 0 {
			e := &Event{Key: record.Key, Value: record.Value, BatchId: record.BatchId}
			if record.Type == LogRecordDeleted {
				e.Action = WatchActionDelete
			} else {
				e.Action = WatchActionPut
			}
			b.db.watcher.putEvent(e)
		}
		b.db.recordPool.Put(record)
	}

	b.committed = true
	return nil
}

// Rollback 回滚
func (b *Batch) Rollback() error {
	defer b.unlock()

	if b.db.closed {
		return ErrDBClosed
	}

	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRolledBack
	}

	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}

	if !b.options.ReadOnly {
		// 清空预写日志缓存
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
	}

	b.rollbacked = true
	return nil
}
