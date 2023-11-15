package kvdb

import "errors"

//  定义了项目所有使用到的错误类型，方便错误信息的显示和错误定位

var (
	ErrKeyIsEmpty      = errors.New("the key is empty")
	ErrKeyNotFound     = errors.New("key not found in database")
	ErrDatabaseIsUsing = errors.New("the database directory is used by another process")
	ErrReadOnlyBatch   = errors.New("the batch is read only")
	ErrBatchCommitted  = errors.New("the batch is already committed")
	ErrBatchRolledBack = errors.New("the batch has already rolled back")
	ErrDBClosed        = errors.New("the database is closed")
	ErrMergeRunning    = errors.New("the merge operation is running")
	ErrWatchDisabled   = errors.New("the watch is disabled")
)
