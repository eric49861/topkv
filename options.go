package kvdb

import "os"

// Options 当打开一个数据库目录的时候的配置
type Options struct {
	// SEGMENT文件的存放目录
	DirPath string

	// SegmentSize SEGMENT文件的最大占用空间
	SegmentSize int64

	// BlockCache 用于缓存块数据，提高查找效率
	BlockCache uint32

	// Sync 用于配置是否在将数据写入缓冲的时候立即写入磁盘
	// 如果为false，当数据库宕机时，会发生部分日志记录丢失
	// 如果为true，会损失一部分性能
	Sync bool

	// BytesPerSync 每一次刷新写入磁盘的字节数
	BytesPerSync uint32

	// WatchQueueSize 大于 0 表示启动watch
	WatchQueueSize uint64
}

// BatchOptions 配置batch
type BatchOptions struct {
	// Sync
	Sync bool
	// ReadOnly 表示该batch是可读的
	ReadOnly bool
}

// IteratorOptions 迭代器的配置
type IteratorOptions struct {
	// Prefix 过滤指定前缀的key
	Prefix []byte

	// Reverse 前向迭代和后向迭代
	Reverse bool
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:        tempDBDir(),
	SegmentSize:    1 * GB,
	BlockCache:     0,
	Sync:           false,
	BytesPerSync:   0,
	WatchQueueSize: 0,
}

var DefaultBatchOptions = BatchOptions{
	Sync:     true,
	ReadOnly: false,
}

func tempDBDir() string {
	dir, _ := os.MkdirTemp("", "eric-temp")
	return dir
}
