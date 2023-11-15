package index

import "github.com/eric49861/kvdb/wal"

// Indexer 是一个通用的Index接口，为了方便后序实现不同的索引支持，将其抽象为接口
// 该索引用于存储key和value在文件中的position
// 当启动数据库的时候，会发生索引的重建，从hint file中加载索引
type Indexer interface {
	// Put 将key和对应的position写入预写日志
	Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition

	// Get 获取key在数据库文件中的位置
	Get(key []byte) *wal.ChunkPosition

	// Delete 从索引中删除key，并返回该key在文件中的位置
	Delete(key []byte) (*wal.ChunkPosition, bool)

	// Size 索引中包含的key的数量
	Size() int

	// Ascend 按照升序的方式遍历所有的索引，遍历的结果将作为参数传入回调函数 handleFn
	Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// AscendRange 获取指定key范围内的数据索引，遍历的结果将作为参数传入回调函数 handleFn
	AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// AscendGreaterOrEqual
	AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// Descend 按照降序遍历所有的key，遍历的结果将作为参数传入回调函数 handleFn
	Descend(handleFn func(key []byte, pos *wal.ChunkPosition) (bool, error))

	// DescendRange
	DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))

	// DescendLessOrEqual
	DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
}

type IndexerType = byte

const (
	BTree IndexerType = iota
)

var indexType = BTree

// NewIndexer 根据索引类型创建一个索引的实例
func NewIndexer() Indexer {
	switch indexType {
	case BTree:
		return newBTree()
	default:
		panic("unexpected index type")
	}
}
