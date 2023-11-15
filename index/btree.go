package index

import (
	"bytes"
	"github.com/eric49861/kvdb/wal"
	"github.com/google/btree"
	"sync"
)

// google实现的btree中迭代器的设计使用了一种将接口type为函数的设计方法
// 这样可以实现在传参时既可以传入一个接口的实例也可以传入一个函数

// MemoryBTree 基于内存的B树索引的实现，封装了google/btree
type MemoryBTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// 一个item表示树中节点存储的数据
type item struct {
	key []byte
	pos *wal.ChunkPosition
}

// newBTree 创建一颗B树索引
func newBTree() *MemoryBTree {
	return &MemoryBTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Less 比较两个节点的大小，实际比较的是key
// it.key < bi.key return true
// or, return false
func (it *item) Less(bi btree.Item) bool {
	return bytes.Compare(it.key, bi.(*item).key) < 0
}

// Put 将key加入索引中，如果存在返回key对应的旧值，否则返回nil
// 返回旧值主要用于提供更多索引变更信息
func (mt *MemoryBTree) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	oldValue := mt.tree.ReplaceOrInsert(&item{key: key, pos: position})
	if oldValue != nil {
		return oldValue.(*item).pos
	}
	return nil
}

// Get 从B树中获取key对应的位置信息
// 如果不存在，return nil
func (mt *MemoryBTree) Get(key []byte) *wal.ChunkPosition {
	value := mt.tree.Get(&item{key: key})
	if value != nil {
		return value.(*item).pos
	}
	return nil
}

// Delete 将key对应的节点从B树中删除，返回key对应的位置信息
// 如果key存在，则返回key的位置信息和true
// or，返回nil, false
// todo:考虑是否可以只返回key的位置信息，通过判断位置信息是否为nil来判断是否成功
func (mt *MemoryBTree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	value := mt.tree.Delete(&item{key: key})
	if value != nil {
		return value.(*item).pos, true
	}
	return nil, false
}

// Size 获取B树中节点的数量，即索引key的数量
func (mt *MemoryBTree) Size() int {
	return mt.tree.Len()
}

// Ascend 本质是对B树Ascend接口的封装调用
func (mt *MemoryBTree) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.Ascend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

// Descend 本质是对B树Descend接口的封装调用
func (mt *MemoryBTree) Descend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.Descend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.AscendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.DescendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.AscendGreaterOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (mt *MemoryBTree) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.DescendLessOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}
