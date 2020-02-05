package pddb

import (
	"bytes"
	"unsafe"
)

const (
	MaxKeySize   = 32768
	MaxValueSize = (1 << 31) - 2
)

const DefaultFillPercent = 0.5

const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

type bucket struct {
	root     pgid
	sequence uint64
}

type Bucket struct {
	*bucket
	tx       *Tx
	buckets  map[string]*Bucket
	page     *page
	rootNode *node
	nodes    map[pgid]*node

	FillPercent float64
}

func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writeable {
		return nil, ErrTxNotWriteable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	c := b.Cursor()
	k, _, flags := c.seek(key)

	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		return nil, ErrIncompatibleValue
	}

	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	var value = bucket.write()

	// 写入node
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, bucketLeafFlag)

	b.page = nil

	return b.Bucket(key), nil
}

func (b *Bucket) Bucket(key []byte) *Bucket {
	return nil
}

func (b *Bucket) Cursor() *Cursor {
	// TODO 事务统计
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

func (b *Bucket) Writeable() bool {
	return b.tx.Writeable()
}

func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writeable() {
		return ErrTxNotWriteable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	c := b.Cursor()
	// k, _, flags := c.seek(key)

	// TODO: check keys

	key = cloneBytes(key)
	c.node().put(key, key, value, 0, 0)

	return nil
}

// TODO
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	return b.tx.page(id), nil
}

// 将bucket转换成二进制并写入内存
func (b *Bucket) write() []byte {
	n := b.rootNode
	value := make([]byte, bucketHeaderSize+n.size())

	// 写头
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// 将bucket转换成fake page并写入根节点
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

// 通过pageid创建node, 并与父节点关联
func (b *Bucket) node(pgid pgid, parent *node) *node {
	if b.nodes == nil {
		panic("nodes map is required")
	}

	// 如果对应的page已经在bucket中，直接返回
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// 创建node并缓存到bucket
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	// Use the inline page if this is an inline bucket.
	var p = b.page
	if p == nil {
		p = b.tx.page(pgid)
	}

	// 读取page并进行缓存
	n.read(p)
	b.nodes[pgid] = n

	return n
}

func newBucket(tx *Tx) Bucket {
	b := Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writeable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// 深拷贝指定的二进制数据
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
