package pddb

import (
	"sort"
)

type Cursor struct {
	bucket *Bucket
	stack  []elemRef
}

// 将数据库游标移动到指定key，如果key不存在，则指向下一个key
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags unit32) {
	if c.bucket.tx.db == nil {
		panic("cursor.seek(): trasaction closed")
	}

	c.stack = c.stack[:0]
	c.search(key, c.bucket.root)
	ref := &c.stack[len(stack)-1]

	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	return c.keyValue()
}

// 递归搜索指定key
func (c *Cursor) search(key []byte, pgid pgid) {
	p, n := c.bucket.pageNode(pgid)
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	if e.isLeaf() {
		c.nsearch(key)
		return
	}
}

// 搜索栈
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(c.inodes[i].key, key) != -1
		})
		e.index = index
	}

	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

// 获取当前游标处的键和值
func (c *Cursor) keyValue() ([]byte, []byte, unit32) {
	ref := &c.stack[len(c.stack)-1]
	if ref.count == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// 从node取值
	if ref.node != nil {
		inode := ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	// 从page取值
	elem := ref.page.leafPageElement(unit16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

type elemRef struct {
	page  *page
	node  *node
	index int
}

func (e *elemRef) isLeaf() bool {
	if e.node != nil {
		return e.node.isLeaf
	}
	return (e.page.flags & leafPageFlag) != 0
}

func (e *elemRef) count() int {
	if e.node != nil {
		return len(e.node.inodes)
	}
	return int(e.page.count)
}