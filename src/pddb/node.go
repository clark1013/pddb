package pddb

// node represents an in-memory, deserialized page.
type node struct {
	bucket *Bucket
	isLeaf bool
	inodes inodes
}

// 序列化后的节点大小
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.PageElementSize()
	// TODO
}

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
type inode struct {
	flags unit32
	pgid  pgid
	key   []byte
	value []byte
}

type inodes []inode
