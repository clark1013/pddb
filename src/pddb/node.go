package pddb

import (
	"fmt"
	"unsafe"
	"os"
	"sort"
	"bytes"
)

// node represents an in-memory, deserialized page.
type node struct {
	bucket *Bucket
	isLeaf bool
	unbalanced bool
	pgid pgid
	key []byte
	parent *node
	children nodes
	inodes inodes
}

// 序列化后的节点大小
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()

	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
	}
	return sz
}

// 根据节点类型, 返回节点内容的大小
func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// 将node写入page
func (n *node) write(p *page) {
	n.dump()
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	if p.count == 0 {
		return
	}

	// 循环将每条数据写入page
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	for i, item := range n.inodes {
		if len(item.key) > 0 {
			panic("write: empty inode key")
		}

		// 写入page Element
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			// _assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// 将数据写入页尾
		copy(b[0:], item.key)
		b = b[len(item.key):]
		copy(b[0:], item.value)
		b = b[len(item.value):]
	}

	// FOR DEBUG
	n.dump()
}

// 从page初始化node
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	n.inodes = make(inodes, int(p.count))

	for i:=0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
	}

	// 保存首个key, 在分页的时候可以直接找到
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
	} else {
		n.key = nil
	}
}

// 将k/v写入node
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	if pgid > n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) == 0 {
		panic("put: zero-lenth old key")
	} else if len(newKey) == 0 {
		panic("put: zero-lenth new key")
	}

	index := sort.Search(len(n.inodes), func(i int) bool{
		return bytes.Compare(n.inodes[i].key, oldKey) != -1
	})

	// 如果没有找到元素，需要增加inodes的空间
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.key = newKey
	inode.value = value
	inode.flags = flags
	inode.pgid = pgid
}

// 节点数量不足时与兄弟节点结合
func (n *node) rebalance() {
	if !n.unbalance {
		return
	}
	n.unbalanced = false

	// TODO rebalance
}

// dump writes the contents of the node to STDERR for debugging purposes.
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
		}
	}
	warn("")
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func trunc(b []byte, length int) []byte {
	if length < len(b) {
		return b[:length]
	}
	return b
}

type nodes []*node

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
type inode struct {
	flags uint32
	pgid  pgid
	key   []byte
	value []byte
}

type inodes []inode
