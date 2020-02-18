package pddb

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	bucket     *Bucket
	isLeaf     bool
	unbalanced bool
	spilled    bool
	pgid       pgid
	key        []byte
	parent     *node
	children   nodes
	inodes     inodes
}

// 返回整个树的根节点
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// 将node写入page
func (n *node) write(p *page) {
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
		if len(item.key) == 0 {
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
	// n.dump()
}

// 从page初始化node
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	n.inodes = make(inodes, int(p.count))

	for i := 0; i < int(p.count); i++ {
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

	index := sort.Search(len(n.inodes), func(i int) bool {
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
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// TODO rebalance
}

// 将node写入脏页, 如果脏页无法分配则报错
func (n *node) spill() error {
	var tx = n.bucket.tx
	if n.spilled {
		return nil
	}

	// 先处理所有子节点
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// 不再需要对于子节点的引用, 因为子节点仅在spill过程中进行追踪
	n.children = nil

	// 将node分割成适当的大小
	var nodes = n.split(tx.db.pageSize)
	for _, node := range nodes {
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}
		// 为node分配连续空间
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}
		// 写入node
		if p.id >= tx.meta.pgid {
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		node.pgid = p.id
		node.write(p)
		node.spilled = true
		// 将node写入父节点的inode
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}
			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
		}
	}
	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}

	return nil
}

// 将node分割成多个更小的node
func (n *node) split(pageSize int) []*node {
	var nodes []*node
	var node = n
	for {
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)
		if b == nil {
			break
		}
		node = b
	}
	return nodes
}

func (n *node) splitTwo(pageSize int) (*node, *node) {
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}
	// 确定开启新node的阈值大小
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// 确定分割位置
	splitIndex, _ := n.splitIndex(threshold)

	// 如果当前节点没有父节点, 为其创建父节点
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}
	// 为父节点创建一个新的子节点
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)
	// 拆分到两个inodes
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	return n, next
}

func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsz := n.pageElementSize() + len(inode.key) + len(inode.value)
		if i >= minKeysPerPage && sz+elsz > threshold {
			break
		}
		sz += elsz
	}
	return
}

func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
		if sz >= v {
			return false
		}
	}
	return true
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

func (s nodes) Len() int           { return len(s) }
func (s nodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool { return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1 }

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
