package pddb

const DefaultFillPercent = 0.5

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

func newBucket(tx *Tx) Bucket {
	b := Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writeable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}
