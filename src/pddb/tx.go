package pddb

type txid uint64

// 只读事务或可读写事务
type Tx struct {
	writeable bool
	db        *DB
	meta      *meta
	root      Bucket
	pages     map[pgid]*page
}

func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil

	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	tx.root = newBucket(tx)
	tx.root.bucket = &bucket{}
	*tx.root.bucket = tx.meta.root

	if tx.writeable {
		tx.pages = make(map[pgid]*page)
		tx.meta.txid += txid(1)
	}
}

func (tx *Tx) DB() *DB {
	return tx.db
}

func (tx *Tx) Writeable() bool {
	return tx.writeable
}

func (tx *Tx) Commit() error {
	// TODO 事务提交
	return nil
}
