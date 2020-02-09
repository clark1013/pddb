package pddb

type txid uint64

// 只读事务或可读写事务
type Tx struct {
	writeable bool
	managed   bool
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

// 提交事务
func (tx *Tx) Commit() error {
	if tx.managed {
		panic("commit on managed transation not allowed")
	} else if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writeable {
		return ErrTxNotWriteable
	}
	// 删除过节点需要重新平衡
	tx.root.rebalance()

	// 数据放到脏页
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	return nil
}

// 创建桶
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

func (tx *Tx) page(id pgid) *page {
	// 先检查脏页
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	// 脏页没有数据直接从数据库获取
	return tx.db.page(id)
}
