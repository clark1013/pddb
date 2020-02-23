package pddb

import (
	"sort"
	"unsafe"
	"fmt"
)

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
		panic(fmt.Sprintf("commit on managed transation not allowed"))
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

	// 释放旧根bucket
	tx.meta.root.root = tx.root.root

	opgid := tx.meta.pgid
	// 释放freelist并分配新的page
	tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	p, err := tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.rollback()
		return err
	}
	if err := tx.db.freelist.write(p); err != nil {
		tx.rollback()
		return err
	}
	tx.meta.freelist = p.id

	// 高水位升高以后需要尝试增大数据库
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// 脏页写入磁盘
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	// 元数据写入磁盘
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}

	// 最终关闭事务
	tx.close()

	return nil
}

// 回滚事务
func (tx *Tx) Rollback() error {
	if tx.managed {
		panic(fmt.Sprintf("rollback on managed transation not allowed"))
	} else if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}

// 创建bucket
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

// 获取bucket
func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

func (tx *Tx) rollback() {
	// TODO 回滚事务
	return
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

func (tx *Tx) write() error {
	// 对于page进行排序
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}
	tx.pages = make(map[pgid]*page)
	sort.Sort(pages)

	// 将page按序写入磁盘
	for _, p := range pages {
		size := (int(p.overflow) + 1) * tx.db.pageSize
		offset := int64(p.id) * int64(tx.db.pageSize)

		// 将page按照chunk大小写
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p))
		for {
			sz := size
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}

			// 写入磁盘
			buf := ptr[:sz]
			if _, err := tx.db.file.WriteAt(buf, offset); err != nil {
				return err
			}
			size -= sz
			if size == 0 {
				break
			}
			offset += int64(sz)
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}
	}

	if err := fdatasync(tx.db); err != nil {
		return err
	}

	// 将小page放回page pool
	for _, p := range pages {
		if int(p.overflow) != 0 {
			continue
		}
		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:tx.db.pageSize]
		// See https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		for i := range buf {
			buf[i] = 0
		}
		tx.db.pagePool.Put(buf)
	}

	return nil
}

func (tx *Tx) writeMeta() error {
	// 为元数据页创建临时buffer
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)
	tx.meta.write(p)

	// 写入文件
	if _, err := tx.db.file.WriteAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}
	if err := fdatasync(tx.db); err != nil {
		return err
	}

	return nil
}

// 返回从指定page开始的连续的内存块
func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}
	// 写入缓存
	tx.pages[p.id] = p
	return p, nil
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writeable {
		// 移除db对事务的引用和写锁
		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}
	// 清除所有引用
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}
