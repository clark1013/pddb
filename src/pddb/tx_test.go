package pddb_test

import (
	"testing"
	"pddb"
)

// 只读事务无法创建bucket
func TestTx_CreateBucket_ErrTxNotWriteable(t *testing.T) {
	db := MustOpenDB()
	defer MustClose(db)
	if err := db.View(func(tx *pddb.Tx) error {
		_, err := tx.CreateBucket([]byte("foo"))
		if err != pddb.ErrTxNotWriteable {
			t.Fatalf("unexpected err, %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}


// 事务关闭后无法再次创建bucket
func TestTx_CreateBucket_ErrTxClosed(t *testing.T) {
	db := MustOpenDB()
	defer MustClose(db)
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.CreateBucket([]byte("foo")); err != pddb.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}


// 测试创建bucket后可以在另一个事务中读取到
func TestTx_CreateBucket(t *testing.T) {
	return
}
