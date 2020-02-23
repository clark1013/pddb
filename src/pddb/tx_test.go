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
	db := MustOpenDB()
	defer MustClose(db)
	if err := db.Update(func(tx *pddb.Tx) error {
		b, err := tx.CreateBucket([]byte("tests"))
		if err != nil {
			t.Fatal(err)
		} else if b == nil {
			t.Fatal("expected write bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 从另一个事务读取bucket
	if err := db.View(func (tx *pddb.Tx) error {
		if tx.Bucket([]byte("tests")) == nil {
			t.Fatal("expected read bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}


// bucket不可重复重建
func TestTx_CreateBucket_ErrBucketExists(t *testing.T) {
	db := MustOpenDB()
	defer MustClose(db)

	// Create a bucket.
	if err := db.Update(func(tx *pddb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Create the same bucket again.
	if err := db.Update(func(tx *pddb.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != pddb.ErrBucketExists {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket is created with a non-blank name.
func TestTx_CreateBucket_ErrBucketNameRequired(t *testing.T) {
	db := MustOpenDB()
	defer MustClose(db)
	if err := db.Update(func(tx *pddb.Tx) error {
		if _, err := tx.CreateBucket(nil); err != pddb.ErrBucketNameRequired {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
