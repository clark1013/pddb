package pddb_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"pddb"
	"testing"
)

// 测试正常打开数据库
func TestOpen(t *testing.T) {
	path := tempfile()
	db, err := pddb.Open(path, 0666, nil)
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}

	if s := db.Path(); s != path {
		t.Fatal("path mismatch")
	}

	if err := db.Close(); err != nil {
		t.Fatal("db close error")
	}
}

// 测试数据库文件路径不能为空
func TestOpen_ErrPathRequired(t *testing.T) {
	_, err := pddb.Open("", 0666, nil)
	if err == nil {
		t.Fatal("path error expected")
	}
}

// 测试不能为错误的文件路径
func TestOpen_ErrNotExsist(t *testing.T) {
	_, err := pddb.Open(filepath.Join(tempfile(), "bad-path"), 0666, nil)
	if err == nil {
		t.Fatal("bad path error expected")
	}
}

// 测试打开不合法的文件
func TestOpen_ErrInvalid(t *testing.T) {
	path := tempfile()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(f, "this is not a pddb database"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)

	if _, err := pddb.Open(path, 0666, nil); err != pddb.ErrInvalid {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a database cannot open a transaction when it's not open.
func TestDB_Begin_ErrDatabaseNotOpen(t *testing.T) {
	var db pddb.DB
	if _, err := db.Begin(false); err != pddb.ErrDatabaseNotOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that a read-write transaction can be retrieved.
func TestDB_BeginRW(t *testing.T) {
	db := MustOpenDB()
	defer MustClose(db)

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	} else if tx == nil {
		t.Fatal("expected tx")
	}

	if tx.DB() != db {
		t.Fatal("unexpected tx database")
	} else if !tx.Writeable() {
		t.Fatal("expected writable tx")
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Ensure that opening a transaction while the DB is closed returns an error.
func TestDB_BeginRW_Closed(t *testing.T) {
	var db pddb.DB
	if _, err := db.Begin(true); err != pddb.ErrDatabaseNotOpen {
		t.Fatalf("unexpected error: %s", err)
	}
}

// MustOpenDB returns a new, open DB at a temporary location.
func MustOpenDB() *pddb.DB {
	db, err := pddb.Open(tempfile(), 0666, nil)
	if err != nil {
		panic(err)
	}
	return db
}

// MustClose closes the database and deletes the underlying file. Panic on error.
func MustClose(db *pddb.DB) {
	if err := db.Close(); err != nil {
		panic(err)
	}
}

// 返回临时文件路径
func tempfile() string {
	file, err := ioutil.TempFile("", "pddb-")
	if err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(file.Name()); err != nil {
		panic(err)
	}
	return file.Name()
}
