package main

import (
	"fmt"
	"pddb"
	"strconv"
)

func insert(db *pddb.DB) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	// defer tx.Rollback()

	// var b *pddb.Bucket
	// Use the transaction...
	for i := 0; i < 10; i++ {
		_, err = tx.CreateBucket([]byte(strconv.Itoa(i)))
		if err != nil {
			return err
		}
	}
	// b = tx.Bucket([]byte("tBucket"))
	// err = b.Put([]byte("answer"), []byte("111"))

	// for i:=0; i<2; i++ {
	// 	b.Put([]byte(strconv.Itoa(i)), []byte("6"))
	// }

	// r := b.Get([]byte("100"))
	// fmt.Println("result is", r)

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func main() {
	db, err := pddb.Open("test", 0666, nil)
	if err != nil {
		fmt.Println("error open")
	}
	defer db.Close()

	err = insert(db)
	if err != nil {
		fmt.Println("error insert %s", err)
	}
}
