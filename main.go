package main

import (
	"fmt"
	"pddb"
	"strconv"
)

func createBucket(db *pddb.DB) (*pddb.Tx, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 10; i++ {
		_, err = tx.CreateBucket([]byte(strconv.Itoa(i)))
		if err != nil {
			return tx, err
		}
	}
	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return tx, err
	}
	return tx, nil
}

func putBucket(tx *pddb.Tx) error {
	b := tx.Bucket([]byte("0"))

	err := b.Put([]byte("0"), []byte("11"))
	if err != nil {
		return err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func main() {
	db, err := pddb.Open("test.db", 0666, nil)
	if err != nil {
		fmt.Println("error open")
	}
	defer db.Close()

	tx, _ := createBucket(db)
	if tx == nil {
		fmt.Println("no tx")
		return
	}
	err = putBucket(tx)
	if err != nil {
		fmt.Println("put error")
	}
}
