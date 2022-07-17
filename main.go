package main

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

func main() {
	a := os.Args[1:]
	if len(a) < 2 {
		log.Fatalln("missing required argument(s)")
	}
	db, err := bolt.Open(a[0], 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	tsv := a[1:]
	for _, t := range tsv {
		if _, err := os.Stat(t); errors.Is(err, os.ErrNotExist) {
			db.Close()
			_ = os.Remove(a[0])
			log.Fatalln("file does not exist")
		}
		data, err := os.ReadFile(t)
		if err != nil {
			log.Fatalln(err)
		}
		bucketName := strings.TrimSuffix(t, filepath.Ext(t))
		if err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			log.Fatalln(err)
		}
		r := csv.NewReader(strings.NewReader(string(data)))
		r.Comma = '\t'
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatalln(err)
			}
			if err := db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte(bucketName))
				if err := b.Put([]byte(record[0]), []byte(record[1])); err != nil {
					return err
				}
				return nil
			}); err != nil {
				log.Fatalln(err)
			}
		}
	}
}
