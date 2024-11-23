package store

import (
	"log"
	"os"

	"github.com/tidwall/wal"
)

type Database struct {
	path        string // name
	log         *wal.Log
	collections map[string]*Collection
}

type Collection struct {
	fIndexs *os.File
	ref     uint8 // reference collection
	lastId  uint64
}

func NewIndexs(path string) *os.File {
	findexs, err := os.Open(path + ".indexs")
	if err != nil {
		return nil
	}
	return findexs
}

func NewDatabase(path string) (*Database, error) {
	opt := &wal.Options{}
	db, err := wal.Open(path, opt)
	if err != nil {
		return nil, err
	}
	coll := &Collection{
		fIndexs: NewIndexs("test"),
	}

	colls := make(map[string]*Collection, 0)
	colls["test"] = coll

	return &Database{
		path:        path,
		log:         db,
		collections: colls,
	}, nil
}

// insert appends data
func (db *Database) insert(coll, data string) error {
	collref, ok := db.collections[coll]
	if !ok {
		db.collections[coll] = &Collection{
			fIndexs: NewIndexs(coll),
		}

	}
	_ = collref.ref
	db.log.Write(db.collections[coll].lastId, []byte(data))

	return nil
}

// getData reads data from wall file
func (db *Database) get(id uint64) ([]byte, error) {
	return db.log.Read(id)
}

func (db *Database) markDelete(id uint64) error {
	_ = id
	return nil
}

// read index from data & build indexs
func (db *Database) buildIndexs() (indexs []uint64) {
	return indexs
}

func (db *Database) Close() error {
	err := db.log.Close()
	if err != nil {
		log.Fatal(err)
	}
	return err
}
