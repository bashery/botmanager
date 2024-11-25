package store

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/tidwall/wal"
)

type Database struct {
	path      string // name
	log       *wal.Log
	lastindex uint64
	tables    map[string]*Table
}

type Table struct {
	fIndexs *os.File
	tref    uint8 // reference collection
	lastId  uint64
}

func NewIndexs(path string) *os.File {
	findexs, err := os.Open(path + ".indexs")
	if err != nil {
		return nil
	}
	return findexs
}

func initIndexs(path string) {
	dir, err := os.Open(path)
	if err != nil {
		print(err)
	}

	files, err := dir.Readdir(-1)
	if err != nil {
		print(err)
	}

	for _, f := range files {
		fmt.Println(f.Name())
	}
}

// init exists db or create new
func InitDatabase(path string) (*Database, error) {
	// TODO init all exists tables

	opt := &wal.Options{}
	db, err := wal.Open(path, opt)
	if err != nil {
		return nil, err
	}

	table := &Table{
		// TODO read all tables
		fIndexs: NewIndexs("test"),
	}

	tables := make(map[string]*Table, 0)
	tables["test"] = table

	return &Database{
		path:   path,
		log:    db,
		tables: tables,
	}, nil
}

// insert appends data
func (db *Database) Insert(table, data string) error {
	tref, ok := db.tables[table]
	if !ok {
		db.tables[table] = &Table{
			fIndexs: NewIndexs(table),
			tref:    0,
			lastId:  1,
		}
		tref = db.tables[table]
		tref.tref = uint8(len(db.tables)) + 1
	}

	blid := make([]byte, 8) // lastId as binay
	binary.BigEndian.PutUint64(blid, db.tables[table].lastId)
	fmt.Println("blid:", blid)

	blid = append(blid, []byte{tref.tref}...)
	fmt.Println("blid+tref:", blid)

	bdata := append(blid, []byte(data)...)
	fmt.Println("bdata:", bdata)

	err := db.log.Write(db.tables[table].lastId, bdata)
	if err != nil {
		return err
	}
	db.tables[table].lastId++
	return nil
}

// getData reads data from wall file
func (db *Database) get(ref uint64) ([]byte, error) {
	return db.log.Read(ref)
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
