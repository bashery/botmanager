package store

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/tidwall/wal"
)

type Database struct {
	path      string // name
	lastindex uint64
	log       *wal.Log
	tables    map[string]*Table
}

//var db *Database

type Table struct {
	name      string
	indexfile *os.File //  index file
	lastId    uint64
	tref      uint8 // table reference
}

func initIndexFile(path string) (*os.File, error) {
	indexFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	return indexFile, nil
}

func initTables(path string) map[string]*Table {
	dir, _ := os.Open(path)
	files, _ := dir.Readdir(-1)

	tables := make(map[string]*Table, 0)

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".pk") {
			tableName := file.Name()[:len(file.Name())-3]

			indxfile, _ := initIndexFile(path + Slash() + file.Name())
			lindx := file.Size() / 8 // last index
			//if lindx == 0 {lindx = 1}
			tables[tableName] = &Table{
				name:      tableName,
				indexfile: indxfile,
				lastId:    uint64(lindx),
			}
		}
	}

	if len(tables) == 0 {
		indexfile, _ := initIndexFile(path + Slash() + "test.pk")
		tables["test"] = &Table{
			indexfile: indexfile,
		}
	}

	return tables
}

// init exists or create new db
func InitDatabase(path string) (*Database, error) {
	opt := &wal.Options{}
	wl, err := wal.Open(path, opt)
	if err != nil {
		return nil, err
	}

	return &Database{
		path:   path,
		log:    wl,
		tables: initTables(path),
	}, nil
}

// insert appends data
func (db *Database) Insert(table, data string) error {
	fmt.Println(db.tables)
	tref, ok := db.tables[table]
	if !ok {
		fmt.Println("no table named", table, "so new is done")
		indexFile, _ := initIndexFile(db.path + Slash() + table + ".pk")
		db.tables[table] = &Table{
			name:      table,
			indexfile: indexFile,
			tref:      0,
			lastId:    0,
		}
		tref = db.tables[table]
		tref.tref = uint8(len(db.tables)) + 1
	}
	db.tables[table].lastId++

	blid := make([]byte, 8) // lastId as binay
	binary.BigEndian.PutUint64(blid, db.tables[table].lastId)

	blid = append(blid, []byte{tref.tref}...)

	bdata := append(blid, []byte(data)...)

	fmt.Printf("insert into %s\n", table)
	fmt.Println("db.tables[table].lastId: ", db.tables[table].lastId)

	err := db.log.Write(db.tables[table].lastId, bdata)
	if err != nil {
		fmt.Println("out of range becose:", db.tables[table].lastId)
		db.tables[table].lastId--
		return err
	}
	err = db.appendId(table, blid)
	if err != nil {
		fmt.Println("when appendId", err)
	}

	return nil
}

func (db *Database) appendId(tableName string, bLastId []byte) error {
	fmt.Println("append into table name: ", tableName)
	table := db.tables[tableName]
	at := int64(table.lastId * 8)
	fmt.Println("at is ", at)
	if _, err := table.indexfile.WriteAt(bLastId, at-8); err != nil {
		return err
	}
	fmt.Println("id in append is ", table.lastId)
	return nil
}

func (db *Database) createTable(path string) {
	//table, _ := os.Create(path)
}

// getData reads data from wall file
func (db *Database) get(ref uint64) ([]byte, error) {
	return db.log.Read(ref)
}

func (db *Database) markDelete(id uint64) error {
	_ = id
	return nil
}

func NewIndexs(path string) *os.File {
	findexs, err := os.Open(path + ".indexs")
	if err != nil {
		return nil
	}
	return findexs
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
