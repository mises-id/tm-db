package db

import (
	"fmt"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

type BackendType string

// These are valid backend types.
const (
	// GoLevelDBBackend represents goleveldb (github.com/syndtr/goleveldb - most
	// popular implementation)
	//   - pure go
	//   - stable
	//   - use goleveldb build tag (go build -tags goleveldb)
	GoLevelDBBackend BackendType = "goleveldb"
	// CLevelDBBackend represents cleveldb (uses levigo wrapper)
	//   - fast
	//   - requires gcc
	//   - use cleveldb build tag (go build -tags cleveldb)
	CLevelDBBackend BackendType = "cleveldb"
	// MemDBBackend represents in-memory key value store, which is mostly used
	// for testing.
	//   - use memdb build tag (go build -tags memdb)
	MemDBBackend BackendType = "memdb"
	// BoltDBBackend represents bolt (uses etcd's fork of bolt -
	// github.com/etcd-io/bbolt)
	//   - EXPERIMENTAL
	//   - may be faster is some use-cases (random reads - indexer)
	//   - use boltdb build tag (go build -tags boltdb)
	BoltDBBackend BackendType = "boltdb"
	// RocksDBBackend represents rocksdb (uses github.com/tecbot/gorocksdb)
	//   - EXPERIMENTAL
	//   - requires gcc
	//   - use rocksdb build tag (go build -tags rocksdb)
	RocksDBBackend BackendType = "rocksdb"
	// BadgerDBBackend represents badger (uses github.com/dgraph-io/badger/v2)
	//   - EXPERIMENTAL
	//   - use badgerdb build tag (go build -tags badgerdb)
	BadgerDBBackend BackendType = "badgerdb"
	// MongoDBBackend represents mongo
	//   - EXPERIMENTAL
	MongoDBBackend BackendType = "mongodb"
)

type DbCreator func(name string, dir string) (DB, error)

var backends = map[BackendType]DbCreator{}

func RegisterDBCreator(backend BackendType, creator DbCreator, force bool) {
	_, ok := backends[backend]
	if !force && ok {
		return
	}
	backends[backend] = creator
}
func Backends() map[BackendType]DbCreator {

	return backends
}

// NewDB creates a new database of type backend with the given name.
func NewDB(name string, backend BackendType, dir string) (DB, error) {
	dbCreator, ok := backends[backend]
	if !ok {
		keys := make([]string, 0, len(backends))
		for k := range backends {
			keys = append(keys, string(k))
		}
		return nil, fmt.Errorf("unknown db_backend %s, expected one of %v",
			backend, strings.Join(keys, ","))
	}

	db, err := dbCreator(name, dir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	return db, nil
}

type MemDB struct {
	DB
}
type GoLevelDB struct {
	d DB
}

func NewMemDB() *MemDB {
	db, _ := NewDB("mem", MemDBBackend, "")
	db.Get([]byte("test"))
	return &MemDB{db}
}

func NewGoLevelDB(name string, dir string) (*GoLevelDB, error) {
	db, err := NewDB(name, GoLevelDBBackend, dir)
	return &GoLevelDB{db}, err
}

func (db *GoLevelDB) DB() *leveldb.DB {
	rawdb := db.d.(RawDB)
	return rawdb.Raw().(*leveldb.DB)
}
func (db *GoLevelDB) Close() error {
	return db.d.Close()
}
func (db *GoLevelDB) Delete(key []byte) error {
	return db.d.Delete(key)
}
func (db *GoLevelDB) DeleteSync(key []byte) error {
	return db.d.DeleteSync(key)
}
func (db *GoLevelDB) Get(key []byte) ([]byte, error) {
	return db.d.Get(key)
}
func (db *GoLevelDB) Has(key []byte) (bool, error) {
	return db.d.Has(key)
}
func (db *GoLevelDB) Iterator(start, end []byte) (Iterator, error) {
	return db.d.Iterator(start, end)
}
func (db *GoLevelDB) NewBatch() Batch {
	return db.d.NewBatch()
}
func (db *GoLevelDB) Print() error {
	return db.d.Print()
}
func (db *GoLevelDB) ReverseIterator(start, end []byte) (Iterator, error) {
	return db.d.ReverseIterator(start, end)
}
func (db *GoLevelDB) Set(key []byte, value []byte) error {
	return db.d.Set(key, value)
}
func (db *GoLevelDB) SetSync(key []byte, value []byte) error {
	return db.d.SetSync(key, value)
}
func (db *GoLevelDB) Stats() map[string]string {
	return db.d.Stats()
}
func (db *GoLevelDB) IsTrackable() bool {
	return db.d.IsTrackable()
}
