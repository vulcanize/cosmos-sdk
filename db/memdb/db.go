package memdb

import (
	"bytes"
	"fmt"
	"sync"

	dbm "github.com/cosmos/cosmos-sdk/db"
	"github.com/google/btree"
)

const (
	// The approximate number of items and children per B-tree node. Tuned with benchmarks.
	bTreeDegree = 32
)

// item is a btree.Item with byte slices as keys and values
type item struct {
	key   []byte
	value []byte
}

// Less implements btree.Item.
func (i *item) Less(other btree.Item) bool {
	// this considers nil == []byte{}, but that's ok since we handle nil endpoints
	// in iterators specially anyway
	return bytes.Compare(i.key, other.(*item).key) == -1
}

// newKey creates a new key item.
func newKey(key []byte) *item {
	return &item{key: key}
}

// newPair creates a new pair item.
func newPair(key, value []byte) *item {
	return &item{key: key, value: value}
}

// MemDB is an in-memory database backend using a B-tree for storage.
//
// For performance reasons, all given and returned keys and values are pointers to the in-memory
// database, so modifying them will cause the stored values to be modified as well. All DB methods
// already specify that keys and values should be considered read-only, but this is especially
// important with MemDB.
//
// Versioning is implemented by maintaining references to copy-on-write clones of the backing btree.
// TODO: Transactional semantics.
type MemDB struct {
	dbVersion
	saved map[uint64]*btree.BTree
	last  uint64
}

type dbVersion struct {
	mtx   sync.RWMutex
	btree *btree.BTree
}

var _ dbm.DBConnection = (*MemDB)(nil)
var _ dbm.DBReader = (*dbVersion)(nil)
var _ dbm.DBReadWriter = (*dbVersion)(nil)

// NewDB creates a new in-memory database.
func NewDB() *MemDB {
	return &MemDB{
		dbVersion: dbVersion{btree: btree.New(bTreeDegree)},
		saved:     make(map[uint64]*btree.BTree),
	}
}

// Close implements DB.
// Close is a noop since for an in-memory database, we don't have a destination to flush
// contents to nor do we want any data loss on invoking Close().
// See the discussion in https://github.com/tendermint/tendermint/libs/pull/56
func (db *MemDB) Close() error {
	return nil
}

// Versions implements DBConnection.
func (db *MemDB) Versions() dbm.VersionSet {
	var ret []uint64
	for ver, _ := range db.saved {
		ret = append(ret, uint64(ver))
	}
	return dbm.NewVersionManager(ret)
}

// Reader implements DBConnection.
func (db *MemDB) Reader() dbm.DBReader {
	return &db.dbVersion
}

// ReaderAt implements DBConnection.
func (db *MemDB) ReaderAt(version uint64) (dbm.DBReader, error) {
	tree, ok := db.saved[version]
	if !ok {
		return nil, dbm.ErrVersionDoesNotExist
	}
	return &dbVersion{btree: tree}, nil
}

// Writer implements DBConnection.
func (db *MemDB) Writer() dbm.DBWriter {
	return &db.dbVersion
}

// ReadWriter implements DBConnection.
func (db *MemDB) ReadWriter() dbm.DBReadWriter {
	return &db.dbVersion
}

func (db *MemDB) SaveVersion(target uint64) (uint64, error) {
	db.dbVersion.mtx.Lock()
	defer db.dbVersion.mtx.Unlock()

	if target == 0 {
		target = db.last + 1
	}
	if _, ok := db.saved[target]; ok {
		return 0, fmt.Errorf("version exists: %v", target)
	}
	db.saved[target] = db.btree
	// BTree's Clone() makes a CoW extension of the current tree
	db.dbVersion.btree = db.btree.Clone()
	db.last = target
	return target, nil
}

// Get implements DBReader.
func (db *dbVersion) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, dbm.ErrKeyEmpty
	}
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	i := db.btree.Get(newKey(key))
	if i != nil {
		return i.(*item).value, nil
	}
	return nil, nil
}

// Has implements DBReader.
func (db *dbVersion) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, dbm.ErrKeyEmpty
	}
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	return db.btree.Has(newKey(key)), nil
}

// Set implements DBWriter.
func (db *dbVersion) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return dbm.ErrKeyEmpty
	}
	if value == nil {
		return dbm.ErrValueNil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.set(key, value)
	return nil
}

// set sets a value without locking the mutex.
func (db *dbVersion) set(key []byte, value []byte) {
	db.btree.ReplaceOrInsert(newPair(key, value))
}

// Delete implements DBWriter.
func (db *dbVersion) Delete(key []byte) error {
	if len(key) == 0 {
		return dbm.ErrKeyEmpty
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.delete(key)
	return nil
}

// delete deletes a key without locking the mutex.
func (db *dbVersion) delete(key []byte) {
	db.btree.Delete(newKey(key))
}

// Iterator implements DBReader.
// Takes out a read-lock on the database until the iterator is closed.
func (db *dbVersion) Iterator(start, end []byte) (dbm.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, dbm.ErrKeyEmpty
	}
	return newMemDBIterator(db, start, end, false), nil
}

// ReverseIterator implements DBReader.
// Takes out a read-lock on the database until the iterator is closed.
func (db *dbVersion) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, dbm.ErrKeyEmpty
	}
	return newMemDBIterator(db, start, end, true), nil
}

// Commit implements DBWriter.
func (db *dbVersion) Commit() error {
	// no-op, like Close()
	return nil
}
func (db *dbVersion) Discard() {}

// Print prints the database contents.
func (db *MemDB) Print() error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	db.btree.Ascend(func(i btree.Item) bool {
		item := i.(*item)
		fmt.Printf("[%X]:\t[%X]\n", item.key, item.value)
		return true
	})
	return nil
}

// Stats implements DBConnection.
func (db *MemDB) Stats() map[string]string {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	stats := make(map[string]string)
	stats["database.type"] = "memDB"
	stats["database.size"] = fmt.Sprintf("%d", db.btree.Len())
	return stats
}
