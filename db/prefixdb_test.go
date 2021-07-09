package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	tmdb "github.com/cosmos/cosmos-sdk/db"
	"github.com/cosmos/cosmos-sdk/db/dbtest"
	"github.com/cosmos/cosmos-sdk/db/memdb"
)

func fillDBWithStuff(t *testing.T, db tmdb.DBReadWriter) {
	// Under "key" prefix
	require.NoError(t, db.Set([]byte("key"), []byte("value")))
	require.NoError(t, db.Set([]byte("key1"), []byte("value1")))
	require.NoError(t, db.Set([]byte("key2"), []byte("value2")))
	require.NoError(t, db.Set([]byte("key3"), []byte("value3")))
	require.NoError(t, db.Set([]byte("something"), []byte("else")))
	require.NoError(t, db.Set([]byte("k"), []byte("val")))
	require.NoError(t, db.Set([]byte("ke"), []byte("valu")))
	require.NoError(t, db.Set([]byte("kee"), []byte("valuu")))
}

func mockDBWithStuff(t *testing.T) tmdb.DB {
	db := memdb.NewDB()
	fillDBWithStuff(t, db.ReadWriter())
	return db
}

func makePrefixReader(t *testing.T, db tmdb.DB, pre []byte) tmdb.DBReader {
	view := db.Reader()
	require.NotNil(t, view)
	return tmdb.NewPrefixReader(view, pre)
}

func TestPrefixDBSimple(t *testing.T) {
	pdb := makePrefixReader(t, mockDBWithStuff(t), []byte("key"))

	dbtest.AssertValue(t, pdb, []byte("key"), nil)
	dbtest.AssertValue(t, pdb, []byte("key1"), nil)
	dbtest.AssertValue(t, pdb, []byte("1"), []byte("value1"))
	dbtest.AssertValue(t, pdb, []byte("key2"), nil)
	dbtest.AssertValue(t, pdb, []byte("2"), []byte("value2"))
	dbtest.AssertValue(t, pdb, []byte("key3"), nil)
	dbtest.AssertValue(t, pdb, []byte("3"), []byte("value3"))
	dbtest.AssertValue(t, pdb, []byte("something"), nil)
	dbtest.AssertValue(t, pdb, []byte("k"), nil)
	dbtest.AssertValue(t, pdb, []byte("ke"), nil)
	dbtest.AssertValue(t, pdb, []byte("kee"), nil)
}

func TestPrefixDBIterator1(t *testing.T) {
	pdb := makePrefixReader(t, mockDBWithStuff(t), []byte("key"))

	itr, err := pdb.Iterator(nil, nil)
	require.NoError(t, err)
	dbtest.AssertDomain(t, itr, nil, nil)
	dbtest.AssertItem(t, itr, []byte("1"), []byte("value1"))
	dbtest.AssertNext(t, itr, true)
	dbtest.AssertItem(t, itr, []byte("2"), []byte("value2"))
	dbtest.AssertNext(t, itr, true)
	dbtest.AssertItem(t, itr, []byte("3"), []byte("value3"))
	dbtest.AssertNext(t, itr, false)
	dbtest.AssertInvalid(t, itr)
	itr.Close()
}

func TestPrefixDBReverseIterator1(t *testing.T) {
	pdb := makePrefixReader(t, mockDBWithStuff(t), []byte("key"))

	itr, err := pdb.ReverseIterator(nil, nil)
	require.NoError(t, err)
	dbtest.AssertDomain(t, itr, nil, nil)
	dbtest.AssertItem(t, itr, []byte("3"), []byte("value3"))
	dbtest.AssertNext(t, itr, true)
	dbtest.AssertItem(t, itr, []byte("2"), []byte("value2"))
	dbtest.AssertNext(t, itr, true)
	dbtest.AssertItem(t, itr, []byte("1"), []byte("value1"))
	dbtest.AssertNext(t, itr, false)
	dbtest.AssertInvalid(t, itr)
	itr.Close()
}

func TestPrefixDBReverseIterator5(t *testing.T) {
	pdb := makePrefixReader(t, mockDBWithStuff(t), []byte("key"))

	itr, err := pdb.ReverseIterator([]byte("1"), nil)
	require.NoError(t, err)
	dbtest.AssertDomain(t, itr, []byte("1"), nil)
	dbtest.AssertItem(t, itr, []byte("3"), []byte("value3"))
	dbtest.AssertNext(t, itr, true)
	dbtest.AssertItem(t, itr, []byte("2"), []byte("value2"))
	dbtest.AssertNext(t, itr, true)
	dbtest.AssertItem(t, itr, []byte("1"), []byte("value1"))
	dbtest.AssertNext(t, itr, false)
	dbtest.AssertInvalid(t, itr)
	itr.Close()
}

func TestPrefixDBReverseIterator6(t *testing.T) {
	pdb := makePrefixReader(t, mockDBWithStuff(t), []byte("key"))

	itr, err := pdb.ReverseIterator([]byte("2"), nil)
	require.NoError(t, err)
	dbtest.AssertDomain(t, itr, []byte("2"), nil)
	dbtest.AssertItem(t, itr, []byte("3"), []byte("value3"))
	dbtest.AssertNext(t, itr, true)
	dbtest.AssertItem(t, itr, []byte("2"), []byte("value2"))
	dbtest.AssertNext(t, itr, false)
	dbtest.AssertInvalid(t, itr)
	itr.Close()
}

func TestPrefixDBReverseIterator7(t *testing.T) {
	pdb := makePrefixReader(t, mockDBWithStuff(t), []byte("key"))

	itr, err := pdb.ReverseIterator(nil, []byte("2"))
	require.NoError(t, err)
	dbtest.AssertDomain(t, itr, nil, []byte("2"))
	dbtest.AssertItem(t, itr, []byte("1"), []byte("value1"))
	dbtest.AssertNext(t, itr, false)
	dbtest.AssertInvalid(t, itr)
	itr.Close()
}

func TestPrefixDBViewVersion(t *testing.T) {
	prefix := []byte("key")
	db := memdb.NewDB()
	fillDBWithStuff(t, db)
	id := db.SaveVersion()
	pdb := tmdb.NewPrefixReadWriter(db.ReadWriter(), prefix)

	pdb.Set([]byte("1"), []byte("newvalue1"))
	pdb.Delete([]byte("2"))
	pdb.Set([]byte("4"), []byte("newvalue4"))

	dbview := db.ReaderAt(id)
	require.NotNil(t, dbview)
	view := tmdb.NewPrefixReader(dbview, prefix)
	require.NotNil(t, view)

	dbtest.AssertValue(t, view, []byte("1"), []byte("value1"))
	dbtest.AssertValue(t, view, []byte("2"), []byte("value2"))
	dbtest.AssertValue(t, view, []byte("4"), nil)
}
