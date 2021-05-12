package decoupled

import (
	"testing"

	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"

	"github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/store/types"
)

var (
	cacheSize = 100
)

func newStoreWithData(t *testing.T, db dbm.DB, storeData map[string]string) (*Store, types.CommitID) {
	sc := newSCStore(t, db)
	store := newStore(db, sc)

	for k, v := range storeData {
		store.Set([]byte(k), []byte(v))
	}
	id := sc.Commit()

	return store, id
}

func newSCStore(t *testing.T, db dbm.DB) types.CommitKVStore {
	sc, err := iavl.LoadStore(db, types.CommitID{}, false)
	require.NoError(t, err)
	return sc
}

func TestGetSetHasDelete(t *testing.T) {
	db := dbm.NewMemDB()
	storeData := map[string]string{
		"hello": "goodbye",
		"aloha": "shalom",
	}
	store, _ := newStoreWithData(t, db, storeData)

	key := "hello"

	exists := store.Has([]byte(key))
	require.True(t, exists)

	value := store.Get([]byte(key))
	require.EqualValues(t, value, storeData[key])

	value2 := "notgoodbye"
	store.Set([]byte(key), []byte(value2))

	value = store.Get([]byte(key))
	require.EqualValues(t, value, value2)

	store.Delete([]byte(key))

	exists = store.Has([]byte(key))
	require.False(t, exists)
}

func TestIterators(t *testing.T) {
	store, _ := newStoreWithData(t, dbm.NewMemDB(), map[string]string{
		string([]byte{0x00}):       "0",
		string([]byte{0x00, 0x00}): "0 0",
		string([]byte{0x00, 0x01}): "0 1",
		string([]byte{0x00, 0x02}): "0 2",
		string([]byte{0x01}):       "1",
	})

	var testCase = func(t *testing.T, iter types.Iterator, expected []string) {
		var i int
		for i = 0; iter.Valid(); iter.Next() {
			expectedValue := expected[i]
			value := iter.Value()
			require.EqualValues(t, string(value), expectedValue)
			i++
		}
		require.Equal(t, len(expected), i)
	}

	testCase(t, store.Iterator(nil, nil),
		[]string{"0", "0 0", "0 1", "0 2", "1"})
	testCase(t, store.Iterator([]byte{0x00}, nil),
		[]string{"0", "0 0", "0 1", "0 2", "1"})
	testCase(t, store.Iterator([]byte{0x00}, []byte{0x00, 0x01}),
		[]string{"0", "0 0"})
	testCase(t, store.Iterator([]byte{0x00}, []byte{0x01}),
		[]string{"0", "0 0", "0 1", "0 2"})
	testCase(t, store.Iterator([]byte{0x00, 0x01}, []byte{0x01}),
		[]string{"0 1", "0 2"})
	testCase(t, store.Iterator(nil, []byte{0x01}),
		[]string{"0", "0 0", "0 1", "0 2"})

	testCase(t, store.ReverseIterator(nil, nil),
		[]string{"1", "0 2", "0 1", "0 0", "0"})
	testCase(t, store.ReverseIterator([]byte{0x00}, nil),
		[]string{"1", "0 2", "0 1", "0 0", "0"})
	testCase(t, store.ReverseIterator([]byte{0x00}, []byte{0x00, 0x01}),
		[]string{"0 0", "0"})
	testCase(t, store.ReverseIterator([]byte{0x00}, []byte{0x01}),
		[]string{"0 2", "0 1", "0 0", "0"})
	testCase(t, store.ReverseIterator([]byte{0x00, 0x01}, []byte{0x01}),
		[]string{"0 2", "0 1"})
	testCase(t, store.ReverseIterator(nil, []byte{0x01}),
		[]string{"0 2", "0 1", "0 0", "0"})
}

func TestBucketsAreIndependent(t *testing.T) {
	// Use separate dbs for each bucket
	dbsc := dbm.NewMemDB()
	dbss := dbm.NewMemDB()
	dbii := dbm.NewMemDB()
	sc := newSCStore(t, dbsc)
	store := &Store{sc: sc, data: dbss, inv: dbii}

	storeData := map[string]string{
		"test1": "a",
		"test2": "b",
		"test3": "c",
		"foo":   "bar",
	}
	for k, v := range storeData {
		store.Set([]byte(k), []byte(v))
	}
	_ = sc.Commit()

	dbiter, err := dbss.Iterator(nil, nil)
	require.Nil(t, err)
	iter := store.Iterator(nil, nil)
	for ; iter.Valid(); iter.Next() {
		require.True(t, dbiter.Valid(), "Missing records in backing DB")
		require.Equal(t, iter.Key(), dbiter.Key(), "Mismatched key in backing DB")
		require.Equal(t, iter.Value(), dbiter.Value(), "Mismatched key in backing DB")
		dbiter.Next()
	}
	require.False(t, dbiter.Valid(), "Extra records present in backing DB")
	iter.Close()
	dbiter.Close()

	dbiter, err = dbii.Iterator(nil, nil)
	require.Nil(t, err)
	for ; dbiter.Valid(); dbiter.Next() {
		require.False(t, store.Has(dbiter.Key()), "Index key is present in store's data bucket")
	}
	dbiter.Close()
}
