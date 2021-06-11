package decoupled

import (
	"testing"

	"github.com/stretchr/testify/require"

	abci "github.com/tendermint/tendermint/abci/types"

	dbm "github.com/cosmos/cosmos-sdk/db"
	memdb "github.com/cosmos/cosmos-sdk/db/memdb"
	"github.com/cosmos/cosmos-sdk/store/types"
)

var (
	cacheSize = 100
	alohaData = map[string]string{
		"hello": "goodbye",
		"aloha": "shalom",
	}
)

func newStoreWithData(t *testing.T, db dbm.DB, storeData map[string]string) *Store {
	store, err := NewStore(db)
	if err != nil {
		panic(err)
	}

	for k, v := range storeData {
		store.Set([]byte(k), []byte(v))
	}
	return store
}

func newAlohaStore(t *testing.T, db dbm.DB) *Store {
	return newStoreWithData(t, db, alohaData)
}

func TestGetSetHasDelete(t *testing.T) {
	store := newAlohaStore(t, memdb.NewDB())
	key := "hello"

	exists := store.Has([]byte(key))
	require.True(t, exists)

	require.EqualValues(t, []byte(alohaData[key]), store.Get([]byte(key)))

	value2 := "notgoodbye"
	store.Set([]byte(key), []byte(value2))

	require.EqualValues(t, value2, store.Get([]byte(key)))

	store.Delete([]byte(key))

	exists = store.Has([]byte(key))
	require.False(t, exists)
}

func TestIterators(t *testing.T) {
	store := newStoreWithData(t, memdb.NewDB(), map[string]string{
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

// func TestBucketsAreIndependent(t *testing.T) {
// 	// Use separate dbs for each bucket
// 	dbsc := memdb.NewDB()
// 	dbss := memdb.NewDB()
// 	dbii := memdb.NewDB()
// 	sc, err := iavl.LoadStore(dbsc, types.CommitID{}, false)
// 	require.NoError(t, err)
// 	store := &Store{sc: sc, contents: dbss, inv: dbii}

// 	storeData := map[string]string{
// 		"test1": "a",
// 		"test2": "b",
// 		"test3": "c",
// 		"foo":   "bar",
// 	}
// 	for k, v := range storeData {
// 		store.Set([]byte(k), []byte(v))
// 	}
// 	_ = sc.Commit()

// 	dbiter, err := dbss.Iterator(nil, nil)
// 	require.Nil(t, err)
// 	iter := store.Iterator(nil, nil)
// 	for ; iter.Valid(); iter.Next() {
// 		require.True(t, dbiter.Valid(), "Missing records in backing DB")
// 		require.Equal(t, iter.Key(), dbiter.Key(), "Mismatched key in backing DB")
// 		require.Equal(t, iter.Value(), dbiter.Value(), "Mismatched value in backing DB")
// 		dbiter.Next()
// 	}
// 	require.False(t, dbiter.Valid(), "Extra records present in backing DB")
// 	iter.Close()
// 	dbiter.Close()

// 	dbiter, err = dbii.Iterator(nil, nil)
// 	require.Nil(t, err)
// 	for ; dbiter.Valid(); dbiter.Next() {
// 		require.False(t, store.Has(dbiter.Key()), "Index key is present in store's data bucket")
// 	}
// 	dbiter.Close()
// }

// Sanity test for Merkle hashing
func TestMerkleRoot(t *testing.T) {
	store := newStoreWithData(t, memdb.NewDB(), nil)
	idNew := store.Commit()
	store.Set([]byte{0x00}, []byte("a"))
	idOne := store.Commit()
	require.Equal(t, idNew.Version+1, idOne.Version)
	require.NotEqual(t, idNew.Hash, idOne.Hash)

	store.Delete([]byte{0x00})
	idEmptied := store.Commit()
	require.Equal(t, idNew.Hash, idEmptied.Hash)
}

func TestQuery(t *testing.T) {
	db := memdb.NewDB()
	store := newAlohaStore(t, db)
	store.Commit()

	store.Set([]byte("hello"), []byte("adios"))
	cID := store.Commit()

	// TODO: new semantics?
	// _, err = store.AtVersion(cID.Version + 1)
	// require.NoError(t, err)

	// view, err := store.viewVersion(uint64(cID.Version - 1))
	// require.NoError(t, err)
	// require.Equal(t, []byte("goodbye"), view.Get([]byte("hello")))

	// view, err = store.viewVersion(uint64(cID.Version))
	// require.NoError(t, err)
	// require.Equal(t, []byte("adios"), view.Get([]byte("hello")))

	res := store.Query(abci.RequestQuery{
		Data:   []byte("hello"),
		Height: cID.Version,
		Path:   "/key",
		Prove:  true,
	})
	require.Equal(t, []byte("adios"), res.Value)
	require.NotNil(t, res.ProofOps)
}
