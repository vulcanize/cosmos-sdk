package decoupled

import (
	"testing"

	"github.com/stretchr/testify/require"

	abci "github.com/tendermint/tendermint/abci/types"

	dbm "github.com/cosmos/cosmos-sdk/db"
	"github.com/cosmos/cosmos-sdk/db/memdb"
	"github.com/cosmos/cosmos-sdk/store/types"
	"github.com/cosmos/cosmos-sdk/types/kv"
)

var (
	cacheSize = 100
	alohaData = map[string]string{
		"hello": "goodbye",
		"aloha": "shalom",
	}
)

func newStoreWithData(t *testing.T, db dbm.DBConnection, storeData map[string]string) *Store {
	store, err := NewStore(db, nil)
	require.NoError(t, err)

	for k, v := range storeData {
		store.Set([]byte(k), []byte(v))
	}
	return store
}

func newAlohaStore(t *testing.T, db dbm.DBConnection) *Store {
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

func TestStoreNoNilSet(t *testing.T) {
	store := newAlohaStore(t, memdb.NewDB())

	require.Panics(t, func() { store.Set(nil, []byte("value")) }, "setting a nil key should panic")
	require.Panics(t, func() { store.Set([]byte(""), []byte("value")) }, "setting an empty key should panic")

	require.Panics(t, func() { store.Set([]byte("key"), nil) }, "setting a nil value should panic")
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

	testCase(t, types.KVStorePrefixIterator(store, []byte{0}),
		[]string{"0", "0 0", "0 1", "0 2"})
	testCase(t, types.KVStoreReversePrefixIterator(store, []byte{0}),
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
	store := newStoreWithData(t, memdb.NewDB(), nil)

	k1, v1 := []byte("key1"), []byte("val1")
	k2, v2 := []byte("key2"), []byte("val2")
	v3 := []byte("val3")

	ksub := []byte("key")
	KVs0 := kv.Pairs{}
	KVs1 := kv.Pairs{
		Pairs: []kv.Pair{
			{Key: k1, Value: v1},
			{Key: k2, Value: v2},
		},
	}
	KVs2 := kv.Pairs{
		Pairs: []kv.Pair{
			{Key: k1, Value: v3},
			{Key: k2, Value: v2},
		},
	}

	valExpSubEmpty, err := KVs0.Marshal()
	require.NoError(t, err)

	valExpSub1, err := KVs1.Marshal()
	require.NoError(t, err)

	valExpSub2, err := KVs2.Marshal()
	require.NoError(t, err)

	cid := store.Commit()
	ver := cid.Version
	query := abci.RequestQuery{Path: "/key", Data: k1, Height: ver}
	querySub := abci.RequestQuery{Path: "/subspace", Data: ksub, Height: ver}

	// query subspace before anything set
	qres := store.Query(querySub)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, valExpSubEmpty, qres.Value)

	// set data
	store.Set(k1, v1)
	store.Set(k2, v2)

	// set data without commit, doesn't show up
	qres = store.Query(query)
	require.Equal(t, uint32(0), qres.Code)
	require.Nil(t, qres.Value)

	// commit it, but still don't see on old version
	cid = store.Commit()
	qres = store.Query(query)
	require.Equal(t, uint32(0), qres.Code)
	require.Nil(t, qres.Value)

	// but yes on the new version
	query.Height = cid.Version
	qres = store.Query(query)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, v1, qres.Value)

	// and for the subspace
	qres = store.Query(querySub)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, valExpSub1, qres.Value)

	// modify
	store.Set(k1, v3)
	cid = store.Commit()

	// query will return old values, as height is fixed
	qres = store.Query(query)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, v1, qres.Value)

	// update to latest in the query and we are happy
	query.Height = cid.Version
	qres = store.Query(query)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, v3, qres.Value)
	query2 := abci.RequestQuery{Path: "/key", Data: k2, Height: cid.Version}

	qres = store.Query(query2)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, v2, qres.Value)
	// and for the subspace
	qres = store.Query(querySub)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, valExpSub2, qres.Value)

	// default (height 0) will show latest -1
	query0 := abci.RequestQuery{Path: "/key", Data: k1}
	qres = store.Query(query0)
	require.Equal(t, uint32(0), qres.Code)
	require.Equal(t, v1, qres.Value)
}
