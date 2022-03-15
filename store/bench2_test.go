package store

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cosmos/cosmos-sdk/db"
	"github.com/cosmos/cosmos-sdk/db/badgerdb"
	"github.com/cosmos/cosmos-sdk/db/memdb"
	dbutil "github.com/cosmos/cosmos-sdk/internal/db"
	"github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/store/rootmulti"
	"github.com/cosmos/cosmos-sdk/store/types"
	storev2types "github.com/cosmos/cosmos-sdk/store/v2"
	storev2 "github.com/cosmos/cosmos-sdk/store/v2/multi"
	"github.com/cosmos/cosmos-sdk/store/v2/smt"
	tmdb "github.com/tendermint/tm-db"
)

func BenchmarkNewKVStoreV1(b *testing.B) {
	for _, dbt := range []dbCtor{memdbCtor, badgerCtor} {
		runRW(b, newIavlStore, dbt)
		runGets(b, newIavlStore, dbt)
	}
}
func BenchmarkNewMultiStoreV1(b *testing.B) {
	for _, dbt := range []dbCtor{memdbCtor, badgerCtor} {
		runGetPast(b, newMultiV1, dbt)
		f := func(b *testing.B, dbc db.DBConnection, _ *types.CommitID) store { return newMultiV1(b, dbc) }
		runRW(b, f, dbt)
	}
}

func BenchmarkNewKVStoreV2(b *testing.B) {
	for _, dbt := range []dbCtor{memdbCtor, badgerCtor} {
		runRW(b, newSmtStore, dbt)
		runGets(b, newSmtStore, dbt)
	}
}
func BenchmarkNewMultiStoreV2(b *testing.B) {
	for _, dbt := range []dbCtor{memdbCtor, badgerCtor} {
		runGetPast(b, newMultiV2, dbt)
		f := func(b *testing.B, dbc db.DBConnection, _ *types.CommitID) store { return newMultiV2(b, dbc) }
		runRW(b, f, dbt)
	}
}

type smtStore struct {
	*smt.Store
	db db.DBConnection
	rw db.DBReadWriter
}

type versionedStore interface {
	store
	LoadVersion(int64)
}

type multistoreV2 struct {
	*storev2.Store
	storev2types.KVStore
}

type multistoreV1 struct {
	*rootmulti.Store
	types.KVStore
}

func (s *multistoreV1) LoadVersion(v int64) {
	s.Store.LoadVersion(v)
}

func (s *multistoreV2) LoadVersion(v int64) {
	vs, err := s.GetVersion(v)
	if err != nil {
		panic(err)
	}
	s.KVStore = vs.GetKVStore(skey_1)
}

func (s *smtStore) Commit() types.CommitID {
	err := s.Store.Commit()
	if err != nil {
		panic(err)
	}
	root := s.Root()
	if err = s.rw.Commit(); err != nil {
		panic(err)
	}
	s.rw = s.db.ReadWriter()
	s.Store = smt.LoadStore(s.rw, root)
	return types.CommitID{Hash: root}
}

type storeCtor = func(*testing.B, db.DBConnection, *types.CommitID) store
type versionedStoreCtor = func(*testing.B, db.DBConnection) versionedStore

func newSmtStore(b *testing.B, dbc db.DBConnection, cid *types.CommitID) store {
	rw := dbc.ReadWriter()
	var s *smt.Store
	if cid == nil {
		s = smt.NewStore(rw)
	} else {
		s = smt.LoadStore(rw, cid.Hash)
	}
	return &smtStore{s, dbc, rw}
}

func newIavlStore(b *testing.B, dbc db.DBConnection, cidp *types.CommitID) store {
	var cid types.CommitID
	if cidp != nil {
		cid = *cidp
	}
	s, err := iavl.LoadStore(dbutil.ConnectionAsTmdb(dbc), cid, false, cacheSize)
	if err != nil {
		panic(err)
	}
	return s
}

type dbCtor struct {
	typ tmdb.BackendType
	new func(string) db.DBConnection
}

var memdbCtor = dbCtor{
	tmdb.MemDBBackend,
	func(string) db.DBConnection { return memdb.NewDB() },
}
var badgerCtor = dbCtor{
	tmdb.BadgerDBBackend,
	func(dir string) db.DBConnection {
		d, err := badgerdb.NewDB(dir)
		if err != nil {
			panic(err)
		}
		return d
	},
}

// creates a unique key for int x
func createDistinctKey(x int) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	wrote := binary.PutVarint(buf, int64(x))
	sum := sha256.Sum256(buf[:wrote])
	return sum[:]
}

func distinctKeys(from, to int) (ret [][]byte) {
	for i := from; i < to; i++ {
		ret = append(ret, createDistinctKey(i))
	}
	return
}

// run pure-read and pure-write cases
func runRW(b *testing.B, sctor storeCtor, dbctor dbCtor) {
	dir := b.TempDir()
	rand.Seed(seed)

	cases := []percentages{{0, 100, 0, 0}, {0, 0, 100, 0}}
	nValues := 10_000

	var values [][]byte
	for _, pct := range cases {
		name := fmt.Sprintf("%s-%s", dbctor.typ, pct.String())
		b.Run(name, func(b *testing.B) {
			if values == nil {
				values = prepareValues(nValues)
			}
			db := dbctor.new(filepath.Join(dir, name))
			s := sctor(b, db, nil)
			for i, v := range values { // add existing data
				s.Set(createSineKey(i), v)
				if i%commitInterval == 0 {
					s.Commit()
				}
			}
			runRandomizedOperations(b, s, 1000, pct)
			b.StopTimer()
			db.Close()
		})
	}
}

// runs get ops for all-present or all-absent keys
func runGets(b *testing.B, sctor storeCtor, dbctor dbCtor) {
	dir := b.TempDir()
	rand.Seed(seed)

	nValues := 100_000
	totalOpsCount := 1000

	db := dbctor.new(dir)
	store := sctor(b, db, nil)
	values := prepareValues(nValues)
	keys := distinctKeys(0, nValues)
	nonkeys := distinctKeys(nValues, nValues*2)
	for i, v := range values {
		store.Set(keys[i], v)
	}
	// Reloads store - massive drop in IAVL perf
	// cid := store.Commit()
	// store = sctor(b, db, &cid)

	b.Run(fmt.Sprintf("%s-get-present", dbctor.typ), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < totalOpsCount; j++ {
				ki := rand.Intn(nValues)
				store.Get(keys[ki])
			}
		}
	})

	b.Run(fmt.Sprintf("%s-get-absent", dbctor.typ), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < totalOpsCount; j++ {
				ki := rand.Intn(nValues)
				store.Get(nonkeys[ki])
			}
		}
	})
	db.Close()
}

func newMultiV1(b *testing.B, dbc db.DBConnection) versionedStore {
	store := rootmulti.NewStore(dbutil.ConnectionAsTmdb(dbc))
	store.MountStoreWithDB(skey_1, types.StoreTypeIAVL, nil)
	require.NoError(b, store.LoadLatestVersion())
	sub := store.GetKVStore(skey_1)
	return &multistoreV1{store, sub}
}

func newMultiV2(b *testing.B, dbc db.DBConnection) versionedStore {
	simpleStoreParams, err := simpleStoreParams()
	require.NoError(b, err)
	root, err := storev2.NewStore(dbc, simpleStoreParams)
	require.NoError(b, err)
	store := root.GetKVStore(skey_1)
	return &multistoreV2{root, store}
}

func runGetPast(b *testing.B, sctor versionedStoreCtor, dbctor dbCtor) {
	dir := b.TempDir()
	rand.Seed(seed)

	// test historical version access - use read-only test cases
	nValues := 10_000
	totalOpsCount := 1000

	name := fmt.Sprintf("%s-getpast", dbctor.typ)
	b.Run(name, func(b *testing.B) {
		values := prepareValues(nValues)
		db := dbctor.new(filepath.Join(dir, name))
		store := sctor(b, db)
		var lastversion int64
		for i, v := range values {
			store.Set(createSineKey(i*2), v) // half of read key will be present
			if i%commitInterval == 0 {
				cid := store.Commit()
				lastversion = cid.Version
			}
		}
		if lastversion < 2 {
			b.Fatal("historical version requires > 1 commits")
		}
		store.LoadVersion(lastversion - 1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < totalOpsCount; j++ {
				ki := rand.Intn(nValues)
				store.Get(createSineKey(ki))
			}
		}
		b.StopTimer()
		db.Close()
	})

}
