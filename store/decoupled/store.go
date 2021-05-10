package decoupled

import (
	"crypto/sha256"
	"io"
	"sync"

	// abci "github.com/tendermint/tendermint/abci/types"
	abci "github.com/tendermint/tendermint/abci/types"
	dbm "github.com/tendermint/tm-db"

	"github.com/cosmos/cosmos-sdk/store/cachekv"
	"github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/store/tracekv"
	"github.com/cosmos/cosmos-sdk/store/types"
	// "github.com/cosmos/cosmos-sdk/telemetry"
)

const (
	defaultIAVLCacheSize = 10000
)

var (
	_ types.KVStore       = (*Store)(nil)
	_ types.CommitStore   = (*Store)(nil)
	_ types.CommitKVStore = (*Store)(nil)
	// _ types.Queryable               = (*Store)(nil)
	// _ types.StoreWithInitialVersion = (*Store)(nil)
)

var (
	versionsPrefix = []byte{0}
	dataPrefix     = []byte{1}
	indexPrefix    = []byte{2}
)

// A store which uses separate data structures for state storage (SS) and state commitments (SC)
// TODO:
// DB interface and SC store must support versioning
type Store struct {
	// Direct KV mapping (SS)
	data dbm.DB
	// Inverted index of SC values to SS keys
	inv dbm.DB
	// State commitments layer
	sc types.CommitKVStore
	// Mutex needed to lock stores in tandem during writes
	mtx sync.Mutex
}

// TODO:
// version tracking
// constructors?
// separate backing DBs for SC/SS?

// Create a new, empty store
func NewStore(db dbm.DB) (*Store, error) {
	// tree, err := iavl.NewMutableTree(db, defaultIAVLCacheSize)
	sc, err := iavl.LoadStore(db, types.CommitID{}, false)
	if err != nil {
		return nil, err
	}
	return newStore(db, sc), nil
}

// TODO: review logic, motivation for initial version - desired for this store? lazy loading?
func LoadStore(db dbm.DB, id types.CommitID, initialVersion uint64) (types.CommitKVStore, error) {
	sc, err := iavl.LoadStoreWithInitialVersion(db, id, false, initialVersion)
	if err != nil {
		return nil, err
	}
	return newStore(db, sc), nil
}

// Create a new store from SC store and DB
func newStore(db dbm.DB, sc types.CommitKVStore) *Store {
	return &Store{
		sc:   sc,
		data: dbm.NewPrefixDB(db, dataPrefix),
		inv:  dbm.NewPrefixDB(db, indexPrefix),
	}
}

// implement KVStore
func (s *Store) Get(key []byte) []byte {
	val, err := s.data.Get(key)
	if err != nil {
		panic(err)
	}
	return val
}

func (s *Store) Has(key []byte) bool {
	has, err := s.data.Has(key)
	return err == nil && has
}

func (s *Store) Set(key []byte, value []byte) {
	kvHash := sha256.Sum256(append(key, value...))

	s.mtx.Lock()
	defer s.mtx.Unlock()

	err := s.data.Set(key, value)
	if err != nil {
		panic(err.Error())
	}
	err = s.inv.Set(kvHash[:], key)
	if err != nil {
		panic(err.Error())
	}
	s.sc.Set(key, kvHash[:]) // TODO: key or hash(key)?
	if err != nil {
		panic(err.Error())
	}
}

func (s *Store) Delete(key []byte) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.sc.Delete(key)

	defer func() {
		_ = s.data.Delete(key)
	}()

	value, err := s.data.Get(key)
	if err != nil {
		panic(err.Error())
	}
	kvHash := sha256.Sum256(append(key, value...))
	_ = s.inv.Delete(kvHash[:])
}

func (s *Store) Iterator(start, end []byte) types.Iterator {
	iter, err := s.data.Iterator(start, end)
	if err != nil {
		panic(err)
	}
	return iter
}

func (s *Store) ReverseIterator(start, end []byte) types.Iterator {
	iter, err := s.data.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}
	return iter
}

// implement Store
func (s *Store) GetStoreType() types.StoreType {
	return types.StoreTypeDecoupled
}

// implement CacheWrapper
func (s *Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(s)
}
func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(s, w, tc))
}

// implement Committer
func (s *Store) Commit() types.CommitID {
	return s.sc.Commit()
}

func (s *Store) LastCommitID() types.CommitID {
	return s.sc.LastCommitID()
}

func (s *Store) SetPruning(types.PruningOptions) {
	// TODO
}
func (s *Store) GetPruning() types.PruningOptions {
	// TODO
	return types.PruningOptions{}
}

// implement StoreWithInitialVersion
func (s *Store) SetInitialVersion(version int64) {
	// TODO - should StoreWithInitialVersion include CommitKVStore?
	s.sc.(types.StoreWithInitialVersion).SetInitialVersion(version)
}

// TODO:
// Query
// AtVersion, DeleteVersion etc.

func (s *Store) Query(req abci.RequestQuery) (res abci.ResponseQuery) {
	return s.sc.(types.Queryable).Query(req)
}

// DeleteVersions deletes a series of versions from the MutableTree.
func (s *Store) DeleteVersions(versions ...int64) error {
	s.sc.(*iavl.Store).DeleteVersions(versions...)

	// TODO: data pruning

	return nil
}

func (s *Store) AtVersion(version int64) (*Store, error) {
	versionData := s.data //.AtVersion(version)
	versionSC, err := s.sc.(*iavl.Store).GetImmutable(version)
	if err != nil {
		return nil, err
	}
	return newStore(versionData, versionSC), nil
}
