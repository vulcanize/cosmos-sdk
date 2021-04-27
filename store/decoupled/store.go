package decoupled

import (
	"crypto/sha256"
	"io"
	"sync"

	"github.com/cosmos/cosmos-sdk/store/cachekv"
	"github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/store/tracekv"
	"github.com/cosmos/cosmos-sdk/store/types"
	// "github.com/cosmos/cosmos-sdk/telemetry"

	// abci "github.com/tendermint/tendermint/abci/types"
	dbm "github.com/tendermint/tm-db"
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
type Store struct {
	// State commitments layer
	sc types.CommitKVStore
	// Direct KV mapping (SS)
	data dbm.DB
	// Inverted index of SC values to SS keys
	inv dbm.DB

	mtx sync.RWMutex
}

// TODO:
// version tracking, construction?
// separate backing DB?
func NewStore(db dbm.DB, id types.CommitID, lazyLoading bool) (*Store, error) {
	sc, err := iavl.LoadStore(db, id, lazyLoading)
	if err != nil {
		return nil, err
	}
	return &Store{
		sc:   sc,
		data: dbm.NewPrefixDB(db, dataPrefix),
		inv:  dbm.NewPrefixDB(db, indexPrefix),
	}, nil
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

// CommitKVStore = Committer + KVStore
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

// TODO:
// SetPruning, GetPruning
// Query
// SetInitialVersion
