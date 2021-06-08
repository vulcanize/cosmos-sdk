package decoupled

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"

	dbm "github.com/cosmos/cosmos-sdk/db"
	abci "github.com/tendermint/tendermint/abci/types"

	"github.com/cosmos/cosmos-sdk/store/cachekv"
	"github.com/cosmos/cosmos-sdk/store/listenkv"
	"github.com/cosmos/cosmos-sdk/store/smt"
	"github.com/cosmos/cosmos-sdk/store/tracekv"
	"github.com/cosmos/cosmos-sdk/store/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/types/kv"
)

const (
	defaultIAVLCacheSize = 10000
)

var (
	_ types.KVStore                 = (*Store)(nil)
	_ types.CommitStore             = (*Store)(nil)
	_ types.CommitKVStore           = (*Store)(nil)
	_ types.Queryable               = (*Store)(nil)
	_ types.StoreWithInitialVersion = (*Store)(nil)
)

var (
	versionRootKey = []byte{0}
	dataPrefix     = []byte{1}
	indexPrefix    = []byte{2}
	scPrefix       = []byte{3}
)

var ErrVersionDoesNotExist = errors.New("version does not exist")

// TODO:
// telemetry
// AddListener, AddTrace
// Specify thread safety for this and other KV stores?

type storeOptions struct {
	initialVersion uint64
	pruningOptions types.PruningOptions
	// Whether the SC and SS use different dbs
	separateDBs bool
}

type Store struct {
	// DB connection, needed for version access
	db dbm.DB
	// RW access for current version
	dbrw dbm.DBReadWriter
	// SC data for current version
	sc *smt.Store

	mtx sync.RWMutex
	// TODO: unused
	// opts storeOptions
}

// Create a new, empty store from a single DB
func NewStore(db dbm.DB) (*Store, error) {
	if saved := len(db.Versions()); saved != 0 {
		return nil, fmt.Errorf("DB contains %v existing versions", saved)
	}
	dbrw := db.NewWriter()
	return &Store{
		db:   db,
		dbrw: dbrw,
		sc:   smt.NewStore(dbm.NewPrefixWriter(dbrw, scPrefix)),
		// opts:    storeOptions{initialVersion: db.InitialVersion()},
	}, nil
}

func (s *Store) contents() dbm.DBReadWriter {
	return dbm.NewPrefixWriter(s.dbrw, dataPrefix)
}
func (s *Store) index() dbm.DBReadWriter {
	return dbm.NewPrefixWriter(s.dbrw, indexPrefix)
}

func (s *Store) lastVersion() uint64 {
	versions := s.db.Versions()
	if len(versions) == 0 {
		return 0
	}
	return versions[len(versions)-1]
}

// Access the underlying SMT as a basic KV store
func (s *Store) GetSCStore() types.BasicKVStore {
	return s.sc
}

// implement KVStore
func (s *Store) Get(key []byte) []byte {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	val, err := s.contents().Get(key)
	if err != nil {
		panic(err)
	}
	return val
}

func (s *Store) Has(key []byte) bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	has, err := s.contents().Has(key)
	if err != nil {
		panic(err)
	}
	return has
}

func (s *Store) Set(key []byte, value []byte) {
	kvHash := sha256.Sum256(append(key, value...))
	s.mtx.Lock()
	defer s.mtx.Unlock()

	err := s.contents().Set(key, value)
	if err != nil {
		panic(err.Error())
	}
	err = s.index().Set(kvHash[:], key)
	if err != nil {
		panic(err.Error())
	}
	s.sc.Set(key, kvHash[:]) // TODO: key or hash(key)?
}

func (s *Store) Delete(key []byte) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	kvHash := s.sc.Get(key)

	_, err := s.contents().Get(key)
	if err != nil {
		panic(err)
	}
	s.sc.Delete(key)
	_ = s.index().Delete(kvHash[:])
	_ = s.contents().Delete(key)
}

func (s *Store) Iterator(start, end []byte) types.Iterator {
	iter, err := s.contents().Iterator(start, end)
	if err != nil {
		panic(err)
	}
	return iter
}

func (s *Store) ReverseIterator(start, end []byte) types.Iterator {
	iter, err := s.contents().ReverseIterator(start, end)
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
func (s *Store) CacheWrapWithListeners(storeKey types.StoreKey, listeners []types.WriteListener) types.CacheWrap {
	return cachekv.NewStore(listenkv.NewStore(s, storeKey, listeners))
}

// implement Committer
func (s *Store) Commit() types.CommitID {
	root := s.sc.Root()
	s.dbrw.Set(versionRootKey, root)
	s.dbrw.Commit()
	s.db.SaveVersion()

	// // TODO: more elegant solution?
	// if s.opts.separateDBs {
	// 	scver := s.scdb.SaveVersion()
	// 	if last := s.lastVersion(); last != scver {
	// 		panic(fmt.Errorf("Storage DB version (%v) does not match SC DB version (%v)", last, scver))
	// 	}
	// }

	s.dbrw = s.db.NewWriter()
	s.sc = smt.LoadStore(dbm.NewPrefixWriter(s.dbrw, scPrefix), root)
	return s.LastCommitID()
}

func (s *Store) LastCommitID() types.CommitID {
	last := s.lastVersion()
	if last == 0 {
		return types.CommitID{}
	}
	dbr := s.db.NewReaderAt(last)
	hash, err := dbr.Get(versionRootKey)
	if err != nil {
		panic(err)
	}
	return types.CommitID{
		Version: int64(last),
		Hash:    hash,
	}
}

// TODO: these should be implemented in DB
func (s *Store) SetPruning(types.PruningOptions)  {}
func (s *Store) GetPruning() types.PruningOptions { return types.PruningOptions{} }
func (s *Store) SetInitialVersion(version int64)  {}

func (s *Store) versionExists(v uint64) bool {
	r := s.db.NewReaderAt(v)
	return r != nil
}

// Query implements ABCI interface, allows queries
//
// by default we will return from (latest height -1),
// as we will have merkle proofs immediately (header height = data height + 1)
// If latest-1 is not present, use latest (which must be present)
// if you care to have the latest data to see a tx results, you must
// explicitly set the height you want to see
func (s *Store) Query(req abci.RequestQuery) (res abci.ResponseQuery) {
	// defer telemetry.MeasureSince(time.Now(), "store", "iavl", "query")

	if len(req.Data) == 0 {
		return sdkerrors.QueryResult(sdkerrors.Wrap(sdkerrors.ErrTxDecode, "query cannot be zero length"))
	}

	// store chosen height the response, with 0 changed to latest height
	height := uint64(req.Height)
	if height == 0 {
		current := s.db.CurrentVersion()
		if s.versionExists(current - 1) {
			height = current - 1
		} else {
			height = current
		}
	}
	res.Height = int64(height)

	switch req.Path {
	case "/key":
		var err error
		res.Key = req.Data // data holds the key bytes

		dbr := s.db.NewReaderAt(height)
		if dbr == nil {
			return sdkerrors.QueryResult(sdkerrors.ErrInvalidHeight)
		}
		contents := dbm.NewPrefixReader(dbr, dataPrefix)
		res.Value, err = contents.Get(res.Key)
		if err != nil {
			return sdkerrors.QueryResult(sdkerrors.ErrKeyNotFound)
		}
		if !req.Prove {
			break
		}
		root, err := dbr.Get(versionRootKey)
		if err != nil {
			panic(err)
		}
		treedb := dbm.NewWriterFromReader(dbm.NewPrefixReader(dbr, scPrefix))
		tree := smt.LoadStore(treedb, root)
		res.ProofOps, err = tree.GetProof(res.Key)
		if err != nil {
			panic(err)
		}

	case "/subspace":
		pairs := kv.Pairs{
			Pairs: make([]kv.Pair, 0),
		}

		subspace := req.Data
		res.Key = subspace

		iterator := types.KVStorePrefixIterator(s, subspace)
		for ; iterator.Valid(); iterator.Next() {
			pairs.Pairs = append(pairs.Pairs, kv.Pair{Key: iterator.Key(), Value: iterator.Value()})
		}
		iterator.Close()

		bz, err := pairs.Marshal()
		if err != nil {
			panic(fmt.Errorf("failed to marshal KV pairs: %w", err))
		}

		res.Value = bz

	default:
		return sdkerrors.QueryResult(sdkerrors.Wrapf(sdkerrors.ErrUnknownRequest, "unexpected query path: %v", req.Path))
	}

	return res
}
