package decoupled

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"

	dbm "github.com/cosmos/cosmos-sdk/db"
	abci "github.com/tendermint/tendermint/abci/types"
	tmcrypto "github.com/tendermint/tendermint/proto/tendermint/crypto"

	"github.com/cosmos/cosmos-sdk/store/cachekv"
	"github.com/cosmos/cosmos-sdk/store/dbadapter"
	"github.com/cosmos/cosmos-sdk/store/iavl"
	"github.com/cosmos/cosmos-sdk/store/listenkv"
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
	versionsPrefix = []byte{0}
	dataPrefix     = []byte{1}
	indexPrefix    = []byte{2}
)

var ErrVersionDoesNotExist = errors.New("version does not exist")

// TODO:
// initial version logic - should be handled at DB level
// separate backing DBs for SC/SS - desired? and test
// telemetry
// AddListener, AddTrace

// A store which uses separate data structures for state storage (SS) and state commitments (SC)
type Store struct {
	// db   dbm.DB
	scdb dbm.DB
	// Direct KV mapping (SS)
	contents dbm.DB
	// Inverted index of SC values to SS keys
	inv dbm.DB
	// State commitments layer
	sc types.CommitKVStore
	// Mutex needed to lock stores in tandem during writes
	mtx sync.Mutex
	// TODO: unused
	opts storeOptions
	// Whether the SC and SS use different dbs
	separateDBs bool
}

type storeOptions struct {
	initialVersion uint64
	pruningOptions types.PruningOptions
}

// Create a new, empty store from a single DB
func NewStore(db dbm.DB) (*Store, error) {
	current := db.CurrentVersion()
	initial := db.InitialVersion()
	if current != initial {
		return nil, fmt.Errorf("DB contains existing versions (initial: %v; current: %v)", initial, current)
	}
	return loadStore(db, db, false)
}

// Create a new store from SC store and DB
func loadStore(db dbm.DB, scdb dbm.DB, separate bool) (*Store, error) {
	dbVersion := db.CurrentVersion()
	iavl, err := iavl.LoadStore(scdb, types.CommitID{}, false)
	if err != nil {
		return nil, err
	}
	scVersion := iavl.LastCommitID().Version + 1
	if int64(dbVersion) != scVersion {
		return nil, fmt.Errorf("Current version of SS and SC DBs do not match (%v != %v)", dbVersion, scVersion)
	}
	return makeStore(db, scdb, iavl, separate), nil
}

func makeStore(db dbm.DB, scdb dbm.DB, sc types.CommitKVStore, separate bool) *Store {
	return &Store{
		scdb:        scdb,
		sc:          sc,
		contents:    dbm.NewPrefixDB(db, dataPrefix),
		inv:         dbm.NewPrefixDB(db, indexPrefix),
		separateDBs: separate,
		// opts:     storeOptions{initialVersion: db.InitialVersion()},
	}
}

func (s *Store) currentVersion() uint64 {
	return s.contents.CurrentVersion()
}

// Accessors for underlying store.
// Injecting internal trace/listeners will need additional methods
func (s *Store) GetCommitmentStorage() types.CommitKVStore {
	return s.sc
}
func (s *Store) GetStateStorage() types.KVStore {
	return dbadapter.Store{DB: s.contents}
}
func (s *Store) GetStateIndex() types.KVStore {
	return dbadapter.Store{DB: s.inv}
}

// implement KVStore
func (s *Store) Get(key []byte) []byte {
	val, err := s.contents.Get(key)
	if err != nil {
		panic(err)
	}
	return val
}

func (s *Store) Has(key []byte) bool {
	has, err := s.contents.Has(key)
	return err == nil && has
}

func (s *Store) Set(key []byte, value []byte) {
	kvHash := sha256.Sum256(append(key, value...))

	s.mtx.Lock()
	defer s.mtx.Unlock()

	err := s.contents.Set(key, value)
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

	kvHash := s.sc.Get(key)
	_, err := s.contents.Get(key)
	if err != nil {
		panic(err.Error())
	}
	s.sc.Delete(key)
	_ = s.inv.Delete(kvHash[:])
	_ = s.contents.Delete(key)
}

func (s *Store) Iterator(start, end []byte) types.Iterator {
	iter, err := s.contents.Iterator(start, end)
	if err != nil {
		panic(err)
	}
	return iter
}

func (s *Store) ReverseIterator(start, end []byte) types.Iterator {
	iter, err := s.contents.ReverseIterator(start, end)
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
	cid := s.sc.Commit()
	dbver := s.scdb.SaveVersion()
	if dbver != uint64(cid.Version) {
		panic(fmt.Errorf("SC, Merkle versions: %v %v", dbver, cid.Version))
	}
	// TODO: more elegant solution?
	if s.separateDBs {
		ssver := s.contents.SaveVersion()
		if dbver != ssver {
			panic(fmt.Errorf("SC, SS vers: %v %v", dbver, ssver))
		}
	}
	return cid
}

func (s *Store) LastCommitID() types.CommitID {
	return s.sc.LastCommitID()
}

// TODO: these should be implemented in DB
func (s *Store) SetPruning(types.PruningOptions)  {}
func (s *Store) GetPruning() types.PruningOptions { return types.PruningOptions{} }
func (s *Store) SetInitialVersion(version int64) {
	s.sc.(types.StoreWithInitialVersion).SetInitialVersion(version)
}

func (s *Store) versionExists(v uint64) bool {
	_, err := s.contents.AtVersion(v)
	return err == nil // TODO: check error?
}

// A read-only view of a store
type storeView struct {
	db, scdb       dbm.DB
	version        uint64
	initialVersion uint64
}

func (s *Store) viewVersion(version uint64) (*storeView, error) {
	scView, err := s.scdb.AtVersion(version)
	if err != nil {
		return nil, err
	}
	dbView, err := s.contents.AtVersion(version)
	if err != nil {
		return nil, fmt.Errorf("version %v exists in commitments DB but not storage DB: %w", version, err)
	}
	return &storeView{
		db:      dbView,
		scdb:    scView,
		version: version,
		// initialVersion: s.opts.initialVersion,
	}, nil
}

func (sv *storeView) Get(key []byte) []byte {
	val, err := sv.db.Get(key)
	if err != nil {
		panic(err)
	}
	return val
}

func (sv *storeView) GetProof(key []byte, exists bool) *tmcrypto.ProofOps {
	merkle, err := iavl.LoadVersionView(sv.scdb, int64(sv.version), sv.initialVersion)
	if err != nil {
		panic(err.Error())
	}
	return merkle.(*iavl.Store).GetProof(key, exists)
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
		current := s.currentVersion()
		if s.versionExists(current - 1) {
			height = current - 1
		} else {
			height = current
		}
	}
	res.Height = int64(height)

	switch req.Path {
	case "/key": // get by key
		res.Key = req.Data // data holds the key bytes
		view, err := s.viewVersion(height)
		if err != nil {
			panic(err)
		}
		res.Value = view.Get(res.Key)
		if err != nil {
			panic(fmt.Sprintf("db get %v", res.Key)) // TODO
		}
		if !req.Prove {
			break
		}
		res.ProofOps = view.GetProof(res.Key, res.Value != nil)

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
