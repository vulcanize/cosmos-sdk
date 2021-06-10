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
	scPrefix       = []byte{1}
	dataPrefix     = []byte{2}
	indexPrefix    = []byte{3}
)

var ErrVersionDoesNotExist = errors.New("version does not exist")

// TODO:
// telemetry
// AddListener, AddTrace
// Specify thread safety for this and other KV stores?
// do we want a version access method (like GetImmutable)?
// separate DBs require fully matched version sets or a more complex strategy to handle a mismatch

type storeOptions struct {
	// initialVersion uint64
	// pruningOptions types.PruningOptions
	// Whether the SC and SS use different dbs
	separateDBs bool
}

type dbHandle struct {
	// R/W access for current version
	dbm.DBReadWriter
	// DB connection, needed for version access
	conn dbm.DB
}

type Store struct {
	ss, sc dbHandle
	// SC KV store for current version
	sckv *smt.Store

	// opts storeOptions
	mtx sync.RWMutex
}

// Create a new, empty store from a single DB
// If scdb is nil, the same DB is used for SS and SC storage.
func NewStore(db, scdb dbm.DB) (*Store, error) {
	if saved := len(db.Versions()); saved != 0 {
		return nil, fmt.Errorf("DB contains %v existing versions", saved)
	}
	ss := newDBHandle(db)
	if scdb != nil {
		// Separate DBs for SS and SC
		if saved := len(scdb.Versions()); saved != 0 {
			return nil, fmt.Errorf("SC DB contains %v existing versions", saved)
		}
	} else {
		scdb = db
	}
	sc := newDBHandle(scdb)
	return &Store{
		ss: ss, sc: sc,
		// Prefix still needed for separate SC DB since version key must be partitioned
		sckv: smt.NewStore(dbm.NewPrefixReadWriter(sc, scPrefix)),
	}, nil
}

// Load existing store from a DB
// If scdb is nil, the same DB is used for SS and SC storage.
func LoadStore(db, scdb dbm.DB) (*Store, error) {
	ss := newDBHandle(db)
	if scdb != nil {
		if db.CurrentVersion() != scdb.CurrentVersion() {
			return nil, fmt.Errorf("Current version of SS (%v) and SC (%v) DBs do not match",
				db.CurrentVersion(), scdb.CurrentVersion())
		}
	} else {
		scdb = db
	}
	sc := newDBHandle(scdb)
	root, err := sc.Get(versionRootKey)
	if err != nil {
		panic(err)
	}
	return &Store{
		ss: ss, sc: sc,
		sckv: smt.LoadStore(dbm.NewPrefixReadWriter(sc, scPrefix), root),
	}, nil
}

func newDBHandle(db dbm.DB) dbHandle {
	return dbHandle{conn: db, DBReadWriter: db.NewWriter()}
}

func lastVersion(db dbm.DB) int64 {
	versions := db.Versions()
	if len(versions) == 0 {
		return 0
	}
	return int64(versions[len(versions)-1])
}

// SS bucket and inverted index accessors
// prefixer is cheap to create, so just wrap in this call

func (s *Store) contents() dbm.DBReadWriter {
	return dbm.NewPrefixReadWriter(s.ss, dataPrefix)
}
func (s *Store) index() dbm.DBReadWriter {
	return dbm.NewPrefixReadWriter(s.ss, indexPrefix)
}

func (s *Store) separateDBs() bool { return s.ss.conn != s.sc.conn }

// Access the underlying SMT as a basic KV store
func (s *Store) GetSCStore() types.BasicKVStore {
	return s.sckv
}

// Get implements KVStore.
// Get implements KVStore.
func (s *Store) Get(key []byte) []byte {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	val, err := s.contents().Get(key)
	if err != nil {
		panic(err)
	}
	return val
}

// Has implements KVStore.
func (s *Store) Has(key []byte) bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	has, err := s.contents().Has(key)
	if err != nil {
		panic(err)
	}
	return has
}

// Set implements KVStore.
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
	s.sckv.Set(key, kvHash[:])
}

// Delete implements KVStore.
func (s *Store) Delete(key []byte) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	kvHash := s.sckv.Get(key)

	_, err := s.contents().Get(key)
	if err != nil {
		panic(err)
	}
	s.sckv.Delete(key)
	_ = s.index().Delete(kvHash[:])
	_ = s.contents().Delete(key)
}

// Iterator implements KVStore.
func (s *Store) Iterator(start, end []byte) types.Iterator {
	iter, err := s.contents().Iterator(start, end)
	if err != nil {
		panic(err)
	}
	return iter
}

// ReverseIterator implements KVStore.
func (s *Store) ReverseIterator(start, end []byte) types.Iterator {
	iter, err := s.contents().ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}
	return iter
}

// GetStoreType implements Store.
func (s *Store) GetStoreType() types.StoreType {
	return types.StoreTypeDecoupled
}

// CacheWrap implements CacheWrapper.
func (s *Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(s)
}

// CacheWrapWithTrace implements CacheWrapper.
func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(s, w, tc))
}

// CacheWrapWithListeners implements CacheWrapper.
func (s *Store) CacheWrapWithListeners(storeKey types.StoreKey, listeners []types.WriteListener) types.CacheWrap {
	return cachekv.NewStore(listenkv.NewStore(s, storeKey, listeners))
}

// Commit implements Committer.
func (s *Store) Commit() types.CommitID {
	root := s.sckv.Root()
	s.sc.Set(versionRootKey, root)
	s.sc.Commit()
	s.ss.Commit()
	s.ss.conn.SaveVersion()

	// TODO: more elegant solution?
	if s.separateDBs() {
		scver := s.sc.conn.SaveVersion()
		if last := lastVersion(s.ss.conn); last != int64(scver) {
			panic(fmt.Errorf("Latest version of SS (%v) and SC (%v) DBs do not match", last, scver))
		}
	}

	s.sc = newDBHandle(s.sc.conn)
	s.ss = newDBHandle(s.ss.conn)
	s.sckv = smt.LoadStore(dbm.NewPrefixReadWriter(s.sc, scPrefix), root)
	return s.LastCommitID()
}

// LastCommitID implements KVStore.
func (s *Store) LastCommitID() types.CommitID {
	last := lastVersion(s.ss.conn)
	if last == 0 {
		return types.CommitID{}
	}
	// dbr := s.sc.conn.NewReaderAt(uint64(last))
	// Latest Merkle root should be the one currently stored
	hash, err := s.sc.Get(versionRootKey)
	if err != nil {
		panic(err)
	}
	return types.CommitID{
		Version: last,
		Hash:    hash,
	}
}

// TODO: these should be implemented in DB
func (s *Store) SetPruning(types.PruningOptions)  {}
func (s *Store) GetPruning() types.PruningOptions { return types.PruningOptions{} }
func (s *Store) SetInitialVersion(version int64)  {}

func (s *Store) versionExists(version int64) bool {
	if version < 0 {
		return false
	}
	r := s.ss.conn.NewReaderAt(uint64(version))
	return r != nil
	// for _, valid := range s.ss.Versions() {
	//	if valid == uint64(version) { return true}
	// }
	// return false
}

// Query implements ABCI interface, allows queries.
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

	// if height is 0, use the latest height
	height := req.Height
	if height == 0 {
		latest := lastVersion(s.ss.conn)
		if s.versionExists(latest - 1) {
			height = latest - 1
		} else {
			height = latest
		}
	}
	res.Height = height

	switch req.Path {
	case "/key":
		var err error
		res.Key = req.Data // data holds the key bytes

		dbr := s.ss.conn.NewReaderAt(uint64(height))
		defer dbr.Discard()
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
		scr := s.sc.conn.NewReaderAt(uint64(height))
		defer scr.Discard()
		if scr == nil {
			return sdkerrors.QueryResult(sdkerrors.ErrInvalidHeight)
		}
		root, err := scr.Get(versionRootKey)
		if err != nil {
			panic(err)
		}
		sc := smt.LoadStore(dbm.NewWriterFromReader(dbm.NewPrefixReader(scr, scPrefix)), root)
		res.ProofOps, err = sc.GetProof(res.Key)
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
