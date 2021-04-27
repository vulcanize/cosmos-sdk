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

var (
	_ types.KVStore                 = (*Store)(nil)
	_ types.CommitStore             = (*Store)(nil)
	_ types.CommitKVStore           = (*Store)(nil)
	_ types.Queryable               = (*Store)(nil)
	_ types.StoreWithInitialVersion = (*Store)(nil)
)

var (
	versionRootKey = []byte{0}
	merklePrefix   = []byte{1}
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
	conn dbm.DBConnection
}

type Store struct {
	stateDB, merkleDB dbHandle
	// SC KV store for current version
	merkleStore *smt.Store

	// opts storeOptions
	mtx sync.RWMutex
}

// Create a new, empty store from a single DB
// If merkdb is nil, the same DB is used for SS and SC storage.
func NewStore(db, merkdb dbm.DBConnection) (*Store, error) {
	if saved := db.Versions().Count(); saved != 0 {
		return nil, fmt.Errorf("DB contains %v existing versions", saved)
	}
	stateDB := newDBHandle(db)
	if merkdb != nil {
		// Separate DBs for SS and SC
		if saved := merkdb.Versions().Count(); saved != 0 {
			return nil, fmt.Errorf("SC DB contains %v existing versions", saved)
		}
	} else {
		merkdb = db
	}
	merkleDB := newDBHandle(merkdb)
	return &Store{
		stateDB: stateDB, merkleDB: merkleDB,
		// Prefix still needed for separate SC DB since version key must be partitioned
		merkleStore: smt.NewStore(dbm.NewPrefixReadWriter(merkleDB, merklePrefix)),
	}, nil
}

// Load existing store from a DB
// If merkdb is nil, the same DB is used for SS and SC storage.
func LoadStore(db, merkdb dbm.DBConnection) (*Store, error) {
	stateDB := newDBHandle(db)
	if merkdb != nil {
		if !db.Versions().Equal(merkdb.Versions()) {
			return nil, fmt.Errorf("Saved versions of SS and SC DBs do not match")
		}
	} else {
		merkdb = db
	}
	merkleDB := newDBHandle(merkdb)
	root, err := merkleDB.Get(versionRootKey)
	if err != nil {
		panic(err)
	}
	return &Store{
		stateDB: stateDB, merkleDB: merkleDB,
		merkleStore: smt.LoadStore(dbm.NewPrefixReadWriter(merkleDB, merklePrefix), root),
	}, nil
}

func newDBHandle(db dbm.DBConnection) dbHandle {
	return dbHandle{conn: db, DBReadWriter: db.ReadWriter()}
}

func lastVersion(db dbm.DBConnection) int64 {
	versions := db.Versions()
	return int64(versions.Last())
}

// SS bucket and inverted index accessors
// prefixer is cheap to create, so just wrap in this call

func (s *Store) contents() dbm.DBReadWriter {
	return dbm.NewPrefixReadWriter(s.stateDB, dataPrefix)
}
func (s *Store) index() dbm.DBReadWriter {
	return dbm.NewPrefixReadWriter(s.stateDB, indexPrefix)
}

func (s *Store) separateDBs() bool { return s.stateDB.conn != s.merkleDB.conn }

// Access the underlying SMT as a basic KV store
func (s *Store) GetSCStore() types.BasicKVStore {
	return s.merkleStore
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
	s.merkleStore.Set(key, kvHash[:])
}

// Delete implements KVStore.
func (s *Store) Delete(key []byte) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	kvHash := s.merkleStore.Get(key)

	_, err := s.contents().Get(key)
	if err != nil {
		panic(err)
	}
	s.merkleStore.Delete(key)
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
	cid, err := s.commit(0)
	if err != nil {
		panic(err)
	}
	return *cid
}

func (s *Store) commit(target uint64) (*types.CommitID, error) {
	root := s.merkleStore.Root()
	s.merkleDB.Set(versionRootKey, root)
	s.merkleDB.Commit()
	s.stateDB.Commit()
	version, err := s.stateDB.conn.SaveVersion(target)
	if err != nil {
		return nil, err
	}

	// TODO: more elegant solution?
	if s.separateDBs() {
		scver, err := s.merkleDB.conn.SaveVersion(version)
		if err != nil {
			return nil, err // TODO: roll back state save?
		}
		if last := lastVersion(s.stateDB.conn); last != int64(scver) {
			return nil, fmt.Errorf("Latest version of SS (%v) and SC (%v) DBs do not match", last, scver)
		}
	}

	s.merkleDB = newDBHandle(s.merkleDB.conn)
	s.merkleStore = smt.LoadStore(dbm.NewPrefixReadWriter(s.merkleDB, merklePrefix), root)
	s.stateDB = newDBHandle(s.stateDB.conn)
	// TODO: test for match with LastCommitID
	return &types.CommitID{Version: int64(version), Hash: root}, nil
}

// LastCommitID implements KVStore.
func (s *Store) LastCommitID() types.CommitID {
	last := lastVersion(s.stateDB.conn)
	if last == 0 {
		return types.CommitID{}
	}
	// Latest Merkle root should be the one currently stored
	hash, err := s.merkleDB.Get(versionRootKey)
	if err != nil {
		panic(err)
	}
	return types.CommitID{Version: last, Hash: hash}
}

// TODO: these should be implemented in DB
func (s *Store) SetPruning(types.PruningOptions)  {}
func (s *Store) GetPruning() types.PruningOptions { return types.PruningOptions{} }
func (s *Store) SetInitialVersion(version int64)  {}

func (s *Store) versionExists(version int64) bool {
	if version < 0 {
		return false
	}
	return s.stateDB.conn.Versions().Exists(uint64(version))
}

// Query implements ABCI interface, allows queries.
//
// by default we will return from (latest height -1),
// as we will have merkle proofs immediately (header height = data height + 1)
// If latest-1 is not present, use latest (which must be present)
// if you care to have the latest data to see a tx results, you must
// explicitly set the height you want to see
func (s *Store) Query(req abci.RequestQuery) (res abci.ResponseQuery) {
	// defer telemetry.MeasureSince(time.Now(), "store", "decoupled", "query")

	if len(req.Data) == 0 {
		return sdkerrors.QueryResult(sdkerrors.Wrap(sdkerrors.ErrTxDecode, "query cannot be zero length"))
	}

	// if height is 0, use the latest height
	height := req.Height
	if height == 0 {
		latest := lastVersion(s.stateDB.conn)
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

		dbr, err := s.stateDB.conn.ReaderAt(uint64(height))
		if err == dbm.ErrVersionDoesNotExist {
			return sdkerrors.QueryResult(sdkerrors.ErrInvalidHeight)
		}
		if err != nil {
			return sdkerrors.QueryResult(err)
		}
		defer dbr.Discard()
		contents := dbm.NewPrefixReader(dbr, dataPrefix)
		res.Value, err = contents.Get(res.Key)
		if err != nil {
			return sdkerrors.QueryResult(sdkerrors.ErrKeyNotFound)
		}
		if !req.Prove {
			break
		}
		scr, err := s.merkleDB.conn.ReaderAt(uint64(height))
		if err != nil { //
			return sdkerrors.QueryResult(sdkerrors.Wrapf(err,
				"cannot access SC DB version which exists in SS DB"))
		}
		defer scr.Discard()
		if scr == nil {
			return sdkerrors.QueryResult(sdkerrors.ErrInvalidHeight)
		}
		root, err := scr.Get(versionRootKey)
		if err != nil {
			panic(err)
		}
		merkleDB := smt.LoadStore(dbm.ReaderAsReadWriter(dbm.NewPrefixReader(scr, merklePrefix)), root)
		res.ProofOps, err = merkleDB.GetProof(res.Key)
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
