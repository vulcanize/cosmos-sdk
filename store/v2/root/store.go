package root

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	abci "github.com/tendermint/tendermint/abci/types"

	dbm "github.com/cosmos/cosmos-sdk/db"
	"github.com/cosmos/cosmos-sdk/db/prefix"
	util "github.com/cosmos/cosmos-sdk/internal"
	"github.com/cosmos/cosmos-sdk/store/cachekv"
	"github.com/cosmos/cosmos-sdk/store/listenkv"
	"github.com/cosmos/cosmos-sdk/store/tracekv"
	"github.com/cosmos/cosmos-sdk/store/types"
	"github.com/cosmos/cosmos-sdk/store/v2/smt"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/types/kv"
)

var (
	_ types.KVStore       = (*Store)(nil)
	_ types.CommitKVStore = (*Store)(nil)
	_ types.Queryable     = (*Store)(nil)
)

var (
	merkleRootKey     = []byte{0} // Key for root hash of Merkle tree
	dataPrefix        = []byte{1} // Prefix for state mappings
	indexPrefix       = []byte{2} // Prefix for Store reverse index
	merkleNodePrefix  = []byte{3} // Prefix for Merkle tree nodes
	merkleValuePrefix = []byte{4} // Prefix for Merkle value mappings
)

var (
	ErrVersionDoesNotExist = errors.New("version does not exist")
	ErrMaximumHeight       = errors.New("maximum block height reached")
)

type StoreConfig struct {
	// Version pruning options for backing DBs.
	Pruning        types.PruningOptions
	InitialVersion uint64
	// The backing DB to use for the state commitment Merkle tree data.
	// If nil, Merkle data is stored in the state storage DB under a separate prefix.
	StateCommitmentDB dbm.DBConnection
}

// Store is a CommitKVStore which handles state storage and commitments as separate concerns,
// optionally using separate backing key-value DBs for each.
// Allows synchronized R/W access by locking.
type Store struct {
	stateDB            dbm.DBConnection
	stateTxn           dbm.DBReadWriter
	dataBucket         dbm.DBReadWriter
	indexBucket        dbm.DBReadWriter
	stateCommitmentTxn dbm.DBReadWriter
	// State commitment (SC) KV store for current version
	stateCommitmentStore *smt.Store

	opts StoreConfig
	mtx  sync.RWMutex
}

var DefaultStoreConfig = StoreConfig{Pruning: types.PruneDefault, StateCommitmentDB: nil}

// NewStore creates a new Store, or loads one if the DB contains existing data.
func NewStore(db dbm.DBConnection, opts StoreConfig) (ret *Store, err error) {
	versions, err := db.Versions()
	if err != nil {
		return
	}
	loadExisting := false
	// If the DB is not empty, attempt to load existing data
	if saved := versions.Count(); saved != 0 {
		if opts.InitialVersion != 0 && versions.Last() < opts.InitialVersion {
			return nil, fmt.Errorf("latest saved version is less than initial version: %v < %v",
				versions.Last(), opts.InitialVersion)
		}
		loadExisting = true
	}
	err = db.Revert()
	if err != nil {
		return
	}
	stateTxn := db.ReadWriter()
	defer func() {
		if err != nil {
			err = util.CombineErrors(err, stateTxn.Discard(), "stateTxn.Discard also failed")
		}
	}()
	stateCommitmentTxn := stateTxn
	if opts.StateCommitmentDB != nil {
		var mversions dbm.VersionSet
		mversions, err = opts.StateCommitmentDB.Versions()
		if err != nil {
			return
		}
		// Version sets of each DB must match
		if !versions.Equal(mversions) {
			err = fmt.Errorf("Storage and StateCommitment DB have different version history") //nolint:stylecheck
			return
		}
		err = opts.StateCommitmentDB.Revert()
		if err != nil {
			return
		}
		stateCommitmentTxn = opts.StateCommitmentDB.ReadWriter()
	}

	var stateCommitmentStore *smt.Store
	if loadExisting {
		var root []byte
		root, err = stateTxn.Get(merkleRootKey)
		if err != nil {
			return
		}
		if root == nil {
			err = fmt.Errorf("could not get root of SMT")
			return
		}
		stateCommitmentStore = loadSMT(stateCommitmentTxn, root)
	} else {
		merkleNodes := prefix.NewPrefixReadWriter(stateCommitmentTxn, merkleNodePrefix)
		merkleValues := prefix.NewPrefixReadWriter(stateCommitmentTxn, merkleValuePrefix)
		stateCommitmentStore = smt.NewStore(merkleNodes, merkleValues)
	}
	return &Store{
		stateDB:              db,
		stateTxn:             stateTxn,
		dataBucket:           prefix.NewPrefixReadWriter(stateTxn, dataPrefix),
		indexBucket:          prefix.NewPrefixReadWriter(stateTxn, indexPrefix),
		stateCommitmentTxn:   stateCommitmentTxn,
		stateCommitmentStore: stateCommitmentStore,
		opts:                 opts,
	}, nil
}

func (s *Store) Close() error {
	err := s.stateTxn.Discard()
	if s.opts.StateCommitmentDB != nil {
		err = util.CombineErrors(err, s.stateCommitmentTxn.Discard(), "stateCommitmentTxn.Discard also failed")
	}
	return err
}

// Get implements KVStore.
func (s *Store) Get(key []byte) []byte {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	val, err := s.dataBucket.Get(key)
	if err != nil {
		panic(err)
	}
	return val
}

// Has implements KVStore.
func (s *Store) Has(key []byte) bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	has, err := s.dataBucket.Has(key)
	if err != nil {
		panic(err)
	}
	return has
}

// Set implements KVStore.
func (s *Store) Set(key, value []byte) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	err := s.dataBucket.Set(key, value)
	if err != nil {
		panic(err)
	}
	s.stateCommitmentStore.Set(key, value)
	khash := sha256.Sum256(key)
	err = s.indexBucket.Set(khash[:], key)
	if err != nil {
		panic(err)
	}
}

// Delete implements KVStore.
func (s *Store) Delete(key []byte) {
	khash := sha256.Sum256(key)
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.stateCommitmentStore.Delete(key)
	_ = s.indexBucket.Delete(khash[:])
	_ = s.dataBucket.Delete(key)
}

type contentsIterator struct {
	dbm.Iterator
	valid bool
}

func newIterator(source dbm.Iterator) *contentsIterator {
	ret := &contentsIterator{Iterator: source}
	ret.Next()
	return ret
}

func (it *contentsIterator) Next()       { it.valid = it.Iterator.Next() }
func (it *contentsIterator) Valid() bool { return it.valid }

// Iterator implements KVStore.
func (s *Store) Iterator(start, end []byte) types.Iterator {
	iter, err := s.dataBucket.Iterator(start, end)
	if err != nil {
		panic(err)
	}
	return newIterator(iter)
}

// ReverseIterator implements KVStore.
func (s *Store) ReverseIterator(start, end []byte) types.Iterator {
	iter, err := s.dataBucket.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}
	return newIterator(iter)
}

// GetStoreType implements Store.
func (s *Store) GetStoreType() types.StoreType {
	return types.StoreTypeDecoupled
}

// Commit implements Committer.
func (s *Store) Commit() types.CommitID {
	versions, err := s.stateDB.Versions()
	if err != nil {
		panic(err)
	}
	target := versions.Last() + 1
	if target > math.MaxInt64 {
		panic(ErrMaximumHeight)
	}
	// Fast forward to initialversion if needed
	if s.opts.InitialVersion != 0 && target < s.opts.InitialVersion {
		target = s.opts.InitialVersion
	}
	cid, err := s.commit(target)
	if err != nil {
		panic(err)
	}

	previous := cid.Version - 1
	if s.opts.Pruning.KeepEvery != 1 && s.opts.Pruning.Interval != 0 && cid.Version%int64(s.opts.Pruning.Interval) == 0 {
		// The range of newly prunable versions
		lastPrunable := previous - int64(s.opts.Pruning.KeepRecent)
		firstPrunable := lastPrunable - int64(s.opts.Pruning.Interval)
		for version := firstPrunable; version <= lastPrunable; version++ {
			if s.opts.Pruning.KeepEvery == 0 || version%int64(s.opts.Pruning.KeepEvery) != 0 {
				s.stateDB.DeleteVersion(uint64(version))
				if s.opts.StateCommitmentDB != nil {
					s.opts.StateCommitmentDB.DeleteVersion(uint64(version))
				}
			}
		}
	}
	return *cid
}

func (s *Store) commit(target uint64) (id *types.CommitID, err error) {
	root := s.stateCommitmentStore.Root()
	err = s.stateTxn.Set(merkleRootKey, root)
	if err != nil {
		return
	}
	err = s.stateTxn.Commit()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err = util.CombineErrors(err, s.stateDB.Revert(), "stateDB.Revert also failed")
		}
	}()
	err = s.stateDB.SaveVersion(target)
	if err != nil {
		return
	}

	stateTxn := s.stateDB.ReadWriter()
	defer func() {
		if err != nil {
			err = util.CombineErrors(err, stateTxn.Discard(), "stateTxn.Discard also failed")
		}
	}()
	stateCommitmentTxn := stateTxn

	// If DBs are not separate, StateCommitment state has been commmitted & snapshotted
	if s.opts.StateCommitmentDB != nil {
		defer func() {
			if err != nil {
				if delerr := s.stateDB.DeleteVersion(target); delerr != nil {
					err = fmt.Errorf("%w: commit rollback failed: %v", err, delerr)
				}
			}
		}()

		err = s.stateCommitmentTxn.Commit()
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				err = util.CombineErrors(err, s.opts.StateCommitmentDB.Revert(), "stateCommitmentDB.Revert also failed")
			}
		}()

		err = s.opts.StateCommitmentDB.SaveVersion(target)
		if err != nil {
			return
		}
		stateCommitmentTxn = s.opts.StateCommitmentDB.ReadWriter()
	}

	s.stateTxn = stateTxn
	s.dataBucket = prefix.NewPrefixReadWriter(stateTxn, dataPrefix)
	s.indexBucket = prefix.NewPrefixReadWriter(stateTxn, indexPrefix)
	s.stateCommitmentTxn = stateCommitmentTxn
	s.stateCommitmentStore = loadSMT(stateCommitmentTxn, root)

	return &types.CommitID{Version: int64(target), Hash: root}, nil
}

// LastCommitID implements Committer.
func (s *Store) LastCommitID() types.CommitID {
	versions, err := s.stateDB.Versions()
	if err != nil {
		panic(err)
	}
	last := versions.Last()
	if last == 0 {
		return types.CommitID{}
	}
	// Latest Merkle root is the one currently stored
	hash, err := s.stateTxn.Get(merkleRootKey)
	if err != nil {
		panic(err)
	}
	return types.CommitID{Version: int64(last), Hash: hash}
}

func (s *Store) GetPruning() types.PruningOptions   { return s.opts.Pruning }
func (s *Store) SetPruning(po types.PruningOptions) { s.opts.Pruning = po }

// Query implements ABCI interface, allows queries.
//
// by default we will return from (latest height -1),
// as we will have merkle proofs immediately (header height = data height + 1)
// If latest-1 is not present, use latest (which must be present)
// if you care to have the latest data to see a tx results, you must
// explicitly set the height you want to see
func (s *Store) Query(req abci.RequestQuery) (res abci.ResponseQuery) {
	if len(req.Data) == 0 {
		return sdkerrors.QueryResult(sdkerrors.Wrap(sdkerrors.ErrTxDecode, "query cannot be zero length"), false)
	}

	// if height is 0, use the latest height
	height := req.Height
	if height == 0 {
		versions, err := s.stateDB.Versions()
		if err != nil {
			return sdkerrors.QueryResult(errors.New("failed to get version info"), false)
		}
		latest := versions.Last()
		if versions.Exists(latest - 1) {
			height = int64(latest - 1)
		} else {
			height = int64(latest)
		}
	}
	if height < 0 {
		return sdkerrors.QueryResult(fmt.Errorf("height overflow: %v", height), false)
	}
	res.Height = height

	switch req.Path {
	case "/key":
		var err error
		res.Key = req.Data // data holds the key bytes

		view, err := s.GetVersion(height)
		if err != nil {
			if errors.Is(err, dbm.ErrVersionDoesNotExist) {
				err = sdkerrors.ErrInvalidHeight
			}
			return sdkerrors.QueryResult(err, false)
		}
		res.Value = view.Get(res.Key)
		if !req.Prove {
			break
		}
		res.ProofOps, err = view.GetStateCommitmentStore().GetProof(res.Key)
		if err != nil {
			return sdkerrors.QueryResult(fmt.Errorf("Merkle proof creation failed for key: %v", res.Key), false) //nolint: stylecheck // proper name
		}

	case "/subspace":
		pairs := kv.Pairs{
			Pairs: make([]kv.Pair, 0),
		}

		subspace := req.Data
		res.Key = subspace

		iterator := s.Iterator(subspace, types.PrefixEndBytes(subspace))
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
		return sdkerrors.QueryResult(sdkerrors.Wrapf(sdkerrors.ErrUnknownRequest, "unexpected query path: %v", req.Path), false)
	}

	return res
}

func loadSMT(stateCommitmentTxn dbm.DBReadWriter, root []byte) *smt.Store {
	merkleNodes := prefix.NewPrefixReadWriter(stateCommitmentTxn, merkleNodePrefix)
	merkleValues := prefix.NewPrefixReadWriter(stateCommitmentTxn, merkleValuePrefix)
	return smt.LoadStore(merkleNodes, merkleValues, root)
}

func (s *Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(s)
}

func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(s, w, tc))
}

func (s *Store) CacheWrapWithListeners(storeKey types.StoreKey, listeners []types.WriteListener) types.CacheWrap {
	return cachekv.NewStore(listenkv.NewStore(s, storeKey, listeners))
}