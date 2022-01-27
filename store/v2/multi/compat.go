package multi

import (
	"errors"
	"io"

	dbm "github.com/tendermint/tm-db"

	v1 "github.com/cosmos/cosmos-sdk/store/types"
	// v2 "github.com/cosmos/cosmos-sdk/store/v2"
)

var (
	_ v1.CommitMultiStore = (*compatStore)(nil)
	_ v1.CacheMultiStore  = (*compatCacheStore)(nil)
	_ v1.Queryable        = (*compatStore)(nil)

	ErrInvalidVersion = errors.New("invalid version")
)

type compatStore struct {
	*Store
}

type compatCacheStore struct {
	*cacheStore
	source *Store
}

func StoreAsV1CommitMultiStore(s *Store) v1.CommitMultiStore { return &compatStore{s} }

func (st *compatStore) GetStoreType() v1.StoreType {
	return v1.StoreTypeMulti
}

func (st *compatStore) CacheWrap() v1.CacheWrap {
	return st.CacheMultiStore()
}

// TODO: v1 MultiStore ignores args, do we as well?
func (st *compatStore) CacheWrapWithTrace(io.Writer, v1.TraceContext) v1.CacheWrap {
	return st.CacheWrap()
}
func (st *compatStore) CacheWrapWithListeners(v1.StoreKey, []v1.WriteListener) v1.CacheWrap {
	return st.CacheWrap()
}

func (st *compatStore) CacheMultiStore() v1.CacheMultiStore {
	return &compatCacheStore{newCacheStore(st.Store), st.Store}
}
func (st *compatStore) CacheMultiStoreWithVersion(version int64) (v1.CacheMultiStore, error) {
	view, err := st.GetVersion(version)
	if err != nil {
		return nil, err
	}
	return &compatCacheStore{newCacheStore(view), st.Store}, nil
}

func (st *compatStore) GetStore(k v1.StoreKey) v1.Store {
	return st.GetKVStore(k)
}

func (st *compatStore) GetCommitStore(key v1.StoreKey) v1.CommitStore {
	panic("unsupported: GetCommitStore")
}
func (st *compatStore) GetCommitKVStore(key v1.StoreKey) v1.CommitKVStore {
	panic("unsupported: GetCommitKVStore")
}

func (st *compatStore) SetTracer(w io.Writer) v1.MultiStore {
	st.Store.SetTracer(w)
	return st
}
func (st *compatStore) SetTracingContext(tc v1.TraceContext) v1.MultiStore {
	st.Store.SetTracingContext(tc)
	return st
}

func (st *compatStore) MountStoreWithDB(key v1.StoreKey, typ v1.StoreType, db dbm.DB) {
	panic("unsupported: MountStoreWithDB")
}

func (st *compatStore) LoadLatestVersion() error {
	return nil // this store is always at the latest version
}
func (st *compatStore) LoadLatestVersionAndUpgrade(upgrades *v1.StoreUpgrades) error {
	panic("unsupported: LoadLatestVersionAndUpgrade")
}
func (st *compatStore) LoadVersionAndUpgrade(ver int64, upgrades *v1.StoreUpgrades) error {
	panic("unsupported: LoadLatestVersionAndUpgrade")
}

func (st *compatStore) LoadVersion(ver int64) error {
	// TODO
	// cache a viewStore representing "current" version?
	return nil
}

func (st *compatStore) SetInterBlockCache(v1.MultiStorePersistentCache) {
	panic("unsupported: SetInterBlockCache")
}
func (st *compatStore) SetInitialVersion(version int64) error {
	if version < 0 {
		return ErrInvalidVersion
	}
	return st.Store.SetInitialVersion(uint64(version))
}
func (st *compatStore) SetIAVLCacheSize(size int) {
	panic("unsupported: SetIAVLCacheSize")
}

func (cs *compatCacheStore) GetStoreType() v1.StoreType { return v1.StoreTypeMulti }
func (cs *compatCacheStore) CacheWrap() v1.CacheWrap {
	return cs.CacheMultiStore()
}
func (cs *compatCacheStore) CacheWrapWithTrace(w io.Writer, tc v1.TraceContext) v1.CacheWrap {
	return cs.CacheWrap()
}
func (cs *compatCacheStore) CacheWrapWithListeners(storeKey v1.StoreKey, listeners []v1.WriteListener) v1.CacheWrap {
	return cs.CacheWrap()
}
func (cs *compatCacheStore) CacheMultiStore() v1.CacheMultiStore {
	return &compatCacheStore{newCacheStore(cs.cacheStore), cs.source}
}
func (cs *compatCacheStore) CacheMultiStoreWithVersion(version int64) (v1.CacheMultiStore, error) {
	view, err := cs.source.GetVersion(version)
	if err != nil {
		return nil, err
	}
	return &compatCacheStore{newCacheStore(view), cs.source}, nil
}

func (cs *compatCacheStore) GetStore(k v1.StoreKey) v1.Store { return cs.GetKVStore(k) }

func (cs *compatCacheStore) SetTracer(w io.Writer) v1.MultiStore {
	cs.cacheStore.SetTracer(w)
	return cs
}
func (cs *compatCacheStore) SetTracingContext(tc v1.TraceContext) v1.MultiStore {
	cs.cacheStore.SetTracingContext(tc)
	return cs
}
