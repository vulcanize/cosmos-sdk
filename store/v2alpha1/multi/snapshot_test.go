package multi

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/cosmos-sdk/db"
	"github.com/cosmos/cosmos-sdk/db/memdb"
	pruningtypes "github.com/cosmos/cosmos-sdk/pruning/types"
	"github.com/cosmos/cosmos-sdk/snapshots"
	snapshottypes "github.com/cosmos/cosmos-sdk/snapshots/types"
	"github.com/cosmos/cosmos-sdk/store/types"
)

const expectedAppVersion = uint64(10)

var testStoreKeys []types.StoreKey

func makeStoreKeys(upto int) {
	if len(testStoreKeys) >= upto {
		return
	}
	for i := len(testStoreKeys); i < upto; i++ {
		skey := types.NewKVStoreKey(fmt.Sprintf("store%d", i))
		testStoreKeys = append(testStoreKeys, skey)
	}
}

func multiStoreConfig(t *testing.T, stores int) StoreParams {
	opts := DefaultStoreParams()
	opts.Pruning = pruningtypes.NewPruningOptions(pruningtypes.PruningNothing)

	makeStoreKeys(stores)
	for i := 0; i < stores; i++ {
		require.NoError(t, opts.RegisterSubstore(testStoreKeys[i], types.StoreTypePersistent))
	}

	return opts
}

func newMultiStoreWithGeneratedData(t *testing.T, db dbm.DBConnection, stores int, storeKeys uint64) *Store {
	cfg := multiStoreConfig(t, stores)
	store, err := NewStore(db, cfg)
	require.NoError(t, err)
	r := rand.New(rand.NewSource(49872768940)) // Fixed seed for deterministic tests

	var sKeys []string
	for sKey := range store.schema {
		sKeys = append(sKeys, sKey.Name())
	}

	sort.Slice(sKeys, func(i, j int) bool {
		return strings.Compare(sKeys[i], sKeys[j]) == -1
	})

	for _, sKey := range sKeys {
		sStore, err := store.getSubstore(sKey)
		require.NoError(t, err)
		for i := uint64(0); i < storeKeys; i++ {
			k := make([]byte, 8)
			v := make([]byte, 1024)
			binary.BigEndian.PutUint64(k, i)
			_, err := r.Read(v)
			if err != nil {
				panic(err)
			}
			sStore.Set(k, v)
		}
	}

	require.NoError(t, store.SetAppVersion(expectedAppVersion))

	store.Commit()
	return store
}

func newMultiStoreWithBasicData(t *testing.T, db dbm.DBConnection, stores int) *Store {
	cfg := multiStoreConfig(t, stores)
	store, err := NewStore(db, cfg)
	require.NoError(t, err)

	for sKey := range store.schema {
		sStore, err := store.getSubstore(sKey.Name())
		require.NoError(t, err)
		for k, v := range alohaData {
			sStore.Set([]byte(k), []byte(v))
		}
	}

	store.Commit()
	return store
}

func newMultiStore(t *testing.T, db dbm.DBConnection, stores int) *Store {
	cfg := multiStoreConfig(t, stores)
	store, err := NewStore(db, cfg)
	require.NoError(t, err)
	return store
}

func TestMultistoreSnapshot_Errors(t *testing.T) {
	store := newMultiStoreWithBasicData(t, memdb.NewDB(), 4)
	testcases := map[string]struct {
		height     uint64
		expectType error
	}{
		"0 height": {0, snapshottypes.ErrInvalidSnapshotVersion},
		"1 height": {1, nil},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			chunks := make(chan io.ReadCloser)
			streamWriter := snapshots.NewStreamWriter(chunks)
			err := store.Snapshot(tc.height, streamWriter)
			if tc.expectType != nil {
				assert.True(t, errors.Is(err, tc.expectType))
			}
		})
	}
}

func TestMultistoreRestore_Errors(t *testing.T) {
	store := newMultiStoreWithBasicData(t, memdb.NewDB(), 4)
	testcases := map[string]struct {
		height          uint64
		format          uint32
		expectErrorType error
	}{
		"0 height":       {0, snapshottypes.CurrentFormat, nil},
		"0 format":       {1, 0, snapshottypes.ErrUnknownFormat},
		"unknown format": {1, 9, snapshottypes.ErrUnknownFormat},
	}
	for name, tc := range testcases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			_, err := store.Restore(tc.height, tc.format, nil)
			require.Error(t, err)
			if tc.expectErrorType != nil {
				assert.True(t, errors.Is(err, tc.expectErrorType))
			}
		})
	}
}

func TestMultistoreSnapshot_Checksum(t *testing.T) {
	store := newMultiStoreWithGeneratedData(t, memdb.NewDB(), 5, 10000)
	version := uint64(store.LastCommitID().Version)

	testcases := []struct {
		format      uint32
		chunkHashes []string
	}{
		{snapshottypes.CurrentFormat, []string{
			"5eba26f24ff1d70b9a631815ee115a0a90cac46d2956e5a2958b8a40bd261dd0",
			"5c27478b7a5745e7ff6192bddebf756bf875a46163ed0f7f6cef777130e78b33",
			"c586a922b4ef44455a7d9aed231740fb5cd106afa0863ec11f9ff9e152eb9012",
			"3c2bfb03ff5e70ddda33221aef545efcb4317430aea9a54b588b243bdbb55a53",
			"42709e0a747b99cd29a968b12e2b795387b87dfa2f10edd2a6cc7745e16b8e85",
			"e79b663620c1984ab2d143003382d6306b23fc3ad9eb79ca526cd1c6e5f8bf6e",
		}},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(fmt.Sprintf("Format %v", tc.format), func(t *testing.T) {
			chunks := make(chan io.ReadCloser, 100)
			hashes := []string{}
			go func() {
				streamWriter := snapshots.NewStreamWriter(chunks)
				defer streamWriter.Close()
				require.NotNil(t, streamWriter)
				err := store.Snapshot(version, streamWriter)
				require.NoError(t, err)
			}()
			hasher := sha256.New()
			for chunk := range chunks {
				hasher.Reset()
				_, err := io.Copy(hasher, chunk)
				require.NoError(t, err)
				hashes = append(hashes, hex.EncodeToString(hasher.Sum(nil)))
			}
			assert.Equal(t, tc.chunkHashes, hashes, "Snapshot output for format %v has changed", tc.format)
		})
	}
}

func TestMultistoreSnapshotRestore(t *testing.T) {
	source := newMultiStoreWithGeneratedData(t, memdb.NewDB(), 3, 4)
	target := newMultiStore(t, memdb.NewDB(), 3)
	require.Equal(t, source.LastCommitID().Version, int64(1))
	version := uint64(source.LastCommitID().Version)
	// check for target store restore
	require.Equal(t, target.LastCommitID().Version, int64(0))

	dummyExtensionItem := snapshottypes.SnapshotItem{
		Item: &snapshottypes.SnapshotItem_Extension{
			Extension: &snapshottypes.SnapshotExtensionMeta{
				Name:   "test",
				Format: 1,
			},
		},
	}

	chunks := make(chan io.ReadCloser, 100)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks)
		require.NotNil(t, streamWriter)
		defer streamWriter.Close()
		err := source.Snapshot(version, streamWriter)
		require.NoError(t, err)
		// write an extension metadata
		err = streamWriter.WriteMsg(&dummyExtensionItem)
		require.NoError(t, err)
	}()

	streamReader, err := snapshots.NewStreamReader(chunks)
	require.NoError(t, err)
	nextItem, err := target.Restore(version, snapshottypes.CurrentFormat, streamReader)
	require.NoError(t, err)
	require.Equal(t, *dummyExtensionItem.GetExtension(), *nextItem.GetExtension())

	assert.Equal(t, source.LastCommitID(), target.LastCommitID())

	// check that the app version is restored from a snapshot.
	appVersion, err := target.GetAppVersion()
	require.NoError(t, err)
	require.Equal(t, expectedAppVersion, appVersion)

	for sKey := range source.schema {
		sourceSubStore, err := source.getSubstore(sKey.Name())
		require.NoError(t, err)
		targetSubStore, err := target.getSubstore(sKey.Name())
		require.NoError(t, err)
		require.Equal(t, sourceSubStore, targetSubStore, sKey)
	}

	// checking snapshot restoring for store with existed schema and without existing versions
	target3 := newMultiStore(t, memdb.NewDB(), 4)
	chunks3 := make(chan io.ReadCloser, 100)
	go func() {
		streamWriter3 := snapshots.NewStreamWriter(chunks3)
		require.NotNil(t, streamWriter3)
		defer streamWriter3.Close()
		err := source.Snapshot(version, streamWriter3)
		require.NoError(t, err)
	}()
	streamReader3, err := snapshots.NewStreamReader(chunks3)
	require.NoError(t, err)
	_, err = target3.Restore(version, snapshottypes.CurrentFormat, streamReader3)
	require.Error(t, err)
}

func BenchmarkMultistoreSnapshot100K(b *testing.B) {
	benchmarkMultistoreSnapshot(b, 10, 10000)
}

func BenchmarkMultistoreSnapshot1M(b *testing.B) {
	benchmarkMultistoreSnapshot(b, 10, 100000)
}

func BenchmarkMultistoreSnapshotRestore100K(b *testing.B) {
	benchmarkMultistoreSnapshotRestore(b, 10, 10000)
}

func BenchmarkMultistoreSnapshotRestore1M(b *testing.B) {
	benchmarkMultistoreSnapshotRestore(b, 10, 100000)
}

func benchmarkMultistoreSnapshot(b *testing.B, stores int, storeKeys uint64) {
	b.Skip("Noisy with slow setup time, please see https://github.com/cosmos/cosmos-sdk/issues/8855.")

	b.ReportAllocs()
	b.StopTimer()
	source := newMultiStoreWithGeneratedData(nil, memdb.NewDB(), stores, storeKeys)

	version := source.LastCommitID().Version
	require.EqualValues(b, 1, version)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		target := newMultiStore(nil, memdb.NewDB(), stores)
		require.EqualValues(b, 0, target.LastCommitID().Version)

		chunks := make(chan io.ReadCloser)
		go func() {
			streamWriter := snapshots.NewStreamWriter(chunks)
			require.NotNil(b, streamWriter)
			err := source.Snapshot(uint64(version), streamWriter)
			require.NoError(b, err)
		}()
		for reader := range chunks {
			_, err := io.Copy(io.Discard, reader)
			require.NoError(b, err)
			err = reader.Close()
			require.NoError(b, err)
		}
	}
}

func benchmarkMultistoreSnapshotRestore(b *testing.B, stores int, storeKeys uint64) {
	b.Skip("Noisy with slow setup time, please see https://github.com/cosmos/cosmos-sdk/issues/8855.")

	b.ReportAllocs()
	b.StopTimer()
	source := newMultiStoreWithGeneratedData(nil, memdb.NewDB(), stores, storeKeys)
	version := uint64(source.LastCommitID().Version)
	require.EqualValues(b, 1, version)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		target := newMultiStore(nil, memdb.NewDB(), stores)
		require.EqualValues(b, 0, target.LastCommitID().Version)

		chunks := make(chan io.ReadCloser)
		go func() {
			writer := snapshots.NewStreamWriter(chunks)
			require.NotNil(b, writer)
			err := source.Snapshot(version, writer)
			require.NoError(b, err)
		}()

		reader, err := snapshots.NewStreamReader(chunks)
		require.NoError(b, err)
		_, err = target.Restore(version, snapshottypes.CurrentFormat, reader)
		require.NoError(b, err)
		require.Equal(b, source.LastCommitID(), target.LastCommitID())
	}
}
