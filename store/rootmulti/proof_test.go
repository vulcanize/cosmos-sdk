package rootmulti

import (
	"testing"

	"github.com/stretchr/testify/require"
	abci "github.com/tendermint/tendermint/abci/types"
	dbm "github.com/tendermint/tm-db"

	"github.com/cosmos/cosmos-sdk/store/decoupled"
	"github.com/cosmos/cosmos-sdk/store/types"
)

func TestVerifyMerkleStoreQueryProof(t *testing.T) {
	// Create main tree for testing.
	db := dbm.NewMemDB()
	mStore, err := decoupled.LoadStore(db, types.CommitID{}, 0)
	store := mStore.(*decoupled.Store)
	require.Nil(t, err)
	store.Set([]byte("MYKEY"), []byte("MYVALUE"))
	cid := store.Commit()

	// Get Proof
	res := store.Query(abci.RequestQuery{
		Path:  "/key", // required path to get key/value+proof
		Data:  []byte("MYKEY"),
		Prove: true,
	})
	require.NotNil(t, res.ProofOps)

	// Verify proof.
	prt := DefaultProofRuntime()
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/MYKEY", []byte("MYVALUE"))
	require.Nil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/MYKEY_NOT", []byte("MYVALUE"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/MYKEY/MYKEY", []byte("MYVALUE"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "MYKEY", []byte("MYVALUE"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/MYKEY", []byte("MYVALUE_NOT"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/MYKEY", []byte(nil))
	require.NotNil(t, err)
}

func TestVerifyMultiStoreQueryProof(t *testing.T) {
	// Create main tree for testing.
	db := dbm.NewMemDB()
	store := NewStore(db)
	merkleStoreKey := types.NewKVStoreKey("merkleStoreKey")

	store.MountStoreWithDB(merkleStoreKey, types.StoreTypeDecoupled, nil)
	require.NoError(t, store.LoadVersion(0))

	merkleStore := store.GetCommitStore(merkleStoreKey).(*decoupled.Store)
	merkleStore.Set([]byte("MYKEY"), []byte("MYVALUE"))
	cid := store.Commit()

	// Get Proof
	res := store.Query(abci.RequestQuery{
		Path:  "/merkleStoreKey/key", // required path to get key/value+proof
		Data:  []byte("MYKEY"),
		Prove: true,
	})
	require.NotNil(t, res.ProofOps)

	// Verify proof.
	prt := DefaultProofRuntime()
	err := prt.VerifyValue(res.ProofOps, cid.Hash, "/merkleStoreKey/MYKEY", []byte("MYVALUE"))
	require.Nil(t, err)

	// Verify proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/merkleStoreKey/MYKEY", []byte("MYVALUE"))
	require.Nil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/merkleStoreKey/MYKEY_NOT", []byte("MYVALUE"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/merkleStoreKey/MYKEY/MYKEY", []byte("MYVALUE"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "merkleStoreKey/MYKEY", []byte("MYVALUE"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/MYKEY", []byte("MYVALUE"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/merkleStoreKey/MYKEY", []byte("MYVALUE_NOT"))
	require.NotNil(t, err)

	// Verify (bad) proof.
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/merkleStoreKey/MYKEY", []byte(nil))
	require.NotNil(t, err)
}

func TestVerifyMultiStoreQueryProofAbsence(t *testing.T) {
	// Create main tree for testing.
	db := dbm.NewMemDB()
	store := NewStore(db)
	merkleStoreKey := types.NewKVStoreKey("merkleStoreKey")

	store.MountStoreWithDB(merkleStoreKey, types.StoreTypeDecoupled, nil)
	err := store.LoadVersion(0)
	require.NoError(t, err)

	merkleStore := store.GetCommitStore(merkleStoreKey).(*decoupled.Store)
	merkleStore.Set([]byte("MYKEY"), []byte("MYVALUE"))
	cid := store.Commit() // Commit with empty merkle store.

	// Get Proof
	res := store.Query(abci.RequestQuery{
		Path:  "/merkleStoreKey/key", // required path to get key/value+proof
		Data:  []byte("MYABSENTKEY"),
		Prove: true,
	})
	require.NotNil(t, res.ProofOps)

	// Verify proof.
	prt := DefaultProofRuntime()
	err = prt.VerifyAbsence(res.ProofOps, cid.Hash, "/merkleStoreKey/MYABSENTKEY")
	require.Nil(t, err)

	// Verify (bad) proof.
	prt = DefaultProofRuntime()
	err = prt.VerifyAbsence(res.ProofOps, cid.Hash, "/MYABSENTKEY")
	require.NotNil(t, err)

	// Verify (bad) proof.
	prt = DefaultProofRuntime()
	err = prt.VerifyValue(res.ProofOps, cid.Hash, "/merkleStoreKey/MYABSENTKEY", []byte(""))
	require.NotNil(t, err)
}
