package store

import (
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/lazyledger/smt"

	"github.com/cosmos/cosmos-sdk/db/memdb"
	"github.com/cosmos/cosmos-sdk/db/prefix"
	smtstore "github.com/cosmos/cosmos-sdk/store/v2/smt"
)

// compare SMT with different mapstore backends - memdb vs hashmap
func BenchmarkSMT(b *testing.B) {
	tree := smt.NewSparseMerkleTree(smt.NewSimpleMap(), smt.NewSimpleMap(), sha256.New())
	b.Run("sm", func(b *testing.B) {
		runTreeGets(b, tree)
	})

	db := memdb.NewDB()
	rw := db.ReadWriter()
	nodesTxn := prefix.NewPrefixReadWriter(rw, []byte{0})
	valuesTxn := prefix.NewPrefixReadWriter(rw, []byte{1})
	nodes := smt.NewCachedMap(smtstore.DbMapStore{nodesTxn}, 0)
	values := smt.NewCachedMap(smtstore.DbMapStore{valuesTxn}, 0)
	tree = smt.NewSparseMerkleTree(nodes, values, sha256.New())
	b.Run("memdb", func(b *testing.B) {
		runTreeGets(b, tree)
	})
	db.Close()
}

type mapstoreCtor = func() smt.MapStore

func runTreeGets(b *testing.B, tree *smt.SparseMerkleTree) {
	nValues := 100_000
	totalOpsCount := 1000

	values := prepareValues(nValues)
	keys := distinctKeys(0, nValues)
	nonkeys := distinctKeys(nValues, nValues*2)
	for i, v := range values {
		tree.Update(keys[i], v)
	}

	b.Run("get-present", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < totalOpsCount; j++ {
				ki := rand.Intn(nValues)
				tree.Get(keys[ki])
			}
		}
	})

	b.Run("get-absent", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < totalOpsCount; j++ {
				ki := rand.Intn(nValues)
				tree.Get(nonkeys[ki])
			}
		}
	})
}
