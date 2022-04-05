package store

import (
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/lazyledger/smt"

	"github.com/cosmos/cosmos-sdk/db/memdb"
	"github.com/cosmos/cosmos-sdk/db/prefix"
	smtstore "github.com/cosmos/cosmos-sdk/store/v2alpha1/smt"
)

// compare SMT with different mapstore backends - memdb vs hashmap
func BenchmarkSMT(b *testing.B) {
	{
		nodes, values := smt.NewSimpleMap(), smt.NewSimpleMap()
		b.Run("sm", func(b *testing.B) { runTreeSuite(b, nodes, values, false) })
	}
	for _, cache := range []bool{false, true} {
		db := memdb.NewDB()
		rw := db.ReadWriter()
		nodes := smtstore.DbMapStore{prefix.NewPrefixReadWriter(rw, []byte{0})}
		values := smtstore.DbMapStore{prefix.NewPrefixReadWriter(rw, []byte{1})}
		name := "memdb"
		if cache {
			name += "+cache"
		}
		b.Run(name, func(b *testing.B) { runTreeSuite(b, nodes, values, cache) })
		db.Close()
	}
}

// func runTreeRW(b *testing.B, tree *smt.SparseMerkleTree) {
func runTreeSuite(b *testing.B, nodesmap, valuesmap smt.MapStore, cache bool) {
	nValues := 200_000
	totalOpsCount := 1000

	values := prepareValues(nValues)
	keys := distinctKeys(0, nValues)
	nonkeys := distinctKeys(nValues, nValues*2)

	if cache {
		nodesmap = smt.NewCachedMap(nodesmap, 0)
		valuesmap = smt.NewCachedMap(valuesmap, 0)
	}
	tree := smt.NewSparseMerkleTree(nodesmap, valuesmap, sha256.New())
	for i, v := range values {
		tree.Update(keys[i], v)
	}
	if cache {
		if err := nodesmap.(smt.CommitMapStore).Commit(); err != nil {
			b.Fatal(err)
		}
		if err := valuesmap.(smt.CommitMapStore).Commit(); err != nil {
			b.Fatal(err)
		}
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

	b.Run("set-present", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < totalOpsCount; j++ {
				ki := rand.Intn(nValues)
				tree.Update(keys[ki], values[len(values)-1-ki])
			}
		}
	})

	b.Run("set-absent", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < totalOpsCount; j++ {
				ki := rand.Intn(nValues)
				tree.Update(nonkeys[ki], values[ki])
			}
		}
	})
}
