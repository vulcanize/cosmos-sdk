package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/cosmos-sdk/db"
)

// Test that VersionManager satisfies the behavior of VersionSet
func TestVersionManager(t *testing.T) {
	vm := dbm.NewVersionManager(nil)
	require.Equal(t, uint64(0), vm.Last())
	require.Equal(t, uint64(0), vm.Initial())
	require.Equal(t, 0, vm.Count())
	require.True(t, vm.Equal(vm))
	require.False(t, vm.Exists(0))

	id, err := vm.Save(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)

	id, err = vm.Save(id) // can't save existing id
	require.Error(t, err)

	id, err = vm.Save(0)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, vm.All())
	require.Equal(t, uint64(1), vm.Initial())
	require.Equal(t, uint64(2), vm.Last())

	vmc := vm.Copy()
	id, err = vmc.Save(0)
	require.NoError(t, err)
	require.False(t, vm.Exists(id)) // true copy is made

	vm2 := dbm.NewVersionManager([]uint64{1, 2})
	require.True(t, vm.Equal(vm2))
}
