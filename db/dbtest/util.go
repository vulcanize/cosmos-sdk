package dbtest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cosmos/cosmos-sdk/db"
)

func Valid(t *testing.T, itr dbm.Iterator, expected bool) {
	valid := itr.Valid()
	require.Equal(t, expected, valid)
}

func Next(t *testing.T, itr dbm.Iterator, expected bool) {
	itr.Next()
	// assert.NoError(t, err) TODO: look at fixing this
	valid := itr.Valid()
	require.Equal(t, expected, valid)
}

func NextPanics(t *testing.T, itr dbm.Iterator) {
	assert.Panics(t, func() { itr.Next() }, "checkNextPanics expected an error but didn't")
}

func Domain(t *testing.T, itr dbm.Iterator, start, end []byte) {
	ds, de := itr.Domain()
	assert.Equal(t, start, ds, "checkDomain domain start incorrect")
	assert.Equal(t, end, de, "checkDomain domain end incorrect")
}

func Item(t *testing.T, itr dbm.Iterator, key []byte, value []byte) {
	v := itr.Value()

	k := itr.Key()

	assert.Exactly(t, key, k)
	assert.Exactly(t, value, v)
}

func Invalid(t *testing.T, itr dbm.Iterator) {
	Valid(t, itr, false)
	KeyPanics(t, itr)
	ValuePanics(t, itr)
	NextPanics(t, itr)
}

func KeyPanics(t *testing.T, itr dbm.Iterator) {
	assert.Panics(t, func() { itr.Key() }, "checkKeyPanics expected panic but didn't")
}

func Value(t *testing.T, db dbm.DBReader, key []byte, valueWanted []byte) {
	valueGot, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, valueWanted, valueGot)
}

func ValuePanics(t *testing.T, itr dbm.Iterator) {
	assert.Panics(t, func() { itr.Value() })
}

func CleanupDBDir(dir, name string) {
	err := os.RemoveAll(filepath.Join(dir, name) + ".db")
	if err != nil {
		panic(err)
	}
}
