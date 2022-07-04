package db

import (
	"github.com/cosmos/cosmos-sdk/db/types"

	_ "github.com/cosmos/cosmos-sdk/db/internal/backends"
)

type (
	DBConnection    = types.DBConnection
	DBReader        = types.DBReader
	DBWriter        = types.DBWriter
	DBReadWriter    = types.DBReadWriter
	Iterator        = types.Iterator
	VersionSet      = types.VersionSet
	VersionIterator = types.VersionIterator
	BackendType     = types.BackendType
)

var (
	ErrVersionDoesNotExist = types.ErrVersionDoesNotExist

	MemDBBackend    = types.MemDBBackend
	RocksDBBackend  = types.RocksDBBackend
	BadgerDBBackend = types.BadgerDBBackend

	NewDB              = types.NewDB
	ReaderAsReadWriter = types.ReaderAsReadWriter
	NewVersionManager  = types.NewVersionManager
)
