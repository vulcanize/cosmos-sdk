package db

import (
	tmdb "github.com/tendermint/tm-db"
)

// FIXME: This adapter is used to wrap objects conforming to the new
// DB interface so that they support the old tm-db interface, and vice
// versa. This is needed as an intermediate step in introducing the
// new interface without refactoring all of the existing multi-store
// and IAVL code, as they are both likely to be deprecated before the
// ADR is completely implemented.

// Pretends a new DBRW is a tm-db DB
type tmdbAdapter struct {
	DBReadWriter
}

var _ tmdb.DB = tmdbAdapter{}

func AdaptTmdb(db DBReadWriter) tmdbAdapter { return tmdbAdapter{db} }

func (d tmdbAdapter) Close() error { d.Discard(); return nil }

func (d tmdbAdapter) DeleteSync(k []byte) error { return d.DBReadWriter.Delete(k) }
func (d tmdbAdapter) SetSync(k, v []byte) error { return d.DBReadWriter.Set(k, v) }
func (d tmdbAdapter) Iterator(s, e []byte) (tmdb.Iterator, error) {
	return d.DBReadWriter.Iterator(s, e)
}
func (d tmdbAdapter) ReverseIterator(s, e []byte) (tmdb.Iterator, error) {
	return d.DBReadWriter.ReverseIterator(s, e)
}
func (d tmdbAdapter) NewBatch() tmdb.Batch     { return nil }
func (d tmdbAdapter) Print() error             { return nil }
func (d tmdbAdapter) Stats() map[string]string { return nil }

// Pretends a tm-db DB is DBRW
type dbrwAdapter struct {
	tmdb.DB
}

var _ DBReadWriter = dbrwAdapter{}

func AdaptDBRW(db tmdb.DB) dbrwAdapter { return dbrwAdapter{db} }

func (d dbrwAdapter) Commit() error { return nil }
func (d dbrwAdapter) Discard()      { d.Close() }
func (d dbrwAdapter) Iterator(s, e []byte) (Iterator, error) {
	return d.DB.Iterator(s, e)
}
func (d dbrwAdapter) ReverseIterator(s, e []byte) (Iterator, error) {
	return d.DB.ReverseIterator(s, e)
}
