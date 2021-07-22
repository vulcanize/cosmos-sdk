package types

import "errors"

// ExportDone is returned by Exporter.Next() when all items have been exported.
var ExportDone = errors.New("export is complete")

type PortableStore interface {
	Export(uint64) (Exporter, error)
	Import(uint64) (Importer, error)
}

type Exporter interface {
	Next() (*SnapshotItem, error)
	Close()
}

type Importer interface {
	Add(*SnapshotItem) error
	Commit() error
	// Close()
}
