package snapshots

import (
	"bufio"
	"compress/zlib"
	"io"
	// "math"

	protoio "github.com/gogo/protobuf/io"

	"github.com/cosmos/cosmos-sdk/snapshots/types"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
)

const (
	// Do not change chunk size without new snapshot format (must be uniform across nodes)
	snapshotChunkSize   = uint64(10e6)
	snapshotBufferSize  = int(snapshotChunkSize)
	snapshotMaxItemSize = int(64e6) // SDK has no key/value size limit, so we set an arbitrary limit
)

//---------------------- Snapshotting ------------------

// Snapshot implements Snapshotter. The snapshot output for a given format must be
// identical across nodes such that chunks from different sources fit together. If the output for a
// given format changes (at the byte level), the snapshot format must be bumped - see
// TestMultistoreSnapshot_Checksum test.
func CreateSnapshot(
	store storetypes.PortableStore, height uint64,
	format uint32,
) (<-chan io.ReadCloser, error) {
	if format != types.CurrentFormat {
		return nil, sdkerrors.Wrapf(types.ErrUnknownFormat, "format %v", format)
	}

	// Spawn goroutine to generate snapshot chunks and pass their io.ReadClosers through a channel
	ch := make(chan io.ReadCloser)
	go func() {
		// Set up a stream pipeline to serialize snapshot nodes:
		// ExportNode -> delimited Protobuf -> zlib -> buffer -> chunkWriter -> chan io.ReadCloser
		chunkWriter := NewChunkWriter(ch, snapshotChunkSize)
		defer chunkWriter.Close()
		bufWriter := bufio.NewWriterSize(chunkWriter, snapshotBufferSize)
		defer func() {
			if err := bufWriter.Flush(); err != nil {
				chunkWriter.CloseWithError(err)
			}
		}()
		zWriter, err := zlib.NewWriterLevel(bufWriter, 7)
		if err != nil {
			chunkWriter.CloseWithError(sdkerrors.Wrap(err, "zlib failure"))
			return
		}
		defer func() {
			if err := zWriter.Close(); err != nil {
				chunkWriter.CloseWithError(err)
			}
		}()
		protoWriter := protoio.NewDelimitedWriter(zWriter)
		defer func() {
			if err := protoWriter.Close(); err != nil {
				chunkWriter.CloseWithError(err)
			}
		}()

		// Store is serialized as a stream of SnapshotItem Protobuf messages.
		exporter, err := store.Export()
		if err != nil {
			chunkWriter.CloseWithError(err)
			return
		}
		defer exporter.Close()

		for {
			item, err := exporter.Next()
			if err == storetypes.ExportDone {
				break
			} else if err != nil {
				chunkWriter.CloseWithError(err)
				return
			}
			err = protoWriter.WriteMsg(item)
			if err != nil {
				chunkWriter.CloseWithError(err)
				return
			}
		}
	}()

	return ch, nil
}

// Restore implements Snapshotter.
func RestoreSnapshot(
	store storetypes.PortableStore, height uint64, format uint32, chunks <-chan io.ReadCloser, ready chan<- struct{},
) error {
	if format != types.CurrentFormat {
		return sdkerrors.Wrapf(types.ErrUnknownFormat, "format %v", format)
	}

	// Signal readiness. Must be done before the readers below are set up, since the zlib
	// reader reads from the stream on initialization, potentially causing deadlocks.
	if ready != nil {
		close(ready)
	}

	// Set up a restore stream pipeline
	// chan io.ReadCloser -> chunkReader -> zlib -> delimited Protobuf -> ExportNode
	chunkReader := NewChunkReader(chunks)
	defer chunkReader.Close()
	zReader, err := zlib.NewReader(chunkReader)
	if err != nil {
		return sdkerrors.Wrap(err, "zlib failure")
	}
	defer zReader.Close()
	protoReader := protoio.NewDelimitedReader(zReader, snapshotMaxItemSize)
	defer protoReader.Close()

	// Import nodes into the store. Read a SnapshotKVItem until reaching EOF.
	// var importer Importer
	importer, err := store.Import()
	if err != nil {
		return sdkerrors.Wrap(err, "import failed")
	}
	for {
		item := &storetypes.SnapshotItem{}
		err := protoReader.ReadMsg(item)
		if err == io.EOF {
			break
		} else if err != nil {
			return sdkerrors.Wrap(err, "invalid protobuf message")
		}
		err = importer.Add(item)
		if err != nil {
			return sdkerrors.Wrap(err, "snapshot node import failed")
		}
	}

	err = importer.Commit()
	if err != nil {
		return sdkerrors.Wrap(err, "IAVL commit failed")
	}
	importer.Close()

	// flushMetadata(store.db, int64(height), store.buildCommitInfo(int64(height)), []int64{})
	// return store.LoadLatestVersion()
	return nil
}
