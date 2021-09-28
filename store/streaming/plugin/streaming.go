package plugin

import (
	"github.com/cosmos/cosmos-sdk/codec"
	serverTypes "github.com/cosmos/cosmos-sdk/server/types"
	"github.com/cosmos/cosmos-sdk/store/streaming"
	"github.com/cosmos/cosmos-sdk/types"
)

// StreamingServicePlugin interface for plugins that load a streaming.Service
type StreamingServicePlugin interface {
	// LoadStreamingService initializes and returns the streaming.Service from the plugged-in module
	LoadStreamingService(pluginFileName string, opts serverTypes.AppOptions, marshaller codec.BinaryCodec, keys map[string]*types.KVStoreKey) (streaming.Service, error)

	// Plugin is the base Plugin interface
	Plugin
}
