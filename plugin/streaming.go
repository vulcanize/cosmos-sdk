package plugin

import (
	"sync"

	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/types"
)

// STREAMING_TOML_KEY is the top-level TOML key for configuring streaming service plugins
const STREAMING_TOML_KEY = "streaming"

// StateStreaming interface for plugins that load a baseapp.StreamingService implementaiton from a plugin onto a baseapp.BaseApp
type StateStreaming interface {
	// Register configures and registers the plugin streaming service with the BaseApp
	Register(bApp *baseapp.BaseApp, marshaller codec.BinaryCodec, keys map[string]*types.KVStoreKey) error

	// Start starts the background streaming process of the plugin streaming service
	Start(wg *sync.WaitGroup)

	// Plugin is the base Plugin interface
	Plugin
}
