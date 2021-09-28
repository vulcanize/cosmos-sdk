package main

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/cosmos/cosmos-sdk/codec"
	serverTypes "github.com/cosmos/cosmos-sdk/server/types"
	"github.com/cosmos/cosmos-sdk/store/streaming"
	"github.com/cosmos/cosmos-sdk/store/streaming/plugin/plugins/file/service"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

const (
	PLUGIN_NAME = "file" // name for this streaming service plugin
	PLUGIN_VERSION = "0.0.1" // version for this streaming service plugin

	// Params expected in the
	PREFIX_PARAM = "prefix" // an optional prefix to prepend to the files we write
	WRITE_DIR_PARAM = "writeDir" // the directory we want to write files out to
	KEYS_PARAM = "keys" // a list of the StoreKeys we want to expose for this streaming service
)

// StreamingServicePlugin is the exported symbol for loading the file streaming service as a plugin
var StreamingServicePlugin streamingServicePlugin

type streamingServicePlugin struct{}

// Name satisfies the plugin.StreamingServicePlugin interface
func (ssp streamingServicePlugin) Name() string {
	return PLUGIN_NAME
}

// Version satisfies the plugin.StreamingServicePlugin interface
func (ssp streamingServicePlugin) Version() string {
	return PLUGIN_VERSION
}

// Init satisfies the plugin.StreamingServicePlugin interface
func (ssp streamingServicePlugin) Init(env serverTypes.AppOptions) error {
	return nil
}

// LoadStreamingService satisfies the plugin.StreamingServicePlugin interface
func (ssp streamingServicePlugin) LoadStreamingService(pluginFileName string, opts serverTypes.AppOptions, marshaller codec.BinaryCodec, keys map[string]*sdk.KVStoreKey) (streaming.Service, error) {
	// load all the params required for this plugin from the provided AppOptions
	filePrefix := cast.ToString(opts.Get(fmt.Sprintf("streaming.%s.%s", pluginFileName, PREFIX_PARAM)))
	fileDir := cast.ToString(opts.Get(fmt.Sprintf("streaming.%s.%s", pluginFileName, WRITE_DIR_PARAM)))
	// get the store keys allowed to be exposed for this streaming service
	exposeKeyStrings := cast.ToStringSlice(opts.Get(fmt.Sprintf("streaming.%s.%s", pluginFileName, KEYS_PARAM)))
	var exposeStoreKeys []sdk.StoreKey
	if len(exposeKeyStrings) > 0 {
		exposeStoreKeys = make([]sdk.StoreKey, 0, len(exposeKeyStrings))
		for _, keyStr := range exposeKeyStrings {
			if storeKey, ok := keys[keyStr]; ok {
				exposeStoreKeys = append(exposeStoreKeys, storeKey)
			}
		}
	} else { // if none are specified, we expose all the keys
		exposeStoreKeys = make([]sdk.StoreKey, 0, len(keys))
		for _, storeKey := range keys {
			exposeStoreKeys = append(exposeStoreKeys, storeKey)
		}
	}
	return service.NewFileStreamingService(fileDir, filePrefix, exposeStoreKeys, marshaller)
}
