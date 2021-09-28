package plugin

import (
	"fmt"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"github.com/spf13/cast"

	"github.com/cosmos/cosmos-sdk/baseapp"
	"github.com/cosmos/cosmos-sdk/codec"
	serverTypes "github.com/cosmos/cosmos-sdk/server/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

const (
	DEFAULT_PLUGIN_DIR_PREFIX = "./plugins/"
	STREAMING_SERVICE_PLUGIN_SYMBOL = "StreamingServicePlugin"
)

// LoadStreamingServicePlugins loads streaming.Service plugins onto the provided BaseApp
func LoadStreamingServicePlugins(bApp *baseapp.BaseApp, appOpts serverTypes.AppOptions, appCodec codec.BinaryCodec, keys map[string]*sdk.KVStoreKey) (*sync.WaitGroup, chan struct{}, error) {
	// waitgroup and quit channel for optional shutdown coordination of the streaming service(s)
	wg := new(sync.WaitGroup)
	quitChan := make(chan struct{})
	// configure state listening capabilities using AppOptions
	pluginNames := cast.ToStringSlice(appOpts.Get("store.streaming.pluginFileNames"))
	for _, pluginName := range pluginNames {
		// trim off the .so suffix if it exists
		pluginName = strings.TrimSuffix(pluginName, ".so")
		// lookup the directory for this plugin
		pluginDir := cast.ToString(appOpts.Get(fmt.Sprintf("streaming.%s.pluginDir", pluginName)))
		if pluginDir == "" {
			pluginDir = filepath.Join(DEFAULT_PLUGIN_DIR_PREFIX, pluginName)
		}
		// load the streaming.ServicePlugin
		streamingServicePlugin, err := loadStreamingServicePlugin(pluginDir, pluginName)
		if err != nil {
			// close the quitChan to shut down any services we may have already spun up before hitting the error on this one
			close(quitChan)
			return nil, nil, err
		}
		// construct the streaming.Service from the plugin
		streamingService, err := streamingServicePlugin.LoadStreamingService(pluginName, appOpts, appCodec, keys)
		if err != nil {
			// close the quitChan to shut down any services we may have already spun up before hitting the error on this one
			close(quitChan)
			return nil, nil, err
		}
		// register the streaming service with the BaseApp
		bApp.SetStreamingService(streamingService)
		// kick off the background streaming service loop
		streamingService.Stream(wg, quitChan)
	}
	return wg, quitChan, nil
}

// loadStreamingServicePlugin loads and returns a streaming.Service from the plugin linked to at the provided path and file name
func loadStreamingServicePlugin(dir, fileName string) (StreamingServicePlugin, error) {
	pluginFile := filepath.Join(dir, fmt.Sprintf("%s.so", fileName))
	plug, err := plugin.Open(pluginFile)
	if err != nil {
		return nil, fmt.Errorf("unable to load plugin module in directory %s with file name %s: %v", dir, fileName, err)
	}
	symServicePlugin, err := plug.Lookup(STREAMING_SERVICE_PLUGIN_SYMBOL)
	if err != nil {
		return nil, fmt.Errorf("unable to find symbol %s: %v", STREAMING_SERVICE_PLUGIN_SYMBOL, err)
	}
	servicePlugin, ok := symServicePlugin.(StreamingServicePlugin)
	if !ok {
		return nil, fmt.Errorf("unable to assert symbol %s type as a StreamingServicePlugin", STREAMING_SERVICE_PLUGIN_SYMBOL)
	}
	return servicePlugin, nil
}
