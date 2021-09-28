# State Streaming Service
This package contains the interface for the `streaming.Service` used to write state changes out from individual KVStores to a
file or stream, as described in [ADR-038](../docs/architecture/adr-038-state-listening.md) and defined in [types/streaming.go](../types/streaming.go).
The child directories contain the implementations for specific output destinations.

Specific `streaming.Service` implementations are written and loaded as plugins.
The plugin interface and loader functions are defined in the `plugin` subdirectory.
The `StreamingServicePlugin` has a single method that is used to initialize and return an arbitrary `streaming.Service` implementation,
it takes the plugin file name,

```go
// StreamingServicePlugin interface for loading a streaming.Service from a plugin
type StreamingServicePlugin interface {
	// LoadStreamingService initializes and returns the streaming.Service from the plugged-in module
	LoadStreamingService(pluginFileName string, opts serverTypes.AppOptions, marshaller codec.BinaryCodec, keys map[string]*types.KVStoreKey) (streaming.Service, error)
}
```

A `streaming.Service` is configured from within an App using the `AppOptions` loaded from the app.toml file:

```toml
[store]
    [store.streaming]
        pluginFileNames = [ # if we have any plugins, we are streaming
            "file", # name of the .so file we are loading as a plugin; don't include the .so file extension
            "anotherPluginName",
            "yetAnotherPluginName",
        ]

[streaming] # a list of plugin-specific streaming service parameters, mapped to their pluginFileName
    [streaming.file]
        # pluginDir is required for every plugin and will default to a directory located at store/streaming/plugin/plugins with the same name as the plugin
        pluginDir = "path to the directory with the plugins"
        keys = ["list", "of", "store", "keys", "we", "want", "to", "expose", "for", "this", "streaming", "service"]
        writeDir = "path to the write directory"
        prefix = "optional prefix to prepend to the generated file names"
    [streaming.anotherPluginName]
        # params for anotherPluginName
    [streaming.yetAnotherPluginName]
        # params for yetAnotherPluginName
```

`store.streaming.pluginFileNames` contains a list of the .so file names of the `streaming.Service` plugins to load onto the App.
These names are used to load the plugin-specific parameters for each plugin, from the `streaming.{pluginFileName}` mappings.
These parameters will depend on the specific `streaming.Service` implementation, but every plugin is expected to have a `pluginDir` parameter
and very likely will possess a `keys` parameter.


The returned `StreamingService` is then loaded into the BaseApp using the BaseApp's `SetStreamingService` method.
The `Stream` method is called on the service to begin the streaming process. Depending on the implementation this process
may be synchronous or asynchronous with the message processing of the state machine. For the file streaming service the process is synchronous.

```go
bApp.SetStreamingService(streamingService)
wg := new(sync.WaitGroup)
quitChan := make(chan struct{})
streamingService.Stream(wg, quitChan)
```
