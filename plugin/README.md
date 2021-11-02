# Comsos-SDK plugins
This package contains an extensible plugin system for the Cosmos-SDK. Included in this top-level package is the base interface
for a Cosmos-SDK plugin, as well as more specific plugin interface definitions that build on top of this base interface.
The [loader](./loader) sub-directory contains the Go package and scripts for loading plugins into the SDK. The [plugins](./plugins)
sub-directory contains the preloaded plugins and a script for building them, this is also the directory that the plugin loader will look
for non-preloaded plugins by default.

The base plugin interface is defined as:
```go
// Plugin is the base interface for all kinds of cosmos-sdk plugins
// It will be included in interfaces of different Plugins
type Plugin interface {
    // Name should return unique name of the plugin
    Name() string
    
    // Version returns current version of the plugin
    Version() string
    
    // Init is called once when the Plugin is being loaded
    // The plugin is passed the AppOptions for configuration
    // A plugin will not necessarily have a functional Init
    Init(env serverTypes.AppOptions) error
    
    // Closer interface to shutting down the plugin process
    io.Closer
}
```

Specific plugin types extend this interface, enabling them to work with the loader tooling defined in the [loader sub-directory](./loader).

The plugin system itself is configured using the `plugins` TOML mapping in the App's app.toml file. There are three
parameters for configuring the plugins: `plugins.on`, `plugins.disabled` and `plugins.dir`. `plugins.on` is a bool that turns on or off the plugin
system at large, `plugins.dir` directs the system to a directory to load plugins from, and `plugins.disabled` is a list
of names for the plugins we want to disable (useful for disabling preloaded plugins).

```toml
[plugins]
    on = false # turn the plugin system, as a whole, on or off
    disabled = ["list", "of", "plugin", "names", "to", "disable"]
    dir = "the directory to load non-preloaded plugins from; defaults to "
    [plugins.streaming] # a mapping of plugin-specific streaming service parameters, mapped to their pluginFileName
        [plugins.streaming.file] # the specific parameters for the file streaming service plugin
            keys = ["list", "of", "store", "keys", "we", "want", "to", "expose", "for", "this", "streaming", "service"]
            writeDir = "path to the write directory"
            prefix = "optional prefix to prepend to the generated file names"
```

As mentioned above, some plugins can be preloaded. This means they do not need to be loaded from the specified `plugins.dir` and instead
are loaded by default. At this time the only preloaded plugin is the [file streaming service plugin](./plugins/file).
Plugins can be added to the preloaded set by adding the plugin to the [plugins dir](../../plugin/plugin.go) and modifying the [preload_list](../../plugin/loader/preload_list).

In your application, if the  `plugins.on` is set to `true` use this to direct the invocation of `NewPluginLoader` and walk through
the steps of plugin loading, initialization, injection, starting, and closure.
