package plugin

import (
	"io"

	serverTypes "github.com/cosmos/cosmos-sdk/server/types"
)

const (
	// PLUGINS_SYMBOL is the symbol for loading Cosmos-SDK plugins from a linked .so file
	PLUGINS_SYMBOL = "Plugins"

	// PLUGIN_TOML_KEY is the top-level TOML key for plugin configuration
	PLUGIN_TOML_KEY = "plugins"

	// PLUGIN_ON_TOML_KEY is the second-level TOML key for turning on the plugin system as a whole
	PLUGIN_ON_TOML_KEY = "on"

	// PLUGIN_DIR_TOML_KEY is the second-level TOML key for the directory to load plugins from
	PLUGIN_DIR_TOML_KEY = "dir"

	// PLUGIN_DISABLED_TOML_KEY is the second-level TOML key for a list of plugins to disable
	PLUGIN_DISABLED_TOML_KEY = "disabled"

	// DEFAULT_PLUGIN_DIRECTORY is the default directory to load plugins from
	DEFAULT_PLUGIN_DIRECTORY = "src/github.com/cosmos/cosmos-sdk/plugin/plugins"
)

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

	// Closer interface for shutting down the plugin process
	io.Closer
}
