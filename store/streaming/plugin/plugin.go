package plugin

import serverTypes "github.com/cosmos/cosmos-sdk/server/types"

// Plugin is the base interface for all kinds of cosmos-sdk plugins
// It will be included in interfaces of different Plugins
//
// Optionally, Plugins can implement io.Closer if they want to
// have a termination step when unloading.
type Plugin interface {
	// Name should return unique name of the plugin
	Name() string

	// Version returns current version of the plugin
	Version() string

	// Init is called once when the Plugin is being loaded
	// The plugin is passed the AppOptions for configuration
	// Not every plugin will have a functional Init
	Init(env serverTypes.AppOptions) error
}
