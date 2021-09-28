// +build noplugin

package loader

import (
	"errors"

	cplugin "github.com/cosmos/cosmos-sdk/store/streaming/plugin"
)

func init() {
	loadPluginFunc = nopluginLoadPlugin
}

func nopluginLoadPlugin(string) ([]cplugin.Plugin, error) {
	return nil, errors.New("not built with plugin support")
}
