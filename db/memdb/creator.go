// No build directive, memdb is always built
package memdb

import (
	"github.com/cosmos/cosmos-sdk/db/types"
)

func init() {
	creator := func(name string, dir string) (types.DBConnection, error) {
		return NewDB(), nil
	}
	types.RegisterCreator(types.MemDBBackend, creator, false)
}
