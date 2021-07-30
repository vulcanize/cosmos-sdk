package db

import (
	"bytes"
	"errors"
	"os"
	"sort"
)

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Returns a slice of the same length (big endian)
// except incremented by one.
// Returns nil on overflow (e.g. if bz bytes are all 0xFF)
// CONTRACT: len(bz) > 0
func cpIncr(bz []byte) (ret []byte) {
	if len(bz) == 0 {
		panic("cpIncr expects non-zero bz length")
	}
	ret = cp(bz)
	for i := len(bz) - 1; i >= 0; i-- {
		if ret[i] < byte(0xFF) {
			ret[i]++
			return
		}
		ret[i] = byte(0x00)
		if i == 0 {
			// Overflow
			return nil
		}
	}
	return nil
}

// IsKeyInDomain returns true iff a key lies within a domain.
// See DB interface documentation for more information.
func IsKeyInDomain(key, start, end []byte) bool {
	if bytes.Compare(key, start) < 0 {
		return false
	}
	if end != nil && bytes.Compare(end, key) <= 0 {
		return false
	}
	return true
}

// FileExists returns true iff a file path exists.
func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// VersionManager encapsulates the current valid versions of a DB and computes
// the next version.
type VersionManager struct {
	versions      map[uint64]struct{}
	initial, last uint64
}

var _ VersionSet = (*VersionManager)(nil)

// NewVersionManager creates a VersionManager from a sorted slice of ascending version ids.
func NewVersionManager(versions []uint64) *VersionManager {
	vmap := make(map[uint64]struct{})
	var init, last uint64
	for _, ver := range versions {
		vmap[ver] = struct{}{}
	}
	if len(versions) > 0 {
		init = versions[0]
		last = versions[len(versions)-1]
	}
	return &VersionManager{versions: vmap, initial: init, last: last}
}

// Exists implements VersionSet.
func (vm *VersionManager) Exists(version uint64) bool {
	_, has := vm.versions[version]
	return has
}

// Initial implements VersionSet.
func (vm *VersionManager) Initial() uint64 {
	return vm.initial
}

// Last implements VersionSet.
func (vm *VersionManager) Last() uint64 {
	return vm.last
}

// Next implements VersionSet.
func (vm *VersionManager) Next() uint64 {
	return vm.Last() + 1
}

// Save implements VersionSet.
func (vm *VersionManager) Save(target uint64) (uint64, error) {
	next := vm.Next()
	if target == 0 {
		target = next
	}
	if target < next {
		return 0, errors.New("target version cannot be less than next sequential version")
	}
	vm.versions[target] = struct{}{}
	vm.last = target
	if len(vm.versions) == 1 {
		vm.initial = target
	}
	return target, nil
}

// All implements VersionSet.
func (vm *VersionManager) All() []uint64 {
	var ret []uint64
	for ver, _ := range vm.versions {
		ret = append(ret, ver)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret
}

// Count implements VersionSet.
func (vm *VersionManager) Count() int { return len(vm.versions) }

// Equal implements VersionSet.
func (vm *VersionManager) Equal(that VersionSet) bool {
	mine := vm.All()
	theirs := that.All()
	if len(mine) != len(theirs) {
		return false
	}
	for i, v := range mine {
		if v != theirs[i] {
			return false
		}
	}
	return true
}

func (vm *VersionManager) Copy() *VersionManager {
	vmap := make(map[uint64]struct{})
	for ver, _ := range vm.versions {
		vmap[ver] = struct{}{}
	}
	return &VersionManager{versions: vmap, initial: vm.initial, last: vm.last}
}
