package db

import (
	"bytes"
	"errors"
	"os"
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
	versions []uint64
}

func NewVersionManager(versions []uint64) *VersionManager {
	return &VersionManager{versions: versions}
}

func (vm *VersionManager) Exists(version uint64) bool {
	// todo: maybe use map to avoid linear search
	for _, ver := range vm.versions {
		if ver == version {
			return true
		}
	}
	return false
}

func (vm *VersionManager) Initial() uint64 {
	if len(vm.versions) == 0 {
		return 1
	}
	return vm.versions[0]
}

func (vm *VersionManager) Last() uint64 {
	if len(vm.versions) == 0 {
		return 0
	} else {
		return vm.versions[len(vm.versions)-1]
	}
}

func (vm *VersionManager) Next() uint64 {
	return vm.Last() + 1
}

func (vm *VersionManager) Save(target uint64) (uint64, error) {
	next := vm.Next()
	if target == 0 {
		target = next
	}
	if target < next {
		return 0, errors.New("target version cannot be less than next sequential version")
	}
	vm.versions = append(vm.versions, target)
	return target, nil
}

var _ VersionSet = (*VersionManager)(nil)

func (vm *VersionManager) All() []uint64 { return vm.versions }
func (vm *VersionManager) Count() int    { return len(vm.versions) }

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
	vs := make([]uint64, len(vm.versions))
	copy(vs, vm.versions)
	return NewVersionManager(vs)
}
