package types

// CurrentFormat is the currently used format for snapshots.
// Essentially, it versions the format of the snapshot data.
// Snapshots using the same format must be identical across all
// nodes for a given height, so this must be bumped when the binary
// snapshot output changes.
// 1: the original format, consisting of IAVL tree nodes.
// 2: used by store/v2alpha1; consists of store key/value pairs.
// 3: like 1, but serializes the app version in addition to the original snapshot data.
const CurrentFormat uint32 = 2
