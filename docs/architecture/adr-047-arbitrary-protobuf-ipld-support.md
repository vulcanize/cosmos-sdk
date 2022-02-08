# ADR {ADR-NUMBER}: {TITLE}

## Changelog

- Feb 7th, 2022: Initial Draft

## Status

DRAFT

This ADR describes an IPLD content addressing scheme for arbitrary protobuf types stored in SMT.

## Abstract

In this ADR we present an IPLD schema for representing arbitrary Cosmos blockchain state as IPLD, with rich
content-typing of arbitrary protobuf types.


## Context

IPLD stands for InterPlanetary Linked Data. IPLD is a self-describing data model for arbitrary hash-linked data.
IPLD enables a universal namespace for all Merkle data structures and the representation, access, and exploring of these
data structures using the same set of tools. IPLD is the data model for IPFS, Filecoin, and a growing number of other
distributed protocols.

At the core of IPLD are three concepts: Content Identifiers (CIDs), IPLD objects, and IPLD blocks.

### IPLD Objects 
IPLD objects are abstract representations of nodes in some hash-linked data structure. In this representation,
the hash-links that exist in the native, persistent, concrete representation of the underlying data structure
(e.g. the SHA-2-256 hashes of an SMT intermediate node) are transformed into CIDs- hash-links that describe the
content they reference.

### IPLD Blocks
IPLD blocks are simply the binary encoding of an IPLD object, they contain (or are) the binary content that is hashed
and referenced by CID.

### CIDs 
CIDs are self-describing content-addressed identifiers for IPLD blocks, they are composed of a content-hash of an
IPLD block prefixed with bytes that identify the hashing algorithm applied on that block to produce that 
hash (multihash-prefix), a byte prefix that identifies the content type of the IPLD 
block (multicodec-content-type), a prefix that identifies the version of the CID itself (multicodec-version),
and a prefix that identifies the base encoding of the CID (multibase).

`<cidv1> ::= <multibase><multicodec-version><multicodec-content-type><multihash-preifx><content-hash>`

### IPLD Codecs
IPLD blocks are converted to and from in-memory IPLD objects using IPLD Codecs, which marshal/unmarshal an IPLD object
to/from the binary IPLD block encoding. Just as `multihash-content-address` maps to a specific hash function or
algorithm, `multicodec-content-type` maps to a specific Codec for encoding and decoding the content. These mappings are
maintained in a codec registry. These Codecs contain custom logic that resolve native hash-links to CIDs.

For Tendermint-Cosmos blockchains, the IPLD blocks are the consensus binary encodings
of the Merkle/SMT nodes of the various Merkle trees and the values they reference.

Because all values stored in the state of a ComsosSDK chain are protobuf encoded, we are presented with a unique
opportunity to provide extremely rich IPLD content-typing information for the arbitrary types stored in
(referenced from) the SMT in a generic fashion. This feat doesn't come without added complications, though.

## Decision

### Protobuf-Type Multicodec-Content-Type
Define a new `multicodec-content-type` for a
[self-describing protobuf message](https://developers.google.com/protocol-buffers/docs/techniques?authuser=2#self-description):
`protobuf-type`. This `multicodec-content-type` will be used to create CID references to the protobuf encodings of such
self-describing messages.

CIDv1 for a `protobuf-type` IPLD:

`<multibase><multicodec-version><protobuf-type><SHA-2-256-mh-prefix><SHA-2-256(protobuf-encoded-self-describing-message)>`

### Typed-Protobuf Multicodec-Content-Type
Define a new `multicodec-content-type` that specifies that the first x bytes of the hash-linked IPLD block are a
hash-link to a self-describing protobuf message (to a `protobuf-type` block, as described above)
that represents the contents of the .proto file that compiles into the protobuf message that the remaining bytes of the
hash-linked IPLD object can be unmarshalled into: `typed-protobuf`.

In this `multicodec-content-type` specification we also stipulate that the content-hash referencing the IPLD block
only includes as digest input the protobuf encoded bytes for the storage value whereas the `protobuf-type` link prefix is excluded.
This is a significant and unprecedented deviation from previous IPLD Codecs, but is necessary to maintain the native consensus
hashes and hash-to-content mappings.

In other-words, the `content-hash` of a `typed-protobuf` CID is `hash(<protobuf-encoded-value>)` instead of `hash(<protobuf-type-CID><protobuf-encoded-storage-value>)`.

Otherwise `content-hash` would not match the hashes we see natively in the Tendermint+Cosmos Merkle DAG, and could not be directly derived from them.

CIDv1 for a `typed-protobuf` IPLD:

`<multibase><multicodec-version><typed-protobuf><SHA-2-256-mh-prefix><SHA-2-256(protobuf-encoded-storage-value)>`

`typed-protobuf` IPLD:

`<protobuf-type-CID><protobuf-encoded-storage-value>`

Another major deviation this necessitates is the requirement of an IPLD retrieval, unmarshalling, and protobuf
compilation step in order to fully unmarshal the `protobuf-encoded-storage-value` stored in a `typed-protobuf` block.

The algorithm will look like:

1. Fetch the `typed-protobuf` block binary
2. Decode the `protobuf-type` CID from the block's byte prefix
3. Use that CID to fetch the `protobuf-type` block binary
4. Decode that binary into a self-describing protobuf message
5. Use that self-describing protobuf message to create and compile the proto message for the `protobuf-encoded-storage-value`
6. Decode the `protobuf-encoded-storage-value` binary into the proto message type compiled in step 5

### Protobuf-SMT Multicodec-Content-Type
Define a new `multicodec-content-type` for SMT nodes wherein the IPLD object representation of a leaf nodes converts
the canonical `content-hash` (i.e. SHA-2-256(`protobuf-encoded-storage-value`)) into a `typed-protobuf` CID, as
described above. Intermediate node representation is no different from a standard SMT representation.


### Additional work

#### IPLD aware protobuf compiler
For simple protobuf definitions which have no external dependencies on other protobuf definitions, this work will not
be necessary- a single standalone self-describing protobuf message will be enough to generate and compile the protobuf
types necessary for unpacking an arbitrary protobuf value. On the other hand, if we have more complex protobuf
definitions with external dependencies (that we cannot inline) we need some way of resolving these dependencies within
the context of the IPLD Codec performing the object unmarshalling. To this end, we propose to create an IPLD
aware/compatible protobuf compiler that is capable of rendering and resolving dependency trees as IPLD.

Further specification and discussion of this is, perhaps, outside the content of the SDK.

#### IPLD middleware for the SDK
In order to leverage this model for a Cosmos blockchain, features need to be introduced into the SDK (or as auxiliary
services) for

1. Generating self-describing messages from .proto definitions of state storage types in modules
2. Publishing or exposing these as `protobuf-type` IPLD blocks
3. Mapping state storage objects- as they are streamed out using the features in ADR-038 and ADR-048- to their
respective `protobuf-type` 

The above process is very similar, at a high level, to the ORM work that has already been done in the SDK.

These will be discussed further in a following ADR.


## Consequences

There are no direct consequences on the other components of the SDK, as everything discussed here is entirely optional.
In fact, at this stage, everything is theoretical and only exists as an abstract data model with no integration into
the SDK. This model does not impose or require any changes on/to Cosmos blockchain state, it is an abstract representation
of that state which can be materialized in external systems (such as IPFS).

Nonetheless, there are consequences for defining and attempting to standardize a new abstract data model. The approval
of this model should only occur once it has been determined to a satisfactory degree that it is the best available model
for representing Cosmos state as IPLD.

In order to introduce generic support for arbitrary protobuf types in state storage, the models proposed here deviate
from previous IPLD Codecs and content-hash-to-content standards. For better, or worse, this could set new precedents for
IPLD that need to be considered within the context of the greater IPLD ecosystem. We propose that this work be seen as 
extensions to the standard CID and IPLD concepts and believe that these types of deviations would provide improved
flexibility/generalizability of the IPLD model in other contexts as well.

### Backwards Compatibility

No backwards incompatibilities.

### Positive

Define a generic IPLD model for arbitrary Cosmos state. This model will enable universal IPLD integration for any and
all "canonical" Cosmos blockchain (e.g. if they don't use SMT or don't require Protobuf encoding, this falls apart),
improving interoperability with various protocols that leverage IPLD. The concrete implementation of the tools to
(optionally) integrate and leverage this model within the SDK will be proposed and discussed in a later ADR.

### Negative

Code and documentation bloat/bandwidth.

Because of the deviations we make from the current historical precedence for IPLD codec, we suspect upstreaming registration
and support of these Codecs into Protocol Labs repositories will be a complicated process.


### Neutral


## Further Discussions

While an ADR is in the DRAFT or PROPOSED stage, this section should contain a summary of issues to be solved in future iterations (usually referencing comments from a pull-request discussion).
Later, this section can optionally list ideas or improvements the author or reviewers found during the analysis of this ADR.

## Test Cases [optional]

None, except the IPLD Codec tests in the Codec repo linked below.

## References

Existing IPLD Codec implementations for Tendermint and Cosmos data structures: https://github.com/vulcanize/go-codec-dagcosmos
Existing IPLD Spec/Schemas for Tendermint and Cosmos data structures: https://github.com/vulcanize/ipld/tree/cosmos_specs

The above need to be updated to include the model proposed here, once/if it is finalized.
