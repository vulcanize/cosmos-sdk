# ADR {ADR-NUMBER}: {TITLE}

## Changelog

- Feb 7th, 2022: Initial Draft

## Status

DRAFT

This ADR describes an IPLD content addressing scheme for arbitrary protobuf types stored in SMT.

## Abstract

In this ADR we present an IPLD schema for representing arbitrary Cosmos blockchain state as IPLD, with rich content-typing
of arbitrary protobuf types.


## Context

IPLD stands for InterPlanetary Linked Data. IPLD is a self-describing data model for arbitrary hash-linked. IPLD enables a universal namespace
for all Merkle data structures and the representation, access, and exploring of these data structures using the same set of tools.
IPLD is the data model for IPFS, Filecoin, and a growing number of other distributed protocols.

At the core of IPLD are two concepts: Content Identifiers (CIDs) and IPLD blocks. IPLB blocks are the binary encoding of
an IPLD object. CIDs are self-describing content-addressed identifiers for IPLD blocks, they are composed of a content
hash of an IPLD block prefixed with bytes that identify the hashing algorithm applied on that block to produce that hash
(multihash-content-address), a byte prefix that identifies the content type of the IPLD block (multicodec-content-type),
a prefix that identifies the version of the CID itself (multicodec-version), and a prefix that identifies the base
encoding of the CID (multibase).

`<cidv1> ::= <multibase><multicodec-version><multicodec-content-type><multihash-content-address>`

IPLD blocks are converted to and from in-memory IPLD objects using IPLD Codecs, which marshal/unmarshal an IPLD object
to/from the binary IPLD block encoding. Just as `multihash-content-address` maps to a specific hash function or
algorithm, `multicodec-content-type` maps to a specific Codec for encoding and decoding the content. These mappings are
maintained in a codec registry. These Codecs contain custom logic that resolve hash-links in the IPLD objects to CID
references.

For Merkle data structures such as a Tendermint-Cosmos blockchain, the IPLD blocks are the consensus binary encodings
of the Merkle/SMT nodes of the various Merkle trees and the values they reference.

Because all values stored in the state of a ComsosSDK chain are protobuf encoded, we are presented with a unique
opportunity to provide extremely rich IPLD content-typing information for the values of arbitrary type stored in
(referenced from) the SMT in a generic fashion. This feat doesn't come without added complications, though.

## Decision

> This section describes our response to these forces. It is stated in full sentences, with active voice. "We will ..."
> {decision body}

## Consequences

There are no direct consequences on the other components of the SDK, as everything discussed here is entirely optional.
In fact, at this stage, everything is theoretical and only exists as an abstract data model with no integration into
the SDK. This model does not impose or require any changes on/to Cosmos blockchain state, it is an abstract representation
of that state which can be materialized in external systems (such as IPFS).

Nonetheless, there are consequences for defining and attempting to standardize a new abstract data model. The approval
of this model should only occur once it has been determined to a satisfactory degree that it is the best available model
for representing Cosmos state as IPLD.

### Backwards Compatibility

No backwards incompatibilities.

### Positive

Define a generic IPLD model for arbitrary Cosmos state. This model will enable universal IPLD integration for any and
all "canonical" Cosmos blockchain (e.g. if they don't use SMT or don't require Protobuf encoding, this falls apart),
improving interoperability with various protocols that leverage IPLD. The concrete implementation of the tools to
(optionally) integrate and leverage this model within the SDK will be proposed and discussed in a later ADR.

### Negative

Code and documentation bloat/bandwidth.

### Neutral


## Further Discussions

While an ADR is in the DRAFT or PROPOSED stage, this section should contain a summary of issues to be solved in future iterations (usually referencing comments from a pull-request discussion).
Later, this section can optionally list ideas or improvements the author or reviewers found during the analysis of this ADR.

## Test Cases [optional]

None, except the IPLD Codec tests in the Codec repo linked below.

## References

Existing IPLD Codec implementations for Tendermint and Cosmos data structures:
Existing IPLD Spec for Tendermint and Cosmos data sturctures:

The above need to be updated to include the model proposed here.
