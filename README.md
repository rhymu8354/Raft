# Raft

This is a library which implements certain aspects of the [Raft Consensus
Algorithm](https://raft.github.io/), which is used to get a cluster of servers
to agree on server state, as that state changes over time.

[![Crates.io](https://img.shields.io/crates/v/raft.svg)](https://crates.io/crates/raft)
[![Documentation](https://docs.rs/raft/badge.svg)][dox]

More information about the Rust implementation of this library can be found in
the [crate documentation][dox].

[dox]: https://docs.rs/raft

The `Raft::Server` type represents one server in the cluster.  It holds both
the persistent and volatile server state that is specific to the algorithm.
The actual "log", as described by the algorithm, is handled as an abstraction,
so that the algorithm can be applied to many different server implementations.

This is a multi-language library containing independent implementations
for the following programming languages:

* C++
* Rust

## Building the C++ Implementation

A portable library is built which depends on the C++11 compiler, the C++
standard library, and non-standard dependencies listed below.  It should be
supported on almost any platform.  The following are recommended toolchains for
popular platforms.

* Windows -- [Visual Studio](https://www.visualstudio.com/) (Microsoft Visual
  C++)
* Linux -- clang or gcc
* MacOS -- Xcode (clang)

## Building

This library is not intended to stand alone.  It is intended to be included in
a larger solution which uses [CMake](https://cmake.org/) to generate the build
system and build applications which will link with the library.

There are two distinct steps in the build process:

1. Generation of the build system, using CMake
2. Compiling, linking, etc., using CMake-compatible toolchain

### Prerequisites

* [AsyncData](https://github.com/rhymu8354/AsyncData.git) - a library
  implementing lock-free data structures useful for concurrent processing
* [CMake](https://cmake.org/) version 3.8 or newer
* C++11 toolchain compatible with CMake for your development platform (e.g.
  [Visual Studio](https://www.visualstudio.com/) on Windows)
* [Json](https://github.com/rhymu8354/Json.git) - a library which implements
  [RFC 7159](https://tools.ietf.org/html/rfc7159), "The JavaScript Object
  Notation (JSON) Data Interchange Format".
* [SystemAbstractions](https://github.com/rhymu8354/SystemAbstractions.git) - a
  cross-platform adapter library for system services whose APIs vary from one
  operating system to another
* [Serialization](https://github.com/rhymu8354/Serialization.git) - a library
  used to convert data to/from strings of bytes
* [Timekeeping](https://github.com/rhymu8354/Timekeeping.git) - a library
  of classes and interfaces dealing with tracking time and scheduling work

### Build system generation

Generate the build system using [CMake](https://cmake.org/) from the solution
root.  For example:

```bash
mkdir build
cd build
cmake -G "Visual Studio 15 2017" -A "x64" ..
```

### Compiling, linking, et cetera

Either use [CMake](https://cmake.org/) or your toolchain's IDE to build.
For [CMake](https://cmake.org/):

```bash
cd build
cmake --build . --config Release
```
