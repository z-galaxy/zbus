# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

For contribution conventions — commit-message format, atomic commits, code layout, and
more — follow the guidelines in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Project Overview

zbus is a pure Rust implementation of D-Bus communication providing a safe, high-level API without C library dependencies. It's organized as a Cargo workspace with multiple interconnected crates for different aspects of D-Bus functionality.

## Common Development Commands

### Building and Testing
```bash
# Full test suite (requires D-Bus session bus)
cargo test --all-features

# Test individual crates
cargo test -p zbus
cargo test -p zbus_macros
cargo test -p zbus_xml

# Test the wire-format-only build (the D-Bus API, i.e. the `comms` feature, off)
cargo test -p zbus --no-default-features

# Test with specific features
cargo test --no-default-features --features tokio
cargo test --features uuid,url,time,chrono,option-as-array,vsock,bus-impl

# Run single test
cargo test basic_connection
cargo test --test e2e specific_test_name
```

### Code Quality
```bash
# Format code (requires nightly)
cargo +nightly fmt --all

# Lint with clippy
cargo clippy -- -D warnings

# Check cross-platform compatibility
cargo check --target x86_64-pc-windows-gnu
cargo check --target x86_64-apple-darwin
cargo check --target x86_64-unknown-freebsd
```

### Documentation
```bash
# Build docs for individual crates
cargo doc --all-features -p zbus
cargo doc --no-default-features -p zbus
cargo doc --all-features -p zbus_xml

# Build the mdbook (in book/ directory)
cd book && mdbook build
```

### Benchmarks and Fuzzing
```bash
# Run benchmarks
cargo bench

# Fuzz testing (requires nightly and cargo-fuzz)
cargo install cargo-fuzz
cargo fuzz run --fuzz-dir zbus/fuzz dbus
```

## Workspace Architecture

### Core Crates
- **zbus**: Everything a user sees. The D-Bus API (connection, proxy, object server) sits behind
  the `comms` feature; the `wire` module (D-Bus wire format) and the `names` module (D-Bus name
  types) are always compiled
- **zbus_macros**: Procedural macros: `#[interface]`, `#[proxy]`, `#[derive(DBusError)]` and the
  wire derives (`Type`, `Value`, `OwnedValue`, `SerializeDict`, `DeserializeDict`, `signature!`)
- **zbus_utils**: Signature parser, D-Bus name validators and the derive codegen shared by zbus
  and zbus_macros; the sibling zgvariant project shares it from its 2.0 release
- **zbus_xml**: D-Bus introspection XML handling
- **zbus_xmlgen**: Code generation from D-Bus interface XML

GVariant is not supported; it lives in the separate zgvariant crate.

### Key Design Patterns

**Async-first**:
- The API is async; a program with no runtime of its own drives it with `zbus::block_on`
- Runtime agnostic but with special tokio integration

**Type Safety**:
- D-Bus types mapped to Rust types via derive macros
- Compile-time interface validation with `#[interface]` and `#[proxy]`
- Bus name types prevent runtime errors

**Connection Management**:
- Session, system, and P2P connections via `Connection::builder()`
- Automatic authentication and capability negotiation
- Transport abstraction (Unix sockets, TCP, VS_SOCK)

## Architecture Overview

```
zbus/src/
├── wire/               # D-Bus wire format (de)serialization; always compiled
├── names/              # D-Bus bus name types; always compiled
├── zvariant.rs         # Deprecated aliases for `wire` (removed in 7.0)
├── error.rs            # The single `Error`/`Result` for all of the above
├── connection/         # Core connection handling & handshake
├── proxy/              # Client-side proxy objects with #[proxy] macro
├── object_server/      # Service-side interface implementation
├── message/            # D-Bus message serialization/parsing
├── address/            # Transport layer abstraction
├── fdo/                # Standard D-Bus interfaces (Peer, Properties, etc.)
└── blocking/           # Sync wrappers around async API
```

Everything below `error.rs` in that list is behind the `comms` feature.

**Message Flow**: Connection ↔ Message ↔ `zbus::wire` serialization ↔ Transport

**Service Pattern**: Use `#[interface]` macro on trait impl, register with `ObjectServer`

**Client Pattern**: Use `#[proxy]` macro on trait, create proxy from `Connection`

## Development Guidelines

- **MSRV**: 1.87.0
- **Commit style**: Emoji prefix + package abbreviation (e.g., "🐛 zb: Fix connection timeout")
- **Changelog**: `CHANGELOG.md` files are managed by [release-plz] — do **not** hand-edit
  them. Write a good commit message (conventional-commits-ish) and release-plz will
  generate the entry at release time.
- **Changelog-skip trailer**: end a commit message with a `Changelog: skip` git trailer to
  keep it out of the user-facing changelog (use for AI-workflow artifacts such as design docs
  and implementation plans).
- **Testing**: Integration tests require D-Bus session bus
- **Cross-platform**: Validate changes work on Linux, Windows, macOS
- **Dependencies**: Check compatibility with async runtimes and optional features

[release-plz]: https://release-plz.ieni.dev/

## Key Files for Understanding

- `zbus/src/connection/mod.rs`: Core connection abstraction
- `zbus/src/proxy/mod.rs`: Client proxy generation
- `zbus/src/object_server/mod.rs`: Service object management
- `zbus/src/wire/mod.rs`: Serialization system entry point
- `zbus/src/error.rs`: The unified error type
- `zbus_macros/src/iface.rs`: `#[interface]` macro implementation
- `zbus_macros/src/proxy.rs`: `#[proxy]` macro implementation