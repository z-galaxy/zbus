# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 6.0.1 - 2026-11-13

### ⚙️ Other
- 🚨 Convert argument names to snake case. #167
- 🚨 Allow the fixture lints in the compile test. #1943

## 6.0.0 - 2026-10-16

The book's [Upgrading to zbus 6.0](https://z-galaxy.github.io/zbus/upgrading-to-6.html) chapter
is the complete list of this release's breaking changes; the Breaking group below holds only some
of them.

### ✨ Added
- Maintain definition order.
- Pass '-' to 'file' to read from stdin.
- Defer proxy builder errors to build().
- Use Serde and Type for proxy properties.

### 💥 Breaking
- Return Iterators to access Interface elements.
- Stop generating blocking proxies.
- Borrow strings from the introspection document. #9

### ♻️ Changed
- Emit the zbus attribute spelling in generated code.
- Rename Value to Variant. #1947
- Enable zbus's unixexec and ibus features for the CLI.
- Generate bindings with root wire types.
- Port the in-tree callers to zbus::wire.
- Move zvariant into zbus as the wire module.

### 📦 Dependencies
- Bump the MSRV to 1.89.

### 🐛 Fixed
- Avoid generating duplicate `receive_*_changed` methods. #231

### 🔥 Removed
- Drop the serde leftovers of the quick-xml parser.
- Drop pre-6.0 deprecated APIs. #1915

## 5.4.1 - 2026-07-17

### Other
- 🚨 Fix new clippy 1.97 warnings.

## 5.4.0 - 2026-07-07

### Added
- ✨ Generate Rust types from Telepathy type definitions. #255
- ✨ Warn about ignored unsupported XML elements. #256
- ✨ Turn Telepathy docstrings into doc comments. #255

### Changed
- ♻️ Add CodeGenerator, deprecate GenTrait & write_interfaces.

### Testing
- ✅ Add regression test for bare struct arg signatures.

## 5.3.1 - 2026-04-26

### Documentation
- 📝 Configure docs.rs to build for all supported targets.

### Fixed
- 🐛 Emit owned types for Variant/Structure property setters. #1770

## 5.3.0 - 2026-02-22

### Added
- 🚸 make error compatible with anyhow.

### Fixed
- 🐛 Distinguish struct return from multiple returns. #1241

### Other
- 🤖 Fix formatting of CHANGELOG files.
- 🤖 Use the default header in changelog.

## 5.2.0 - 2026-01-09

### Changed
- 🔧 use edition from workspace.
- 🎨 Format all files (rust 1.85).
- 🚚 Update name of Github space from dbus2 to z-galaxy.
- 🎨 Satisfy latest clippy.

### Fixed
- 🩹 Don't use workspace for local deps.

### Other
- 🧑‍💻 Bump rust version to 1.85.
- 🚨 Fix against latest clippy.
- 🧑‍💻 Use workspace dependencies.

### Performance
- ⚡️ Remove a needless iteration.

### Removed
- ➖ Allow the library part not depend on clap.
