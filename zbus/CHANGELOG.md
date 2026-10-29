# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 6.2.0 - 2026-10-29

### ✨ Added
- Add Connection::send_all. #2078
- Add WriteHalf::send_messages.

### ♻️ Changed
- Hand several buffers to each send of a runtime's socket.

### 🐛 Fixed
- Limit nesting for each signature, and count dict entries. #2087
- Reject a received message whose path field isn't an object path.
- Accept references to unsized types as interface args again. #2076

### ⚙️ Other
- 🦺 Reject a header with an unended string or a repeated field.
- 🦺 Let a connection check the body of each message it receives.
- 🦺 Add Body::validate, for a bus to check bodies with.
- 🦺 Check a received message as a whole, not only its fields.

### ⚡️ Performance
- Call the read half itself rather than through its Box.
- Read ahead of each message on the sockets that zbus drives. #2085
- Allocate a received message once, at its exact length.
- Leave the maybe type of GVariant to the signature parser.
- Add Message::with_sender, for a bus to set senders with.
- Leave room for None in where a header field lies.
- Parse message headers with winnow rather than serde. #2080
- Read the header fields of a message without checking them again.

### ✅ Testing
- Hold deserializing to the nesting that Body::validate allows.
- Benchmark parsing and checking short signatures.
- Fuzz the parser of message headers against serde.
- Benchmark decoding a received message.

## 6.1.0 - 2026-10-22

### ✨ Added
- Support the eavesdrop key of match rules.

### 💥 Breaking
- Go back to a HashMap for the interfaces of fdo::ManagedObjects, which 6.0.0 turned into a
  BTreeMap, to match the InterfacesAdded signal and PropertiesProxy::get_all. #2064

### 🐛 Fixed
- Escape apostrophes in match rule strings.
- Parse match rule strings as the specification has them.
- Reply InvalidArgs to arguments that don't deserialize.

### ⚡️ Performance
- Don't copy the string of an OwnedMatchRule to deserialize it.

## 6.0.0 - 2026-10-16

The book's [Upgrading to zbus 6.0](https://z-galaxy.github.io/zbus/upgrading-to-6.html) chapter
is the complete list of this release's breaking changes; the Breaking group below holds only some
of them.

### ✨ Added
- Run connections on Tokio on WASI.
- Add spawn_process to traits::Runtime.
- Add connect_tcp and connect_unix to traits::Runtime.
- Add a flush hook to WriteHalf.
- Make block_on part of the public API.
- Do every socket's I/O through the connection's runtime.
- Make a build with neither backend valid.
- Accept any runtime through Builder::runtime.
- Add the traits through which a runtime serves a connection.
- Add FilePath::as_c_str.
- Mark the builders and their setters as must_use.
- Defer connection builder errors to build().
- Defer message builder errors to build().
- Defer proxy builder errors to build().
- Defer match rule builder errors to build().
- Accept more typed values in name and path conversions.
- Add Structure::builder.
- Expose common wire types at the crate root.
- Use Serde and Type for interface properties.
- Use Serde and Type for proxy properties.
- Gate the D-Bus API behind the comms feature.
- Add ConnectionCredentials::into_process_fd.
- Support conversion from/to BTreeMap for Variant and OwnedVariant.

### 💥 Breaking
- Declare AsyncDrop without async-trait.
- Drop the From<hex::FromHexError> impl of Error.
- Take std socket types in Builder::unix_stream and tcp_stream, and drop
  Builder::async_io_unix_stream and async_io_tcp_stream.
- Drop the unused lifetime of StructureSeed.
- Return StartServiceReply from start_service_by_name.
- Drop the VSOCK transport.
- Drop the enumflags2 feature.
- Make MethodFlags a bitflags type.
- Make message::Flags a bitflags type.
- Make RequestNameFlags a bitflags type.
- Take and give queue sizes as NonZeroUsize.
- Hand out listeners of zbus's own types.
- Replace the `async-io` feature and crates with `default-rt`: zruntime's runtime, one per
  thread that calls block_on and driven by that thread.
- Remove the blocking API.
- Stop generating blocking proxies.
- Drop the properties cache from proxies without properties.
- Drop the format from the wire Context.
- Drop the Format dispatch from Signature.
- Unify the error types.

### 👷 CI
- Measure the single-thread benchmarks by CPU simulation.

### ♻️ Changed
- Rename DispatchResult2 to DispatchResult.
- Convert Interest to zruntime's in a private function.
- Poll the runtime-less block_on's future without futures-lite.
- Race a timeout's future and its sleep without futures-lite.
- Take the next item of a stream without futures-lite.
- Rename Value to Variant. #1947
- Register sockets with Tokio on Windows through its own stream.
- Make the unixexec and ibus features non-default.
- Connect to an address without cloning it.
- Remove redundant downcast checks.
- Take the blocking-work thread from zruntime.
- Take the async locks from zruntime.
- Run transport helper processes on the connection's runtime.
- Hand blocking work to the runtime when it is asked for.
- Connect every transport on the connection's runtime.
- Dispatch every runtime arm through the trait.
- Take a connection's tasks and timers from one runtime.
- Rename the `abstractions` module to `runtime`.
- Rename Body::deserialize_unchecked.
- Make `tracing` an optional feature. #331
- Put the service-side ObjectManager behind a feature.
- Put the ibus transport behind a feature.
- Put the unixexec transport behind a feature.
- Keep the signal path non-generic.
- Keep the method call path non-generic.
- Build signature mismatch errors out of line.
- Write introspection doc comments through a helper.
- Convert method handler errors in one place.
- Build replies in one non-generic step.
- Write message bodies through a trait object.
- Use the root wire API in callers and examples.
- Add `service` feature. #1135
- Add `proxy` feature.
- Declare the standard interface proxies separately.
- Convert property values in memory.
- Remove no-op `async-fs` cargo feature. #1287
- Gate integration tests in modules.
- Rename zvariant_utils to zbus_utils.
- Tidy the imports the crate merge left behind.
- Move the zvariant derives into zbus_macros.
- Port the in-tree callers to zbus::wire.
- Move zvariant into zbus as the wire module.
- Move zbus_names into zbus as the names module.
- Reach the bus name types through the re-export.
- Deduplicate the default-interface name checks.

### 📦 Dependencies
- Bump the MSRV to 1.89.
- Update tokio to v1.53.3 (#2028).
- Drop futures-lite from the library's dependencies.
- Drop the Tokio features nothing uses.
- Drop the async-recursion dependency.
- Move to zruntime's broadcast channel.
- Build on zruntime's Event instead of event-listener.
- Drop the async-lock feature and crate.
- Drop proc-macro-crate (#1365).

### 🗑️ Deprecated
- Add deprecated zvariant aliases for the wire module.

### 📝 Documentation
- Document the public items of fdo, proxy and the error types.
- Document the public items of connection and address.
- Document the public items of names, message and match_rule.
- Document the public items of the wire module.
- Say how the set_max_queued methods treat a smaller size.
- Say what block_on runs on with the tokio feature.
- List the object-manager feature in the README.
- Document the support of WASI. #1452
- Point the book at the runtime traits' docs.
- Move the example Socket implementation to the trait's docs.
- Document the runtime trait's connects and helper process.
- Document the fields of `Error::Connection`.
- Document the built-in runtime.
- Point synchronous programs at zbus::block_on.
- Document the runtime and socket changes of 6.0.
- Document the `proxy` and `service` features.
- Add usage example to ObjectManager docs.
- Update the book for zbus 6.0.
- Document the wire and names modules.
- Make two intra-doc links feature-independent.

### 🐛 Fixed
- Fail where WASI has no default bus address or machine ID.
- Make `PropertyStream` yield changes if service starts later.
- Convert `Message` to `Error::FDO` for matching method errors.
- End a connection whose socket reader task is gone.
- Accept a map in as_value::Deserialize.
- Carry any D-Bus error in `zbus::Error`.
- Let property handlers fail with any `DBusError`. #313
- Make `ObjectServer::at` hand back the interface on conflict. #235
- Fix a small build issue for unix targets.
- Reject non-basic dictionary keys. #1637
- Don't leak auto-created nodes when interface addition fails.
- Don't destroy objects still in use on interface removal.
- Prune empty ancestor nodes on interface removal. #1916
- Validate names in Variant and OwnedVariant conversions.
- Check the Optional null sentinel before converting.

### ⚙️ Other
- 🚨 Warn about undocumented public items.
- 📦️ Describe the wire format in the package metadata.
- ✏️ Fix the proxy name in the README example.
- 💣 Ask for no process ID on WASI.
- 🛂 Authenticate as the user `Builder::user_id` names on WASI.
- 🔌 Have no unix-domain connect on WASI.
- 🔌 Do a connection's socket I/O on WASI.
- 🧵 Run blocking work on the calling thread on WASI.
- ✏️ Fix a typo in the AuthMechanism docs.
- 🧑‍💻 Convert lifetime-free newtypes from Values of any lifetime.
- 💬 Reword the `Error::Connection` message.
- 🦺 Replace unsafe downcast API w/ std's.
- 🧵 Use a single-threaded Tokio runtime in block_on by default.
- 🚨 Collapse nested `if`s into let chains.
- 🚨 Use `MAX` rather than the deprecated `max_value()` in examples.
- 🚨 Box the recursive connect calls instead of using async-recursion.
- 💬 Don't name the Tokio runtime flavour in block_on's panic message.
- 🚨 Fix clippy warnings in tests.
- 🏷️ Use String as owned name NoneType.
- 🏷️ Implement Basic for scalar representations.
- 🚨 Fix clippy drain_collect lints.

### ⚡️ Performance
- Pipeline the whole client-side handshake.
- Collect an exited helper process without a blocking worker.
- Serialize the message header in one pass.
- Serialize message bodies in one pass.
- Box the address in Error::Connection.
- Sort interfaces by name in managed objects.
- Don't require an owned value for property setters.

### 🔥 Removed
- Drop the xdg-dbus-proxy workarounds. #781
- Drop pre-6.0 deprecated APIs. #1915
- Remove GVariant support, which lives on in the zgvariant crate.

### 🔒️ Security
- Make FilePath to std conversions sound & lossless. #1906

### ✅ Testing
- Test a flags type defined with bitflags.
- Test `PropertyStream` of a service that starts later.
- Let criterion choose the benchmarks' time and sample count.
- Time only the operation in the per-pair runtime benchmarks.
- Run the concurrent calls benchmark's ends on their own threads.
- Benchmark what a connection costs on its runtime.
- Don't assume little-endian host in dict_value test.
- Prove a one-thread runtime runs a connection with no zbus thread.
- Pass the unixexec test where systemd-stdio-bridge is absent.
- Test that a message rejects a maybe body signature.
- Cover Serde property conversion.
- Test message streams with each runtime.
- Test additive TCP builder signatures.
- Test additive Unix builder signatures.

## 5.19.0 - 2026-08-09

### Added
- ✨ Add Builder::async_io_unix_stream and async_io_tcp_stream.
- ✨ Make the tokio and async-io features additive.

### Changed
- ♻️ Remove unnecessary clones.

### Deprecated
- 🗑️ Deprecate unix_stream/tcp_stream when tokio is disabled.

### Documentation
- 📝 Document the tokio and async-io runtime selection.

### Fixed
- 🥅 throw Error::Connection on connection failure.
- 🥅 Add new error variant Error::Connection.

### Other
- ✏️ Fix a typo in the executor docs.

### Testing
- ✅ Test runtime selection with both backends enabled.
- ✅ add tests for Error::Connection.

## 5.18.0 - 2026-07-17

### Added
- ✨ Add Connection::closed() and is_closed() API.

### Documentation
- 📝 Document ObjectManager-ancestor deadlock in ObjectServer::at.

### Other
- 🚨 Replace manual match-and-return with the ? operator.

### Testing
- ✅ Test registering an interface from a &mut self callback.

## 5.17.0 - 2026-07-07

### Changed
- ♻️ Move pending method call to separate module.

### Other
- 🧐 Add concurrent method-call benchmark.

### Performance
- ⚡️ Route method replies directly by serial.

## 5.16.0 - 2026-05-27

### Changed
- 🔧 Restrict vsock features to Linux targets.

### Documentation
- 📝 Replace docs.rs all-features with explicit feature list.

### Fixed
- 🚑️ Fix sendmsg for iOS and other apple OS.

### Other
- 🔊 warn on GetAll error during property cache init. #1325

## 5.15.0 - 2026-04-26

### Added
- ✨ Introduce DispatchResult2 with fdo::Result for dispatch futures.
- ✨ Add Builder::build_message_stream.

### Changed
- ♻️ Port Interface and dispatch sites to DispatchResult2.

### Deprecated
- 🗑️ Deprecate DispatchResult in favour of DispatchResult2.

### Documentation
- 📝 Configure docs.rs to build for all supported targets.

### Testing
- ✅ Cover D-Bus error name preservation on property setters.
- ✅ Explicitly choose host endianess in a test.

## 5.14.0 - 2026-02-22

### Added
- ✨ Add helper for IBus connection creation. #964
- 🚸 Add Display trait to D-Bus name request reply types.

### Changed
- 🔧 Extend process module run() to all Unix platforms.

### Fixed
- 🐛 Do not use SendFlags::NOSIGNAL on Redox.

### Other
- 📦️ Add async-recursion for Unix targets.
- 🚨 silence unused import on windows.
- 🚨 silence unused warning on windows test.

## 5.13.2 - 2026-01-19

### Fixed
- 🐛 fix regression on windows build. #1686
- 🐛 Correct Peer interface to work on any arbitrary object path.

## 5.13.1 - 2026-01-11

### Fixed
- 🐛 Implement `get_machine_id()` for *BSD platforms.

## 5.13.0 - 2026-01-09

### Added
- ✨ Add crate attribute for custom crate paths.

### Changed
- 🎨 Format all files (rust 1.85).

### Fixed
- 🚑️ Send on unix sockets w/ `MSG_NOSIGNAL` flag enabled. #1657
- 🐛 Fix `get_machine_id` for macOS.

### Other
- 🧱 Fix all clippy warnings (rust 1.85).
- 🧑‍💻 Bump rust version to 1.85.
- 🔊 lower trace/instrument verbosity.

### Testing
- ✅ Add introspection test for out_args with single output.
- ✅ Remove unused imports from tests.
