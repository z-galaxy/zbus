# Built-in Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the async-io backend of `zbus::runtime` with readiness, timers, tasks and locks
maintained inside zbus, so that a default build of zbus depends on none of async-io,
async-executor, async-task, async-lock, blocking or polling, with benchmarks and binary-size
fixtures in place first so the replacement is measured against what it replaces.

**Architecture:** The public `zbus::runtime::traits` contract from #1960 stays untouched. A new
crate-private backend `runtime::builtin` implements `traits::Runtime` with three pieces: a safe
task scheduler (`Arc<TaskCell>` cells on a ready queue, no `unsafe`, no atomics), a reactor that
watches sockets with `poll(2)` on unix and `select()` on Windows and keeps timers in a
`BTreeMap` of deadlines, and one worker thread per runtime that runs ready tasks and waits on
the reactor, started on first use and exiting when nothing is left to run, watch or time.
Blocking work goes through the trait's default thread-per-call hook. Locks become
`runtime::locks::{Mutex, RwLock}` built on `event-listener`, re-exporting `tokio::sync` instead
when the `tokio` feature is on.

**Tech Stack:** Rust 1.87 (MSRV), `rustix` (`event::poll`, `pipe`), `windows-sys` (`select`),
`event-listener`, `socket2`, criterion via `codspeed-criterion-compat` for benchmarks.

**Spec:** [RFC #1959](https://github.com/z-galaxy/zbus/issues/1959) (the proposal, updated on
2026-09-17 for what #1960 delivered) and
`docs/superpowers/specs/2026-09-12-external-runtime-design.md` (the runtime contract this builds
on). Both are read-only inputs; this plan records the decisions the maintainer made on
2026-09-17 in "Rulings" below.

## Global Constraints

- MSRV 1.87.0; `cargo +nightly fmt --all` clean (nightly fmt options are in use; never ignore its
  warnings); `cargo clippy -- -D warnings` clean on every commit for every feature set listed in
  "Verification per commit" below.
- No `unsafe` outside two places: the FFI call to `select` on Windows and the lock guards'
  `UnsafeCell` access. Every `unsafe` block carries a `// SAFETY:` comment stating the invariant
  it relies on. No `#[allow(...)]` anywhere; fix the cause. No atomic types: a flag or a counter
  lives under the mutex that already guards the state it describes.
- 100 columns in every text file, code and comments alike. Sentences in comments end with `.`.
- Comments and doc comments explain the code to a reader who has never seen this plan or the
  PR: never "now", "no longer", "previously", "yet", "still", "so far", "kept so that", and
  never a reference to an issue, a review or a commit.
- Tests: no `test_` prefix; feature gates on the test module (`#[cfg(...)]` in the file), never
  new `[[test]]` entries in `Cargo.toml`. Benchmarks are the one exception: criterion needs a
  `[[bench]]` entry with `harness = false`, as the existing ones have.
- Imports: on a name clash import the module (`use module::{self, item}`), never alias.
  Directory modules are `module/mod.rs`. `where` clauses for trait bounds. `value.clone()`, not
  `Arc::clone(&value)`. Usage before definition, `pub` before `pub(crate)` before private.
  Poisoned std mutexes are taken with `unwrap_or_else(PoisonError::into_inner)` through one
  `lock` helper per module, as `zbus/tests/polling_runtime/runtime/mod.rs` does.
- Dependencies are added with `cargo add` and removed with `cargo remove`, never by typing a
  version. `Cargo.lock` is tracked and CI runs with `--locked`: every commit that touches a
  manifest (a member added, a dependency or feature changed) includes the `Cargo.lock` it
  produces.
- Commits: one logical change each, every hunk covered by the message, no drive-by tidying of
  lines a commit merely passes through. Subject: a gimoji emoji copied verbatim (with its U+FE0F
  where the set has one) + ` zb: ` (or `zb,book: ` etc.) + imperative title; commitlint counts
  UTF-16 units, so the header is at most 72 of those (an astral-plane emoji such as 🔥 or 📝
  counts 2, `♻️` counts 2) and body lines at most 74 characters (the lint warns at 75). Body says
  why. Trailers, exactly these: `Assisted-by: Claude Fable 5.1 (claude-fable-5-1)` and
  `Claude-Session: https://claude.ai/code/session_01S2Kxkut4b2ny7gCtkctZc7`; never
  `Co-Authored-By`, never `Signed-off-by`. Commit with
  `/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign`. Emoji by kind:
  `✅` tests and benchmarks, `✨` a feature, `♻️` a refactor, `💥` a breaking change, `➖` a
  dependency removed, `🔥` code or files removed, `👷` CI, `📝` docs.
- Tooling in this environment: `git`, `grep` and `cargo` on `PATH` are shims with altered
  output; use `/usr/bin/git`, `/usr/bin/grep`, `~/.cargo/bin/cargo`. Only one cargo at a time:
  wrap every cargo invocation in `flock /tmp/claude-1000/cargo.lock` with
  `CARGO_TARGET_DIR=/home/zeenix/checkout/z-galaxy/zbus/target`. The untracked directory `5.x/`
  at the repository root is an old checkout: every repository-wide grep excludes it
  (`| /usr/bin/grep -v "^./5.x\|^./target\|^./docs/superpowers"`).
- Branch off `origin/main` after `/usr/bin/git fetch origin`; worktrees live inside the repo
  directory (`./wt-<name>`) and are listed in `.git/info/exclude`. Push to the `zeenix` remote
  only.
- Integration tests need a session bus: run them under
  `dbus-run-session --config-file /tmp/dbus-session.conf --` where the config is produced by
  `sed -e s/UID/$UID/ -e s/PATH/path/ CI/dbus-session.conf > /tmp/dbus-session.conf`.
- The Windows target is checked with `cargo check --target x86_64-pc-windows-gnu -p zbus
  --all-targets --features p2p` (and the macOS one with `x86_64-apple-darwin`); no Windows
  machine runs the tests here, so Windows-only code gets its logic tested through the unix path
  where shared and reviewed by hand where not. `--all-features` never goes to a non-Linux
  target: the `vsock` feature has a `compile_error!` off Linux.

## Rulings

Decisions the maintainer made on 2026-09-17, plus rulings this plan makes where the RFC left a
gap. Each is changeable in the one place it names.

1. **A new default feature selects the built-in runtime.** Its name is `builtin-runtime`; the
   name appears in `zbus/Cargo.toml`, in every `cfg(feature = "builtin-runtime")`, in
   `.github/workflows/rust.yml` and in the docs. Rename with a single `sed` before Task 9 if a
   better name comes up. The `async-io` feature goes with the crate: no alias, no deprecated
   stand-in (6.0 is a breaking release and the feature was rarely enabled by name).
2. **Readiness is `poll(2)` on unix and `select()` on Windows.** No epoll, kqueue or IOCP: a
   connection watches one socket and at most two pipes, so O(n) per wait costs nothing.
   `poll(2)` is POSIX and native on Linux, the BSDs, macOS, illumos and Android; macOS's one
   documented gap (its man page: "The poll() system call currently does not support devices")
   is character devices, which zbus never watches. rustix rounds a sub-millisecond timeout up
   to a millisecond where the platform `poll` takes milliseconds, and the Windows `TIMEVAL` is
   rounded up the same way, so a deadline close ahead never turns a wait into a spin. Not
   `WSAPoll`: before Windows 10 version 2004 it reports nothing for a TCP connect that fails
   (Microsoft's own note on the call), and Windows Server 2019 and LTSC 2019 are supported
   until 2029; `select` reports a failed connect in `exceptfds` on every Windows, which is why
   `runtime/io/connect.rs` already asks it. On Windows zbus watches sockets only (helper
   processes and their pipes are unix-only), and `select` takes nothing else.
3. **Tasks run on a safe scheduler** (`Arc<TaskCell>` cells, boxed futures, std `Wake`), the
   withdrawn prototype's shape. Task 1's benchmarks gate it: `method-call/roundtrip` or
   `method-call/1000-concurrent-p2p` slower by more than 10% against the async-io baseline
   reopens the question of importing async-task's core; that would be a follow-up plan, not
   this one. The gate is measured locally, wall-clock, on one machine: three `cargo bench`
   runs each side, criterion's `--noise-threshold 0.05`, the median of the three reported
   means compared. CodSpeed's instrumentation numbers (instruction counts on the benchmark's
   thread under Valgrind, which serialises threads) are the trend line in CI, not the gate.
4. **Blocking work is a thread per call**, the `traits::Runtime::spawn_blocking` default that
   external runtimes already get. `runtime/unblock.rs` and the `blocking` crate go. The one
   public path that pays a thread start per call is the first `Connection::peer_creds()` on
   Linux (its supplementary-group lookup); `blocking-hook/peer-credentials` measures it, and
   Task 8 reports the difference in the PR body for the maintainer to weigh, without a stop.
5. **The `async-lock` feature goes.** `runtime::locks` is zbus's own `Mutex`/`RwLock` on
   `event-listener`, and re-exports `tokio::sync`'s when `tokio` is on. Only `Mutex::lock`,
   `RwLock::read`, `RwLock::write` and the three guard types are needed (the inventory in
   Task 3); no semaphore, no `try_lock`.
6. **Dev-dependencies of the test doubles stay.** `zbus/src/runtime/test_runtime.rs` (on
   async-io and async-executor) and `zbus/tests/polling_runtime/` (on polling and async-task)
   exist to exercise the external path with a runtime that is not the built-in one, and lose
   that independence if rebuilt on it. `async-io`, `async-executor`, `async-task` and `polling`
   remain `[dev-dependencies]` of `zbus` (and `async-io` of `zbus_macros`, whose doctest uses
   it); `blocking` and `async-lock` leave those too. The RFC's "tests, fixtures" clause is read
   as the normal and build graphs of every feature set plus the fixtures' graphs. Flag to the
   maintainer in the PR body.
7. **A panicking task is caught, logged and fails its handle** with
   `io::Error::other("the task panicked")`, and so is a panic in a task future's `Drop` on the
   worker; the worker thread keeps running. A panic anywhere else on the worker (the reactor,
   a waker) ends that thread with its running flag cleared, so the next spawn, registration or
   sleep starts a new one instead of hanging on a worker that is gone. (On async-executor a
   panic in a detached object-server task kills the executor thread and stalls every task of
   the connection.)
8. **The reference runtime in `zbus/tests/polling_runtime/` is unchanged.** Its lifecycle test
   is the proof that a connection on an external runtime starts no zbus thread, and it holds
   before and after this plan.
9. **A runtime per connection.** `Runtime::default_for_build` makes a `Builtin` per connection
   it builds, so each connection on the built-in runtime costs one thread and one wake pipe
   (two descriptors), where async-io costs one executor thread per connection plus one reactor
   thread and one `epoll` descriptor for the whole process. A connection built inside a Tokio
   runtime with the `tokio` feature on costs neither. Sharing one worker across connections is
   the RFC's separate optimisation and not part of this plan; Task 10 documents the cost.

## File Structure

New:

- `zbus/benches/runtime.rs` — connection-level benchmarks (setup and teardown, round trip,
  large body, signal, spawn, blocking hook, blocking API) over a unix socket pair; `[[bench]]`
  entry in `zbus/Cargo.toml`.
- `test_fixtures/geoclue_service/{Cargo.toml,src/main.rs}` and
  `test_fixtures/geoclue_client/{Cargo.toml,src/main.rs}` — the book's GeoClue2 interfaces as
  a service and a client, each enabling only the zbus features it needs; workspace members.
- `CI/binary-size.sh` — builds both fixtures under the `size` profile for the built-in and the
  Tokio runtime, prints a Markdown table of stripped sizes, runs the pair once on a session bus.
- `.github/workflows/size.yml` — runs the script and appends the table to the job summary.
- `zbus/src/runtime/locks/{mod.rs,mutex.rs,rwlock.rs}` — zbus's own locks (replaces
  `zbus/src/runtime/locks.rs`).
- `zbus/src/runtime/builtin/mod.rs` — `Builtin`, the `traits::Runtime` implementation, its
  `Inner` state and the worker start/exit discipline.
- `zbus/src/runtime/builtin/scheduler.rs` — the task cells, ready queue and join handles.
- `zbus/src/runtime/builtin/reactor.rs` — registered sources, their wakers, and the timers.
- `zbus/src/runtime/builtin/poll/{mod.rs,unix.rs,windows.rs}` — the platform wait: `poll(2)`
  with a wake pipe; `select()` with a loopback wake pair.
- `zbus/src/runtime/builtin/worker.rs` — the loop the worker thread runs.
- `zbus/src/runtime/builtin/tests.rs` — the backend's tests.

Modified:

- `zbus/Cargo.toml` — `[[bench]]`, features (`builtin-runtime` replaces `async-io`,
  `async-lock` removed), dependencies trimmed, `[[example]]` and `[package.metadata.docs.rs]`
  feature names.
- `Cargo.toml` (workspace) — members, `[profile.size]`, `rustix` features, workspace
  dependencies trimmed of what no member uses; `Cargo.lock` with each of those.
- `zbus/src/runtime/mod.rs`, `task.rs`, `io/mod.rs` — the `AsyncIo` arms become `Builtin`.
- `zbus/src/runtime/unblock.rs`, `zbus/src/runtime/async_io.rs`, `zbus/src/runtime/locks.rs` —
  deleted.
- `zbus/src/utils.rs` — `block_on` for the non-Tokio builds.
- `zbus/src/lib.rs` — the lock `compile_error!` goes.
- `zbus/src/runtime/test_runtime.rs` — `spawn_blocking` through `blocking_thread`.
- `zbus/src/connection/mod.rs`, `zbus/tests/builder_message_stream.rs` — the two tests that
  name the async-io backend.
- `zbus/examples/watch-systemd-jobs.rs`, doctests naming `async_io` — driven differently.
- `.github/workflows/rust.yml`, `.github/workflows/bench.yml`, `book/src/connection.md`,
  `book/src/faq.md`, `book/src/upgrading-to-6.md`, `zbus/src/runtime/{mod,traits}.rs` docs —
  feature names, the dependency guard, the runtime description.

## Verification per commit

Every commit (not only the tip) passes, in this order:

```sh
cargo +nightly fmt --all -- --check
cargo clippy -p zbus --all-targets --features p2p -- -D warnings
cargo clippy -p zbus --all-features --all-targets -- -D warnings
cargo clippy -p zbus --no-default-features \
    --features tokio,proxy,service,blocking-api,p2p --all-targets -- -D warnings
cargo clippy -p zbus --no-default-features \
    --features proxy,service,unixexec,ibus,p2p --all-targets -- -D warnings
cargo check --target x86_64-pc-windows-gnu -p zbus --all-targets --features p2p
cargo check --target x86_64-apple-darwin -p zbus --all-targets --features p2p
RUSTDOCFLAGS="-D warnings -D rustdoc::broken_intra_doc_links" \
    cargo doc -p zbus --all-features --no-deps --document-private-items
```

(`--features p2p` is what makes the p2p benches part of `--all-targets`; without it cargo skips
them silently. Before Task 3 lands, the fourth clippy line needs `async-lock` in its feature
list and the fifth cannot pass at all: `zbus/src/lib.rs` refuses a `comms` build without
`async-lock` or `tokio` until Task 3 deletes that `compile_error!`. Before Task 9,
`builtin-runtime` in any command reads `async-io`.) Tests, at the commits the tasks name:

```sh
S="dbus-run-session --config-file /tmp/dbus-session.conf --"
$S cargo test -p zbus --all-features -- --skip fdpass_systemd --skip ibus_connection
$S cargo test -p zbus --no-default-features \
    --features tokio,proxy,service,blocking-api,p2p --tests
$S cargo test -p zbus --no-default-features \
    --features proxy,service,object-manager,unixexec,ibus,tracing,p2p --tests
cargo test -p zbus --doc
cargo test -p zbus_macros --doc
```

(The third suite is the external-only build; it too needs Task 3 first.) Known pre-existing
failures to ignore: workspace-wide clippy on `zbus_xmlgen` test data;
`cargo check --target x86_64-unknown-freebsd --all-features` on `vsock`.

---

## Part 1: Measurement baseline

### Task 1: Connection benchmarks

**Files:**
- Create: `zbus/benches/runtime.rs`
- Modify: `zbus/Cargo.toml` (the `[[bench]]` list, after the `concurrent_method_calls` entry),
  `.github/workflows/bench.yml`

**Interfaces:**
- Consumes: `std::os::unix::net::UnixStream::pair()`, `zbus::connection::Builder::unix_stream`,
  `Builder::server(guid)`, `Builder::p2p()`, `Builder::serve_at` and `Builder::method_timeout`
  (all return `Self`), `zbus::Guid::generate()`, `zbus::block_on` (`#[doc(hidden)]`, picks the
  build's backend), `Connection::spawn` (`#[doc(hidden)]`, returns `impl TaskHandle<T>`, a
  future of `io::Result<T>` that needs no trait in scope to await),
  `Connection::peer_creds()` (caches its answer, so it is measured on a fresh pair),
  `Connection::graceful_shutdown()`, `zbus::blocking::Connection: From<zbus::Connection>`.
- Produces: criterion groups with benchmark ids `connection/build-and-drop`,
  `connection/graceful-shutdown`, `method-call/roundtrip`, `method-call/1MiB-body`,
  `signal/emit-receive`, `spawn/100-tasks`, `blocking-hook/peer-credentials`,
  `blocking-api/roundtrip`. Later tasks compare these ids against a saved baseline.

Why a real socket pair: `zbus::connection::socket::Channel::pair()`, which the existing
`concurrent_method_calls` bench uses, is an in-process channel over `async-broadcast` with no
descriptor behind it, so a connection on it never reaches the runtime's readiness path. Every
benchmark here runs over `UnixStream::pair()`, with the server end doing the p2p handshake
(`EXTERNAL`, the unix socket's default), so the reactor, the wake-ups and the socket reads and
writes are on the measured path. The bench is unix-only for that reason; on other targets it
compiles to an empty `main`.

- [ ] **Step 1: Add the bench target**

In `zbus/Cargo.toml`, after the `concurrent_method_calls` `[[bench]]`:

```toml
[[bench]]
name = "runtime"
harness = false
required-features = ["p2p", "service", "blocking-api"]
```

- [ ] **Step 2: Write the benchmarks**

`zbus/benches/runtime.rs`:

```rust
//! What a connection costs on the runtime it is built on: its setup and teardown, a method
//! call's round trip, a large body, a signal, the runtime's task spawn, its blocking hook, and
//! the blocking API's own `block_on`. Every benchmark runs over a unix socket pair with a p2p
//! handshake, so nothing here depends on a bus and everything goes through the runtime's
//! readiness path, which an in-process channel would bypass.

#[cfg(unix)]
mod unix {
    use std::{hint::black_box, os::unix::net::UnixStream, time::Duration};

    use criterion::{BatchSize, Criterion, Throughput, criterion_group};
    use futures_util::StreamExt;
    use zbus::{Connection, Message, connection::Builder, message::Type};

    const PATH: &str = "/org/zbus/Benchmark";
    const INTERFACE: &str = "org.zbus.Benchmark";
    const BIG: usize = 1024 * 1024;

    fn runtime(c: &mut Criterion) {
        let mut group = c.benchmark_group("connection");
        group.sample_size(20);
        group.bench_function("build-and-drop", |b| {
            b.iter(|| black_box(zbus::block_on(pair())));
        });
        group.bench_function("graceful-shutdown", |b| {
            b.iter_batched(
                || zbus::block_on(pair()),
                |(server, client)| {
                    zbus::block_on(async {
                        futures_util::join!(server.graceful_shutdown(), client.graceful_shutdown())
                    })
                },
                BatchSize::PerIteration,
            );
        });
        group.finish();

        let (server, client) = zbus::block_on(pair());

        let mut group = c.benchmark_group("method-call");
        group.bench_function("roundtrip", |b| {
            b.iter(|| zbus::block_on(ping(&client, 1)));
        });
        group.sample_size(10);
        group.throughput(Throughput::Bytes(BIG as u64));
        let body = vec![7u8; BIG];
        group.bench_function("1MiB-body", |b| {
            b.iter(|| zbus::block_on(echo(&client, &body)));
        });
        group.finish();

        let mut group = c.benchmark_group("signal");
        let mut signals = zbus::block_on(async {
            zbus::MessageStream::for_match_rule(
                zbus::MatchRule::builder()
                    .msg_type(Type::Signal)
                    .interface(INTERFACE)
                    .build()
                    .unwrap(),
                &client,
                None,
            )
            .await
            .unwrap()
        });
        group.bench_function("emit-receive", |b| {
            b.iter(|| {
                zbus::block_on(async {
                    server
                        .emit_signal(None::<()>, PATH, INTERFACE, "Tick", &())
                        .await
                        .unwrap();
                    black_box(signals.next().await.unwrap().unwrap());
                })
            });
        });
        group.finish();

        let mut group = c.benchmark_group("spawn");
        group.throughput(Throughput::Elements(100));
        group.bench_function("100-tasks", |b| {
            b.iter(|| {
                zbus::block_on(async {
                    let tasks: Vec<_> = (0..100u32)
                        .map(|i| client.spawn("bench", async move { i }))
                        .collect();
                    for task in tasks {
                        black_box(task.await.unwrap());
                    }
                })
            });
        });
        group.finish();

        // The credentials are cached on the connection, so only a fresh pair pays the lookup
        // and the blocking hook behind it.
        let mut group = c.benchmark_group("blocking-hook");
        group.sample_size(20);
        group.bench_function("peer-credentials", |b| {
            b.iter_batched(
                || zbus::block_on(pair()),
                |(_server, client)| {
                    zbus::block_on(async { black_box(client.peer_creds().await.unwrap().clone()) })
                },
                BatchSize::PerIteration,
            );
        });
        group.finish();

        let (_blocking_server, blocking_client) = zbus::block_on(pair());
        let blocking_client = zbus::blocking::Connection::from(blocking_client);
        let mut group = c.benchmark_group("blocking-api");
        group.bench_function("roundtrip", |b| {
            b.iter(|| {
                black_box(
                    blocking_client
                        .call_method(None::<()>, PATH, Some(INTERFACE), "Ping", &1u32)
                        .unwrap(),
                )
            });
        });
        group.finish();
    }

    /// A server and a client over a fresh socket pair, handshake included.
    async fn pair() -> (Connection, Connection) {
        let (server_end, client_end) = UnixStream::pair().unwrap();
        let server = Builder::unix_stream(server_end)
            .server(zbus::Guid::generate())
            .p2p()
            .serve_at(PATH, Benchmark)
            .build();
        let client = Builder::unix_stream(client_end)
            .p2p()
            .method_timeout(Duration::from_secs(30))
            .build();
        futures_util::try_join!(server, client).unwrap()
    }

    async fn ping(client: &Connection, value: u32) -> Message {
        client
            .call_method(None::<()>, PATH, Some(INTERFACE), "Ping", &value)
            .await
            .unwrap()
    }

    async fn echo(client: &Connection, body: &[u8]) -> Message {
        client
            .call_method(None::<()>, PATH, Some(INTERFACE), "Echo", &body)
            .await
            .unwrap()
    }

    struct Benchmark;

    #[zbus::interface(name = "org.zbus.Benchmark")]
    impl Benchmark {
        fn ping(&self, value: u32) -> u32 {
            value
        }

        fn echo(&self, body: Vec<u8>) -> Vec<u8> {
            body
        }
    }

    criterion_group!(benches, runtime);
}

#[cfg(unix)]
criterion::criterion_main!(unix::benches);

#[cfg(not(unix))]
fn main() {}
```

`Builder::unix_stream(...).server(guid).p2p()` against `Builder::unix_stream(...).p2p()` is
the form the `unix_p2p` tests in `zbus/src/connection/mod.rs` use. `build-and-drop` times a
socket pair, two builds with the handshake between them, and the drop of both ends, which is
what a short-lived connection pays; on the built-in runtime that includes starting and stopping
its thread. `graceful-shutdown` and `peer-credentials` build outside the timing
(`BatchSize::PerIteration`, one pair at a time so no descriptors pile up) and time one call.

- [ ] **Step 3: Run it once and save the async-io baseline**

```sh
cargo bench -p zbus --features p2p --bench runtime -- --save-baseline async-io
cargo bench -p zbus --features p2p --bench concurrent_method_calls -- --save-baseline async-io
```

(`p2p` is not a default feature and cargo refuses a named bench whose `required-features` are
off.) Expected: eight benchmark ids print with times for the first, one for the second; no
panics. Repeat each three times as ruling 3 says and copy the three tables into the plan
workspace's `progress.md` under "Baseline (async-io)". `--save-baseline` stores the last run
under `target/criterion/*/async-io` for Task 8's and Task 12's comparisons. The
`concurrent_method_calls` bench stays on the in-process channel: it measures the scheduler
under a burst, and the two numbers are read together.

- [ ] **Step 4: Make CI build the p2p benches**

`.github/workflows/bench.yml` runs `cargo codspeed build` with default features, and cargo
skips a bench whose `required-features` are off, so CodSpeed has never run
`concurrent_method_calls` (its runs on `main` list 24 benchmarks, none from that file). Change
the build step to:

```yaml
      - name: Build the benchmark target(s)
        run: cargo codspeed build --features zbus/p2p
```

(`zbus/p2p` is the workspace-root spelling; verified accepted by cargo. Not `--all-features`:
with the `tokio` feature on, `zbus::block_on` runs everything on a Tokio runtime, and the
benchmarks would silently measure Tokio.) Confirm on the PR's CodSpeed run that
`concurrent_method_calls` and `runtime` benchmark ids appear. If that run takes more than
twice as long as before, gate the `connection` group with `#[cfg(not(codspeed))]`
(`cargo-codspeed` builds with `--cfg codspeed`; confirm with `cargo codspeed build -v`) and say
so in the commit body: thread starts per iteration are slow under Valgrind and are measured
locally anyway.

- [ ] **Step 5: Verify and commit**

Run the per-commit checks. Two commits, the first:

```
✅ zb: Benchmark what a connection costs on its runtime

The runtime behind a connection is about to be swapped for one kept in
zbus, and the existing benchmarks measure serialisation and a burst of
concurrent calls over an in-process channel, which never reaches the
runtime's readiness path. These benchmarks run over a unix socket pair
with a p2p handshake and measure what a single connection pays the
runtime: its setup and teardown, a round trip (a task wake, a socket
poll, a timer armed and cancelled), a large body, a signal, the spawn
hook, the blocking hook and the blocking API's own `block_on`, so the
swap can be judged against numbers taken before it.
```

and `👷 zb: Build the p2p benchmarks for CodSpeed` for `bench.yml` (body: `p2p` is not a
default feature, cargo skips benches whose required features are off, and the dashboard shows
none of the p2p benchmarks ever ran). Both with the trailers.

### Task 2: Binary-size fixtures

**Files:**
- Create: `test_fixtures/geoclue_service/Cargo.toml`, `test_fixtures/geoclue_service/src/main.rs`,
  `test_fixtures/geoclue_client/Cargo.toml`, `test_fixtures/geoclue_client/src/main.rs`,
  `CI/binary-size.sh`, `.github/workflows/size.yml`
- Modify: `Cargo.toml` (workspace `members`, new `[profile.size]`), `Cargo.lock` (the two new
  members), `.github/workflows/rust.yml` (two clippy lines)

**Interfaces:**
- Consumes: the book's `Manager`/`Client`/`Location` proxies (`book/src/client.md:210-250`),
  `zbus::interface`, `zbus::connection::Builder::session()` (returns `Self`; a missing bus is
  reported by `build`), `Builder::name`, `Builder::serve_at` (both return `Self`),
  `LocationProxy::builder(&conn).destination(..).path(..)` (both return `Self`).
- Produces: packages `geoclue_service_fixture` and `geoclue_client_fixture`, each with features
  `builtin` (default; maps to `zbus/async-io` until Task 9 renames it) and `tokio`;
  `CI/binary-size.sh` printing a table with rows `service`/`client` × `builtin`/`tokio`.

- [ ] **Step 1: Workspace entries**

In the root `Cargo.toml`, add to `members` after `"test_fixtures/blocking_api",`:

```toml
    "test_fixtures/geoclue_client",
    "test_fixtures/geoclue_service",
```

and after `[profile.bench]`:

```toml
# What a shipped binary weighs: the fixtures under test_fixtures/geoclue_* are built with it.
[profile.size]
inherits = "release"
lto = "fat"
codegen-units = 1
strip = true
```

- [ ] **Step 2: The service**

`test_fixtures/geoclue_service/Cargo.toml`:

```toml
[package]
name = "geoclue_service_fixture"
version = "0.0.0"
edition = { workspace = true }
rust-version = { workspace = true }
publish = false

description = "Size fixture: a GeoClue2-shaped service enabling only the zbus features it needs"

[features]
default = ["builtin"]
builtin = ["zbus/async-io"]
tokio = ["zbus/tokio", "dep:tokio"]

[dependencies]
zbus = { path = "../../zbus", default-features = false, features = ["service"] }
tokio = { workspace = true, optional = true, features = ["rt", "macros"] }

[lints]
workspace = true
```

(Task 9 changes `zbus/async-io` to `zbus/builtin-runtime`.) `src/main.rs`:

```rust
//! A service shaped like GeoClue2's `Manager`, `Client` and `Location` objects, for measuring
//! what a service binary weighs with only the zbus features it uses. `GetClient` hands out one
//! client object; `Start` on it publishes one location and announces it.

use zbus::{
    ObjectPath, OwnedObjectPath, connection::Builder, interface, object_server::SignalEmitter,
};

const NAME: &str = "org.zbus.GeoClue2Fixture";
const CLIENT: &str = "/org/freedesktop/GeoClue2/Client/1";
const LOCATION: &str = "/org/freedesktop/GeoClue2/Location/1";

struct Manager;

#[interface(name = "org.freedesktop.GeoClue2.Manager")]
impl Manager {
    fn get_client(&self) -> OwnedObjectPath {
        ObjectPath::from_static_str_unchecked(CLIENT).into()
    }
}

struct Client {
    desktop_id: String,
}

#[interface(name = "org.freedesktop.GeoClue2.Client")]
impl Client {
    async fn start(
        &self,
        #[zbus(signal_emitter)] emitter: SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        let none = ObjectPath::from_static_str_unchecked("/");
        let location = ObjectPath::from_static_str_unchecked(LOCATION);
        Self::location_updated(&emitter, none, location).await?;
        Ok(())
    }

    fn stop(&self) {}

    #[zbus(property)]
    fn desktop_id(&self) -> &str {
        &self.desktop_id
    }

    #[zbus(property)]
    fn set_desktop_id(&mut self, id: String) {
        self.desktop_id = id;
    }

    #[zbus(signal)]
    async fn location_updated(
        emitter: &SignalEmitter<'_>,
        old: ObjectPath<'_>,
        new: ObjectPath<'_>,
    ) -> zbus::Result<()>;
}

struct Location;

#[interface(name = "org.freedesktop.GeoClue2.Location")]
impl Location {
    #[zbus(property)]
    fn latitude(&self) -> f64 {
        59.3293
    }

    #[zbus(property)]
    fn longitude(&self) -> f64 {
        18.0686
    }
}

async fn serve() -> zbus::Result<()> {
    let _conn = Builder::session()
        .name(NAME)
        .serve_at("/org/freedesktop/GeoClue2/Manager", Manager)
        .serve_at(CLIENT, Client { desktop_id: String::new() })
        .serve_at(LOCATION, Location)
        .build()
        .await?;
    std::future::pending().await
}

#[cfg(feature = "tokio")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> zbus::Result<()> {
    serve().await
}

#[cfg(not(feature = "tokio"))]
fn main() -> zbus::Result<()> {
    block_on::run(serve())
}

#[cfg(not(feature = "tokio"))]
mod block_on {
    //! Drives one future on the calling thread; the connection's own runtime does the rest.

    use std::{
        future::Future,
        pin::pin,
        sync::Arc,
        task::{Context, Poll, Wake},
        thread::{self, Thread},
    };

    struct Unpark(Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    pub(super) fn run<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let mut future = pin!(future);
        let waker = Arc::new(Unpark(thread::current())).into();
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
            thread::park();
        }
    }
}
```

The `#[zbus(signal)]` declaration form and the `#[zbus(signal_emitter)]` parameter are the
ones `zbus/tests/iface_and_proxy/iface.rs` uses. `Builder::session`, `name` and `serve_at` all
return `Self`: the first error surfaces from `build`.

- [ ] **Step 3: The client**

`test_fixtures/geoclue_client/Cargo.toml`: as the service's with `name = "geoclue_client_fixture"`,
description `"Size fixture: the book's GeoClue2 client enabling only the zbus features it needs"`,
`features = ["proxy"]` on the zbus dependency, and `futures-util = { workspace = true }` added to
`[dependencies]`. `src/main.rs`:

```rust
//! The book's GeoClue2 client against the fixture service, for measuring what a client binary
//! weighs with only the zbus features it uses. It asks for a client object, starts it, prints
//! the first location it is told about and exits.

use futures_util::StreamExt;
use zbus::{Connection, ObjectPath, Result, proxy};

const NAME: &str = "org.zbus.GeoClue2Fixture";

#[proxy(
    default_service = "org.zbus.GeoClue2Fixture",
    interface = "org.freedesktop.GeoClue2.Manager",
    default_path = "/org/freedesktop/GeoClue2/Manager"
)]
trait Manager {
    #[zbus(object = "Client")]
    fn get_client(&self);
}

#[proxy(
    default_service = "org.zbus.GeoClue2Fixture",
    interface = "org.freedesktop.GeoClue2.Client"
)]
trait Client {
    fn start(&self) -> Result<()>;
    fn stop(&self) -> Result<()>;

    #[zbus(property)]
    fn set_desktop_id(&mut self, id: &str) -> Result<()>;

    #[zbus(signal)]
    fn location_updated(&self, old: ObjectPath<'_>, new: ObjectPath<'_>) -> Result<()>;
}

#[proxy(
    default_service = "org.zbus.GeoClue2Fixture",
    interface = "org.freedesktop.GeoClue2.Location"
)]
trait Location {
    #[zbus(property)]
    fn latitude(&self) -> Result<f64>;
    #[zbus(property)]
    fn longitude(&self) -> Result<f64>;
}

async fn locate() -> Result<()> {
    let conn = Connection::session().await?;
    let manager = ManagerProxy::new(&conn).await?;
    let mut client = manager.get_client().await?;
    client.set_desktop_id("org.zbus.fixture").await?;
    let mut updates = client.receive_location_updated().await?;
    client.start().await?;
    let update = updates.next().await.expect("the service announces a location");
    let args = update.args()?;
    let location = LocationProxy::builder(&conn)
        .destination(NAME)
        .path(args.new())
        .build()
        .await?;
    println!(
        "Latitude: {}\nLongitude: {}",
        location.latitude().await?,
        location.longitude().await?,
    );
    client.stop().await
}

#[cfg(feature = "tokio")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    locate().await
}

#[cfg(not(feature = "tokio"))]
fn main() -> Result<()> {
    block_on::run(locate())
}
```

followed by the service's `block_on` module, copied as it is.

- [ ] **Step 4: The script and the workflow**

`CI/binary-size.sh`:

```sh
#!/bin/sh
# Builds the GeoClue2 fixtures under the `size` profile on each runtime, prints their stripped
# sizes as a Markdown table, and runs the pair once on a private session bus to prove the
# binaries work. Run from the repository root.
set -eu

table="| binary | builtin | tokio |
|---|---|---|"
for bin in service client; do
    row="| $bin |"
    for runtime in builtin tokio; do
        cargo build --locked --profile size -p "geoclue_${bin}_fixture" \
            --no-default-features --features "$runtime" >/dev/null
        size=$(stat -c %s "target/size/geoclue_${bin}_fixture")
        row="$row $((size / 1024)) KiB |"
        cp "target/size/geoclue_${bin}_fixture" "target/size/geoclue_${bin}_${runtime}"
    done
    table="$table
$row"
done
echo "$table"

for runtime in builtin tokio; do
    dbus-run-session -- sh -c "
        target/size/geoclue_service_$runtime &
        sleep 1
        target/size/geoclue_client_$runtime
        kill \$!
    " | grep -q '^Latitude: 59.3293$'
done
```

(`stat -c %s` is GNU; on macOS the script is not run.) `chmod +x CI/binary-size.sh`.

`.github/workflows/size.yml`:

```yaml
name: Binary size

on:
  push:
    branches: ["main"]
  pull_request:

jobs:
  size:
    name: GeoClue2 fixtures
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: moonrepo/setup-rust@v1
        with:
          channel: stable
      - name: Install a session bus
        run: sudo apt-get update && sudo apt-get install -y dbus
      - name: Build, measure and run the fixtures
        run: |
          echo "## GeoClue2 fixture sizes (\`size\` profile, stripped)" >> "$GITHUB_STEP_SUMMARY"
          CI/binary-size.sh | tee -a "$GITHUB_STEP_SUMMARY"
```

- [ ] **Step 5: Run and record**

```sh
CI/binary-size.sh
```

Expected: a four-cell table and a silent exit code 0 (the `grep -q` proves each client printed
the latitude). Copy the table into `progress.md` under "Baseline sizes (async-io)".

Also lint both fixture shapes in CI: in `.github/workflows/rust.yml`, next to the
`blocking_api_fixture` clippy lines, add

```sh
cargo --locked clippy -p geoclue_service_fixture -p geoclue_client_fixture
cargo --locked clippy -p geoclue_service_fixture -p geoclue_client_fixture \
    --no-default-features --features tokio
```

(`--all-features` would lint the union of both runtimes, a shape the script never builds.)

- [ ] **Step 6: Verify and commit**

Per-commit checks plus the two clippy lines above with `-- -D warnings`. Two commits: one
`✅ zb: Add GeoClue2-shaped fixtures for measuring binary size` (fixtures, workspace members and
`Cargo.lock`, profile, script) whose body says what the fixtures mirror (the book's client
chapter), why each enables only its own zbus features (the size a client or a service pays for
zbus, not for the other half), and why the script runs them (a size of something that does not
work measures nothing); one `👷 zb: Report the fixtures' binary sizes in CI` (workflow and the
rust.yml clippy lines).

---

## Part 2: Locks

### Task 3: zbus's own `Mutex` and `RwLock`

**Files:**
- Create: `zbus/src/runtime/locks/mod.rs`, `zbus/src/runtime/locks/mutex.rs`,
  `zbus/src/runtime/locks/rwlock.rs`
- Modify: `zbus/src/lib.rs` (the lock `compile_error!`)
- Delete: `zbus/src/runtime/locks.rs`

**Interfaces:**
- Consumes: `event_listener::Event` (`listen()`, `notify(1)`); `std::sync::Mutex` for the
  locks' state; the withdrawn prototype `/usr/bin/git show 56715312:zbus/src/runtime/sync.rs`
  as the source to adapt.
- Produces, `pub(crate)`, unchanged call sites: `Mutex<T: ?Sized>::new(T) -> Self` (const),
  `Mutex::lock(&self) -> impl Future<Output = MutexGuard<'_, T>>`, `MutexGuard: Deref +
  DerefMut`; `RwLock<T: ?Sized>::new(T)`, `RwLock::read(&self) -> impl Future<Output =
  RwLockReadGuard<'_, T>>`, `RwLock::write(&self) -> impl Future<Output = RwLockWriteGuard<'_,
  T>>`, the read guard `Deref`, the write guard `Deref + DerefMut`. `Mutex<T>` and `RwLock<T>`
  are `Send + Sync` for `T: Send` / `T: Send + Sync` as std's, and support `Arc<RwLock<dyn
  Interface>>` (unsized coercion, so `?Sized` everywhere and `value: UnsafeCell<T>` last).

The inventory of every call site (the only methods used across the crate):

- `Mutex::new`, `.lock().await`: `zbus/src/connection/mod.rs` (`registered_names`,
  `socket_write`, `msg_senders`, `subscriptions`) and `connection/socket_reader.rs` (`senders`).
- `RwLock::new`, `.read().await`, `.write().await`: `zbus/src/object_server/mod.rs`
  (`root: Arc<RwLock<Node>>`, `Arc<RwLock<dyn Interface>>`) and
  `object_server/interface/{mod,interface_ref}.rs`.
- `RwLockReadGuard<'d, dyn Interface>`, `RwLockWriteGuard<'d, dyn Interface>`: fields of
  `object_server/interface/interface_deref.rs`.

- [ ] **Step 1: Write the failing test file**

`zbus/src/runtime/locks/mod.rs` starts as the module with tests only (the lock modules do not
exist, so it fails to compile):

```rust
//! The async locks a connection holds.
//!
//! These belong to no particular runtime: any of them can be taken from a future polled
//! anywhere, so they are zbus's own, on `event-listener`, and Tokio's stand in where the
//! `tokio` feature is on so that a Tokio build pulls in no second implementation. Only the
//! object server takes readers-writer locks, so those come along with the `service` feature.

#[cfg(not(feature = "tokio"))]
mod mutex;
#[cfg(not(feature = "tokio"))]
pub(crate) use mutex::Mutex;
#[cfg(all(not(feature = "tokio"), feature = "service"))]
mod rwlock;
#[cfg(all(not(feature = "tokio"), feature = "service"))]
pub(crate) use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "tokio")]
pub(crate) use tokio::sync::Mutex;
#[cfg(all(feature = "tokio", feature = "service"))]
pub(crate) use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(all(test, not(feature = "tokio")))]
mod tests {
    use std::{
        future::Future,
        pin::pin,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use super::*;

    fn poll_once<F>(future: &mut F) -> Poll<F::Output>
    where
        F: Future + Unpin,
    {
        pin!(future).poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn a_mutex_admits_one_holder_at_a_time() {
        let mutex = Mutex::new(0);
        let guard = zbus_block_on(mutex.lock());
        let mut second = Box::pin(mutex.lock());
        assert!(poll_once(&mut second).is_pending());
        drop(guard);
        assert!(poll_once(&mut second).is_ready());
    }

    #[test]
    fn a_dropped_lock_future_does_not_strand_the_next_waiter() {
        let mutex = Mutex::new(());
        let guard = zbus_block_on(mutex.lock());
        let mut first = Box::pin(mutex.lock());
        assert!(poll_once(&mut first).is_pending());
        let mut second = Box::pin(mutex.lock());
        assert!(poll_once(&mut second).is_pending());
        drop(first);
        drop(guard);
        assert!(poll_once(&mut second).is_ready());
    }

    #[test]
    fn a_mutex_is_shared_across_threads() {
        let mutex = Arc::new(Mutex::new(0u32));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let mutex = mutex.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        *zbus_block_on(mutex.lock()) += 1;
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(*zbus_block_on(mutex.lock()), 8000);
    }

    #[cfg(feature = "service")]
    #[test]
    fn readers_share_and_a_writer_excludes() {
        let lock = RwLock::new(1);
        let first = zbus_block_on(lock.read());
        let second = zbus_block_on(lock.read());
        let mut writer = Box::pin(lock.write());
        assert!(poll_once(&mut writer).is_pending());
        drop(first);
        drop(second);
        let Poll::Ready(mut guard) = poll_once(&mut writer) else {
            panic!("the last reader lets the writer in");
        };
        *guard = 2;
        let mut reader = Box::pin(lock.read());
        assert!(poll_once(&mut reader).is_pending());
        drop(guard);
        let Poll::Ready(guard) = poll_once(&mut reader) else {
            panic!("the writer's release lets readers in");
        };
        assert_eq!(*guard, 2);
    }

    #[cfg(feature = "service")]
    #[test]
    fn a_waiting_writer_holds_new_readers_back() {
        let lock = RwLock::new(());
        let reader = zbus_block_on(lock.read());
        let mut writer = Box::pin(lock.write());
        assert!(poll_once(&mut writer).is_pending());
        let mut late_reader = Box::pin(lock.read());
        assert!(poll_once(&mut late_reader).is_pending());
        drop(reader);
        assert!(poll_once(&mut writer).is_ready());
    }

    #[cfg(feature = "service")]
    #[test]
    fn a_cancelled_writer_lets_readers_in_again() {
        let lock = RwLock::new(());
        let reader = zbus_block_on(lock.read());
        let mut writer = Box::pin(lock.write());
        assert!(poll_once(&mut writer).is_pending());
        drop(writer);
        assert!(poll_once(&mut Box::pin(lock.read())).is_ready());
        drop(reader);
    }

    #[cfg(feature = "service")]
    #[test]
    fn a_rwlock_coerces_to_an_unsized_value() {
        let lock: Arc<RwLock<dyn std::fmt::Debug + Send + Sync>> = Arc::new(RwLock::new(5u8));
        assert_eq!(format!("{:?}", *zbus_block_on(lock.read())), "5");
    }

    fn zbus_block_on<F>(future: F) -> F::Output
    where
        F: Future,
    {
        futures_lite::future::block_on(future)
    }
}
```

- [ ] **Step 2: Run to verify the failure**

```sh
cargo test -p zbus --lib runtime::locks
```

Expected: a compile error, `mutex` and `rwlock` unresolved.

- [ ] **Step 3: Write `mutex.rs` and `rwlock.rs`**

Start from `/usr/bin/git show 56715312:zbus/src/runtime/sync.rs` and split it: `Mutex`,
`MutexGuard` and their impls into `mutex.rs`; `RwLock`, `RwState`, `WaitingWriter`, the two
guards and their impls into `rwlock.rs`. Leave `Semaphore` and `SemaphorePermit` out. Apply
these changes while copying:

1. Every `unsafe impl Send/Sync` and every `unsafe { &*self.value.get() }` gets a
   `// SAFETY:` line naming the invariant: a guard exists only while the lock is held, the lock
   is held by one writer or by readers only, and the `UnsafeCell` is reached through a guard
   alone.
2. The prototype's `Mutex` keeps its `locked` flag in an `AtomicBool`; replace it with a
   `std::sync::Mutex<bool>` (the global constraint on atomics), taken only for the instant of
   the try and the release. `RwLock`'s counters are under a std mutex already.
3. `RwLock`: replace `state.readers += 1` with
   `state.readers = state.readers.checked_add(1).expect("more readers than a usize counts")`
   and `state.readers -= 1` with
   `state.readers = state.readers.checked_sub(1).expect("a reader released twice")`. (With
   overflow checks off a plain `+= 1` wraps to zero and lets a writer in beside live readers.)
4. Module docs on each file say what the lock is for a first-time reader (why zbus needs a
   lock a future can hold across an await and why `std::sync::Mutex`'s guard cannot be that:
   it is `!Send`). No mention of async-lock, Tokio, prototypes or reviews.
5. Keep `Mutex::new` a `const fn`. Keep the `listen()`-then-recheck loop shape in `lock`,
   `read` and `write`: try, listen, try again, await, repeat.
6. Fairness: `MutexGuard::drop` uses `notify(1)`; `RwLockWriteGuard::drop` notifies all
   readers (`notify(usize::MAX)`) and one writer; the last `RwLockReadGuard::drop` notifies one
   writer, as the prototype does.

Also update `zbus/src/lib.rs`: remove the `compile_error!` at lines 57-61 that requires
`async-lock` or `tokio` for a `comms` build (the item and its `#[cfg]`; nothing else uses
them).

- [ ] **Step 4: Run the tests**

```sh
cargo test -p zbus --lib runtime::locks
cargo test -p zbus --no-default-features --features proxy,service,p2p --lib runtime::locks
```

Expected: all seven tests pass in both builds (the second has no backend).

- [ ] **Step 5: Miri on the locks**

```sh
cargo +nightly miri test -p zbus --no-default-features --features service --lib runtime::locks
```

Expected: pass. If Miri rejects `event-listener` internals (an "unsupported operation" on a
foreign item), record the exact message in `progress.md` and move on; the locks' own code is
the target.

- [ ] **Step 6: Verify and commit**

Per-commit checks, with `async-lock` still in the feature list of the Tokio-only clippy line
(the feature exists until Task 4). Commit `✨ zb: Give the connection locks of zbus's own`:
body says why a connection needs an async lock at all, why the guards need `UnsafeCell` (a
`std::sync::MutexGuard` cannot cross an await in a `Send` task), the writer-preferring
`RwLock` and what a cancelled write does, the checked reader count, and that Tokio's locks stand
in on a Tokio build so it carries no second implementation.

### Task 4: Drop `async-lock`

**Files:**
- Modify: `zbus/Cargo.toml` (features `async-io`, `async-lock`; dependency `async-lock`;
  `[package.metadata.docs.rs]`'s feature list), `Cargo.toml` (the workspace `async-lock` line;
  only `zbus` uses it), `Cargo.lock`, `.github/workflows/rust.yml` (every `async-lock` in a
  `--features` list), `book/src/connection.md`, `book/src/faq.md`, `book/src/upgrading-to-6.md`,
  `zbus/src/runtime/mod.rs` (every mention of `async-lock`).

- [ ] **Step 1: Remove the feature and the dependency**

`cargo remove -p zbus async-lock`; delete the `async-lock = ["comms", "dep:async-lock"]`
feature and its comment; delete `"async-lock",` from the `async-io` feature list and from
`[package.metadata.docs.rs]`. Remove the workspace dependency line.

- [ ] **Step 2: Sweep the mentions**

```sh
/usr/bin/grep -rn "async-lock\|async_lock" \
    --include=*.rs --include=*.md --include=*.toml --include=*.yml . \
    | /usr/bin/grep -v "^./5.x\|^./target\|^./docs/superpowers"
```

Every hit is fixed: CI feature lists lose `async-lock,`; docs describe the locks as zbus's own
(with Tokio's on a Tokio build). The book chapters are compiled as doctests by
`zbus/src/lib.rs`, so `cargo test -p zbus --doc` runs after the edit. `docs/superpowers/specs/*`
are historical records and stay.

- [ ] **Step 3: Verify and commit**

Per-commit checks (the Tokio-only clippy line without `async-lock`), the three test suites and
the doctests. Commit `➖ zb: Drop the async-lock feature and crate` (body: the locks come from
zbus, so a build on a runtime of its own needs no lock feature; the feature was public only in
an unreleased 6.0).

---

## Part 3: The built-in runtime

The backend lands in one commit (Task 8), built from parts each tested on its own in Tasks 5 to
7 as files under `zbus/src/runtime/builtin/`. Until Task 8 wires it in, `zbus/src/runtime/mod.rs`
declares the module as `#[cfg(test)] mod builtin;` (Task 5 adds the line): the files compile
and their tests run, and nothing is dead code in a non-test build. Task 8 changes the gate to
the feature.

### Task 5: The scheduler

**Files:**
- Create: `zbus/src/runtime/builtin/mod.rs` (module declarations only, for now),
  `zbus/src/runtime/builtin/scheduler.rs`
- Modify: `zbus/src/runtime/mod.rs` (add `#[cfg(test)] mod builtin;`)

**Interfaces:**
- Consumes: `/usr/bin/git show ceb13704:zbus/src/runtime/scheduler.rs` (the prototype to
  adapt), `std::task::Wake`.
- Produces (`pub(super)`):

```rust
pub(super) struct Scheduler {
    state: Mutex<State>,
    /// Called, from any thread, whenever the worker has something new to look at.
    notify: Box<dyn Fn() + Send + Sync>,
}
struct State {
    ready: VecDeque<Arc<TaskCell>>,
    /// Every cell that holds a future: spawned and neither finished, cancelled nor panicked.
    live: HashMap<u64, Arc<TaskCell>>,
    next_id: u64,
}
struct TaskCell {
    id: u64,
    name: Box<str>,
    scheduler: Weak<Scheduler>,
    cell: Mutex<CellState>,
}
struct CellState {
    stage: Stage,              // Idle(BoxFuture<'static, ()>) | Running | Done
    queued: bool,              // in `ready`
    woken_while_running: bool, // a wake arrived during the poll; run again
    cancelled: bool,           // a handle dropped during the poll; drop the future after it
}
impl Scheduler {
    pub(super) fn new(notify: impl Fn() + Send + Sync + 'static) -> Self;
    pub(super) fn spawn<T>(
        self: &Arc<Self>,
        name: &str,
        future: impl Future<Output = T> + Send + 'static,
    ) -> JoinHandle<T>
    where
        T: Send + 'static;
    /// Polls one ready task. `false` when the queue is empty.
    pub(super) fn run_one(&self) -> bool;
    pub(super) fn has_ready(&self) -> bool;
    pub(super) fn live_tasks(&self) -> usize;   // state.live.len()
}
pub(super) struct JoinHandle<T> { /* Arc<TaskCell>, Arc<Mutex<Joint<T>>>, detached: bool */ }
impl<T> JoinHandle<T> { pub(super) fn detach(self); }
impl<T> Future for JoinHandle<T> { type Output = io::Result<T>; }
impl<T> Drop for JoinHandle<T> { /* cancels unless detached */ }
```

Lock order everywhere: a cell's `cell` before the scheduler's `state`, never the reverse. No
lock is held while a future is polled or dropped, and none while a waker is woken, because a
future's `Drop` or a waker may spawn on this scheduler (`traits.rs` promises that `spawn` may be
called from whichever thread drops the last holder, a task the runtime drops included).

The paths, spelled out:

- **Wake** (`impl Wake for TaskCell`): lock `cell`; if `Running`, set `woken_while_running`; if
  `Idle` and not `queued`, set `queued` and lock `state` to push the cell; unlock both; if the
  cell was pushed, call `notify`. A wake for a `Done` cell or a queued cell does nothing, so a
  task woken three times is queued once.
- **`run_one`**: lock `state`, pop; unlock. Lock `cell`: clear `queued`; if `Done`, return
  `true` and move on; take the future out (`Idle` → `Running`); unlock. Poll it inside
  `catch_unwind(AssertUnwindSafe(..))`. Then lock `cell` and settle: `Ready` or a panic → `Done`,
  remove from `live`, and after unlocking drop the future inside `catch_unwind` too; a panic
  puts `Err(io::Error::other("the task panicked"))` in the joint, wakes the joiner and logs
  `tracing::error!(task = %name, "a task panicked")` (gated the way other `runtime/` files gate
  `tracing`); `Pending` with `cancelled` → `Done`, remove from `live`, drop the future after
  unlocking; `Pending` with `woken_while_running` → `Idle(future)`, `queued`, push; plain
  `Pending` → `Idle(future)`.
- **Cancel** (`JoinHandle::drop` unless detached): lock `cell`; `Idle` → take the future,
  `Done`, remove from `live`; `Running` → set `cancelled`; unlock; drop the taken future (on
  the caller's thread; a panic there is the caller's); put `Err(io::Error::other("the task was
  cancelled"))` in the joint if it is empty; call `notify` in every case, so that a worker
  waiting with nothing else to do learns it can retire.
- **Output**: `spawn` wraps the user's future as `async move { joint.finish(Ok(future.await)) }`
  where `Joint<T> { output: Option<io::Result<T>>, waker: Option<Waker> }`; `finish` stores
  and wakes the joiner. `JoinHandle::poll` returns the output when present, otherwise stores
  the waker.
- **`Drop for Scheduler`**: take every `live` cell and every queued one, and for each take its
  future, mark it `Done` and fail its joint; drop the futures after all locks are released,
  each inside `catch_unwind`.

- [ ] **Step 1: Write the tests** at the bottom of `scheduler.rs` (`#[cfg(test)] mod tests`).
  Take the prototype's tests as the starting point; their driver is a helper
  `fn drive(scheduler: &Scheduler)` looping `while scheduler.run_one() {}`, and `notify` in
  tests is a closure incrementing a counter under an `Arc<Mutex<usize>>` so a test can assert a
  wake reached the worker. Tests to have, by name:
  `a_spawned_task_runs_and_hands_its_output_back`,
  `a_wake_from_another_thread_notifies_the_worker`,
  `a_task_woken_during_its_own_poll_runs_again`,
  `dropping_the_handle_cancels_and_drops_the_future`,
  `cancelling_notifies_the_worker` (the counter rises on the drop of a handle),
  `a_detached_task_runs_to_completion`,
  `a_panicking_task_fails_its_handle_and_is_forgotten` (`Err` of kind `Other`, `live_tasks()`
  is 0 after),
  `a_panic_in_a_futures_drop_is_contained` (a future that completes on its first poll and
  whose `Drop` panics; `run_one` returns normally, the handle resolves `Ok`, and the next task
  runs),
  `a_ready_task_is_queued_once_however_often_it_is_woken` (wake a cell three times before
  running; `run_one` returns true once then false),
  `a_cancelled_futures_drop_may_spawn_on_the_scheduler` (the future's `Drop` calls `spawn`
  through a captured `Arc<Scheduler>`; no deadlock, and the spawned task runs on the next
  `drive`),
  `live_tasks_counts_unfinished_futures_only`,
  `dropping_the_scheduler_fails_pending_joins`.

- [ ] **Step 2: Run to verify failure**

```sh
cargo test -p zbus --lib runtime::builtin::scheduler
```

Expected: compile error (no `Scheduler`).

- [ ] **Step 3: Adapt the prototype**

Copy `ceb13704:zbus/src/runtime/scheduler.rs` to `builtin/scheduler.rs` and rework it to the
design above: `drivers`, `register_driver`, `wake_drivers`, `tick`, `run`, `is_empty`, `forget`
and `BATCH` go; the `AtomicBool`s become the fields of `CellState`; the `Vec` of tasks becomes
the keyed `live` map (the object server detaches one task per method call, and a linear scan
per completion is quadratic over a burst); `spawn` takes `name`. Every comment that names a
"driver", "external reactor", "host", the async-io backend or any other runtime is rewritten
for this design: the scheduler feeds one worker thread which it notifies.

- [ ] **Step 4: Run the tests**

```sh
cargo test -p zbus --lib runtime::builtin::scheduler
cargo test -p zbus --release --lib runtime::builtin::scheduler
```

Expected: all pass.

- [ ] **Step 5: Verify and commit**

Per-commit checks. Commit `✨ zb: Add a task scheduler with no thread of its own`: body explains
the cell/queue design, that a wake from any thread enqueues and notifies, cancel on drop,
detach, the panic policy (ruling 7), the lock order and why no lock is held around a poll or a
drop, and why there is no `unsafe` and no atomic.

### Task 6: The reactor and the platform poll

**Files:**
- Create: `zbus/src/runtime/builtin/reactor.rs`, `zbus/src/runtime/builtin/poll/mod.rs`,
  `zbus/src/runtime/builtin/poll/unix.rs`, `zbus/src/runtime/builtin/poll/windows.rs`
- Modify: `zbus/src/runtime/builtin/mod.rs` (module lines), `Cargo.toml` (the workspace
  `rustix` features: add `"event"` and `"pipe"` to `["net", "process", "std"]`), `Cargo.lock`

**Interfaces:**
- Consumes: `IoSource` (`AsFd` and `Clone` on unix, `AsSocket` on Windows), `Interest`,
  `traits::PollIo`, `rustix::event::{poll, PollFd, PollFlags, Timespec}` (`Timespec` is
  re-exported by the `event` module; `rustix::time` is a separate, unused feature),
  `rustix::pipe::{pipe_with, PipeFlags}`, `rustix::io::{read, write, Errno}`,
  `windows_sys::Win32::Networking::WinSock::{select, FD_SET, TIMEVAL, SOCKET, SOCKET_ERROR,
  WSAGetLastError}` (the imports `zbus/src/runtime/io/connect.rs` already uses for its
  zero-timeout `select`), the shapes of `zbus/tests/polling_runtime/runtime/{io.rs,timer.rs}`,
  and `worker::on_worker_thread()` from Task 7 (a `pub(super) fn` returning `false` until the
  worker exists; Task 6 adds it to `builtin/mod.rs` as a stub that Task 7 moves).
- Produces (`pub(super)`):

```rust
// poll/mod.rs
/// What to watch a source for.
pub(super) struct Want {
    pub(super) key: usize,
    pub(super) readable: bool,
    pub(super) writable: bool,
}
/// What a source was found ready for; `Want`'s shape.
pub(super) struct Ready {
    pub(super) key: usize,
    pub(super) readable: bool,
    pub(super) writable: bool,
}
pub(super) struct Poller { /* platform */ }
impl Poller {
    pub(super) fn new() -> io::Result<Self>;
    /// Breaks a `wait` in progress or the next one; callable from any thread.
    pub(super) fn notify(&self) -> io::Result<()>;
    /// Waits until a wanted source is ready, `notify` is called or `timeout` passes; `None`
    /// waits without limit. The sources are held for the whole call, so no descriptor in the
    /// set can close under it.
    pub(super) fn wait(
        &self,
        sources: &[(IoSource, Want)],
        timeout: Option<Duration>,
    ) -> io::Result<Vec<Ready>>;
}

// reactor.rs
pub(super) struct Reactor {
    poller: Poller,
    sources: Mutex<Sources>,   // HashMap<usize, Arc<SourceState>> and the next key
    timers: Mutex<Timers>,     // BTreeMap<(Instant, u64), Waker> and the next id
}
struct SourceState { source: IoSource, wakers: Mutex<Wakers>, key: usize }
#[derive(Default)]
struct Wakers { readable: Option<Waker>, writable: Option<Waker> }
pub(super) struct RegisteredIoSource { reactor: Arc<Reactor>, state: Arc<SourceState> }
pub(super) struct Sleep { reactor: Arc<Reactor>, deadline: Instant, id: Option<u64> }
impl Reactor {
    pub(super) fn new() -> io::Result<Self>;
    /// Wakes a worker inside its wait, unless called from that worker, which sees every
    /// change before its next wait anyway.
    pub(super) fn notify(&self);
    pub(super) fn register(self: &Arc<Self>, source: IoSource) -> io::Result<RegisteredIoSource>;
    pub(super) fn sleep(self: &Arc<Self>, duration: Duration) -> Sleep;
    /// One wait on the poller, bounded by `at_most` and by the nearest deadline, then the
    /// wakes for what it found ready and for the timers that are due.
    pub(super) fn wait(&self, at_most: Option<Duration>) -> io::Result<()>;
    /// Wakes every stored waker, sources and timers alike; what a failed wait falls back on,
    /// so that each waiter retries its operation and sees its own error.
    pub(super) fn wake_everything(&self);
    /// No registered source and no pending timer.
    pub(super) fn is_idle(&self) -> bool;
}
impl traits::PollIo for RegisteredIoSource { ... }
impl Future for Sleep { type Output = (); }
impl Drop for Sleep { /* removes its deadline */ }
impl Drop for RegisteredIoSource { /* removes the source, then notifies */ }
```

Lock order: `sources` before a `SourceState`'s `wakers`; `timers` on its own. No lock is held
across the poller's wait or across a wake.

- [ ] **Step 1: Write the tests** in `reactor.rs` (`#[cfg(test)] mod tests`, gated `unix` where
  they use socket pairs). Use `std::os::unix::net::UnixStream::pair()` turned into `IoSource`
  via `IoSource::from(OwnedFd::from(stream))`, set non-blocking. A counting waker is an
  `Arc<Counter>` with `impl Wake` that increments a `Mutex<usize>`. Tests:
  - `a_readable_source_wakes_its_waker`: register one end, `poll_io(Readable, read)` returns
    Pending with a counting waker; write on the other end; `reactor.wait(Some(1s))`; the count
    is 1; the next `poll_io` reads the byte.
  - `a_source_written_before_registration_is_seen`: write first, register, `poll_io` succeeds
    at once.
  - `notify_breaks_a_wait`: a thread calls `notify()` after 50 ms; `wait(None)` returns within
    the second.
  - `a_wait_ends_at_the_nearest_deadline`: `sleep(20ms)` polled once with a counting waker;
    `wait(None)` returns within 500 ms with the count at 1, and `is_idle()` after.
  - `a_dropped_sleep_leaves_no_deadline`: poll once, drop, `is_idle()`.
  - `a_dropped_registration_stops_the_watch`: register, want readable, drop the registration,
    write on the peer, `wait(Some(50ms))` returns with the count at 0 and `is_idle()`.
  - `two_sleeps_with_one_deadline_both_fire`: two `sleep(Duration::from_millis(5))` created
    back to back, both polled, one `wait(None)` afterwards; both counters are 1. (A zero
    duration resolves on its first poll without touching the map, so it proves nothing here.)
  - `a_failed_wait_wakes_everything`: with a registered source wanting readable and a polled
    sleep, `wake_everything()` brings both counts to 1.

- [ ] **Step 2: Run to verify failure** (`cargo test -p zbus --lib runtime::builtin::reactor`):
  compile error.

- [ ] **Step 3: The unix poller** (`poll/unix.rs`):

```rust
//! The wait on unix: `poll(2)` over the sources' descriptors and a pipe that breaks the wait.

use std::{
    io,
    os::fd::{AsFd, OwnedFd},
    time::Duration,
};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    io::{Errno, read, write},
    pipe::{PipeFlags, pipe_with},
};

use super::{Ready, Want};
use crate::runtime::IoSource;

pub(super) struct Poller {
    wake_read: OwnedFd,
    wake_write: OwnedFd,
}

impl Poller {
    pub(super) fn new() -> io::Result<Self> {
        let (wake_read, wake_write) = pipe_with(PipeFlags::CLOEXEC | PipeFlags::NONBLOCK)?;
        Ok(Self { wake_read, wake_write })
    }

    pub(super) fn notify(&self) -> io::Result<()> {
        match write(&self.wake_write, &[1]) {
            // A full pipe holds a wake-up already.
            Ok(_) | Err(Errno::AGAIN) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) fn wait(
        &self,
        sources: &[(IoSource, Want)],
        timeout: Option<Duration>,
    ) -> io::Result<Vec<Ready>> {
        let mut fds = Vec::with_capacity(sources.len() + 1);
        fds.push(PollFd::new(&self.wake_read, PollFlags::IN));
        for (source, want) in sources {
            let mut flags = PollFlags::empty();
            if want.readable {
                flags |= PollFlags::IN;
            }
            if want.writable {
                flags |= PollFlags::OUT;
            }
            fds.push(PollFd::new(source, flags));
        }
        // A duration too long for a `Timespec` is as good as no limit.
        let timeout = timeout.and_then(|t| Timespec::try_from(t).ok());
        match poll(&mut fds, timeout.as_ref()) {
            Ok(_) => {}
            Err(Errno::INTR) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        }
        if fds[0].revents().contains(PollFlags::IN) {
            let mut buf = [0u8; 64];
            while read(&self.wake_read, &mut buf).is_ok_and(|n| n == buf.len()) {}
        }
        Ok(sources
            .iter()
            .zip(&fds[1..])
            .filter_map(|((_, want), fd)| {
                let revents = fd.revents();
                let hung_up = revents.intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL);
                let readable = revents.contains(PollFlags::IN) || hung_up;
                let writable = revents.contains(PollFlags::OUT) || hung_up;
                (readable || writable).then_some(Ready { key: want.key, readable, writable })
            })
            .collect())
    }
}

impl AsFd for Poller {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.wake_read.as_fd()
    }
}
```

`PollFd::new` borrows the `IoSource` clone the caller holds for the whole call, so there is no
`unsafe` here and no descriptor in the set can be closed by another thread while `poll` runs.
rustix 1.1 (the workspace's) takes `Option<&Timespec>` and rounds a sub-millisecond timeout up
where the platform `poll` counts milliseconds.

- [ ] **Step 4: The Windows poller** (`poll/windows.rs`): the same shape over `select`. Three
  `FD_SET`s are filled per call: `readfds` with the wake socket and every source wanting
  readable; `writefds` and `exceptfds` both with every source wanting writable, because Winsock
  reports a connect that failed in `exceptfds` and one that succeeded in `writefds`. The set
  size is `FD_SETSIZE` (64) entries, so `Reactor::register` on Windows refuses the 64th source
  with `io::Error::other("this runtime watches at most 63 sockets")` and `wait` never sees
  more. The timeout is a `TIMEVAL` (`tv_sec`, `tv_usec`, both `i32`): seconds capped at
  `i32::MAX`, the sub-second part rounded *up* to whole microseconds so a deadline close ahead
  is never turned into a zero wait; a null pointer for `None`. The wake pair comes from
  `std::net::TcpListener::bind("127.0.0.1:0")`, `TcpStream::connect` to its address, then
  `accept`, both halves `set_nonblocking(true)` and kept as `OwnedSocket`s; `notify` sends one
  byte with `WouldBlock` treated as a wake-up already pending; the reader is drained when
  `readfds` holds it. A source is readable if the read set holds it after the call, writable
  if the write set or the except set does (`FD_ISSET` is a C macro that windows-sys does not
  export: scan `fd_array[..fd_count as usize]`). The single
  `unsafe { select(0, &mut read, &mut write, &mut except, timeout) }` carries
  `// SAFETY: the three sets and the timeout are live locals for the duration of the call, and
  every socket in them belongs to an \`IoSource\` the caller holds for the whole call.`
  `SOCKET_ERROR` is turned into `io::Error::from_raw_os_error(WSAGetLastError())`. Check it
  with `cargo check --target x86_64-pc-windows-gnu -p zbus --all-targets --features p2p`.

- [ ] **Step 5: `poll/mod.rs`** declares `#[cfg(unix)] mod unix; #[cfg(unix)] pub(super) use
  unix::Poller;` and the Windows pair, and defines `Want` and `Ready`.

- [ ] **Step 6: The reactor** (`reactor.rs`): port `zbus/tests/polling_runtime/runtime/io.rs`
  and `timer.rs` into one file with these differences:

  - `wait(at_most)`, in this order: lock `sources`, and for every state whose `wakers` holds an
    entry push `(state.source.clone(), Want { .. })` onto a local `Vec`; unlock. Lock `timers`,
    read the first key's `Instant`; unlock; the timeout is the smaller of `at_most` and the time
    until that deadline (`saturating_duration_since(Instant::now())`), or whichever exists.
    Call `poller.wait(&wants, timeout)`. Lock `sources` and for each `Ready` take the matching
    wakers out of that state's `wakers` (readable and writable independently; a hung-up source
    is reported as both) into a local `Vec`; unlock; wake them. Then the timers: lock `timers`,
    `split_off(&(Instant::now(), u64::MAX))` as the reference does; unlock; wake what was due.
  - `poll_io` tries the operation first; on `WouldBlock` it stores the waker for the interest
    under that state's `wakers` and calls `reactor.notify()`, so a worker inside its wait
    rebuilds its wants. Level-triggered `poll(2)` needs no re-arm call: every wait builds its
    wants from the stored wakers, and a waker taken by a wake is stored again by the next
    `poll_io` that gets `WouldBlock`.
  - `notify` is `if !super::worker::on_worker_thread() { let _ = self.poller.notify(); }`: the
    worker sees every change made on its own thread before its next wait, so a wake to itself
    would only cost a spurious loop.
  - `Sleep::poll`: `Ready` at or past the deadline; else lock `timers`, take an id on the first
    poll, insert `(deadline, id) -> waker` (a later poll replaces the waker under the same key),
    note whether the key is the map's first, unlock, and call `notify` when it is and this was
    the first poll: a deadline behind the map's first changes nothing about the current wait.
    `Sleep::drop` removes its key and calls `notify` if it was the first: a worker waiting for
    nothing but that deadline can retire at once instead of at the stale deadline.
  - `RegisteredIoSource::drop` locks `sources`, removes its entry, unlocks, then calls
    `notify`. The `IoSource` clone inside the state is released when the last `Arc<SourceState>`
    goes, after any wait holding a clone returns, so the registration has stopped watching
    before the descriptor closes.
  - `wake_everything` takes every stored waker (sources, then timers) under the locks into a
    local `Vec` and wakes them after unlocking.
  - `is_idle` is `sources.states.is_empty() && timers.pending.is_empty()`, each under its lock.

  The module doc explains level-triggered polling, why the wants are rebuilt on every wait, the
  lock order, and that no lock is held across the wait or a wake.

- [ ] **Step 7: Run the tests**

```sh
cargo test -p zbus --lib runtime::builtin
```

Expected: scheduler and reactor tests pass.

- [ ] **Step 8: Verify and commit**

Per-commit checks including both cross-target checks. Commit `✨ zb: Add a reactor over poll(2)
and select with its own timers`: body: what it watches and why `poll` suffices at zbus's
descriptor counts, the wake pipe/pair, level-triggered semantics and the rebuilt wants, the
timer map and when a timer change wakes the worker, the lock order and why no lock spans the
wait, the owned wait list and the drop-order guarantee (`traits::Runtime::register_io_source`
doc: a registration stops watching before the source is released), and the `rustix` features
added.

### Task 7: The worker loop

**Files:**
- Create: `zbus/src/runtime/builtin/worker.rs`, `zbus/src/runtime/builtin/tests.rs`
- Modify: `zbus/src/runtime/builtin/mod.rs` (the `Builtin` type, `Inner`, start/exit)

**Interfaces:**
- Consumes: `Scheduler`, `Reactor`, `JoinHandle`, `RegisteredIoSource`, `Sleep` from Tasks 5
  and 6; `traits::Runtime`, `traits::TaskHandle`; the thread-start discipline of the current
  `zbus/src/runtime/async_io.rs` (`ensure_thread`, `thread_running`, the flag set after the
  spawn succeeds).
- Produces:

```rust
#[derive(Clone)]
pub(crate) struct Builtin { inner: Arc<Inner> }
pub(super) struct Inner {
    scheduler: Arc<Scheduler>,
    reactor: Arc<Reactor>,
    /// Whether a worker thread is running; the lock the start and exit decisions are made under.
    worker: Mutex<bool>,
}
impl Builtin {
    pub(crate) fn new() -> io::Result<Self>;   // the reactor's pipe or wake pair can fail
    #[cfg(test)]
    pub(super) fn worker_running(&self) -> bool;
}
impl fmt::Debug for Builtin { /* the running flag */ }
impl traits::Runtime for Builtin {
    type RegisteredIoSource = reactor::RegisteredIoSource;
    type Sleep = reactor::Sleep;
    type Task<T> = Task<T> where T: Send + 'static;
    // register_io_source, sleep and spawn each call `inner.ensure_worker()` after the reactor
    // or scheduler has the new work; spawn_blocking is not overridden (ruling 4).
}
pub(crate) struct Task<T>(JoinHandle<T>);  impl TaskHandle<T> for Task<T>
pub(super) fn on_worker_thread() -> bool;   // worker.rs; a thread-local the worker sets
pub(super) const THREAD_NAME: &str = "zbus runtime";   // 12 bytes: fits Linux's comm
```

- [ ] **Step 1: Write the tests** in `builtin/tests.rs` (declared `#[cfg(test)] mod tests;` in
  `builtin/mod.rs`). They drive futures with `futures_lite::future::block_on`, and a "worker
  gone" check polls `worker_running()` every 10 ms for up to a second. Tests, by name:
  `a_spawned_task_runs_to_completion`, `a_spawned_task_hands_its_output_back`,
  `the_worker_starts_on_the_first_spawn` (the task reads `thread::current().name()` and hands
  it back: `Some("zbus runtime")`),
  `the_worker_exits_when_nothing_is_left` (a task that sleeps 50 ms, awaited; then the worker
  is gone),
  `cancelling_the_last_task_from_another_thread_lets_the_worker_exit` (spawn `pending()`, drop
  the handle on the test thread; the worker is gone),
  `a_registration_reports_readiness` (unix, socket pair),
  `sleep_resolves_once_the_duration_has_passed`,
  `a_sleep_armed_from_another_thread_bounds_the_wait` (register a socket so the worker sits in
  a wait with no deadline; then `block_on(runtime.sleep(20ms))` on the test thread returns
  within 500 ms),
  `a_dropped_sleep_lets_the_worker_exit` (a `sleep(10s)` polled once then dropped; the worker
  is gone within a second, not ten),
  `a_spawn_during_the_worker_exit_is_not_stranded` (200 times: spawn a trivial task and await
  it, no pause between; each completes within 5 s),
  `a_registration_without_tasks_keeps_the_worker_alive`,
  `a_task_that_panics_leaves_the_worker_running` (one panics, then one returns 7),
  `a_panic_in_a_futures_drop_leaves_the_worker_running` (a task whose future completes on its
  first poll and whose `Drop` panics, so the worker drops it; the next task runs),
  `a_cancelled_futures_drop_may_spawn_on_the_runtime` (the future's `Drop` spawns another
  task through a captured `Builtin` clone; cancelled from the test thread while idle; the
  spawned task completes),
  `a_worker_that_dies_is_replaced` (a `sleep(1 ms)` is polled on the test thread with a waker
  whose `wake` panics; the worker fires it and unwinds; within a second `worker_running()` is
  false, and a fresh spawn then runs to completion on a new worker).

- [ ] **Step 2: Run to verify failure**: compile error (no `Builtin`).

- [ ] **Step 3: Write `worker.rs` and `mod.rs`**

`worker.rs`:

```rust
//! The thread a built-in runtime runs on: ready tasks first, then one wait on the reactor
//! bounded by the nearest deadline, until nothing is left to run, watch or time.

use std::{cell::Cell, sync::Arc, thread, time::Duration};

use super::Inner;

/// How many ready tasks run between two looks at the reactor, so a busy task queue cannot
/// starve a socket or a timer.
const BATCH: usize = 32;

pub(super) const THREAD_NAME: &str = "zbus runtime";

thread_local! {
    static ON_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// Whether the calling thread is a runtime's worker.
pub(super) fn on_worker_thread() -> bool {
    ON_WORKER.with(Cell::get)
}

pub(super) fn run(inner: Arc<Inner>) {
    ON_WORKER.with(|flag| flag.set(true));
    let _guard = ClearOnUnwind(&inner);
    let mut failed_waits = 0u32;
    loop {
        for _ in 0..BATCH {
            if !inner.scheduler.run_one() {
                break;
            }
        }
        let at_most = inner.scheduler.has_ready().then_some(Duration::ZERO);
        match inner.reactor.wait(at_most) {
            Ok(()) => failed_waits = 0,
            Err(e) => {
                failed_waits += 1;
                if failed_waits == 1 {
                    #[cfg(feature = "tracing")]
                    tracing::error!("the runtime's wait failed: {e}");
                    #[cfg(not(feature = "tracing"))]
                    let _ = e;
                }
                // Every waiter retries its own operation and sees its own error; the pause
                // keeps a wait that fails every time from becoming a spin.
                inner.reactor.wake_everything();
                thread::sleep(Duration::from_millis(1 << failed_waits.min(10)));
            }
        }
        if inner.retire_if_idle() {
            return;
        }
    }
}

/// Clears the running flag if the worker unwinds, so that the next spawn, registration or
/// sleep starts a worker instead of waiting on one that is gone.
struct ClearOnUnwind<'a>(&'a Inner);

impl Drop for ClearOnUnwind<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            *super::lock(&self.0.worker) = false;
        }
    }
}
```

In `mod.rs`:

```rust
impl Inner {
    /// Marks the worker as gone if it has nothing left to do. Under the `worker` lock, so that
    /// a spawn or a registration racing with the exit either is seen here or starts a worker
    /// itself once the lock is released.
    fn retire_if_idle(&self) -> bool {
        let mut running = lock(&self.worker);
        let busy = self.scheduler.has_ready()
            || self.scheduler.live_tasks() > 0
            || !self.reactor.is_idle();
        if busy {
            return false;
        }
        *running = false;
        true
    }

    /// Starts the worker unless one is running. Called after the work it is to see is in
    /// place (a task queued, a source registered, a deadline stored), never before.
    fn ensure_worker(self: &Arc<Self>) {
        let mut running = lock(&self.worker);
        if *running {
            return;
        }
        let inner = self.clone();
        std::thread::Builder::new()
            .name(worker::THREAD_NAME.into())
            .spawn(move || worker::run(inner))
            .expect("the thread a connection's runtime runs on");
        *running = true;
    }
}
```

The flag is set after the spawn succeeds: a failed spawn panics with the flag still clear, so
the next call tries again. `Builtin::new` builds the reactor (`Reactor::new()?`) and a
scheduler whose `notify` closure is `move || reactor.notify()` over an `Arc<Reactor>` (nothing
points back from the reactor to the scheduler, so no cycle). `spawn` hands the future to the
scheduler (which queues it and notifies) and then calls `ensure_worker`; `register_io_source`
registers and then calls `ensure_worker`; `sleep` builds the `Sleep` and calls `ensure_worker`
(a sleep never polled costs at most a worker that starts, finds nothing and retires).
`Task<T>` wraps `JoinHandle<T>`; its `detach` is the handle's. The module doc of
`builtin/mod.rs` describes the three parts and the one thread to a reader who knows the
`traits::Runtime` contract, and states the per-connection cost of ruling 9.

- [ ] **Step 4: Run the tests**

```sh
cargo test -p zbus --lib runtime::builtin
for i in $(seq 10); do
    cargo test -p zbus --release --lib a_spawn_during_the_worker_exit_is_not_stranded || break
done
```

Expected: pass, ten times over for the exit race.

- [ ] **Step 5: Verify and commit**

Per-commit checks. Commit `✨ zb: Run a built-in runtime's tasks and reactor on one worker`:
body: the loop, the batch bound, the start/exit discipline and the race it closes (work is
made visible before the flag is checked, on both sides), what keeps the worker alive, what a
failed wait does, and what an unwinding worker leaves behind.

### Task 8: Put the built-in runtime behind the connection

**Files:**
- Modify: `zbus/src/runtime/mod.rs` (`AsyncIo` arm → `Builtin`, `mod builtin` gated on the
  feature, module doc), `zbus/src/runtime/task.rs`, `zbus/src/runtime/io/mod.rs`
  (`Registration::AsyncIo` → `Builtin`), `zbus/src/utils.rs` (`block_on`),
  `zbus/src/runtime/test_runtime.rs` (`spawn_blocking` via `blocking_thread::run`),
  `zbus/src/connection/mod.rs` (the test `unix_p2p_async_io_backend`),
  `zbus/tests/builder_message_stream.rs` (the test
  `build_message_stream_does_not_drop_pipelined_hello_async_io`)
- Delete: `zbus/src/runtime/async_io.rs`, `zbus/src/runtime/unblock.rs`

- [ ] **Step 1: Switch the arms**

`Runtime::AsyncIo(AsyncIo)` becomes `Runtime::Builtin(Builtin)` (variant and every match
arm in `mod.rs`, `timeout.rs` if it matches, `task.rs`, `io/mod.rs`); `default_for_build`'s
async-io arm becomes `Ok(Self::Builtin(Builtin::new()?))` (`crate::Error: From<io::Error>`).
`mod builtin` is gated `#[cfg(feature = "async-io")]`; feature gates stay `feature = "async-io"`
in this commit (Task 9 renames). `utils::block_on`: the `async-io` arm becomes the same
`futures_lite::future::block_on` as the no-backend arm; merge the two into one
`#[cfg(not(feature = "tokio"))]` function with a doc line saying the caller's future is all
there is to poll, the connection's tasks running on its runtime's thread.

- [ ] **Step 2: Remove the pool**

Delete `unblock.rs` and its `mod` line; `test_runtime.rs` implements `spawn_blocking` with
`super::blocking_thread::run(work)` around its `blocking_calls` counter.

- [ ] **Step 3: The two tests that name the backend**

`unix_p2p_async_io_backend` in `zbus/src/connection/mod.rs` (gated on `tokio` and `async-io`
together) proves that a connection built outside any Tokio context lands on this backend even
with Tokio compiled in: rename it `unix_p2p_builtin_runtime_backend`, assert
`Runtime::Builtin(_)`, drive it with `futures_lite::future::block_on` (which stays outside
Tokio, unlike `crate::utils::block_on` on a Tokio build) and replace its
`async_io::Timer::after(Duration::from_secs(5))` with `client1.runtime().sleep(..)` on one of
the connections, whose timer is the one under test. In `zbus/tests/builder_message_stream.rs`,
`build_message_stream_does_not_drop_pipelined_hello_async_io` becomes `..._builtin_runtime` on
`futures_lite::future::block_on` (an integration test may use the package's own dependencies).

- [ ] **Step 4: Tests and the gate**

All three suites, both doctest runs, then:

```sh
cargo bench -p zbus --features p2p --bench runtime -- --baseline async-io
cargo bench -p zbus --features p2p --bench concurrent_method_calls -- --baseline async-io
```

three times each, as ruling 3 says. Record the tables in `progress.md` under "After Task 8".
Ruling 3's gate: `method-call/roundtrip` or `method-call/1000-concurrent-p2p` slower by more
than 10% (median of three) stops the plan here with a report to the maintainer; otherwise
continue. `blocking-hook/peer-credentials` is reported, not gated (ruling 4).

- [ ] **Step 5: Verify and commit**

Per-commit checks, the three suites and the doctests. One commit
`♻️ zb: Run the default connection on zbus's own runtime`: body: what the connection now gets
its readiness, timers and tasks from, the one thread and one pipe per connection and when the
thread exists, the blocking hook being the trait's default, what `block_on` in the blocking API
polls, the two test edits, and the benchmark result against the baseline (numbers).

### Task 9: Rename the feature and drop the crates

**Files:**
- Modify: `zbus/Cargo.toml` (features, dependencies, dev-dependencies, `[[example]]`
  `required-features`, `[package.metadata.docs.rs]`), `Cargo.toml` (the workspace `blocking`
  line; `async-io`, `async-executor`, `async-task` and `polling` stay as dev-dependencies of
  `zbus`, and `async-io` of `zbus_macros`), `Cargo.lock`, every `cfg(feature = "async-io")` in
  `zbus/src`, `zbus/tests`, `zbus/examples`, `zbus/benches`, `test_fixtures/*/Cargo.toml`
  (`zbus/async-io` → `zbus/builtin-runtime`), `.github/workflows/rust.yml`,
  `zbus/examples/watch-systemd-jobs.rs` (`async_io::block_on` → `zbus::block_on`).

- [ ] **Step 1: Cargo**

`cargo remove -p zbus async-io async-executor async-task blocking` (normal dependencies) and
`cargo remove --dev -p zbus blocking`. Replace the `async-io` feature with:

```toml
# The runtime zbus ships: readiness, timers and tasks on one thread per connection (default).
# `tokio` adds the Tokio backend, which a connection built inside a Tokio runtime prefers.
builtin-runtime = ["comms"]
```

and `"async-io"` in `default`, in `[[example]] watch-systemd-jobs`'s `required-features` and in
`[package.metadata.docs.rs]` with `"builtin-runtime"` (`vsock` never listed `async-io`; those
three are the only manifest sites). Remove the workspace `blocking` line.

- [ ] **Step 2: Sweep**

```sh
/usr/bin/grep -rn 'feature = "async-io"\|async-io\|async_io' \
    --include=*.rs --include=*.toml --include=*.yml --include=*.md . \
    | /usr/bin/grep -v "^./5.x\|^./target\|^./docs/superpowers\|CHANGELOG"
```

Every code, manifest and CI hit is renamed or removed (the book and the doc comments are
Task 10; the dev-dependency lines and the test doubles that use them stay). In `rust.yml`, the
guard that runs `cargo tree` on the external-only build (the `FORBIDDEN=` lines) keeps all
nine names it has (`async-io|async-executor|async-task|async-process|blocking|polling|
async-channel|async-signal|piper`), adds `async-lock`, and is widened to run on the default
build and on the Tokio build too, with `-e normal` (dev-dependencies excluded).

- [ ] **Step 3: Verify**

Per-commit checks with `builtin-runtime` in place of `async-io`; the three suites; and:

```sh
GONE="^(async-io|async-executor|async-task|async-lock|blocking|polling|async-channel|piper) "
cargo tree -p zbus -e normal --prefix none | /usr/bin/grep -E "$GONE"; echo "exit $?"
cargo tree -p zbus -e normal --prefix none --no-default-features \
    --features tokio,proxy,service | /usr/bin/grep -E "$GONE"; echo "exit $?"
```

Expected: no line, `exit 1`, both times. Then `CI/binary-size.sh` and record the table under
"After Task 9" in `progress.md`.

- [ ] **Step 4: Commit**

One commit, `💥 zb: Replace the async-io feature and crates with builtin-runtime`. The feature
and the crates go together because an optional dependency left with no `dep:` reference makes
Cargo synthesise a feature of the crate's name, which would resurrect `async-io` as a silent
no-op between two commits. Body: the breaking change and why there is no alias, what each of
the four crates did for zbus and what does it now, the widened CI guard, and the `Cargo.lock`
that goes with it.

### Task 10: Documentation

**Files:**
- Modify: `book/src/connection.md` (the "Runtimes" section), `book/src/faq.md`,
  `book/src/upgrading-to-6.md`, `zbus/src/runtime/mod.rs` and `zbus/src/runtime/traits.rs`
  (module docs; both open with "zbus does not ship a runtime of its own"),
  `zbus/src/lib.rs` (crate docs naming `async-io`), `zbus/Cargo.toml` feature comments, the
  doctests that name `async_io`: hidden `# use async_io::block_on;` lines become
  `# use zbus::block_on;`, and the two visible `async_io::Timer` examples in
  `zbus/src/message_stream.rs` and `zbus/src/proxy/mod.rs` become Tokio-driven
  (`# #[tokio::main] async fn main() -> zbus::Result<()> {` and `tokio::time::sleep`; Tokio is
  a dev-dependency with `macros` and `time`).

- [ ] **Step 1**: find every sentence to rewrite:

```sh
/usr/bin/grep -rn "async-io\|async_io\|async-lock\|runtime of its own\|ships no runtime\|smol" \
    --include=*.md --include=*.rs --include=*.toml book/src zbus/src zbus/Cargo.toml \
    zbus/README.md README.md
```

and rewrite each hit: the default runtime is zbus's own, one thread and one wake pipe per
connection, no dependency; Tokio and external runtimes as before. The book section gets a
short paragraph on what the built-in runtime is, that a program needing no runtime of its own
can drive a connection with any `block_on`, and that a program inside a Tokio runtime avoids
the extra thread with the `tokio` feature. (The root `README.md` and `AGENTS.md` have nothing
to change; the grep proves it.)
- [ ] **Step 2**: `mdbook build book` succeeds; the private-doc build passes; and, because
  `zbus/src/lib.rs` compiles the book chapters as doctests, `cargo test -p zbus --doc` and
  `cargo test -p zbus --no-default-features --features builtin-runtime,proxy --doc` pass.
- [ ] **Step 3**: Commit `📝 zb,book: Document the built-in runtime`.

### Task 11: Windows and macOS review

**Files:** none new.

- [ ] **Step 1**: with `F=builtin-runtime,tokio,proxy,service,blocking-api,object-manager,p2p,\
bus-impl,tracing,unixexec,ibus` (every feature but `vsock`, whose `compile_error!` fires off
Linux): `cargo check --target x86_64-pc-windows-gnu -p zbus --all-targets --features $F`,
`cargo check --target x86_64-apple-darwin -p zbus --all-targets --features $F`,
`cargo check --target x86_64-unknown-freebsd -p zbus --all-targets --features $F`.
- [ ] **Step 2**: A read-only review of `poll/windows.rs` against the WinSock documentation of
  `select` (a failed connect lands in `exceptfds`, a made one in `writefds`; `FD_SETSIZE` is 64;
  a call with three empty sets fails with `WSAEINVAL`, which the wake socket in `readfds` rules
  out; return `SOCKET_ERROR` with `WSAGetLastError`; the rounded-up `TIMEVAL`), written to
  `progress.md`. Any finding is fixed and squashed into Task 6's commit with
  `git commit --fixup` + `git rebase --autosquash`.
- [ ] **Step 3**: Push the branch and read the Windows and macOS CI jobs' results; fix and
  squash as above.

### Task 12: Final measurements and the PR

- [ ] **Step 1**: At the tip, three runs each of
  `cargo bench -p zbus --features p2p --bench runtime -- --baseline async-io` and
  `cargo bench -p zbus --features p2p --bench concurrent_method_calls -- --baseline async-io`,
  then `CI/binary-size.sh`, and the thread-count test of the reference runtime
  (`cargo test -p zbus --no-default-features --features proxy,service,p2p --test
  polling_runtime`). Record all four under "Final" in `progress.md`.
- [ ] **Step 2**: Open the PR against `z-galaxy/zbus` `main` from the `zeenix` fork with a body
  that has: `Closes #1959` on its own line; the commit list with one line each; the benchmark
  table before/after (medians from `progress.md`); the size table before/after; the rulings
  that need the maintainer's eye (1: the feature name; 4: the peer-credentials number; 6:
  dev-dependencies kept; 7: the panic policy; 9: the per-connection cost); and the footer
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

---

## Self-review notes

- RFC coverage: dependency removal (Tasks 4, 9), reuse of upstream shapes (Tasks 3, 5, 6 name
  their sources), task machinery (5), reactor and worker (6, 7), locks (3), blocking (8, ruling
  4), dependency guard in CI (9), docs (10), platform checks (11), performance and size
  comparison (1, 2, 8, 12), MSRV (constraints). Not done by design: Miri beyond the locks (the
  scheduler and the reactor have no `unsafe`), a blocking pool (ruling 4), epoll/kqueue/IOCP
  (ruling 2), a worker shared across connections (ruling 9).
- Names used across tasks: `Builtin`, `Inner`, `Scheduler`, `State`, `TaskCell`, `CellState`,
  `Stage`, `Joint`, `JoinHandle`, `Reactor`, `SourceState`, `Wakers`, `RegisteredIoSource`,
  `Sleep`, `Poller`, `Want`, `Ready`, `worker::run`, `worker::THREAD_NAME`,
  `worker::on_worker_thread`, `retire_if_idle`, `ensure_worker`, `worker_running`,
  `live_tasks`, `has_ready`, `run_one`, `wait(at_most)`, `wake_everything`, `is_idle`; the
  feature `builtin-runtime`; the fixtures `geoclue_service_fixture`/`geoclue_client_fixture`
  with features `builtin`/`tokio`; the bench ids listed in Task 1.
- Reviewed on 2026-09-18 by two independent read-only passes (design; repository mechanics)
  plus the author's; every finding they raised is folded in above.
