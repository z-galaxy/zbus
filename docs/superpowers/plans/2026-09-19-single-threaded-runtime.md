# Single-Threaded Built-in Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make zbus's built-in runtime single-threaded: a program that drives its connections
through `zbus::block_on` runs their tasks, readiness and timers on its own thread and zbus starts
no thread at all; one runtime is shared by every connection in the process; a helper thread
exists only where work is left with no thread inside `block_on` to run it.

**Architecture:** The runtime on the PR branch (`builtin-runtime-plan`, PR #1975) keeps its
scheduler, its reactor, its `poll(2)`/`select` backends and its locks. What changes is who runs
them and how many there are. The `Inner` behind `Builtin` becomes one per process, found through
a registry of one `Weak`; `worker.rs` becomes `driver.rs`: a *seat* that one thread at a time
holds, a *round* (a batch of ready tasks, then one wait on the reactor) that the seat's holder
runs, a `block_on` that takes the seat while it polls its future, and a *helper* thread that
takes the seat when a `block_on` leaves work behind or a connection is polled from some other
executor. `zbus::block_on` (without `tokio`) becomes that `block_on`. The public
`runtime::traits` contract and the `Send`/`Sync` bounds on it are unchanged.

**Tech Stack:** Rust 1.87 (MSRV), `rustix`, `windows-sys`, `event-listener`, criterion via
`codspeed-criterion-compat`.

**Spec:** The maintainer's direction of 2026-09-19: "a single-threaded runtime ... keep things
light-weight for typical D-Bus apps (there is always tokio for complicated high-performance
services/apps)", applied to RFC [#1959](https://github.com/z-galaxy/zbus/issues/1959) and the
runtime contract in `docs/superpowers/specs/2026-09-12-external-runtime-design.md`. The RFC's
"one worker per connection" paragraph is superseded by that direction; everything else in it
stands. The design is in "Design" below; it is the spec for this plan.

## Global Constraints

- MSRV 1.87.0; `cargo +nightly fmt --all` clean (nightly fmt options are in use; never ignore its
  warnings); `cargo clippy -- -D warnings` clean on every commit for every feature set listed in
  "Verification per commit" below.
- No `unsafe` outside the FFI call to `select` on Windows and the lock guards' `UnsafeCell`
  access. Every `unsafe` block carries a `// SAFETY:` comment stating the invariant it relies
  on. No `#[allow(...)]` anywhere; fix the cause. No atomic types: a flag or a counter lives
  under the mutex that already guards the state it describes. Thread-locals hold a `Cell`.
- 100 columns in every text file, code and comments alike. Sentences in comments end with `.`.
- Comments and doc comments explain the code to a reader who has never seen this plan, the PR
  or the worker design: never "now", "no longer", "previously", "yet", "still", "so far",
  "instead of a worker", and never a reference to an issue, a review or a commit.
- Tests: no `test_` prefix; feature gates on the test module, never new `[[test]]` entries in
  `Cargo.toml`. Every test in `zbus/src/runtime/builtin/tests.rs` builds its own runtime with
  `Inner::new()` so that tests running in parallel never share one; only the two tests named in
  Task 3 touch the process-wide registry, and they hold the `REGISTRY_TEST` lock while they do.
- Imports: on a name clash import the module (`use module::{self, item}`), never alias.
  Directory modules are `module/mod.rs`. `where` clauses for trait bounds. `value.clone()`, not
  `Arc::clone(&value)`. Usage before definition, `pub` before `pub(crate)` before private.
  Poisoned std mutexes are taken with `unwrap_or_else(PoisonError::into_inner)` through the one
  `lock` helper of `builtin/mod.rs` (`pub(super) fn lock`).
- No dependency changes in this plan. `Cargo.lock` is tracked and CI runs with `--locked`.
- Commits: one logical change each, every hunk covered by the message, no drive-by tidying of
  lines a commit merely passes through. Subject: a gimoji emoji copied verbatim (with its U+FE0F
  where the set has one) + ` zb: ` (or `zb,book: `) + imperative title; commitlint counts UTF-16
  units, so the header is at most 72 of those (an astral-plane emoji such as 📝 counts 2, `♻️`
  counts 2) and body lines at most 74 characters. Body says why. Trailer, exactly this:
  `Assisted-by: Claude Fable 5.1 (claude-fable-5-1)`; never `Co-Authored-By`, never
  `Signed-off-by`, never a session URL. Commit with
  `/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign`. Emoji by kind:
  `✅` tests, `✨` a feature, `♻️` a refactor, `🐛` a fix, `📝` docs, `🔥` code removed.
- Tooling in this environment: `git`, `grep` and `cargo` on `PATH` are shims with altered
  output; use `/usr/bin/git`, `/usr/bin/grep`, `~/.cargo/bin/cargo`. Only one cargo at a time:
  wrap every cargo invocation in `flock /tmp/claude-1000/cargo.lock` with
  `CARGO_TARGET_DIR=/home/zeenix/checkout/z-galaxy/zbus/target`. The untracked directory `5.x/`
  at the repository root is an old checkout: every repository-wide grep excludes it
  (`| /usr/bin/grep -v "^./5.x\|^./target\|^./docs/superpowers"`).
- Work on the branch `builtin-runtime-plan` (tip 5ef16917 at the time of writing, the branch
  PR #1975 is opened from), new commits on top. Push to the `zeenix` remote only, as
  `builtin-runtime`, with `--force-with-lease`. The threaded design is preserved on the branches
  `builtin-runtime-threaded` and `builtin-runtime-threaded-reactor-waiting`; never touch them.

---

## Design

### Why change the runtime rather than write another

Of the runtime's 2 200 lines, the scheduler (740), the reactor (700), the two poll backends
(330) and the locks are indifferent to which thread runs them: a scheduler hands out ready
tasks to whoever asks, and the reactor's `wait` is a function any thread can call. What ties the
runtime to a thread is 96 lines of `worker.rs` and the `ensure_worker` lifecycle in `mod.rs`
(60 lines). A rewrite would produce the same scheduler, reactor and backends again, and then
face the same question of who waits on the descriptors when no thread of the program is in a
position to. So: replace `worker.rs` with `driver.rs`, make `Inner` process-wide, and leave the
rest as it is.

### Why the `Send` and `Sync` bounds stay

A single-threaded runtime does not let the bounds on `traits::Runtime::spawn`, `Interface` or
the proxies go. `Connection` is `Clone + Send + Sync`, and any thread holding a clone may be the
one to enter `zbus::block_on` and run the tasks, so a task migrates between threads across
`block_on` calls even though it never runs on two at once; that is exactly what `Send` promises.
Dropping the bounds needs a connection confined to one thread (a `!Send` `Connection`), which
is an API decision of its own, not a runtime one, and the Tokio backend needs them regardless.
The bounds cost nothing at run time. Nothing in this plan touches them.

### The seat

`Inner { scheduler, reactor, seat: Mutex<Seat> }`, one per process, created by the first
`Builtin::new()` and dropped once its last handle and its helper are gone (a `static` holds a
`Weak`). `Seat` says who is running the scheduler and the reactor:

- `holder: Holder` — `Nobody`, `BlockOn` (a thread inside `block_on`) or `Helper`.
- `waiting: HashMap<ThreadId, Thread>` — threads parked in `block_on` for want of the seat.
  Every one of them is unparked whenever the seat is freed, and the map emptied; a spurious
  unpark costs one extra turn of a `block_on` loop and nothing else.
- `helper: bool` — whether the helper thread is up. Set under the lock where the thread is
  started, cleared by the helper under the lock where it decides to leave (or on unwind).

A thread that holds the seat has the reactor's address in the thread-local `DRIVER_REACTOR`
(what `on_worker_thread` is today, renamed), so `Reactor::notify` from that thread writes
nothing: the holder looks at the queue, the maps and, for `block_on`, its own `woken` flag
before every wait.

### `block_on`

```text
loop {
    if no seat yet: try to take it (if held, register in `waiting`)
    poll the future; return on Ready
    if in the seat: run a round — up to BATCH ready tasks, then, unless `woken` is set, one
        `reactor.wait(at_most)` with `at_most = ZERO` when a task is still ready
    else: park, unless `woken` is set
}
```

The seat is taken *before* the first poll, so a connection built inside the future finds a
driver in the seat and starts no helper. The future's waker sets `woken`, unparks the thread and
calls `reactor.notify()`; the notify writes nothing when the caller is the seat's holder (a task
on this thread woke this future), and one byte, deduplicated by the reactor's `wake_pending`
flag, otherwise. When the future completes the seat is freed: everyone waiting is unparked, and
if the runtime is busy (a task ready or live, a source registered, a timer pending) and no
helper is up, a helper is started, because whatever is left may be awaited by a thread that is
not going to drive.

A `block_on` on a thread that is already the seat's holder (called from inside a task the
runtime is running) panics with a message that says so, rather than parking forever.

The thread-local `IN_BLOCK_ON: Cell<bool>` is set for the whole call. `ensure_helper` returns
at once on such a thread: a spawn or a registration made while `block_on` is polling its future
on a runtime that did not exist when the loop last looked for one is picked up when the loop
takes the seat on its next turn.

Why `block_on` does not get priority over the helper: a program on the blocking API calls
`block_on` once per call, with the connection's reader task live in between, so a helper is
started at the first `block_on` return and then owns the seat; every later `block_on` parks and
is unparked when its future is done, which is what the threaded design does today and what the
`blocking-api/roundtrip` number measures. Handing the seat back and forth on every call would
cost two hand-offs per call for no gain. An async-main program never returns from its one
`block_on` while its connection is alive, so it never starts a helper.

### The helper

`helper(inner)` is `worker::run` with the seat: take it (or leave at once if a `block_on` holds
it; that `block_on` asks for a helper again when it frees the seat), then loop: a batch of ready
tasks, retire if idle (under the seat lock: `helper = false`, `holder = Nobody`), otherwise
one wait with the existing failed-wait backoff. `ensure_helper` is what `ensure_worker` is
today, called from the same three places, with two more early returns: the calling thread is
inside `block_on`, or a `block_on` holds the seat.

### Windows

One runtime per process means one `select` per process, and Winsock's `FD_SET` holds 64
sockets. Winsock lets a program raise `FD_SETSIZE` before including its headers, and `select`
reads a set as a count followed by that many sockets; the backend declares that raised set
itself (`FdSet`, 1024 entries, `repr(C)`, the layout of `FD_SET` with a longer array) and hands
`select` a pointer to it. This lands first, since the cap would otherwise bite the moment the
runtime is shared.

### What does not change

The scheduler, the reactor's maps, timers, `wait`, `wake_everything` and `is_idle`, the two
poll backends' wait and notify logic, the `Sleep` wrapper's reason for existing (the first
poll of a timer is what needs a driver), the `Task` handle, the locks, the trait, the erased
external runtime, the Tokio backend, `spawn_blocking`, the benchmarks, the size fixtures, the
CI guard. `zbus::block_on` with the `tokio` feature is Tokio's.

## File Structure

- `zbus/src/runtime/builtin/poll/windows.rs` — `FdSet` replaces `FD_SET` as the value type;
  `MAX_SOURCES = SET_SIZE - 1` (Task 2).
- `zbus/src/runtime/builtin/reactor.rs` — the error message in `register` names the new cap
  (Task 2); `notify` asks `driver::on_driver_thread` (Task 4).
- `zbus/src/runtime/builtin/mod.rs` — `Inner::new`, the `SHARED` registry, `Inner::shared`
  (Task 3); `seat` replaces `worker`, `ensure_progress` calls `driver::ensure_helper`,
  `pub(crate) fn block_on` (Task 4); module docs (Task 5).
- `zbus/src/runtime/builtin/driver.rs` — new: `Seat`, `Holder`, `Driving`, `Signal`,
  `block_on`, `ensure_helper`, `helper`, `on_driver_thread`, `THREAD_NAME` (Task 4).
- `zbus/src/runtime/builtin/worker.rs` — deleted (Task 4).
- `zbus/src/runtime/builtin/tests.rs` — `runtime()` builds a private `Inner`; registry tests
  (Task 3); driver tests, `worker` renamed `helper` in names and docs (Task 4).
- `zbus/src/utils.rs` — `block_on` delegates to the runtime on a `builtin-runtime` build
  (Task 4); its doc (Task 5).
- `zbus/src/runtime/mod.rs`, `book/src/connection.md`, `book/src/upgrading-to-6.md` — docs
  (Task 5).

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

Tests, at the commits the tasks name:

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

(`/tmp/dbus-session.conf` exists from the previous plan; if it does not, copy
`zbus/tests/dbus-session.conf` there, or run without `--config-file`.) The runtime's own tests
are `cargo test -p zbus --features p2p --lib runtime::builtin`; the race-prone ones are run ten
times in release: `cargo test -p zbus --release --lib runtime::builtin::tests -- --test-threads=4`
in a `for i in 1 2 3 4 5 6 7 8 9 10` loop. Known pre-existing failures to ignore: the private-doc
build has three rustdoc errors on `main` that are not in `runtime/`; `cargo check --target
x86_64-unknown-freebsd --all-features` on `vsock`.

---

### Task 1: Commit this plan

**Files:**
- Create: `docs/superpowers/plans/2026-09-19-single-threaded-runtime.md` (this file)

- [ ] **Step 1: Commit**

```sh
/usr/bin/git add docs/superpowers/plans/2026-09-19-single-threaded-runtime.md
/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign -F - <<'EOF'
📝 Add the plan for a single-threaded built-in runtime

The built-in runtime is to run on the thread inside zbus::block_on,
shared by every connection in the process, with a helper thread only
where work is left with nobody to drive it. This is the plan for
getting there from the one-worker-per-connection design.

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
Changelog: skip
EOF
```

---

### Task 2: A `select` set with room for a process's sockets

**Files:**
- Modify: `zbus/src/runtime/builtin/poll/windows.rs` (the whole file: every `FD_SET` value)
- Modify: `zbus/src/runtime/builtin/reactor.rs:96-100` (the error message)
- Test: `cargo check --target x86_64-pc-windows-gnu -p zbus --all-targets --features p2p`; the
  behaviour runs on the PR's `windows_test (default)` CI job.

**Interfaces:**
- Produces: `MAX_SOURCES: usize = 1023` (was 63), `struct FdSet`, `const SET_SIZE: usize`.

- [ ] **Step 1: Read the backend**

Read `zbus/src/runtime/builtin/poll/windows.rs` in full. It builds three `FD_SET` values per
wait (`readable`, `writable` and `excepted`, from `FD_SET::default()` around line 98), fills
them with `push`, offers them through `offered` and reads the answer with `holds`. `FD_SET` and
`FD_SETSIZE` come from `windows_sys::Win32::Networking::WinSock`.

- [ ] **Step 2: Declare the larger set**

Replace the `MAX_SOURCES` constant and its doc (lines 27-31) with:

```rust
/// How many sockets one set holds.
///
/// Winsock's own `FD_SET` holds `FD_SETSIZE` of them, sixty-four, and lets a program raise that
/// number before it includes the headers that declare the set; this is the number the backend
/// raises it to. A wait costs `select` a pass over each set, so the number is kept to what a
/// process with a great many connections needs and no more.
const SET_SIZE: usize = 1024;

/// How many sources one wait can take in.
///
/// A set holds `SET_SIZE` sockets, and the read set keeps one of those places for the socket a
/// `notify` writes to.
pub(in crate::runtime::builtin) const MAX_SOURCES: usize = SET_SIZE - 1;

/// A socket set with room for `SET_SIZE` sockets.
///
/// `select` reads a set as a count followed by that many sockets, and takes the count from the
/// set rather than any length from the type it was declared with; this is the set Winsock's
/// headers declare once `FD_SETSIZE` is raised, laid out as `FD_SET` is with a longer array.
#[repr(C)]
struct FdSet {
    fd_count: u32,
    fd_array: [SOCKET; SET_SIZE],
}

impl FdSet {
    /// An empty set.
    fn new() -> Self {
        Self {
            fd_count: 0,
            fd_array: [0; SET_SIZE],
        }
    }
}
```

Drop `FD_SET` and `FD_SETSIZE` from the `use windows_sys::...` list.

- [ ] **Step 3: Use it everywhere a set is built or read**

Every `FD_SET::default()` becomes `FdSet::new()`; `push(set: &mut FD_SET, ...)` becomes
`push(set: &mut FdSet, ...)`; `holds(set: &FD_SET, ...)` becomes `holds(set: &FdSet, ...)`;
`offered` becomes:

```rust
/// A pointer to `set` for `select`, or a null one where nothing was put in it.
///
/// Winsock reads a null set as one it is asked nothing about, while a set it is handed has to
/// hold at least one socket.
fn offered(set: &mut FdSet) -> *mut FD_SET {
    if set.fd_count == 0 {
        return ptr::null_mut();
    }

    ptr::from_mut(set).cast()
}
```

with `FD_SET` back in the import list for this one type. The `// SAFETY:` comment on the
`select` call gains one sentence: "Each set is an `FdSet`: `repr(C)`, a `u32` count followed by
an array of `SOCKET`, the layout `FD_SET` has, and `select` reads and writes only the entries
the count names, which lie within the array." Update the comment on `push` ("A caller never
offers more than `MAX_SOURCES` sources, which is what keeps every set within its length.") only
if its wording names `FD_SETSIZE`.

- [ ] **Step 4: The reactor's message**

In `zbus/src/runtime/builtin/reactor.rs` `register`, the error becomes:

```rust
        #[cfg(windows)]
        if sources.states.len() >= super::poll::MAX_SOURCES {
            return Err(io::Error::other(format!(
                "this runtime watches at most {} sockets",
                super::poll::MAX_SOURCES
            )));
        }
```

- [ ] **Step 5: Verify**

Run the cross check for Windows, `cargo +nightly fmt --all -- --check`, and the first clippy
line from "Verification per commit" (the unix build is untouched but must still be clean). If
the Windows `cfg` test in `builtin/tests.rs` (the one that registers an AF_UNIX socket beside
the wake socket) mentions 63, update it to `MAX_SOURCES`.

- [ ] **Step 6: Commit**

```sh
/usr/bin/git add zbus/src/runtime/builtin/poll/windows.rs zbus/src/runtime/builtin/reactor.rs \
    zbus/src/runtime/builtin/tests.rs
/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign -F - <<'EOF'
🐛 zb: Let a select on Windows watch more than 63 sockets

Winsock's FD_SET holds sixty-four sockets, one of which the wake
socket takes, and a runtime shared by every connection in a process
runs into that in any program with more than a few dozen of them.
Winsock lets a program raise FD_SETSIZE before including its headers;
this declares the set that raise would declare, with room for 1024.

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
EOF
```

---

### Task 3: One runtime per process

**Files:**
- Modify: `zbus/src/runtime/builtin/mod.rs:54-84` (`Builtin::new`), `:189-204` (`Inner`)
- Modify: `zbus/src/runtime/builtin/tests.rs:443-446` (`runtime()`), new tests at the end
- Test: `cargo test -p zbus --features p2p --lib runtime::builtin`

**Interfaces:**
- Produces: `Inner::new() -> io::Result<Arc<Inner>>` (private instances, for tests),
  `Inner::shared() -> io::Result<Arc<Inner>>`, `Builtin::from_inner(Arc<Inner>) -> Builtin`
  (`#[cfg(test)]`), `Builtin::inner(&self) -> &Arc<Inner>` (`#[cfg(test)]`), and the test lock
  `REGISTRY_TEST: Mutex<()>`. Task 4 keeps all of them.

- [ ] **Step 1: Write the failing tests**

Append to `zbus/src/runtime/builtin/tests.rs`:

```rust
/// Serialises the tests that go through the process-wide registry, so that neither sees the
/// runtime the other holds.
static REGISTRY_TEST: Mutex<()> = Mutex::new(());

#[test]
#[timeout(15000)]
fn every_handle_in_a_process_shares_one_runtime() {
    let _serial = lock(&REGISTRY_TEST);
    let first = Builtin::new().unwrap();
    let second = Builtin::new().unwrap();

    assert!(Arc::ptr_eq(first.inner(), second.inner()));
}

#[test]
#[timeout(15000)]
fn the_shared_runtime_goes_with_its_last_handle() {
    let _serial = lock(&REGISTRY_TEST);
    let handle = Builtin::new().unwrap();
    let inner = Arc::downgrade(handle.inner());

    drop(handle);

    // Nothing was spawned or registered, so no thread holds the runtime either.
    assert!(inner.upgrade().is_none());
    // And the next handle brings a fresh one into being.
    let next = Builtin::new().unwrap();
    assert!(inner.upgrade().is_none());
    drop(next);
}
```

Change the `runtime()` helper so every other test keeps a runtime of its own:

```rust
/// A runtime of this test's own, with nothing to do and no thread until it is given something.
fn runtime() -> Builtin {
    Builtin::from_inner(Inner::new().unwrap())
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p zbus --features p2p --lib runtime::builtin::tests::every_handle`
Expected: compile error, `Inner::new`, `from_inner` and `inner` do not exist.

- [ ] **Step 3: The registry**

In `zbus/src/runtime/builtin/mod.rs`, add `Weak` to the `std::sync` import, and replace
`Builtin::new` and the `Inner` block with:

```rust
impl Builtin {
    /// A handle on the process's runtime, brought into being here if none is alive.
    ///
    /// What can fail is the reactor: it opens the channel a wait is broken through.
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            inner: Inner::shared()?,
        })
    }

    /// A handle on `inner`, whatever registry it is or is not in.
    #[cfg(test)]
    pub(super) fn from_inner(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    /// What this handle is on.
    #[cfg(test)]
    pub(super) fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    /// Whether a worker thread is running.
    #[cfg(test)]
    pub(super) fn worker_running(&self) -> bool {
        *lock(&self.inner.worker)
    }
}
```

and, for `Inner`:

```rust
/// The runtime alive in this process, if one is: the one every [`Builtin`] handle is on.
///
/// A `Weak`, so that the runtime and the two descriptors its reactor holds go once the last
/// handle and any thread running it are gone, and the next handle brings a fresh one.
static SHARED: Mutex<Weak<Inner>> = Mutex::new(Weak::new());

/// What a runtime is made of, shared by every handle on it and by the thread that runs it.
pub(super) struct Inner {
    scheduler: Arc<Scheduler>,
    reactor: Arc<Reactor>,
    /// Whether a worker thread is running; the lock the start and exit decisions are made under.
    worker: Mutex<bool>,
}

impl Inner {
    /// The process's runtime, made here if none is alive.
    fn shared() -> io::Result<Arc<Self>> {
        let mut shared = lock(&SHARED);
        if let Some(inner) = shared.upgrade() {
            return Ok(inner);
        }
        let inner = Self::new()?;
        *shared = Arc::downgrade(&inner);

        Ok(inner)
    }

    /// A runtime with a scheduler and a reactor of its own, in no registry.
    pub(super) fn new() -> io::Result<Arc<Self>> {
        let reactor = Arc::new(Reactor::new()?);
        let scheduler = {
            let reactor = reactor.clone();

            // A task that becomes ready breaks the wait its worker is in. Nothing in the reactor
            // points back at the scheduler, so this hook closes no cycle.
            Arc::new(Scheduler::new(move || reactor.notify()))
        };

        Ok(Arc::new(Self {
            scheduler,
            reactor,
            worker: Mutex::new(false),
        }))
    }
```

(the existing `ensure_worker` and `retire_if_idle` follow, unchanged). `lock` stays where it
is but becomes `pub(super) fn lock` so `tests.rs` and, in Task 4, `driver.rs` share it; check
that `tests.rs` does not define a `lock` of its own (it uses `super::*`).

- [ ] **Step 4: Run the runtime tests**

Run: `cargo test -p zbus --features p2p --lib runtime::builtin`
Expected: all pass, the two new ones included. Then the release loop from "Verification per
commit" once.

- [ ] **Step 5: The connection-level tests**

Run the first test suite line (all features, session bus). The `graceful_shutdown` and
thread-related tests in `zbus/tests/` must pass unchanged: a connection's tasks are still joined
through its own handles, not through the runtime. If a test asserts a per-connection thread
count of the built-in runtime, list it in the report as a plan defect; do not change it.

- [ ] **Step 6: Verify and commit**

Run "Verification per commit" in full.

```sh
/usr/bin/git add zbus/src/runtime/builtin/mod.rs zbus/src/runtime/builtin/tests.rs
/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign -F - <<'EOF'
♻️ zb: Share one built-in runtime across a process's connections

A runtime per connection is a wake pipe and, while it has work, a
thread per connection. One per process is enough: a wait can watch
every connection's socket at once, and a thread that runs one
connection's tasks can run them all. This is the step that makes the
runtime process-wide; which thread runs it is the step after.

The runtime lives as long as a handle or a thread holds it, so a
program that has closed every connection holds no descriptors of it.

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
EOF
```

---

### Task 4: `block_on` drives, a helper stands in

**Files:**
- Create: `zbus/src/runtime/builtin/driver.rs`
- Delete: `zbus/src/runtime/builtin/worker.rs`
- Modify: `zbus/src/runtime/builtin/mod.rs` (`mod worker` → `mod driver`; `Inner.worker` →
  `Inner.seat`; `ensure_worker`/`retire_if_idle` removed; `ensure_progress`; `block_on`;
  `worker_running` → `helper_running`; `Debug`)
- Modify: `zbus/src/runtime/builtin/reactor.rs:62-64` (`on_worker_thread` → `on_driver_thread`)
- Modify: `zbus/src/utils.rs:31-43` (`block_on`)
- Modify: `zbus/src/runtime/builtin/tests.rs` (renames, new tests)
- Test: `cargo test -p zbus --features p2p --lib runtime::builtin`, the release loop, then the
  full suites.

**Interfaces:**
- Consumes: `Inner::new`, `Builtin::from_inner`, `Builtin::inner`, `pub(super) fn lock` from
  Task 3; `Scheduler::{run_one, has_ready, live_tasks}`; `Reactor::{wait, wake_everything,
  is_idle, notify}` as they are.
- Produces: `driver::{Seat, Resolve, block_on, ensure_helper, on_driver_thread, THREAD_NAME}`;
  `Inner::seat: Mutex<Seat>`; `pub(crate) fn builtin::block_on<F: Future>(F) -> F::Output`;
  `Builtin::helper_running() -> bool` (`#[cfg(test)]`); `Inner::is_busy() -> bool`.

- [ ] **Step 1: Write the failing tests**

In `zbus/src/runtime/builtin/tests.rs`, first the renames: every `worker_running` becomes
`helper_running`, `worker_gone` becomes `helper_gone`, and "worker" in test names and doc
comments becomes "helper" (`the_worker_starts_on_the_first_spawn` →
`the_helper_starts_on_the_first_spawn`, and so on). The module doc becomes:

```rust
//! Tests of a built-in runtime as a whole: the helper thread that starts on the first piece of
//! work handed to it from outside `block_on`, the `block_on` that runs the scheduler and the
//! reactor on its own thread, and the hand-over between the two.
```

These existing tests keep using `futures_lite::future::block_on` (imported as it is): they are
the tests of the helper, and a foreign `block_on` is exactly what makes a helper necessary.
Then add, before the helpers at the end:

```rust
/// A `block_on` on `runtime`, resolving to that runtime and no other.
fn drive<F: Future>(runtime: &Builtin, future: F) -> F::Output {
    let inner = runtime.inner().clone();

    driver::block_on(Arc::new(move || Some(inner.clone())), future)
}

#[test]
#[timeout(15000)]
fn block_on_runs_the_tasks_on_its_own_thread() {
    let runtime = runtime();
    let task = runtime.spawn("a task that names the thread it runs on", async {
        thread::current().id()
    });

    let ran_on = drive(&runtime, task).unwrap();

    assert_eq!(ran_on, thread::current().id());
    assert!(!runtime.helper_running());
}

#[test]
#[timeout(15000)]
fn block_on_fires_a_timer_on_its_own_thread() {
    let runtime = runtime();
    let started = Instant::now();

    drive(&runtime, runtime.sleep(Duration::from_millis(20)));

    assert!(started.elapsed() >= Duration::from_millis(20));
    assert!(!runtime.helper_running());
}

#[test]
#[timeout(15000)]
fn block_on_waits_for_readiness_on_its_own_thread() {
    let runtime = runtime();
    let (source, mut peer) = pair();
    let registration = runtime.register_io_source(source.clone()).unwrap();
    // Written from another thread once the read below is waiting, so that the byte is one the
    // wait reports rather than one the first attempt to read finds for itself.
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        peer.write_all(&[7]).unwrap();
    });

    let read = drive(
        &runtime,
        poll_fn(|cx| {
            let mut byte = [MaybeUninit::<u8>::uninit(); 1];

            registration.poll_io(cx, Interest::Readable, || {
                SockRef::from(&source).recv(&mut byte)
            })
        }),
    );

    assert_eq!(read.unwrap(), 1);
    assert!(!runtime.helper_running());
    writer.join().unwrap();
}

#[test]
#[timeout(15000)]
fn a_wake_from_another_thread_ends_the_wait_block_on_is_in() {
    let runtime = runtime();
    // Nothing to watch and no deadline, so the wait below is bounded by nothing but a
    // notification.
    let wakers = Arc::new(Mutex::new(None::<Waker>));
    let waker_thread = {
        let wakers = wakers.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            if let Some(waker) = lock(&wakers).take() {
                waker.wake();
            }
        })
    };
    let started = Instant::now();

    drive(&runtime, async {
        let mut polled = false;
        poll_fn(|cx| {
            if polled {
                return Poll::Ready(());
            }
            polled = true;
            *lock(&wakers) = Some(cx.waker().clone());

            Poll::Pending
        })
        .await
    });

    assert!(started.elapsed() >= Duration::from_millis(50));
    assert!(started.elapsed() < Duration::from_secs(5));
    waker_thread.join().unwrap();
}

#[test]
#[timeout(15000)]
fn work_left_behind_by_block_on_goes_to_a_helper() {
    let runtime = runtime();
    let task = runtime.spawn("a task that never finishes", pending::<()>());
    // Spawned from outside `block_on` while nobody drives, so a helper starts here already; it
    // is the one that has to be there once `block_on` has returned that is tested, so the
    // spawn is made from inside.
    drop(task);
    assert!(helper_gone(&runtime));

    let task = drive(&runtime, async {
        runtime.spawn("a task that never finishes", pending::<()>())
    });

    assert!(within_a_second(|| runtime.helper_running()));
    drop(task);
    assert!(helper_gone(&runtime));
}

#[test]
#[timeout(15000)]
fn a_second_block_on_parks_while_the_first_drives() {
    let runtime = runtime();
    let release = Arc::new(Event::new());
    let driver = {
        let runtime = runtime.clone();
        let release = release.clone();
        thread::spawn(move || {
            drive(&runtime, async move {
                release.listen().await;
            })
        })
    };
    // Long enough for the thread above to take the seat.
    thread::sleep(Duration::from_millis(50));
    let task = runtime.spawn("a task that names the thread it runs on", async {
        thread::current().id()
    });

    let ran_on = drive(&runtime, task).unwrap();

    assert_eq!(ran_on, driver.thread().id());
    assert!(!runtime.helper_running());
    release.notify(1);
    driver.join().unwrap();
}

#[test]
#[timeout(15000)]
fn a_block_on_inside_a_task_panics_rather_than_hangs() {
    let runtime = runtime();
    let task = runtime.spawn("a task that calls block_on", {
        let runtime = runtime.clone();
        async move {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drive(&runtime, async {});
            }))
            .is_err()
        }
    });

    assert!(drive(&runtime, task).unwrap());
}

#[test]
#[timeout(15000)]
fn a_panic_in_the_future_frees_the_seat() {
    let runtime = runtime();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drive(&runtime, async { panic!("a future that panics") });
    }));
    assert!(panicked.is_err());

    let task = runtime.spawn("the task after it", async { 7 });
    assert_eq!(drive(&runtime, task).unwrap(), 7);
}
```

Add `Future` and `Poll` to the `std` imports if `super::*` does not already bring them in, and
`driver` to what is used from `super`. The `pair()` helper and the `PollIo` import exist.

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p zbus --features p2p --lib runtime::builtin`
Expected: compile error, `driver` and `helper_running` do not exist.

- [ ] **Step 3: Write `driver.rs`**

Create `zbus/src/runtime/builtin/driver.rs`:

```rust
//! Who runs a built-in runtime: the thread inside [`block_on`], or a helper thread where no such
//! thread is there to do it.
//!
//! The scheduler and the reactor are run by one thread at a time, the one in the driver's seat.
//! A thread that enters `block_on` takes the seat if it is free and keeps it until its future is
//! done: between two polls of that future it runs a batch of ready tasks and then waits on the
//! reactor, so a program that drives its connections through `block_on` runs them on its own
//! thread and starts none. A thread that finds the seat taken parks instead, to be polled again
//! when its future is woken or the seat is freed.
//!
//! Work that outlives every `block_on` — a connection polled from some other executor, or a
//! task left running once `block_on` has returned — is run by a helper thread, started where
//! that work is found with nobody in the seat, and gone once nothing is left to run, watch or
//! time.

use std::{
    cell::Cell,
    collections::HashMap,
    future::Future,
    pin::pin,
    ptr::NonNull,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread, ThreadId},
    time::Duration,
};

use super::{Inner, lock, reactor::Reactor};
use crate::log::error;

/// How many ready tasks run between two looks at the reactor, so a busy task queue cannot
/// starve a socket or a timer.
const BATCH: usize = 32;

/// The name the helper thread carries: twelve bytes, which fits the fifteen Linux keeps for one.
pub(super) const THREAD_NAME: &str = "zbus runtime";

thread_local! {
    /// The reactor of the runtime this thread is in the seat of, and nothing on a thread that
    /// is in no seat at all.
    static DRIVER_REACTOR: Cell<Option<NonNull<Reactor>>> = const { Cell::new(None) };

    /// Whether this thread is inside `block_on`, in the seat or waiting for it.
    static IN_BLOCK_ON: Cell<bool> = const { Cell::new(false) };
}

/// Whether the calling thread is in the seat of the runtime `reactor` belongs to.
///
/// The reactor is named by its address, which stands for it alone while a thread is in its
/// seat: that thread holds the runtime, so the reactor cannot be dropped and its place taken by
/// another until the thread has left.
pub(super) fn on_driver_thread(reactor: &Reactor) -> bool {
    DRIVER_REACTOR.with(Cell::get) == Some(NonNull::from(reactor))
}

/// The runtime a `block_on` is to drive, looked up afresh at each turn of its loop, because the
/// future it polls may be the very thing that brings the runtime into being.
pub(super) type Resolve = Arc<dyn Fn() -> Option<Arc<Inner>> + Send + Sync>;

/// Runs `future` to completion on the calling thread, running the runtime `resolve` names
/// alongside it whenever the seat is free.
///
/// Panics when called from a thread that is in the seat already, which is a call from inside a
/// task the runtime is running: such a call could only wait for the thread it is on.
pub(super) fn block_on<F>(resolve: Resolve, future: F) -> F::Output
where
    F: Future,
{
    let _inside = InBlockOn::enter();
    let mut future = pin!(future);
    let signal = Arc::new(Signal {
        thread: thread::current(),
        woken: Mutex::new(false),
        resolve: resolve.clone(),
    });
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut driving: Option<Driving> = None;
    let mut failed_waits = 0u32;
    loop {
        if driving.is_none() {
            driving = resolve().and_then(|inner| Driving::take(inner, Holder::BlockOn));
        }
        // Cleared before the poll, so that a wake during it is seen by the round or the park
        // that follows.
        *lock(&signal.woken) = false;
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        match &driving {
            Some(driving) => driving.round(&signal.woken, &mut failed_waits),
            None => {
                if !*lock(&signal.woken) {
                    thread::park();
                }
            }
        }
    }
}

/// Starts the helper unless a thread is in the seat or about to take it. Called after the work
/// it is to see is in place (a task queued, a source registered, a deadline stored), never
/// before.
///
/// That order is what makes the hand-off safe either way round: a helper that starts here finds
/// the work, and a thread in the seat either sees it in the round it is in or finds it where it
/// decides whether to leave, which it does under this very lock. A thread inside `block_on`
/// takes the seat on the next turn of its loop and finds the work then.
pub(super) fn ensure_helper(inner: &Arc<Inner>) {
    if IN_BLOCK_ON.with(Cell::get) {
        return;
    }
    let mut seat = lock(&inner.seat);
    if seat.helper || seat.holder == Holder::BlockOn {
        return;
    }
    spawn_helper(inner, &mut seat);
}

/// Who is running a runtime, and who is waiting to.
pub(super) struct Seat {
    holder: Holder,
    /// The threads parked in `block_on` for want of the seat, unparked whenever it is freed.
    ///
    /// Keyed by thread, so that a thread which enters `block_on` again and again while another
    /// keeps the seat has one place here rather than one per call.
    waiting: HashMap<ThreadId, Thread>,
    /// Whether the helper thread is up. Set under the lock where the thread is started and
    /// cleared under it where the thread decides to leave.
    helper: bool,
}

impl Seat {
    /// A seat nobody is in.
    pub(super) fn new() -> Self {
        Self {
            holder: Holder::Nobody,
            waiting: HashMap::new(),
            helper: false,
        }
    }

    /// Whether the helper thread is up.
    pub(super) fn helper_running(&self) -> bool {
        self.helper
    }

    /// Frees the seat, and unparks every thread waiting for it: to take it, or to find its
    /// future done.
    fn free(&mut self) {
        self.holder = Holder::Nobody;
        DRIVER_REACTOR.with(|reactor| reactor.set(None));
        for (_, thread) in self.waiting.drain() {
            thread.unpark();
        }
    }
}

/// Who is in the seat.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Holder {
    Nobody,
    /// A thread inside `block_on`.
    BlockOn,
    /// The helper thread.
    Helper,
}

/// Starts the helper thread. Under the seat lock, which `seat` is the guard of.
fn spawn_helper(inner: &Arc<Inner>, seat: &mut Seat) {
    let inner = inner.clone();
    thread::Builder::new()
        .name(THREAD_NAME.into())
        .spawn(move || helper(inner))
        .expect("the thread a built-in runtime's helper runs on");
    // Set once the thread is there: a spawn that fails panics with the flag clear, so that the
    // next call tries again rather than wait on a thread that was never started.
    seat.helper = true;
}

/// What the helper thread does: the seat, and rounds until nothing is left.
///
/// Each round polls what is ready, a batch at a time, and then asks whether anything is left to
/// run, watch or time. Where nothing is, the thread leaves there and then, rather than sit in a
/// wait until something comes along to tell it what it could have worked out for itself. Where
/// something is, the round ends in one wait on the reactor.
fn helper(inner: Arc<Inner>) {
    let Some(driving) = Driving::take(inner, Holder::Helper) else {
        return;
    };
    let mut failed_waits = 0u32;
    loop {
        driving.run_batch();
        if driving.leave_if_idle() {
            return;
        }
        driving.wait(&mut failed_waits);
    }
}

/// A thread's time in the seat: taken here, given up when this is dropped.
struct Driving {
    inner: Arc<Inner>,
    who: Holder,
}

impl Driving {
    /// Takes the seat as `who` if it is free.
    ///
    /// A `block_on` that finds it taken is put down as waiting, to be unparked when the seat is
    /// freed. A helper that finds it taken leaves, and takes its flag down under the very lock
    /// the seat's holder looks at that flag under when it leaves: whoever is in the seat asks
    /// for a helper again then, if anything is left by then.
    fn take(inner: Arc<Inner>, who: Holder) -> Option<Self> {
        let mut seat = lock(&inner.seat);
        if seat.holder != Holder::Nobody {
            if who == Holder::Helper {
                seat.helper = false;
            } else {
                let thread = thread::current();
                seat.waiting.insert(thread.id(), thread);
            }

            return None;
        }
        seat.holder = who;
        drop(seat);
        DRIVER_REACTOR.with(|reactor| reactor.set(Some(NonNull::from(&*inner.reactor))));

        Some(Self { inner, who })
    }

    /// Polls up to `BATCH` ready tasks.
    fn run_batch(&self) {
        for _ in 0..BATCH {
            if !self.inner.scheduler.run_one() {
                break;
            }
        }
    }

    /// One round for `block_on`: a batch, then one wait on the reactor unless the future this
    /// thread is polling has been woken in the meantime, in which case the poll comes first.
    fn round(&self, woken: &Mutex<bool>, failed_waits: &mut u32) {
        self.run_batch();
        if *lock(woken) {
            return;
        }
        self.wait(failed_waits);
    }

    /// One wait on the reactor: bounded by no time at all where a task is ready, so that the
    /// round after it polls that task, and by nothing where none is, so that the thread sleeps
    /// until a source, a deadline or a notification has something for it.
    ///
    /// `failed_waits` counts the failures in a row, for the pause that keeps a wait which fails
    /// every time from becoming a spin; every waiter retries its own operation and sees its own
    /// error.
    fn wait(&self, failed_waits: &mut u32) {
        let at_most = self.inner.scheduler.has_ready().then_some(Duration::ZERO);
        match self.inner.reactor.wait(at_most) {
            Ok(()) => *failed_waits = 0,
            Err(e) => {
                *failed_waits += 1;
                if *failed_waits == 1 {
                    error!("The runtime's wait failed: {}", e);
                }
                self.inner.reactor.wake_everything();
                thread::sleep(Duration::from_millis(1 << failed_waits.min(10)));
            }
        }
    }

    /// Gives the seat up if nothing is left to run, watch or time; what the helper does before
    /// each wait. Under the seat lock, so that a spawn or a registration racing with it either
    /// is seen here or starts a helper itself once the lock is released.
    fn leave_if_idle(&self) -> bool {
        let mut seat = lock(&self.inner.seat);
        if self.inner.is_busy() {
            return false;
        }
        if self.who == Holder::Helper {
            seat.helper = false;
        }
        seat.free();

        true
    }
}

impl Drop for Driving {
    /// Gives the seat up, unless [`Driving::leave_if_idle`] did already.
    ///
    /// A `block_on` that leaves work behind starts a helper for it, because whatever is left
    /// may be awaited by a thread that is not going to drive; a helper that unwinds out of the
    /// loop clears its flag, so that the next spawn, registration or timer poll starts another.
    fn drop(&mut self) {
        let mut seat = lock(&self.inner.seat);
        if seat.holder != self.who {
            return;
        }
        if self.who == Holder::Helper {
            seat.helper = false;
        }
        seat.free();
        if self.who == Holder::BlockOn && !seat.helper && self.inner.is_busy() {
            spawn_helper(&self.inner, &mut seat);
        }
    }
}

/// The waker of the future a `block_on` polls.
struct Signal {
    thread: Thread,
    /// Whether the future has been woken since it was last polled.
    woken: Mutex<bool>,
    resolve: Resolve,
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    /// Marks the future woken and rouses its thread, wherever that thread is: parked, or in the
    /// seat inside the reactor's wait, which is what the notification ends. A wake from the
    /// thread itself writes no notification, because that thread looks at the flag before its
    /// next wait.
    fn wake_by_ref(self: &Arc<Self>) {
        *lock(&self.woken) = true;
        self.thread.unpark();
        if let Some(inner) = (self.resolve)() {
            inner.reactor.notify();
        }
    }
}

/// What says a thread is inside `block_on`, for as long as it is.
struct InBlockOn;

impl InBlockOn {
    /// Marks the calling thread as inside `block_on`.
    ///
    /// Panics where the thread is in a seat already: the call comes from inside a task the
    /// runtime is running on this thread, and could only ever wait for itself.
    fn enter() -> Self {
        assert!(
            DRIVER_REACTOR.with(Cell::get).is_none(),
            "zbus::block_on called from a task zbus's runtime is running: the call would wait \
             for the thread it is on"
        );
        IN_BLOCK_ON.with(|inside| inside.set(true));

        Self
    }
}

impl Drop for InBlockOn {
    fn drop(&mut self) {
        IN_BLOCK_ON.with(|inside| inside.set(false));
    }
}
```

Points the code above settles that an implementer might be tempted to change:

1. `Driving::take` takes the helper's flag down under the same lock a leaving `block_on` reads
   it under (`Driving::drop`), so a helper that lost the race for the seat and a `block_on`
   that leaves work behind cannot both decide the other has it.
2. `Seat::free` clears `DRIVER_REACTOR` on the thread that calls it, which is always the thread
   in the seat: `leave_if_idle` and `drop` run on that thread and nowhere else.
3. `Drop` runs on unwind as well: a panic in the future polled by `block_on` reaches it with the
   seat held, and the seat is freed all the same, a helper started if work is left. Starting a
   thread while unwinding is fine; `spawn_helper`'s `expect` failing there is a double panic,
   which is an abort, and the same as today's `ensure_worker` on an unwinding thread.
4. `Signal::wake_by_ref` notifies through `resolve` on every wake, including wakes of a thread
   that is parked rather than in the seat; the reactor's `wake_pending` flag makes that at most
   one write per wait of whoever is in the seat.

- [ ] **Step 4: Wire `mod.rs`, the reactor and `utils.rs`**

In `zbus/src/runtime/builtin/mod.rs`: `mod worker;` → `mod driver;`; delete `worker.rs`;
`Inner.worker: Mutex<bool>` → `seat: Mutex<driver::Seat>` (constructed with
`driver::Seat::new()`); every `self.inner.ensure_worker()` → `self.inner.ensure_progress()`;
delete `ensure_worker` and `retire_if_idle` and add:

```rust
    /// Sees to it that the work just handed over is run: a helper is started unless a thread is
    /// in the seat or about to take it. Called after that work is in place, never before.
    fn ensure_progress(self: &Arc<Self>) {
        driver::ensure_helper(self);
    }

    /// Whether anything is left to run, watch or time.
    pub(super) fn is_busy(&self) -> bool {
        self.scheduler.has_ready() || self.scheduler.live_tasks() > 0 || !self.reactor.is_idle()
    }
```

`Builtin::worker_running` becomes:

```rust
    /// Whether the helper thread is running.
    #[cfg(test)]
    pub(super) fn helper_running(&self) -> bool {
        lock(&self.inner.seat).helper_running()
    }
```

and `Debug` prints `.field("helper", &lock(&self.inner.seat).helper_running())`. Add, after
the `Builtin` impl block:

```rust
/// Runs `future` to completion on the calling thread, running the process's runtime alongside
/// it: see [`driver::block_on`].
pub(crate) fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    driver::block_on(Arc::new(|| lock(&SHARED).upgrade()), future)
}
```

Update the three "so that a worker starting here ..." comments in `register_io_source`,
`spawn` and `Sleep::poll` to say "so that a helper starting here" (the reasoning is the same).
`Inner`'s `Scheduler::new` hook comment: "breaks the wait the seat's holder is in".

In `zbus/src/runtime/builtin/reactor.rs`, `notify`: `super::worker::on_worker_thread(self)` →
`super::driver::on_driver_thread(self)`, and its doc's first sentence: "Wakes the thread in
this reactor's seat inside its wait, unless called from that thread, which sees every change
before its next wait anyway."

In `zbus/src/utils.rs`, the non-Tokio `block_on` becomes two:

```rust
#[cfg(all(feature = "builtin-runtime", not(feature = "tokio")))]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    crate::runtime::builtin::block_on(future)
}

#[cfg(not(any(feature = "builtin-runtime", feature = "tokio")))]
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    futures_lite::future::block_on(future)
}
```

each with the doc comment that is there today (Task 5 rewrites it). `zbus::block_on` reaches
the crate root through `pub use utils::*` in `lib.rs`, unchanged. The module is private
(`mod builtin;` at `zbus/src/runtime/mod.rs:24`, behind `#[cfg(feature = "builtin-runtime")]`):
make it `pub(crate) mod builtin;` so `utils.rs` can name `crate::runtime::builtin::block_on`.

- [ ] **Step 5: Run the runtime tests**

Run: `cargo test -p zbus --features p2p --lib runtime::builtin`
Expected: all pass, the eight new ones included. Then the release loop from "Verification per
commit", ten times. A hang in `a_second_block_on_parks_while_the_first_drives` means the
unpark on freeing the seat is missing; a hang in `a_wake_from_another_thread_ends_the_wait...`
means `Signal::wake_by_ref` does not reach `notify`.

- [ ] **Step 6: Run everything**

All five test suite lines and the full "Verification per commit" list. The blocking API's tests
(`zbus/tests/` with `blocking-api`) and the `blocking-hook` and `peer_creds` paths are the ones
most likely to show a hang: a `block_on` in the seat waiting on a thread that itself waits for
the connection. If any test hangs, the report names it with a backtrace of every thread
(`gdb -p`) before anything is changed.

- [ ] **Step 7: Commit**

```sh
/usr/bin/git add zbus/src/runtime/builtin zbus/src/utils.rs
/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign -F - <<'EOF'
✨ zb: Run the built-in runtime on the thread inside block_on

A typical D-Bus program has one thread and drives its connection
through zbus::block_on, and the connection's tasks, sockets and
timers can be run right there, between two polls of the program's own
future. So the runtime has a seat rather than a thread: block_on takes
it while it polls, and a helper thread takes it only where work is
left with nobody in the seat, a connection polled from some other
executor or a task running on after block_on has returned. A program
that never leaves block_on with a connection alive starts no thread.

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
EOF
```

---

### Task 5: Say what the runtime is

**Files:**
- Modify: `zbus/src/runtime/builtin/mod.rs:1-27` (module doc)
- Modify: `zbus/src/runtime/mod.rs:1-13`
- Modify: `zbus/src/utils.rs` (the `block_on` docs)
- Modify: `book/src/connection.md:82-91`, `book/src/upgrading-to-6.md:682-685`
- Modify: `zbus/src/connection/builder.rs:496-500` (only if it says "worker thread")
- Test: the doc build line and `cd book && mdbook build`.

- [ ] **Step 1: Module docs**

`zbus/src/runtime/builtin/mod.rs`, replace the module doc with:

```rust
//! The runtime zbus brings along, for a connection that is handed no runtime of its own.
//!
//! It is made of a scheduler, which holds the tasks and hands them out to be polled, a reactor,
//! which watches the sockets and keeps the timers, and a seat, which one thread at a time is
//! in to run the two. Everything [`traits::Runtime`] asks of a runtime reaches one of them: a
//! spawned future is queued on the scheduler, a registered source is the reactor's and so is a
//! timer, from the first poll of it onwards, and blocking work goes to a thread of its own, as
//! the trait's default has it.
//!
//! There is one runtime in a process, shared by every connection built without one of its own,
//! for as long as any of them or a thread running it is alive: one channel for breaking a wait
//! from another thread — a pipe on unix and a socket pair on Windows, two descriptors either
//! way — and no thread until there is work with nobody to run it.
//!
//! The thread in the seat is the one inside [`block_on`], where a program has one: it runs the
//! tasks and waits on the reactor between two polls of its own future. Where none has — the
//! connection is polled from some other executor, or work is left once `block_on` has returned
//! — a helper thread takes the seat, and leaves in the round it finds nothing left to run,
//! watch or time.
//!
//! A panic in a task is caught and fails that task's handle. A panic outside a task — in a
//! waker, say — unwinds the thread in the seat: out of `block_on`, to whoever called it, with
//! the seat freed on the way; out of the helper's loop, with its flag cleared so that the next
//! spawn, registration or timer poll starts another. The wakers the reactor had taken out of
//! its maps to wake are dropped in the unwind without being woken, and the waiters they
//! belonged to are served by the next thread in the seat.
//!
//! Nothing here takes a runtime down while it has work: a detached task that never finishes
//! keeps a helper, and everything that task's future holds, for the life of the process.
```

`zbus/src/runtime/mod.rs` lines 3-6 become:

```rust
//! By default, a connection runs on the runtime zbus brings along (the `builtin-runtime`
//! feature, on by default): one runtime per process, run by the thread inside
//! [`block_on`](crate::block_on) and by a helper thread only where no thread is inside it, with
//! no runtime dependency of its own. A connection built inside a Tokio runtime runs on it
//! instead (the `tokio` feature). Any other runtime reaches a connection
```

(keep the rest of that paragraph as it is).

- [ ] **Step 2: `block_on`'s doc**

The doc on the `builtin-runtime` `block_on` in `utils.rs`:

```rust
/// Runs a future to completion on the calling thread, and zbus's runtime with it.
///
/// This is for a program that has no async runtime of its own. Between two polls of the future
/// the thread runs the tasks, sockets and timers of every connection built on zbus's built-in
/// runtime, so such a program is a single thread: zbus starts none for it. Where a call
/// returns with a connection still alive, a helper thread runs that connection's work until
/// the next call, or until the connection is gone.
///
/// Do not call this from inside a task zbus is running — from a future polled inside another
/// call to it, or from a method of an interface served on such a connection: the call panics,
/// because it could only wait for the thread it is on. From a future some other runtime is
/// polling it holds that thread until it returns, and where the two end up waiting on each
/// other, neither of them ever does.
```

The no-feature one keeps the doc that is there today, minus its last sentence of the first
paragraph ("The future handed to it is the only one polled on this thread — a connection's own
work runs on a thread of the connection's runtime."), which becomes "The future handed to it is
the only one polled on this thread; a connection's own work runs wherever its runtime runs it."

- [ ] **Step 3: The book**

`book/src/connection.md`, the "Built-in backends" paragraph, lines 83-91, becomes:

```markdown
With the `builtin-runtime` cargo feature (a default feature), a connection runs on the runtime
zbus brings along, one for the whole process and depending on no runtime crate at all. The thread
that drives it is the one inside `zbus::block_on`: between two polls of the future handed to it,
that thread runs every built-in connection's tasks, sockets and timers, so a program that awaits
its work through `zbus::block_on` is a single thread. Only where a connection has work and no
thread is inside `zbus::block_on` — it is polled from some other executor, or a call returned
with the connection alive — does zbus start a helper thread, which leaves once nothing is left
to run. If the `tokio` feature is also enabled and a Tokio runtime is current on the thread that
builds the connection, it runs on Tokio instead. With only `tokio` enabled, a connection always
runs on the Tokio runtime that is current when it is built; building one from a thread with no
such runtime fails with `Error::Unsupported`.
```

`book/src/upgrading-to-6.md` lines 682-685, the clause "— one worker thread and one wake pipe
per connection —" becomes "— one runtime per process, driven by the thread inside
`zbus::block_on` —". Check `zbus/src/connection/builder.rs:496-500` and `zbus/src/lib.rs` for
"worker" with `/usr/bin/grep -rn -i 'worker' zbus/src/lib.rs zbus/src/connection/builder.rs
book/src`; fix any hit that describes the built-in runtime.

- [ ] **Step 4: Verify**

The doc build line, `cd book && mdbook build`, `cargo +nightly fmt --all -- --check`, and a
100-column check: `awk 'length > 100 {print FILENAME": "FNR}'` over the files touched.

- [ ] **Step 5: Commit**

```sh
/usr/bin/git add zbus/src/runtime/builtin/mod.rs zbus/src/runtime/mod.rs zbus/src/utils.rs \
    book/src/connection.md book/src/upgrading-to-6.md zbus/src/connection/builder.rs zbus/src/lib.rs
/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign -F - <<'EOF'
📝 zb,book: Describe the built-in runtime as the thread inside block_on

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
EOF
```

(`git add` only the files that changed.)

---

### Task 6: Measure, update the PR, push

**Files:**
- No source changes. The PR body of https://github.com/z-galaxy/zbus/pull/1975.

- [ ] **Step 1: Benchmarks**

The criterion baselines `async-io` (the 5.x backend) and the current tip's numbers are under
`target/criterion`. Run three times and take medians:

```sh
for i in 1 2 3; do
    flock /tmp/claude-1000/cargo.lock ~/.cargo/bin/cargo bench -p zbus --bench runtime \
        --features p2p -- --baseline async-io 2>&1 \
        | /usr/bin/grep -E '^(connection|method-call|signal|spawn|blocking)|time:'
done
```

Record every one of the nine ids against `async-io`. The expectation from the design:
`spawn/100-tasks` and `connection/graceful-shutdown` fall to the `async-io` level or below (no
cross-thread hand-off per task: the bench thread is in the seat); `method-call/roundtrip`,
`signal/emit-receive` and `blocking-api/roundtrip` hold their gain; `connection/build-and-drop`
loses its thread start. A result outside that shape is reported, not explained away.

- [ ] **Step 2: Binary size**

`CI/binary-size.sh` for the two fixtures (`--profile size`); record beside the table.

- [ ] **Step 3: The PR body**

Rewrite the first paragraph, the "Commits" list, the benchmark table and ruling 5 ("A runtime
per connection") of the PR body — the current one is in this session's scratchpad as
`pr-body.md` and on the PR itself (`gh pr view 1975 --json body -q .body`). Ruling 5 becomes:
"**One runtime per process, run by the thread inside `zbus::block_on`**: a program that drives
its connections through `zbus::block_on` is a single thread; a helper thread exists only while
work is left with no thread inside `block_on`. This supersedes the RFC's one-worker-per-
connection paragraph at the maintainer's direction." Keep the footer. Apply with
`gh pr edit 1975 --body-file <file>`.

- [ ] **Step 4: Push**

```sh
/usr/bin/git fetch origin
/usr/bin/git rebase origin/main
/usr/bin/git push --force-with-lease zeenix builtin-runtime-plan:builtin-runtime
```

Then watch `gh pr checks 1975 --watch`; every check green, `windows_test (default)` included,
is the end of the plan. A red Windows check on Task 2's set is the one outcome that needs a
design change (fall back to `WSAPoll`); report it rather than patch around it.

---

## Self-review

- **Coverage.** Single-threaded on `block_on`: Task 4. One per process: Task 3. Lightweight
  (no thread for an async-main program): Task 4's seat-before-first-poll and `IN_BLOCK_ON`.
  Windows cap that sharing exposes: Task 2. Docs: Task 5. Numbers: Task 6. `Send`/`Sync`: the
  design says why not, no task.
- **Names used across tasks.** `Inner::new`, `Inner::shared`, `Builtin::from_inner`,
  `Builtin::inner`, `REGISTRY_TEST`, `lock` (Task 3) → used in Task 4's `driver.rs` and tests.
  `Seat::new`, `Seat::helper_running`, `driver::block_on(Resolve, F)`, `ensure_helper`,
  `on_driver_thread`, `Inner::is_busy`, `Builtin::helper_running` (Task 4) → used in Task 4's
  `mod.rs`, `reactor.rs`, tests and Task 5's docs. `MAX_SOURCES`, `FdSet`, `SET_SIZE` (Task 2).
- **Known soft spot.** `Driving::drop` on the helper thread while unwinding takes the seat lock
  and unparks waiters; that is what `Marks::drop` does today and is fine. `Signal::wake_by_ref`
  locks the registry on every wake through `resolve`; the registry lock is uncontended and the
  cost is one lock per wake of a `block_on` future, not per task wake. If Task 6 shows it, cache
  the `Weak<Inner>` in `Signal` after the first successful resolve.
