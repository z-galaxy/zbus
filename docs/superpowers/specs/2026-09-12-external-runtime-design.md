# External runtime integration — design

Resolves z-galaxy/zbus#1960 ("Support external runtimes while retaining the async-io default").
The follow-up proposal in #1959 (replacing the smol-based internals of the built-in backend) is
out of scope; this design only has to leave it possible.

Three points of the issue text are deliberately not followed:

- "External connections reuse async-executor through zbus's existing Executor/Task abstractions."
  That sentence entered the issue when it was split out of #1959 and contradicts the point of the
  split: an external-runtime user must not depend on any crate the `async-io` feature owns.
- A caller-polled `Connection::run()` driver. zbus writes no scheduler and no lock of its own
  (those are #1959's, should it be pursued); a runtime that wants to drive zbus has to be able to
  spawn a task, and the runtime abstraction requires exactly that.
- The trait is named `Runtime`, not `Reactor`, because it supplies more than readiness.

## The problem

A connection needs four things from a runtime: readiness wakeups for its socket, timers,
somewhere to run its tasks (the socket reader, the object server dispatcher, name-lost watchers,
spawned method handlers), and async locks that are held across `.await` (the socket's write half,
the message broadcasters, every interface behind the object server). Today all four are coupled
to two compiled-in backends:

- `async-io`: sockets are `Arc<async_io::Async<T>>`, timers are `async_io::Timer`, tasks run on an
  `async_executor::Executor` ticked by a dedicated `zbus::Connection executor` thread that the
  builder spawns after setup (`internal_executor(false)` leaves ticking to the caller through
  `Connection::executor().tick()`), locks are `async_lock`'s.
- `tokio`: sockets are `tokio::net` types, timers `tokio::time`, tasks `tokio::spawn`ed, locks
  `tokio::sync`'s.

Nothing else can supply them. A GLib, `polling`, `mio` or custom event loop host cannot use zbus
without accepting async-io's reactor thread and executor thread in its process, and a Tokio user
on an unusual configuration cannot integrate through Tokio's `AsyncFd` either. The choice is also
made per call in places (`select_runtime!`, `use_tokio()`), which the code itself documents as
safe only for call sites that are independent of the socket's reactor.

## Goals

- Let a host supply readiness, timers, task spawning, async locks and, optionally, blocking work
  through one public trait, while zbus keeps creating and connecting sockets, doing
  authentication, framing and FD passing.
- An external-runtime user depends on none of the crates behind the `async-io` feature:
  `async-io`, `async-executor`, `async-task`, `async-lock`, `async-process`, `blocking` and
  their transitive dependencies. A build with `comms` and neither `async-io` nor `tokio` (an
  "external-only" build) compiles and works with an explicit runtime.
- zbus itself writes no scheduler and no synchronization primitive. Whatever a runtime cannot
  provide through the trait, zbus does not need.
- Keep the default behaviour: session/system connections stay automatic, `async-io` stays the
  default non-Tokio backend, native Tokio keeps spawning tasks directly.
- Add no zbus thread on the external path, at any point of the connection's lifetime.
- Keep `Builder` and `Connection` non-generic and keep wire-only builds untouched.

## Non-goals

- Replacing async-io, async-executor or async-lock for the built-in backend (#1959). The private
  scheduler and locks prototyped for this issue live on branch `zeenix/runtime-scheduler`
  (withdrawn PR #1963) as input to #1959.
- A thread-free `zbus::blocking` facade on top of an external runtime.
- Completion-based hosts (IOCP, io_uring): they need the custom `Socket` route.
- Routing native Tokio through the trait. It stays its own path because Tokio has no `AsyncFd`
  on Windows; a host may still hand zbus a Tokio-backed `Runtime` implementation explicitly.

## Decisions

1. **One reactor path for all non-Tokio I/O.** The async-io backend becomes the built-in
   `zbus::runtime::AsyncIo`, an ordinary implementor of the public trait. The transport connect
   path goes through one private registered-socket wrapper for the built-in and external runtimes
   alike. Native Tokio keeps its own sockets and tasks. Each connection latches a private runtime
   choice at build time; `select_runtime!` and `use_tokio()` are removed.
2. **The runtime supplies tasks and locks.** `traits::Runtime` has associated types for its task
   handle, mutex and readers-writer lock, and constructors for them. zbus's `Executor`/`Task` and
   its private lock wrappers become enums: a zero-cost variant per compiled backend and an erased
   variant for external runtimes. No smol crate moves out of the `async-io` feature.
3. **One ancillary hook, `spawn_blocking`.** It covers DNS, NSS group lookup, nonce-file reads and
   subprocess work. Hosts that do not implement it get `Error::Unsupported` before zbus starts any
   helper.
4. **No driver.** zbus spawns its tasks on the runtime and never needs to be polled; there is no
   `Connection::run()`, no driver hand-off and no driver state. `internal_executor(false)` keeps
   its meaning for the built-in async-io runtime only and is rejected together with an explicit
   runtime.
5. **Four sequential PRs**: the module rename (#1962, open); the runtime abstraction (trait,
   erasure, built-in implementor, task and lock enums, timeouts, external-only builds); the I/O
   path (`IoSource`, the socket wrapper, transports on the runtime); ancillary operations, hosts,
   examples and docs.

## Public API

Everything below is behind the `comms` feature and lives in `zbus::runtime` unless stated.

### Module layout

```text
zbus/src/runtime/
├── mod.rs          # pub mod traits; pub use async_io::AsyncIo; IoSource, Interest; private Runtime
├── traits.rs       # Runtime, IoRegistration, Task, Mutex, RwLock
├── async_io.rs     # the built-in implementor (feature = "async-io")
├── erased.rs       # ErasedRuntime, ErasedRegistration, ErasedTask, ErasedMutex, ErasedRwLock
├── executor.rs     # Executor, Task: enums over the compiled backends and the erased runtime
├── locks.rs        # Mutex, RwLock and guards: enums over the compiled backends and erasure
├── io.rs           # Registered<K> socket wrapper and the connect helpers (private, PR 3)
├── timeout.rs      # timeout over the connection's runtime
├── async_drop.rs   # unchanged
└── process.rs      # gains the runtime-path implementation in PR 4
```

`async_lock.rs` is replaced by `locks.rs`.

### Traits

```rust
pub mod traits {
    pub trait Runtime: Send + Sync + 'static {
        type Registration: IoRegistration;
        type Sleep: Future<Output = ()> + Send + 'static;
        type Task<T: Send + 'static>: Task<T>;
        type Mutex<T: Send + 'static>: Mutex<T>;
        type RwLock<T: Send + Sync + 'static>: RwLock<T>;

        /// Register a socket or pipe for readiness notifications.
        fn register(&self, source: IoSource) -> io::Result<Self::Registration>;

        /// A future that completes at `deadline`. Dropping it cancels the timer.
        fn sleep_until(&self, deadline: Instant) -> Self::Sleep;

        /// Run `future` to completion in the background, starting now or soon.
        fn spawn<T>(&self, future: impl Future<Output = T> + Send + 'static) -> Self::Task<T>
        where
            T: Send + 'static;

        fn mutex<T: Send + 'static>(&self, value: T) -> Self::Mutex<T>;
        fn rwlock<T: Send + Sync + 'static>(&self, value: T) -> Self::RwLock<T>;

        /// Run `work` off the event loop. `None` means the host offers no blocking service.
        fn spawn_blocking<T>(
            &self,
            work: impl FnOnce() -> T + Send + 'static,
        ) -> Option<Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'static>>>
        where
            T: Send + 'static,
        {
            let _ = work;
            None
        }
    }

    pub trait IoRegistration: Send + Sync + 'static {
        /// Wait for `interest` readiness, then run `operation` on the polling thread.
        fn poll_io<T>(
            &self,
            cx: &mut Context<'_>,
            interest: Interest,
            operation: impl FnMut() -> io::Result<T>,
        ) -> Poll<io::Result<T>>;
    }

    /// A handle to a spawned task. Dropping it cancels the task; `detach` lets it run on.
    pub trait Task<T>: Future<Output = io::Result<T>> + Send + Unpin + 'static {
        fn detach(self);
    }

    pub trait Mutex<T>: Send + Sync + 'static {
        type Guard<'a>: DerefMut<Target = T> + Send
        where
            Self: 'a;

        fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + Send;
    }

    pub trait RwLock<T>: Send + Sync + 'static {
        type ReadGuard<'a>: Deref<Target = T> + Send + Sync
        where
            Self: 'a;
        type WriteGuard<'a>: DerefMut<Target = T> + Send
        where
            Self: 'a;

        fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + Send;
        fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send;
    }
}
```

Contracts, documented on the traits:

- `poll_io`: return the first success (a partial write counts) or any error other than
  `WouldBlock` immediately; on `WouldBlock` clear the readiness observed, arrange a wakeup for
  `cx` and return `Pending`; bound retries, never spin, never block, never retain `operation`.
  Deregister the source before its `IoSource` is released. `Interest` is
  `enum Interest { Readable, Writable }`.
- `spawn`: the task runs on the host's executor, concurrently with the caller, on any thread the
  host chooses; the handle resolves to `Err` if the host loses the task. Dropping the handle
  cancels: a Tokio host wraps its `JoinHandle` in an abort-on-drop newtype (ten lines, shown in
  the example), an async-task-style host maps `detach` to its own.
- Locks: the guards are `Send` so that zbus can hold them across `.await` inside `Send` futures;
  a `RwLock` read guard is also `Sync`. Nothing else is required: no `try_` variants, no
  fairness, no `const` construction, because the constructors are methods on the runtime.
- `spawn_blocking` returns an `io::Result<T>` because host executors can lose a task. Its future
  is boxed; blocking work happens a handful of times per connection, at setup.

The `Send` bounds on the returned futures use return-position `impl Trait` in traits (Rust 1.75),
within MSRV 1.87.

### `IoSource`

```rust
#[derive(Clone, Debug)]
pub struct IoSource(Arc<Owned>);   // Owned = OwnedFd on unix, OwnedSocket on windows
```

Implements `AsFd` and `AsRawFd` (unix) or `AsSocket` and `AsRawSocket` (windows). It is a shared
owner: the socket wrapper keeps one clone for I/O and the registration keeps whatever it needs.
Users receive one from `register` and never construct one. `async_io::Async::new` accepts it
directly because `Arc<T>: AsFd` where `T: AsFd`.

### Built-in runtime

```rust
#[cfg(feature = "async-io")]
pub struct AsyncIo { .. }   // zbus::runtime::AsyncIo; `Clone` is an `Arc` clone

impl AsyncIo {
    pub fn new() -> Self;
}
```

It implements `traits::Runtime` with async-io, async-executor and async-lock: `register` wraps
the source in `async_io::Async::new` and `poll_io` is the `poll_readable`/`poll_writable` loop
that `Arc<Async<UnixStream>>` uses today; `sleep_until` is `async_io::Timer::at` behind a small
future that drops the `Instant`; `spawn` goes to an `async_executor::Executor` owned by the
`AsyncIo` instance, whose `zbus::Connection executor` thread starts on the first spawn and runs
`async_io::block_on(executor.run(pending()))` until the instance is dropped; the locks are
`async_lock`'s; `spawn_blocking` returns `blocking::unblock`.

The default path (no explicit runtime, `async-io` compiled) uses the same type through a
zero-cost enum variant, so there is one implementation of the backend, not two. Selecting it
explicitly with `Builder::runtime(AsyncIo::new())` forces async-io even inside a Tokio runtime;
its executor thread is then automatic and `internal_executor(false)` is rejected like for any
explicit runtime.

### Builder and Connection

```rust
impl Builder<'_> {
    /// Use `runtime` for this connection's readiness, timers, tasks and locks.
    pub fn runtime(self, runtime: impl traits::Runtime) -> Self;
    /// Unchanged signature; async-io only, see selection rules.
    pub fn internal_executor(self, enabled: bool) -> Self;
}
```

`runtime` erases the implementation into `Arc<dyn ErasedRuntime>` immediately. Calling `runtime`
and `internal_executor` on the same builder, in either order, records `Error::Unsupported` in the
builder's deferred-error slot and `build()` reports it. `Connection::executor()` stays: with an
external runtime its `tick()` never completes and `is_empty()` is `true`, as with Tokio today.

## Runtime selection

At the start of `build_inner`, before any I/O or discovery, the builder picks one of:

| Builder state | Runtime | Tasks | Locks |
| --- | --- | --- | --- |
| `runtime(r)` | `External(r)` | `r.spawn` | `r.mutex`/`r.rwlock` |
| neither, `tokio` compiled and a runtime is current | `Tokio` | `tokio::spawn` | `tokio::sync` |
| neither, `async-io` compiled | `AsyncIo(default instance)` | async-executor | async-lock |
| neither, no backend compiled | `Error::Unsupported` | | |

The private enum is

```rust
pub(crate) enum Runtime {
    #[cfg(feature = "async-io")]
    AsyncIo(Arc<AsyncIoInner>),
    #[cfg(feature = "tokio")]
    Tokio,
    External(Arc<dyn ErasedRuntime>),
}
```

stored in `ConnectionInner` and passed to `Transport::connect` (PR 3), the handshake, `timeout`
and every place that creates a lock or spawns. `Executor` becomes a thin public wrapper around a
clone of it.

## Internals

### Erasure

```rust
trait ErasedRuntime: Send + Sync {
    fn register(&self, source: IoSource) -> io::Result<Box<dyn ErasedRegistration>>;
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) -> Box<dyn ErasedTask>;
    fn mutex(&self, value: Box<dyn Any + Send>) -> Box<dyn ErasedMutex>;
    fn rwlock(&self, value: Box<dyn Any + Send + Sync>) -> Box<dyn ErasedRwLock>;
    fn spawn_blocking(&self, work: Box<dyn FnOnce() + Send>)
        -> Option<Pin<Box<dyn Future<Output = io::Result<()>> + Send>>>;
}

trait ErasedRegistration: Send + Sync {
    fn poll_io(&self, cx: &mut Context<'_>, interest: Interest,
               operation: &mut dyn FnMut() -> io::Result<()>) -> Poll<io::Result<()>>;
}

trait ErasedTask: Future<Output = io::Result<()>> + Send + Unpin { fn detach(self: Box<Self>); }

trait ErasedMutex: Send + Sync {
    fn lock(&self) -> Pin<Box<dyn Future<Output = Box<dyn ErasedGuard<dyn Any + Send>> + '_>>;
}
// ErasedRwLock: read() and write() likewise; ErasedGuard<T>: DerefMut<Target = T> + Send.
```

A blanket `impl<R: traits::Runtime> ErasedRuntime for R` boxes each registration, sleep future,
task and lock once. The generic-over-`T` operations are erased with the value type erased too:
`spawn` runs `async move { *slot.lock() = Some(future.await) }` and the typed `Task<T>` reads the
slot when the erased handle resolves; a lock stores `Box<dyn Any + Send>` and the typed wrapper
downcasts on every access, which is a `TypeId` comparison. `poll_io` passes a closure that stores
the `T` in a local `Option`, so an I/O poll is one virtual call and no allocation.

Cost on the external path, and only there: one allocation per spawn, and two per lock
acquisition (the future and the guard). Locks are taken once per sent message, once per received
message and once per method call; the message itself already costs more than that. The built-in
variants pay nothing new.

### Locks (`runtime::locks`)

```rust
pub(crate) enum Mutex<T> {
    #[cfg(feature = "async-io")]
    AsyncLock(async_lock::Mutex<T>),
    #[cfg(feature = "tokio")]
    Tokio(tokio::sync::Mutex<T>),
    External(Erased<T>),           // Box<dyn ErasedMutex> + PhantomData<T>
}
```

`lock()` is an `async fn` matching on the variant and returning a `MutexGuard<'_, T>` enum with
the same three shapes; `RwLock` and its two guards follow the same pattern. Locks are created
through the connection's runtime (`runtime.mutex(value)`), which is what selects the variant, so
every site that creates one has a runtime in hand:

- `Connection::new` creates the four connection locks (`socket_write`, `msg_senders`,
  `subscriptions`, `registered_names`).
- The object server wraps each interface as `Arc<RwLock<Box<dyn Interface>>>` when it is added
  (`ObjectServer::at`, which has the connection), no longer in `ArcInterface::new` at
  `Builder::serve_at` time: the builder stores `Box<dyn Interface>` and wraps at build. The
  `Box` is needed because an enum cannot hold an unsized `T`; `InterfaceDeref` derefs through it.
- The flatpak workaround's process-wide `static SERIAL_NUM_SEMAPHORE` becomes a per-connection
  `Mutex<()>`, taken under the same condition. Its purpose (serial assignment and send are one
  atomic step towards xdg-dbus-proxy) is per connection anyway, and it removes the only lock
  that had to be constructed without a runtime.

The proxy's property cache keeps its `std::sync::RwLock`; it is never held across an `.await`.

### Executor and tasks

```rust
pub struct Executor<'a> { runtime: Runtime, lifetime: PhantomData<&'a ()> }

pub struct Task<T>(TaskInner<T>);

enum TaskInner<T> {
    #[cfg(feature = "async-io")]
    AsyncExecutor(async_task::Task<T>),
    #[cfg(feature = "tokio")]
    Tokio(TokioTask<T>),                 // abort-on-drop newtype over JoinHandle
    External { handle: Box<dyn ErasedTask>, output: Arc<std::sync::Mutex<Option<T>>> },
}
```

`spawn`, `is_empty`, `tick`, `run`, `detach` and `Future::poll` dispatch on the variant.
`Executor::new()` gives way to `Executor::from(runtime)`; `needs_internal_driver` is true only
for the `AsyncIo` variant with the legacy manual-tick mode. `Task::spawn_blocking` becomes a
method on `Runtime`: Tokio's pool, `blocking::unblock`, or the external hook, which maps `None`
to `Error::Unsupported` naming the operation. `Executor<'a>` keeps its lifetime only through
`PhantomData` (the enum needs none), which relaxes it from invariant to covariant; nobody can
observe that since only `Executor<'static>` is handed out.

### Timers

`runtime::timeout(runtime, fut, duration)` races `fut` against `tokio::time::sleep`,
`async_io::Timer` or the erased `sleep_until`. `Connection::call_method` passes
`self.inner.runtime`.

### Driving

There is none. Every internal task is spawned on the runtime and the host's executor runs it;
`build()` awaits its own setup steps and spawns the socket reader and object-server dispatcher
like today. `graceful_shutdown(self)` keeps meaning "wait for `ConnectionInner` to be
destroyed": handlers, clones, streams and proxies keep their strong references, permanent service
loops stay weak, and nothing is force-closed. If a host drops or aborts one of zbus's tasks the
task handle resolves to `Err`, which the reader task's owner treats exactly like a Tokio abort
today: the connection reports closed and pending calls fail.

## I/O path (PR 3)

### Registered sockets

```rust
pub(crate) struct Registered<K> {
    source: IoSource,
    registration: Box<dyn ErasedRegistration>,
    kind: K,
}
```

`K` selects the syscalls: `UnixStream` (recvmsg/sendmsg with SCM_RIGHTS via the existing
`fd_recvmsg`/`fd_sendmsg` helpers, peer credentials), `TcpStream` and `VsockStream` (recv/send, no
FDs) and, in PR 4, `Pipe` (read/write for unixexec). `ReadHalf` and `WriteHalf` are implemented on
`Arc<Registered<K>>`, mirroring today's `Arc<Async<T>>` impls, with `read_with`/`write_with` helpers
built on `poll_fn` + `poll_io`. The public `Socket` impls for `Async<T>` and the Tokio types stay
for user-supplied sockets.

### Connecting

Transports create sockets with `socket2` (already in the lock through Tokio; added to `comms`) as
non-blocking, close-on-exec sockets. `connect` either succeeds or pends with `EINPROGRESS`
(`WouldBlock` on unix sockets with a full backlog); a pending connect registers the source, waits
for writable readiness and then checks `take_error()`. This is the same for unix, tcp and vsock
(`SockAddr::vsock` on Linux). Peer credentials on the runtime path call `SO_PEERCRED` /
`getpeereid` / `SO_PEERPIDFD` directly; the supplementary-group lookup (`getpwuid_r`,
`getgrouplist`) goes through `spawn_blocking` and stays `None` when the hook is absent.

### Ancillary operations (PR 4)

| Operation | Tokio | Built-in runtime | External runtime |
| --- | --- | --- | --- |
| DNS for tcp hostnames | tokio resolver | `blocking::unblock` | hook, else `Unsupported` |
| nonce-tcp file | `tokio::fs` | `blocking::unblock` | hook, else `Unsupported` |
| ibus/launchd `output()` | tokio process | async-process | hook + std process, else `Unsupported` |
| unixexec | tokio process | async-process | std spawn, `Registered<Pipe>`, hook reaps |
| autolaunch (Windows) | win32 | win32 | win32 (no blocking involved) |

Every `Unsupported` is returned before a process is spawned or a file is opened.

## Feature and dependency model

The feature lists do not change: `async-io` keeps every smol crate, `tokio` keeps `tokio`, and
`comms` gains only `socket2` (PR 3). What changes is which builds are valid:

- The `compile_error!` for `comms` without a backend goes away in PR 2. In such a build
  `Builder::session()` and friends fail with `Error::Unsupported` unless `runtime(..)` was called;
  `utils::block_on` is `futures_lite::future::block_on` (futures-lite is already a shared
  dependency with `std` on), so `zbus::blocking` compiles and works when the host loop runs on
  another thread. Until PR 3 lands, an external runtime can only connect over a user-supplied
  `Socket` (`Builder::socket`/`authenticated_socket`, e.g. `socket::Channel`); PR 3 adds the
  standard transports.
- CI gains an external-only leg in PR 2: `check`, `clippy` and the unit tests with
  `--no-default-features --features comms,proxy,service`, plus `cargo tree -e normal` asserting
  that none of the `async-io`-owned crates appear in that graph. Dev-dependencies (async-lock for
  the test runtime, tokio for the Tokio host test) are allowed; they never reach users.
- MSRV stays 1.87.

## Documentation

- Rustdoc on `zbus::runtime` explaining what a runtime supplies, the external-only build, and
  which transport/authentication combinations an external runtime supports.
- `Connection::executor()` and `Builder::internal_executor()` docs rewritten as async-io-only.
- Book: a "Runtimes" section in `connection.md` with a caller-supplied runtime example and a
  pointer to the host examples; FAQ entry updated.
- `upgrading-to-6.md`: `Builder::runtime`, the external-only build, and that `comms` without a
  backend no longer fails to compile.

## Testing strategy

PR 1 (#1962): the existing suite in all feature combinations; no behaviour change.

PR 2, in every configuration plus the new external-only leg:

- a test runtime in `zbus/src/runtime/test_runtime.rs` (`cfg(test)`), built from dev-dependencies
  only: `spawn` on std threads running `futures_lite::future::block_on`, `sleep_until` on a
  thread timer, `async-lock` locks, `register` unsupported (PR 3 adds an `async-io`-backed one);
  a p2p `socket::Channel` pair with one end on the test runtime: method calls both ways, a
  served interface, property access, `graceful_shutdown` with retained clones;
- erasure: a spawned task's output arrives, cancel on drop, `detach`; a lock guard held across
  an `.await` in a `Send` future; downcast never fails (a debug assertion);
- `Builder::runtime` + `internal_executor` rejected; a no-backend `Builder::session().build()`
  is `Error::Unsupported`;
- `Executor`/`Task` dispatch through the existing suites (default, tokio-only, all-features);
- the flatpak serial lock keeps sends ordered under `is_flatpak()`.

PR 3, unit tests in `connection/mod.rs` and `runtime/io.rs`:

- an end-to-end unix session connection through `Builder::runtime(AsyncIo::new())` and through
  the default path;
- socket wrapper: partial writes, FD passing, readiness races (a peer that writes before the
  reader registers), timer cancellation (dropping a timed-out `call_method` future);
- the external-only leg connects to the session bus with a test runtime whose `register` is
  backed by the `polling` crate (dev-dependency).

PR 4, integration tests in `zbus/tests/`:

- `polling`-based single-threaded test host (dev-dependency): connect to the session bus, fetch
  peer credentials, run a method call with a timeout, shut down; assert the process thread count
  is unchanged across the whole lifecycle (Linux: `/proc/self/task`); in the external-only
  configuration too;
- Tokio `AsyncFd` host (`tokio` feature): same lifecycle, with the abort-on-drop task newtype;
- unsupported ancillary operations (`tcp:host=name`, `unixexec:`) fail on a host without the hook
  before any helper starts;
- GLib example under an `examples`-only feature (`glib` dev-dependency, optional) so CI needs no
  system library.

Required checks stay: `cargo test --all-features`, `--no-default-features`, `--no-default-features
--features tokio`, and the existing cross-platform `cargo check` targets.

## Risks

- **Trait surface.** Five traits with GATs is more to implement than a reactor alone. Mitigated
  by two complete reference implementations in tree (Tokio and `polling`), each under 200 lines,
  and by the `AsyncIo` implementor being the same code the default path runs.
- **Erased lock cost** on the external path: two allocations per acquisition. Measured with the
  existing `benches/` message round-trip on the `polling` host in PR 4; if it shows, the guard
  box can go by giving `ErasedMutex` a `poll_lock`/`unlock` pair in a later change without
  touching the public trait.
- **Default-path regressions** from routing the built-in runtime through the wrapper. Mitigated by
  the wrapper being the same `poll_readable`/`recvmsg` loop, and by the full suite running on the
  default features in PR 3.
- **Windows unix-socket connect.** `uds_windows` has no non-blocking connect; `socket2` handles
  `AF_UNIX` on Windows, so the same connect helper applies. If it does not on some Windows version,
  the fallback is `spawn_blocking` on the runtime path, documented.
- **GAT-free `spawn_blocking` boxes its future.** Deliberate; see the API section.

## Follow-ups (out of scope)

- #1959: replacing the smol crates behind `AsyncIo`; the withdrawn #1963 branch is its seed.
- Migrating the public `Async<T>` socket impls onto the wrapper.
- A thread-free `blocking` facade.
- Deprecating `internal_executor` and `Connection::executor()`.
- Routing native Tokio through a `Runtime` implementation once Windows can be covered.
