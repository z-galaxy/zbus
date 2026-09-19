# Drop the Blocking API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `zbus::blocking`, the `blocking-api` feature and the blocking proxies the
macros generate, so that a program without an async runtime uses `zbus::block_on` around the
async API and nothing else; and fold the branch's history so PR #1975 presents the built-in
runtime as one design rather than a threaded one replaced by a single-threaded one.

**Architecture:** `zbus::block_on` (public, and the built-in runtime's own loop) makes every
blocking wrapper a one-line `block_on` around the async call, and the maintainer's ruling of
2026-09-19 is that the wrappers were only ever a shortcut for simple programs, which
`block_on` now serves with one dependency and copy-paste examples. Removal is outright (no
alias, no deprecation: 6.0 is a breaking release and the no-compat-shims rule applies), in one
💥 commit for everything that has to compile together and one 📝 commit for prose. Before it,
the branch's 21 commits become 15 with the threaded stage folded away; the threaded design
stays on the branch `builtin-runtime-threaded`.

**Tech Stack:** Rust 1.87 (MSRV), `zbus_macros` (syn/quote), mdbook, criterion.

**Spec:** The maintainer's rulings of 2026-09-19 in this session: "the blocking api/wrappers
were always meant as a quick way to write simple apps ... Now they can just copy&paste the
examples and they work out of the box, with only one dep"; "keep it in the same branch and PR.
What could be now removed is the multithreaded work from earlier, it'll require a history
re-write though"; "it was saved in a branch so nothing is really lost". The inventory the tasks
draw on is the survey in the SDD workspace (`blocking-api-survey.md`).

## Global Constraints

- MSRV 1.87.0; `cargo +nightly fmt --all` clean; `cargo clippy -- -D warnings` clean on every
  commit for every feature set in "Verification per commit". No `#[allow(...)]`.
- 100 columns in every text file; sentences in comments end with `.`; comments and docs explain
  things to a first-time reader: never "now", "no longer", "previously", "yet", "used to",
  "instead of the blocking API", never a reference to an issue, a review or a commit. The
  upgrading guide is the one place that names what was removed, by its 5.x name.
- No compatibility shim of any kind: no `blocking-api` feature that does nothing, no deprecated
  re-export, no `gen_blocking` attribute that is accepted and ignored. A build that names the
  feature fails to resolve it; a `#[proxy]` that names a removed attribute fails to compile with
  the macro's usual unknown-attribute error.
- Every example in the book and in doc comments that replaces a blocking one is a compiled
  doctest of the same kind as the one it replaces (`rust,no_run` where the original was, plain
  `rust` where the original was), and uses `zbus::block_on` with a single `use zbus::...` line
  so it is copy-paste-and-run with `zbus` as the one dependency.
- Tests: no `test_` prefix on new tests; feature gates in the test module, never `[[test]]`
  entries; a removed test is removed, not `#[ignore]`d.
- Dependencies: none added. `Cargo.lock` is tracked and CI runs `--locked`: the commit that
  removes the fixture crate and the feature includes the `Cargo.lock` it produces.
- Commits: one logical change each, every hunk covered by the message; subject: a gimoji
  emoji copied verbatim + ` zb: ` / `zb,zm: ` / `zb,book: ` + imperative title, header ≤ 72
  UTF-16 units (💥 and 📝 count 2), body lines ≤ 74 chars, body says why. Trailer exactly
  `Assisted-by: Claude Fable 5.1 (claude-fable-5-1)`; never `Co-Authored-By`, never
  `Signed-off-by`, never a session URL, never another model's name. Commit with
  `/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign`; `git config
  user.name`/`user.email` must be `Zeeshan Ali Khan`/`zeenix@gmail.com`.
- Tooling: `git`, `grep`, `cargo` on `PATH` are shims; use `/usr/bin/git`, `/usr/bin/grep`,
  `~/.cargo/bin/cargo`; wrap every cargo call in `flock /tmp/claude-1000/cargo.lock` with
  `CARGO_TARGET_DIR=/home/zeenix/checkout/z-galaxy/zbus/target`. Exclude `5.x/`, `target/`,
  `.superpowers/` and `docs/superpowers/` from every repository-wide grep.
- Branch `builtin-runtime-plan` (PR #1975), tip 50daea1f at the time of writing; push to the
  `zeenix` remote only, as `builtin-runtime`, with `--force-with-lease`. Never touch
  `builtin-runtime-threaded` or `builtin-runtime-threaded-reactor-waiting`.

## Verification per commit

```sh
cargo +nightly fmt --all -- --check
cargo clippy -p zbus --all-targets --features p2p -- -D warnings
cargo clippy -p zbus --all-features --all-targets -- -D warnings
cargo clippy -p zbus --no-default-features --features tokio,proxy,service,p2p --all-targets -- -D warnings
cargo clippy -p zbus --no-default-features --features proxy,service,unixexec,ibus,p2p --all-targets -- -D warnings
cargo clippy -p zbus_macros --all-targets -- -D warnings
cargo clippy -p zbus_xmlgen --all-targets -- -D warnings
cargo check --target x86_64-pc-windows-gnu -p zbus --all-targets --features p2p
RUSTDOCFLAGS="-D warnings -D rustdoc::broken_intra_doc_links" cargo doc -p zbus --no-deps
```

Tests (`S="dbus-run-session --config-file /tmp/dbus-session.conf --"`), at the commits the
tasks name:

```sh
$S cargo test -p zbus --all-features -- --skip fdpass_systemd --skip ibus_connection
$S cargo --locked test --release -p zbus --features uuid,url,time,chrono,option-as-array,vsock,bus-impl -- --skip fdpass_systemd --skip ibus_connection
$S cargo test -p zbus --no-default-features --features tokio,proxy,service,p2p --tests
$S cargo test -p zbus --no-default-features --features proxy,service,object-manager,unixexec,ibus,tracing,p2p --tests
$S cargo test -p zbus --doc
$S cargo test -p zbus_macros
cargo test -p zbus_xmlgen
cd book && mdbook build
```

Known pre-existing failures to ignore: the private-doc build has three rustdoc errors outside
`runtime/`; `ibus_connection` needs `~/.cache/ibus/`.

---

### Task 1: Fold the threaded stage out of the history

**Files:** none edited by hand; the tree at the new tip must equal the tree at 50daea1f.

**Interfaces:**
- Produces: a rewritten `builtin-runtime-plan` of 15 commits on d86f6188 whose tip tree is
  identical to 50daea1f's (`/usr/bin/git diff 50daea1f` empty), and a tag `pre-fold` at
  50daea1f for the record until the push.

The 21 commits today (oldest first) and what becomes of each:

| today | becomes |
|---|---|
| d5c2d40d 📝 Add the implementation plan for the built-in runtime | **1** 📝 Add the implementation plans for the built-in runtime — both plan files, with eb347478's file added; new body (below) |
| 2aedd965 ✅ Benchmark what a connection costs on its runtime | **2** unchanged |
| 5da577a9 👷 Build the p2p benchmarks for CodSpeed | **3** unchanged |
| 64bcf6b3 ✅ Add GeoClue2-shaped fixtures for measuring binary size | **4** unchanged |
| 5b4d7ed2 👷 Report the fixtures' binary sizes in CI | **5** unchanged |
| 24ad7aa4 ✨ Give the connection locks of zbus's own | **6** unchanged |
| 5824f9e9 ➖ Drop the async-lock feature and crate | **7** unchanged |
| 765cf464 ✨ Add a task scheduler with no thread of its own | **8** same subject; `scheduler.rs` as at 50daea1f (later comment rewording folded in) |
| f999a2ad ✨ Add a reactor over poll(2) and select with its own timers | **9** same subject; `reactor.rs` and `poll/` as at 50daea1f (f9b9808d's 1024-entry set and 86e5ae5c's `wake_pending` folded in); body gains one sentence on the set size |
| 38036b90 ✨ Run a built-in runtime's tasks and reactor on one worker | **10** ✨ zb: Run the built-in runtime on the thread inside block_on — `driver.rs`, `mod.rs`, `tests.rs`, `utils.rs`, `runtime/mod.rs` as at 50daea1f minus what 11 and 12 add; c2987f8c, 43b6454f and 86e5ae5c folded in; message = 43b6454f's, plus one paragraph on one runtime per process from c2987f8c's |
| b5700e34 ♻️ Run the default connection on zbus's own runtime | **11** same subject; includes `tests/unixexec.rs`'s async yield (from 43b6454f) with a body sentence on why |
| 86e5ae5c ⚡️ Write the wake-up once per wait, not once per wake | folded into 9 and 10 |
| c60c9c44 💥 Replace the async-io feature and crates with builtin-runtime | **12** unchanged in intent; hunks that 43b6454f later rewrote take their final form |
| 5a295afd 📝 Document the built-in runtime | **13** 📝 zb,book: Document the built-in runtime — texts as at 50daea1f (6a45b5ff folded in); 5a295afd's body |
| 5ef16917 ✨ Make block_on part of the public API | **14** same subject; `utils.rs` doc as at 50daea1f |
| eb347478 📝 Add the plan for a single-threaded built-in runtime | folded into 1 |
| f9b9808d 🐛 Let a select on Windows watch more than 63 sockets | folded into 9 |
| c2987f8c ♻️ Share one built-in runtime across a process's connections | folded into 10 |
| 43b6454f ✨ Run the built-in runtime on the thread inside block_on | becomes 10 |
| 6a45b5ff 📝 Describe the built-in runtime as the thread inside block_on | folded into 13 |
| 50daea1f ✅ Benchmark a round trip driven from inside block_on | **15** unchanged |

Body of commit 1 (two paragraphs after the subject):

```
The first plan built the design RFC #1959 describes, one worker thread
per connection, and measured it. The second replaced that with the
runtime running on the thread inside zbus::block_on, one per process,
at the maintainer's direction: lightweight for a typical D-Bus program,
with Tokio there for the demanding ones. The history shows the final
design once; the threaded implementation is kept on a branch.

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
Changelog: skip
```

- [ ] **Step 1: Tag the current tip and confirm the threaded branches exist**

```sh
/usr/bin/git tag pre-fold 50daea1f
/usr/bin/git branch --list 'builtin-runtime-threaded*'   # both must be listed
```

- [ ] **Step 2: Rewrite**

Mechanics are the implementer's choice; the recommended one is an interactive rebase onto
d86f6188 with `GIT_SEQUENCE_EDITOR` set to a script that reorders and marks `fixup` per the
table (eb347478 → after d5c2d40d; f9b9808d and the reactor/poll hunks of 86e5ae5c → after
f999a2ad; c2987f8c, 43b6454f and the rest of 86e5ae5c → after 38036b90; 6a45b5ff → after
5a295afd), resolving every conflict by taking the file's content at 50daea1f for hunks the
later commit owns, then `reword` for commits 1, 9, 10, 11 and 13. Where a fixup drags a hunk
that belongs to a later commit (say, `utils.rs` text that 14 owns), move it forward with
`git commit --fixup` onto that commit and a second autosquash pass. Alternative: build the 15
commits from scratch with `git checkout 50daea1f -- <paths>` per commit on a fresh branch from
d86f6188; either way the invariant below decides.

- [ ] **Step 3: The invariant and per-commit checks**

```sh
/usr/bin/git diff 50daea1f --stat        # must print nothing
/usr/bin/git log --oneline d86f6188..HEAD | wc -l   # 15
```

Then, for each of commits 8 through 15 (`git checkout <sha>` in turn, or `git rebase -x`):
`cargo +nightly fmt --all -- --check`, `cargo clippy -p zbus --all-targets --features p2p -- -D
warnings`, `cargo check --target x86_64-pc-windows-gnu -p zbus --all-targets --features p2p`,
and from commit 10 on `cargo test -p zbus --features p2p --lib runtime::builtin`. Commits 1-7
are unchanged trees and need no re-check. Every commit's message: trailer exactly
`Assisted-by: Claude Fable 5.1 (claude-fable-5-1)` (plus `Changelog: skip` on 1), header ≤ 72
UTF-16 units, body ≤ 74 chars per line, no `fixup!`/`squash!` residue.

- [ ] **Step 4: Record**

`git log --format='%h %s' d86f6188..HEAD` into the report, and `git diff pre-fold --stat`
(empty) beside it. Do not push; Task 5 pushes.

---

### Task 2: Remove the blocking API

**Files:**
- Delete: `zbus/src/blocking/` (eight files, 1814 lines), `zbus/tests/issue/issue_122.rs`,
  `test_fixtures/blocking_api/` (whole crate)
- Modify: `zbus/Cargo.toml` (features `default`, `blocking-api`; docs.rs metadata; `[[bench]]
  runtime` and `[[example]] screen-brightness` `required-features`), root `Cargo.toml`
  (workspace member), `Cargo.lock`, `zbus_xmlgen/Cargo.toml:33`
- Modify: `zbus/src/lib.rs` (lines 49, 149-150, 163-169: the doctest gate, `pub mod blocking`,
  `__if_blocking_api_feature!`), `zbus/src/connection/mod.rs` (162-168 doc `cfg_attr` pair;
  1429-1431 `From`), `zbus/src/object_server/interface/mod.rs` (49-53 doc pair),
  `zbus/src/object_server/mod.rs` (565-567 `From`), `zbus/src/proxy/mod.rs` (1343-1345
  `From`; 1473 `gen_blocking = false` in a test)
- Modify: `zbus_macros/src/proxy.rs` (attributes `blocking_name`, `gen_async`, `gen_blocking`,
  `blocking_object`; `AsyncOpts`; the blocking branch of `expand`, `create_proxy`,
  `gen_proxy_method_call`, `gen_proxy_property`, `gen_proxy_signal`), `zbus_macros/src/iface.rs`
  (`ProxyAttributes` `blocking_name`/`gen_async`/`gen_blocking`, `ProxyMethodAttributes`
  `blocking_object`, `Proxy::add_method` and `Proxy::gen`), `zbus_macros/src/lib.rs` (the
  `proxy` macro doc, lines 33-221, and its doctest), `zbus_macros/tests/tests.rs`
  (`test_proxy_object_list`)
- Modify: `zbus/tests/basic.rs` (33-40, 120-126), `zbus/tests/builder_feature_additivity.rs`
  (18-20, 27-29), `zbus/tests/issue/mod.rs:9`, `zbus/tests/issue/issue_813.rs:43`,
  `zbus/tests/iface_and_proxy/iface.rs:44`
- Modify: `zbus/benches/runtime.rs` (module doc 89-93; group `blocking-api` 136-148),
  `zbus/examples/screen-brightness.rs` (whole file), `zbus_xmlgen/src/main.rs` (14, 36-46,
  147-152)
- Modify: `.github/workflows/rust.yml` (39, 104-108, 120, 126-127, 197-209, 234, 378-380)
- Modify: `book/src/blocking.md` (whole page), `book/src/SUMMARY.md:8`,
  `book/src/service.md:390-401`
- Test: everything in "Verification per commit" plus all test suites.

**Interfaces:**
- Produces: `#[proxy]` and `#[interface(proxy(...))]` accept `async_name`, `async_object`,
  `default_service`, `default_path`, `interface`, `assume_defaults` and the other existing
  keys, and nothing named `gen_blocking`, `gen_async`, `blocking_name` or `blocking_object`;
  every `#[proxy]` generates exactly one proxy, the async one, under the `proxy` feature.
  `zbus::block_on` is unchanged. The book page `blocking.md` keeps its path and is titled
  "Synchronous programs".

- [ ] **Step 1: The macro**

In `zbus_macros/src/proxy.rs`: delete the four attribute keys from `def_attrs!`; delete
`AsyncOpts` and every `if blocking`/`blocking:` branch so that `create_proxy`,
`gen_proxy_method_call`, `gen_proxy_property` and `gen_proxy_signal` have one flavour: the
async one, using `zbus::Connection`, `zbus::Proxy`, `zbus::proxy::Builder`,
`zbus::proxy::ProxyImpl`, `zbus::proxy::PropertyStream`, `zbus::proxy::SignalStream` and
`futures_core::Stream`, exactly as the async branch does today. `expand` builds and returns the
async proxy alone, with no `__if_blocking_api_feature!` wrapper; the "cannot disable both"
assertion goes with `gen_async`. In `iface.rs`: delete the three trait-level and the one
method-level key, and the code in `Proxy::add_method`/`Proxy::gen` that forwards them. Keep
`async_name` and `async_object` as they are (the proxy is async; the names stay accurate). In
`zbus_macros/src/lib.rs` the `proxy` macro doc loses the `gen_blocking` (53-55),
`blocking_name` (59), `blocking_object` (109-114) entries and the blocking half of the
"Signals" paragraph (118-125); its `# Example` doctest keeps only the async half, driven by
`zbus::block_on` as it is already (lines 138, 192), with the `use zbus::blocking::Connection`
and the `SomeIfaceProxyBlocking` lines removed. In `zbus_macros/tests/tests.rs`,
`test_proxy_object_list` builds its connection with `zbus::block_on(async { zbus::connection::
Builder::session()?...build().await })` and uses `ObjectListProxy` through `zbus::block_on`
for each call, asserting what it asserts today.

Run `cargo test -p zbus_macros` (the macro crate's tests and doctests) before moving on; it
does not depend on the rest of this task. Expected: a compile error in `zbus` for
`__if_blocking_api_feature!`, which Step 2 removes; `zbus_macros` itself passes.

- [ ] **Step 2: The crate**

Delete `zbus/src/blocking/`. In `zbus/src/lib.rs`: line 49's doctest gate becomes
`#[cfg(all(feature = "proxy", feature = "service"))]`; lines 149-150 and the
`__if_blocking_api_feature!` definition (163-169) go. Delete the three `From<crate::blocking::
…>` impls and replace each doc `cfg_attr` pair with its non-blocking text. In `zbus/Cargo.toml`:
`blocking-api` leaves `default` and `[features]`, the docs.rs metadata list and the two
`required-features` (the bench keeps `["p2p", "service"]`; the example keeps none). Root
`Cargo.toml`: remove `"test_fixtures/blocking_api"`; delete that directory. `cargo update -w
--offline` if the lockfile does not update by itself on the next build; commit `Cargo.lock`.
`zbus_xmlgen/Cargo.toml:33` drops `features = ["blocking-api"]` (keep the rest of the entry).

- [ ] **Step 3: Tests, bench, example, xmlgen**

`zbus/tests/basic.rs`: the two gated tests become async tests driven by `zbus::block_on`,
keeping their bodies' assertions — each body's `zbus::blocking::Connection::session()` becomes
`zbus::block_on(async { zbus::Connection::session().await })` and every blocking call on it
becomes the async call awaited inside one `zbus::block_on(async { ... })`; their attribute is
`#[test]` with the file-level backend gate unchanged. `builder_feature_additivity.rs`: delete the
two gated statements. `issue/mod.rs`: delete line 9; delete `issue_122.rs`. `issue_813.rs:43`
and `iface_and_proxy/iface.rs:44`: delete the `gen_blocking = …` key (and the `proxy(...)`
parentheses if empty). `zbus/src/proxy/mod.rs:1473`: same. `zbus/benches/runtime.rs`: delete
the `blocking-api` group (136-148) and the sentence of the module doc that names "the blocking
API's own `block_on`". `zbus/examples/screen-brightness.rs` becomes:

```rust
//! Sets the screen brightness through GNOME's settings daemon, driving the call with
//! `zbus::block_on`: no async runtime to pick, zbus as the one dependency.

use zbus::{Connection, Result, proxy};

#[proxy(
    interface = "org.gnome.SettingsDaemon.Power.Screen",
    default_service = "org.gnome.SettingsDaemon.Power",
    default_path = "/org/gnome/SettingsDaemon/Power"
)]
trait Screen {
    #[zbus(property)]
    fn brightness(&self) -> Result<i32>;

    #[zbus(property)]
    fn set_brightness(&self, value: i32) -> Result<()>;
}

fn main() -> Result<()> {
    zbus::block_on(async {
        let connection = Connection::session().await?;
        let screen = ScreenProxy::new(&connection).await?;
        let before = screen.brightness().await?;
        screen.set_brightness(before.saturating_add(10)).await?;
        println!("brightness: {before} -> {}", screen.brightness().await?);

        Ok(())
    })
}
```

(Read the current file first and keep whatever it does that this sketch does not: the exact
interface, path and property names it uses win over the sketch.) `zbus_xmlgen/src/main.rs`:
`use zbus::{Connection, connection, fdo::IntrospectableProxy, ...}`; the three uses at 36-46
and 147-152 become `zbus::block_on(async { ... })` around the async calls, one `block_on` per
place the code blocks today. `cargo test -p zbus_xmlgen` still passes (its tests do not
connect).

- [ ] **Step 4: CI**

`.github/workflows/rust.yml`: drop `blocking-api,` from lines 39, 120 and 234; lines 104-108
become clippy runs with `builtin-runtime,p2p`, `builtin-runtime,p2p,proxy` and
`builtin-runtime,p2p,service` (three lines, no `blocking-api`); delete lines 126-127 (the
fixture's two clippy runs) and the comment lines above them that name it; lines 197-209 (doc
tests with and without `blocking-api`) collapse to the two runs without it, and the comment
above them is reworded to say what remains; lines 378-380 drop `blocking-api,`. Check
`.github/workflows/size.yml` and `bench.yml` for the feature (the survey found none).

- [ ] **Step 5: The book page, compiled**

`book/src/blocking.md` is replaced in full (it is a doctest through `zbus/src/lib.rs:49`, so
its `rust,no_run` blocks must compile with `proxy` and `service` on):

````markdown
# Synchronous programs

zbus's API is async, and a program does not need an async runtime of its own to use it:
`zbus::block_on` runs a future to completion on the calling thread, and with the default
`builtin-runtime` feature that same thread runs the connection's work in between, so the
program below is one thread with zbus as its one dependency. Everything in the other chapters
works the same way inside the future handed to `zbus::block_on`.

Two rules come with it. The future must not block the thread waiting for something the
connection has to do — a synchronous wait for a reply, a busy loop until a signal has arrived —
because that work runs on this very thread between two polls of the future, so such a wait
never ends. And `zbus::block_on` must not be called from inside a task zbus is running: from a
method of an interface the connection serves, say, or from a future polled inside another
`zbus::block_on`. Such a call panics, because it could only wait for the thread it is on.

## Client

```rust,no_run
use zbus::{Connection, Result, proxy, zvariant::ObjectPath};

#[proxy(
    interface = "org.freedesktop.GeoClue2.Manager",
    default_service = "org.freedesktop.GeoClue2",
    default_path = "/org/freedesktop/GeoClue2/Manager"
)]
trait Manager {
    #[zbus(object = "Client")]
    fn get_client(&self);
}

#[proxy(
    interface = "org.freedesktop.GeoClue2.Client",
    default_service = "org.freedesktop.GeoClue2"
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
    interface = "org.freedesktop.GeoClue2.Location",
    default_service = "org.freedesktop.GeoClue2"
)]
trait Location {
    #[zbus(property)]
    fn latitude(&self) -> Result<f64>;
    #[zbus(property)]
    fn longitude(&self) -> Result<f64>;
}

fn main() -> Result<()> {
    zbus::block_on(async {
        use futures_util::stream::StreamExt;

        let conn = Connection::system().await?;
        let manager = ManagerProxy::new(&conn).await?;
        let mut client = manager.get_client().await?;
        // Gotta do this, sorry!
        client.set_desktop_id("org.freedesktop.zbus").await?;

        let mut location_updated = client.receive_location_updated().await?;
        client.start().await?;

        while let Some(signal) = location_updated.next().await {
            let args = signal.args()?;
            let location = LocationProxy::builder(&conn)
                .path(args.new())?
                .build()
                .await?;
            println!(
                "Latitude: {}\nLongitude: {}",
                location.latitude().await?,
                location.longitude().await?,
            );
        }

        Ok(())
    })
}
```

(The `futures_util` crate is in scope here because zbus's own dependency on it is re-exported
for streams; a program adds `futures-util` to its own manifest for `StreamExt`. — Keep this
sentence only if the doctest needs `futures_util` from the crate; drop it if `zbus::export`
provides it. Check which before committing.)

### Watching for properties

```rust,no_run
use zbus::{Connection, Result, proxy};

#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait SystemdManager {
    #[zbus(property)]
    fn log_level(&self) -> Result<String>;
}

fn main() -> Result<()> {
    zbus::block_on(async {
        use futures_util::stream::StreamExt;

        let connection = Connection::session().await?;
        let proxy = SystemdManagerProxy::new(&connection).await?;
        println!("Init service log level: {}", proxy.log_level().await?);
        let mut log_level_changed = proxy.receive_log_level_changed().await;
        while let Some(change) = log_level_changed.next().await {
            println!("Log level changed: {}", change.get().await?);
        }

        Ok(())
    })
}
```

## Server

A service is the same as in the [service chapter](service.md), with the connection built and
the object served inside one `zbus::block_on`: the future that never resolves keeps the
program alive, and the calls arriving on the connection are handled in between its polls.

```rust,no_run
use std::future::pending;
use zbus::{Result, connection, interface};

struct Greeter {
    count: u64,
}

#[interface(name = "org.zbus.MyGreeter1")]
impl Greeter {
    fn say_hello(&mut self, name: &str) -> String {
        self.count += 1;
        format!("Hello {}! I have been called {} times.", name, self.count)
    }
}

fn main() -> Result<()> {
    zbus::block_on(async {
        let _connection = connection::Builder::session()?
            .name("org.zbus.MyGreeter")?
            .serve_at("/org/zbus/MyGreeter", Greeter { count: 0 })?
            .build()
            .await?;

        pending::<()>().await;

        Ok(())
    })
}
```
````

Read today's `blocking.md` first and carry over the interface, path and property names it uses
where the sketch differs; the sketch's shape (one `zbus::block_on` around the whole program) is
what is required. Resolve the `futures_util` note by testing: if `use futures_util::...` does
not compile in the doctest, use `zbus::export::futures_util` or whatever the client chapter's
doctest uses, and delete the note. `book/src/SUMMARY.md:8` becomes
`- [Synchronous programs](blocking.md)`. `book/src/service.md:390-401`: delete the
`gen_blocking = false` key from the `proxy(...)` attribute of that doctest (keep the rest).

- [ ] **Step 6: Verify and commit**

All of "Verification per commit" and all test suites. Then:

```sh
/usr/bin/git add -A zbus zbus_macros zbus_xmlgen test_fixtures Cargo.toml Cargo.lock \
    .github/workflows/rust.yml book/src/blocking.md book/src/SUMMARY.md book/src/service.md
/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign -F - <<'EOF'
💥 zb,zm: Remove the blocking API in favour of zbus::block_on

The blocking wrappers were a shortcut for a simple program that did not
want to pick an async runtime and learn it. With zbus::block_on public
and, on the default runtime, the loop that runs the connection itself,
that program is the async API inside one block_on, with zbus as its one
dependency and the examples copy-and-paste ready. The wrappers, the
blocking-api feature, the blocking proxies the macros generated and
the attributes that controlled them go, with no alias: 6.0 is a
breaking release. The upgrading guide has the recipe.

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
EOF
```

(`git add -A` on those paths stages the deletions; check `git status` shows nothing else.)

---

### Task 3: Prose that still describes the blocking API

**Files:**
- Modify: `book/src/client.md` (91-165: `TProxyBlocking` at 94 and 142, the `[cob]` links at
  94 and 153, the link definition at 607), `book/src/faq.md` (174-176),
  `book/src/upgrading-to-6.md` (94, 419, 452, 714-718, plus a new section), `zbus/README.md`
  (30, 118-122, 153-155, 174-176, 182)
- Test: `cd book && mdbook build`; `cargo test -p zbus --doc` (the README is the crate doc and
  is doctested).

- [ ] **Step 1: The upgrading guide**

Add, after the `### Builder::runtime replaces internal_executor` section of
`book/src/upgrading-to-6.md`, this section (edit its lines 714-718 out of the previous one):

```markdown
### The blocking API is gone

5.x's `zbus::blocking` module — `blocking::Connection`, `blocking::Proxy`, the
`*ProxyBlocking` types `#[proxy]` generated, `blocking::ObjectServer` and the signal, property
and message iterators — and its `blocking-api` cargo feature have no 6.0 equivalent. A program
without an async runtime of its own drives the async API with `zbus::block_on`, which on the
default `builtin-runtime` feature also runs the connection's work on the calling thread:

```rust,ignore
// 5.x
let connection = zbus::blocking::Connection::session()?;
let proxy = FooProxyBlocking::new(&connection)?;
let answer = proxy.bar()?;

// 6.0
let answer = zbus::block_on(async {
    let connection = zbus::Connection::session().await?;
    let proxy = FooProxy::new(&connection).await?;
    proxy.bar().await
})?;
```

One `zbus::block_on` around the whole program is the shape to prefer, because a call returning
with the connection alive leaves its work to a helper thread until the next call. A blocking
iterator becomes the stream it wrapped, driven with `StreamExt::next` inside the future. The
`#[proxy]` attributes `gen_blocking`, `blocking_name` and `blocking_object` are gone with the
proxies they configured, and so is `gen_async`, since the async proxy is the only one;
`async_name` and `async_object` stay. Two rules come with `zbus::block_on`: the future must not
block the thread waiting for work the connection has to do, and the function must not be called
from inside a task zbus is running, where it panics. See the [synchronous programs
chapter](blocking.md).
```

(`rust,ignore` is used here deliberately: the snippet names a `FooProxy` that no crate defines.
This is the one exception to the compiled-doctest rule, and the upgrading guide already uses
`ignore` blocks for before/after pairs — check, and follow the file's convention if it differs.)
Line 94: remove `blocking-api` from the feature list. Line 419: drop "`zbus::blocking::
connection::Builder` mirrors ..." or reword to the async builder alone. Line 452: drop "and its
blocking sibling".

- [ ] **Step 2: client.md, faq.md, README**

`client.md`: remove the `TProxyBlocking` sentences at 94 and 142 and the two `[cob]` links; if
the paragraph at 153 points readers at the blocking chapter for "a program without a runtime",
point it at "the [synchronous programs chapter][cob]" with `[cob]: blocking.html` kept. `faq.md`
174-176: the paragraph on the blocking API driving its connections through its own `block_on`
becomes one on `zbus::block_on` being the driver, with the panic rule. `zbus/README.md`: line
30 loses `blocking-api`; the `## Blocking API` section (118-122) becomes `## Synchronous
programs` with three sentences: the API is async; `zbus::block_on` runs it on the calling
thread with no runtime of the program's own; see the book chapter (link `[bw]` retargeted to
`book/blocking.html`); lines 153-155 and 174-176 lose their `zbus::blocking` mentions.

- [ ] **Step 3: Verify and commit**

`cd book && mdbook build`; `cargo test -p zbus --doc`; `awk 'length > 100'` over the four
files; `/usr/bin/grep -rn -i 'blocking' book/src zbus/README.md` must show only the blocking
hook (`spawn_blocking`), socket modes, and the `blocking` crate in the upgrading guide's
dependency list.

```sh
/usr/bin/git add book/src/client.md book/src/faq.md book/src/upgrading-to-6.md zbus/README.md
/usr/bin/git -c core.hooksPath=/dev/null commit --no-verify --no-gpg-sign -F - <<'EOF'
📝 zb,book: Point synchronous programs at zbus::block_on

Assisted-by: Claude Fable 5.1 (claude-fable-5-1)
EOF
```

---

### Task 4: PR body and push

- [ ] **Step 1: PR body**

Start from this session's scratchpad `pr-body-v2.md` (the current PR body). First paragraph
gains: "The blocking API (`zbus::blocking`, the `blocking-api` feature, the generated
`*ProxyBlocking` types) is gone: a program without a runtime of its own is the async API inside
`zbus::block_on`." The "Commits" list becomes the 17 subjects of the rewritten branch. The
paragraph that opens with "The RFC's one-worker-thread-per-connection design was built first"
stays (it explains the branch `builtin-runtime-threaded`), with one sentence added that the
history was folded to show the final design once. Benchmarks: drop the `blocking-api/roundtrip`
row. Decisions: add **10. The blocking API is removed** with the maintainer's reasoning in two
sentences and the migration recipe's one-liner. Apply with `gh pr edit 1975 --body-file`.

- [ ] **Step 2: Push and watch**

```sh
/usr/bin/git fetch origin && /usr/bin/git rebase origin/main   # no-op if main has not moved
/usr/bin/git push --force-with-lease zeenix builtin-runtime-plan:builtin-runtime
/usr/bin/git tag -d pre-fold
gh pr checks 1975 --watch
```

All checks green is the end of the plan.

## Self-review

- Coverage: every item of the survey's sections 1-6 has a home in Task 2 (module, feature
  gates, macro, tests, fixture, CI, example, xmlgen, book doctest page) or Task 3 (prose).
  Section 8's items are excluded by the constraints. The history fold is Task 1 with a
  byte-identical-tree invariant.
- Names across tasks: `blocking.md` keeps its path (Tasks 2 and 3 link to it);
  `async_name`/`async_object` stay (Task 2 Step 1, Task 3 Step 1 says so).
- Placeholders: the two example sketches say what to check against the current files rather
  than leaving blanks; the `futures_util` note is a check with two outcomes, both specified.
