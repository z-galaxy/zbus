#!/bin/sh
# Fails if any crate zbus's runtimes do not need shows up as a normal dependency of zbus, in
# whichever feature set the caller passes through. zruntime is a task scheduler and poll(2)-based
# reactor of its own: no build of zbus should link async-io, async-executor, async-task, blocking
# or their private dependencies as a normal dependency, whether the default-rt feature, the tokio
# feature, both or neither is on (`-e normal` already excludes dev-dependencies, so the test
# suite's own use of async-io does not trip this, and `--target all` keeps the dependencies of
# every platform in, so the Windows and macOS graphs are guarded along with the host's). zbus
# builds its notifications on zruntime's `Event`, and gets its async locks (where Tokio's do not
# stand in), broadcast channels and the thread its default blocking-work hook runs on from
# zruntime too, so every tree with the `comms` feature has the `zruntime` crate in it, with its
# `event`, `lock`, `broadcast` and `unblock` features always, its `runtime`, `helper`, `tcp`,
# `unix` and `process` features only where zbus's `default-rt` feature is on and its `tracing`
# feature only where zbus's `tracing` is; it is expected there rather than being on the forbidden
# list. So is `signal-hook-registry`, which Tokio's `process` feature adds in the builds with the
# `tokio` feature and a transport that runs a program (`unixexec`, `ibus`, or macOS, where
# `launchd:` does). `fastrand`, which blocking and async-executor are built on, is not on the
# list either: on Windows, `uds_windows` brings it in through `tempfile`.
#
# A build that leaves connections out, such as one with only the `core` feature of the D-Bus API,
# has no use for zruntime or for any other crate that only connections need, so the caller can
# forbid those too, through `FORBIDDEN_EXTRA`, an alternation of crate names like `FORBIDDEN`'s.
#
# Usage: [FORBIDDEN_EXTRA=<names>] forbidden-deps.sh <label> [cargo tree arguments...]
#
# <label> names the build being checked, for the failure message (e.g. "default",
# "tokio-only", "external-only"). Everything after it is passed straight to `cargo tree`, e.g.:
#   CI/forbidden-deps.sh tokio-only --no-default-features --features tokio,proxy,service
#
# The tree is written out before it is searched, so that a `cargo tree` failure fails the job
# instead of handing grep an empty stream. A `cargo tree` line is a prefix of box-drawing
# characters, the crate name and its version, so matching from the start of the line up to the
# version is what keeps a crate whose name merely contains one of these from counting.
set -eu

label=$1
shift

FORBIDDEN="async-io|async-executor|async-task|async-lock|async-process|blocking"
FORBIDDEN="$FORBIDDEN|polling|async-channel|async-signal|piper|futures-lite"
if [ -n "${FORBIDDEN_EXTRA:-}" ]; then
    FORBIDDEN="$FORBIDDEN|$FORBIDDEN_EXTRA"
fi

tree_file=$(mktemp)
trap 'rm -f "$tree_file"' EXIT

cargo --locked tree -e normal -p zbus --target all "$@" > "$tree_file"

if grep -E "^[^a-zA-Z0-9]*($FORBIDDEN) v" "$tree_file"; then
    echo "a crate that the $label build does not need is in its graph" >&2
    exit 1
fi
