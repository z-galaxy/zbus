#!/bin/sh
# Fails if any crate the builtin runtime does not need shows up as a normal dependency of zbus,
# in whichever feature set the caller passes through. The builtin runtime is zbus's own task
# scheduler and its own poll(2)-based reactor: no build of zbus should link async-io,
# async-executor, async-task, blocking or their private dependencies as a normal dependency,
# whether the builtin-runtime feature, the tokio feature, both or neither is on (`-e normal`
# already excludes dev-dependencies, so the test suite's own use of async-io does not trip
# this). `event-listener`, which zbus's own async locks build on, is expected in every tree and
# is deliberately not on the forbidden list.
#
# Usage: forbidden-deps.sh <label> [cargo tree arguments...]
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
FORBIDDEN="$FORBIDDEN|polling|async-channel|async-signal|piper"

tree_file=$(mktemp)
trap 'rm -f "$tree_file"' EXIT

cargo --locked tree -e normal -p zbus "$@" > "$tree_file"

if grep -E "^[^a-zA-Z0-9]*($FORBIDDEN) v" "$tree_file"; then
    echo "a crate the builtin runtime does not need is in the $label graph" >&2
    exit 1
fi
