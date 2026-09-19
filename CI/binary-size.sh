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
    # Capture the client's output instead of piping it straight to grep, so that the exit
    # status this loop sees (via `set -e`) is the client's own, not grep's: a client that
    # prints the latitude and then fails would otherwise still pass. The client is retried
    # rather than given a fixed wait, because how long the service takes to claim its bus
    # name is the machine's business: a loaded runner can take longer than any wait worth
    # writing down. Only the last attempt's output is shown, so that the attempts made
    # before the name is there say nothing and a run that gives up says why.
    if ! output=$(dbus-run-session -- sh -c "
        target/size/geoclue_service_$runtime &
        service=\$!
        attempt=1
        while :; do
            client=\$(target/size/geoclue_client_$runtime 2>&1)
            status=\$?
            if [ \$status -eq 0 ] || [ \$attempt -ge 20 ]; then
                break
            fi
            attempt=\$((attempt + 1))
            sleep 0.25
        done
        kill \"\$service\" 2>/dev/null || true
        printf '%s\n' \"\$client\"
        exit \$status
    "); then
        printf 'the %s pair could not talk over a private session bus:\n%s\n' \
            "$runtime" "$output" >&2
        exit 1
    fi
    printf '%s\n' "$output" | grep -q '^Latitude: 59\.3293$'
done
