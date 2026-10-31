//! What a connection costs zbus to take a burst of messages off its socket: reading them through a
//! buffer of 4 KiB, checking each of them whole, its body included, and handing them to the stream
//! of the connection's messages. That is part of what a bus built on zbus, such as busd, spends on
//! each message that it receives, before it routes it. What the runtime underneath costs is
//! benchmarked in zruntime, against a recreation of a connection's traffic.
//!
//! A client sends `BURST` signals of `PAYLOAD_LEN` bytes each, without a sender, as the clients of
//! libdbus, GLib and sd-bus leave it out, over a unix socket pair, to a connection made as busd
//! makes those of its clients: the server end of a peer-to-peer connection that checks the body of
//! each message that it receives, which it takes off a stream of its messages. The two ends run on
//! threads of their own, as a client and a bus would in processes of their own. Each iteration is a
//! burst, from the sending of the first signal until the client hears that the last was taken off
//! the stream. The signals are built once, the same for every burst, and the client hands them to
//! its socket with `send_all`, several to a write, so that their sending holds up their receiving
//! as little as it can.
//!
//! Unix only: the socket pair is a unix one.

/// The benchmark, which needs a unix socket pair.
#[cfg(unix)]
mod unix {
    use std::{
        hint::black_box,
        os::unix::net::UnixStream,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    use criterion::{Criterion, Throughput, criterion_group};
    use futures_util::TryStreamExt;
    use serde_bytes::Bytes;
    use zbus::{Guid, Message, connection::Builder};
    use zruntime::{Event, SharedRuntime};

    /// Benchmarks `connection/signal-burst`: bursts of `BURST` signals from a client, taken off the
    /// stream of a server end that checks their bodies, as busd checks those of its clients.
    fn connection(c: &mut Criterion) {
        let (server_end, client_end) = UnixStream::pair().unwrap();
        let received = Arc::new(Event::new());
        let receiver = {
            let received = received.clone();
            thread::spawn(move || zruntime::block_on(receive(server_end, &received)))
        };
        // The handshake goes on with the server end's on the thread of the latter.
        let client = zruntime::block_on(Builder::unix_stream(client_end).p2p().build()).unwrap();
        let payload = vec![0u8; PAYLOAD_LEN];
        let burst: Vec<_> = (0..BURST)
            .map(|seq| {
                Message::signal(PATH, INTERFACE, MEMBER)
                    .build(&(seq, Bytes::new(&payload)))
                    .unwrap()
            })
            .collect();

        let mut group = c.benchmark_group("connection");
        group.throughput(Throughput::Elements(BURST));
        group.bench_function("signal-burst", |b| {
            b.iter_custom(|iters| {
                zruntime::block_on(async {
                    let runtime = SharedRuntime::current().unwrap();
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Listened for before the first signal is sent, so that the notice that
                        // the last was received cannot come before it.
                        let received = received.listen();
                        let started = Instant::now();
                        // A burst that lost a signal, or whose receiver stopped reading, would be
                        // waited for for good, in the sending of its signals or after it.
                        runtime
                            .timeout(BURST_LIMIT, async {
                                client.send_all(&burst).await.unwrap();
                                received.await
                            })
                            .await
                            .expect("the burst is received within the limit");
                        total += started.elapsed();
                    }

                    total
                })
            })
        });
        group.finish();

        // Hanging up ends the stream of the server end, and with it, its thread.
        drop(client);
        receiver.join().unwrap();
    }

    /// Makes the server end of the pair out of `socket`, as busd makes the connection of each of
    /// its clients, and takes the signals off its stream, notifying `received` after each burst of
    /// them, until the client hangs up.
    async fn receive(socket: UnixStream, received: &Event) {
        let mut messages = Builder::unix_stream(socket)
            .server(Guid::generate())
            .p2p()
            .validate_bodies()
            .build_message_stream()
            .await
            .unwrap();
        loop {
            for received_so_far in 0..BURST {
                match messages.try_next().await {
                    Ok(Some(msg)) => {
                        black_box(msg);
                    }
                    // The client hangs up between two bursts, once the benchmark is done, which
                    // the stream reports as its end, or as a failed read of the socket.
                    Ok(None) | Err(zbus::Error::InputOutput(_)) if received_so_far == 0 => return,
                    // The client would fail to send, or time out, without saying why otherwise.
                    end => {
                        eprintln!("the stream ended or failed within a burst: {end:?}");
                        std::process::abort();
                    }
                }
            }
            received.notify(1);
        }
    }

    /// How many signals each burst has.
    const BURST: u64 = 1000;

    /// How long a burst gets to be received, far longer than any takes, before the benchmark gives
    /// up on it.
    const BURST_LIMIT: Duration = Duration::from_secs(30);

    /// How many bytes of payload each signal carries, besides its sequence number.
    const PAYLOAD_LEN: usize = 1024;

    /// The path that the signals are emitted from.
    const PATH: &str = "/org/zbus/Benchmark";

    /// The interface of the signals.
    const INTERFACE: &str = "org.zbus.Benchmark";

    /// The member of the signals.
    const MEMBER: &str = "Burst";

    criterion_group!(benches, connection);
}

#[cfg(unix)]
criterion::criterion_main!(unix::benches);

/// Nothing to benchmark, where there are no unix socket pairs.
#[cfg(not(unix))]
fn main() {}
