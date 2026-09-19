//! What a connection costs on the runtime it is built on: its setup and teardown, a method
//! call's round trip — from the thread inside `block_on` and from behind a helper thread — a
//! large body, a signal, the runtime's task spawn and its blocking hook. Every benchmark runs
//! over a unix socket pair with a p2p handshake, so nothing here depends on a bus and everything
//! goes through the runtime's readiness path, which an in-process channel would bypass.

#[cfg(unix)]
mod unix {
    use std::{
        hint::black_box,
        os::unix::net::UnixStream,
        time::{Duration, Instant},
    };

    use criterion::{BatchSize, Criterion, Throughput, criterion_group};
    use futures_util::StreamExt;
    use zbus::{Connection, Message, connection::Builder, message::Type};

    const PATH: &str = "/org/zbus/Benchmark";
    const INTERFACE: &str = "org.zbus.Benchmark";
    const BIG: usize = 1024 * 1024;

    fn runtime(c: &mut Criterion) {
        // Every other id builds its connection pair in one `block_on` and measures in later
        // ones, so a helper thread holds the seat and the calling thread parks on each call.
        // Here the pair is built and every call made inside one `block_on`, so the calling
        // thread is the one running the runtime and zbus starts no thread of its own. The number
        // is per call, comparable with `method-call/roundtrip`.
        let mut group = c.benchmark_group("block_on");
        group.bench_function("roundtrip-inside", |b| {
            b.iter_custom(|iters| {
                zbus::block_on(async move {
                    let (_server, client) = pair().await;
                    let started = Instant::now();
                    for _ in 0..iters {
                        black_box(ping(&client, 1).await);
                    }

                    started.elapsed()
                })
            });
        });
        group.finish();

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
