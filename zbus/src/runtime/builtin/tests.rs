//! Tests of a built-in runtime as a whole: the helper thread that starts on the first piece of
//! work handed to it from outside `block_on`, the `block_on` that runs the scheduler and the
//! reactor on its own thread, and the hand-over between the two.

use std::{
    future::{pending, poll_fn},
    io::Write,
    mem::MaybeUninit,
    pin::pin,
    task::{Wake, Waker},
    thread,
    time::Instant,
};

use event_listener::Event;
use futures_lite::future::{block_on, poll_once};
use ntest::timeout;
use socket2::{SockRef, Socket};

use super::*;
use crate::runtime::{
    Interest,
    traits::{PollIo, Runtime, TaskHandle},
};

#[test]
#[timeout(15000)]
fn a_spawned_task_runs_to_completion() {
    let runtime = runtime();
    let ran = Arc::new(Mutex::new(false));
    let task = {
        let ran = ran.clone();
        runtime.spawn("a task that sets a flag", async move {
            *lock(&ran) = true;
        })
    };

    block_on(task).unwrap();

    assert!(*lock(&ran));
}

#[test]
#[timeout(15000)]
fn a_spawned_task_hands_its_output_back() {
    let runtime = runtime();
    let task = runtime.spawn("an answer", async { 42 });

    assert_eq!(block_on(task).unwrap(), 42);
}

#[test]
#[timeout(15000)]
fn the_helper_starts_on_the_first_spawn() {
    let runtime = runtime();
    let task = runtime.spawn("a task that names the thread it runs on", async {
        thread::current().name().map(str::to_owned)
    });

    let name = block_on(task).unwrap();

    assert_eq!(name.as_deref(), Some("zbus runtime"));
}

#[test]
#[timeout(15000)]
fn the_helper_exits_when_nothing_is_left() {
    let runtime = runtime();
    // A timer for the task to be, so that the deadline is armed by the helper's poll of it
    // rather than from this thread.
    let sleep = runtime.sleep(Duration::from_millis(50));
    let task = runtime.spawn("a task that sleeps", sleep);

    block_on(task).unwrap();

    assert!(helper_gone(&runtime));
}

#[test]
#[timeout(15000)]
fn cancelling_the_last_task_from_another_thread_lets_the_helper_exit() {
    let runtime = runtime();
    let task = runtime.spawn("a task that never finishes", pending::<()>());
    assert!(runtime.helper_running());

    drop(task);

    assert!(helper_gone(&runtime));
}

#[test]
#[timeout(15000)]
fn a_finished_detached_task_lets_the_helper_exit() {
    let runtime = runtime();
    // A timer to wait out first, so that the round the task ends in is one the helper reached
    // from a wait that its own timer broke: the wake channel is empty by then, and nobody is
    // told the task is over, since nobody holds a handle to it.
    let sleep = runtime.sleep(Duration::from_millis(50));

    runtime.spawn("a detached task", sleep).detach();

    // Which leaves the helper to work its own idleness out.
    assert!(helper_gone(&runtime));
}

#[test]
#[timeout(15000)]
fn a_registration_reports_readiness() {
    let runtime = runtime();
    let (source, mut peer) = pair();
    let registration = runtime.register_io_source(source.clone()).unwrap();
    // Written from another thread once the read below is waiting, so that the byte is one the
    // helper's wait reports rather than one the first attempt to read finds for itself.
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        peer.write_all(&[7]).unwrap();
    });

    let read = block_on(poll_fn(|cx| {
        let mut byte = [MaybeUninit::<u8>::uninit(); 1];

        registration.poll_io(cx, Interest::Readable, || {
            SockRef::from(&source).recv(&mut byte)
        })
    }));

    assert_eq!(read.unwrap(), 1);
    writer.join().unwrap();
}

/// A socket of one protocol family is watched beside a wake channel of another.
///
/// Winsock takes every socket of one `select` call to come from a single service provider, and
/// a built-in runtime's wake channel is a loopback TCP connection while a `unix:path=` address
/// gives a connection an `AF_UNIX` socket. A wait therefore holds sockets of both families, and
/// this is where that mixture is asked for: an `AF_UNIX` socket registered on a runtime is
/// reported readable just as a socket of the wake channel's own family is.
#[cfg(windows)]
#[test]
#[timeout(15000)]
fn an_af_unix_socket_is_watched_beside_the_tcp_wake_socket() {
    use std::{env, fs, os::windows::io::AsSocket, process, time::SystemTime};

    use uds_windows::{UnixListener, UnixStream};

    let runtime = runtime();
    // Named after this process and this moment, so that two runs of the suite cannot meet over
    // one path.
    let since_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let path = env::temp_dir().join(format!(
        "zbus-builtin-runtime-{}-{}.sock",
        process::id(),
        since_epoch.as_nanos()
    ));
    let listener = UnixListener::bind(&path).unwrap();
    let client = UnixStream::connect(&path).unwrap();
    let (mut server, _) = listener.accept().unwrap();
    client.set_nonblocking(true).unwrap();
    server.set_nonblocking(true).unwrap();
    // The stream keeps the handle it was made with, so what the source is given is a duplicate
    // of it.
    let source = IoSource::from(client.as_socket().try_clone_to_owned().unwrap());
    let registration = runtime.register_io_source(source.clone()).unwrap();
    // Written from another thread once the read below is waiting, so that the byte is one the
    // helper's wait reports rather than one the first attempt to read finds for itself.
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        server.write_all(&[7]).unwrap();
    });

    let read = block_on(poll_fn(|cx| {
        let mut byte = [MaybeUninit::<u8>::uninit(); 1];

        registration.poll_io(cx, Interest::Readable, || {
            SockRef::from(&source).recv(&mut byte)
        })
    }));

    assert_eq!(read.unwrap(), 1);
    writer.join().unwrap();
    drop(listener);
    fs::remove_file(&path).unwrap();
}

#[test]
#[timeout(15000)]
fn sleep_resolves_once_the_duration_has_passed() {
    let runtime = runtime();
    let started = Instant::now();

    block_on(runtime.sleep(Duration::from_millis(50)));

    assert!(started.elapsed() >= Duration::from_millis(50));
}

#[test]
#[timeout(15000)]
fn a_sleep_polled_after_the_helper_retired_is_fired() {
    let runtime = runtime();
    let sleep = runtime.sleep(Duration::from_millis(20));
    // A timer that has never been polled has no deadline with the reactor, so there is nothing
    // for a helper to do and none of them runs.
    assert!(!runtime.helper_running());
    let started = Instant::now();

    block_on(sleep);

    assert!(started.elapsed() >= Duration::from_millis(20));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
#[timeout(15000)]
fn a_sleep_armed_from_another_thread_bounds_the_wait() {
    let runtime = runtime();
    let (source, _peer) = pair();
    // A source to watch and no deadline: the helper sits in a wait that nothing but a
    // notification brings to an end.
    let _registration = runtime.register_io_source(source).unwrap();
    let started = Instant::now();

    block_on(runtime.sleep(Duration::from_millis(20)));

    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
#[timeout(15000)]
fn a_dropped_sleep_lets_the_helper_exit() {
    let runtime = runtime();
    {
        let mut sleep = pin!(runtime.sleep(Duration::from_secs(10)));
        // The first poll is what hands the deadline to the reactor.
        assert!(block_on(poll_once(sleep.as_mut())).is_none());
        assert!(runtime.helper_running());
    }

    // The deadline went with the timer it belonged to, so the wait it bounded is over too.
    assert!(helper_gone(&runtime));
}

#[test]
#[timeout(15000)]
fn a_spawn_during_the_helper_exit_is_not_stranded() {
    let runtime = runtime();

    // No pause between the rounds, so that each spawn lands wherever the round before it left
    // the helper, the exit it is about to make included.
    for _ in 0..200 {
        let started = Instant::now();
        let task = runtime.spawn("a task that finishes at once", async {});

        block_on(task).unwrap();

        assert!(started.elapsed() < Duration::from_secs(5));
    }
}

#[test]
#[timeout(15000)]
fn a_registration_without_tasks_keeps_the_helper_alive() {
    let runtime = runtime();
    let (source, _peer) = pair();

    let _registration = runtime.register_io_source(source).unwrap();

    assert!(runtime.helper_running());
    // Long enough that a helper taking no account of the registration would have retired.
    thread::sleep(Duration::from_millis(200));
    assert!(runtime.helper_running());
}

#[test]
#[timeout(15000)]
fn a_task_that_panics_leaves_the_helper_running() {
    let runtime = runtime();
    // Keeps the helper from retiring between the tasks below, so that the task after the panic
    // is run by the very helper the panic happened on.
    let _held = runtime.spawn("a task that never finishes", pending::<()>());
    let panicking = runtime.spawn("a task that panics", async {
        panic!("the task panicked on purpose");
    });

    assert!(block_on(panicking).is_err());

    // The task that never finishes rules retirement out, so a helper running here is the one the
    // panic happened on rather than one a later spawn started.
    assert!(runtime.helper_running());
    let next = runtime.spawn("the task after it", async { 7 });
    assert_eq!(block_on(next).unwrap(), 7);
}

#[test]
#[timeout(15000)]
fn a_panic_in_a_futures_drop_leaves_the_helper_running() {
    /// A task that is finished with on its first poll, and whose future panics as the helper
    /// drops it.
    struct PanicOnDrop;

    impl Future for PanicOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("the future's drop panicked on purpose");
        }
    }

    let runtime = runtime();
    let _held = runtime.spawn("a task that never finishes", pending::<()>());
    let going = runtime.spawn("a task that panics as it goes", PanicOnDrop);

    assert!(block_on(going).is_ok());

    // The task that never finishes rules retirement out, so a helper running here is the one
    // that dropped the future above rather than one a later spawn started.
    assert!(runtime.helper_running());
    let next = runtime.spawn("the task after it", async { 7 });
    assert_eq!(block_on(next).unwrap(), 7);
}

#[test]
#[timeout(15000)]
fn a_cancelled_futures_drop_may_spawn_on_the_runtime() {
    /// Spawns a task of its own as it goes, through the runtime it holds.
    struct SpawnOnDrop {
        runtime: Builtin,
        ran: Arc<Mutex<bool>>,
    }

    impl Drop for SpawnOnDrop {
        fn drop(&mut self) {
            let ran = self.ran.clone();
            self.runtime
                .spawn("a task spawned from a drop", async move {
                    *lock(&ran) = true;
                })
                .detach();
        }
    }

    let runtime = runtime();
    let ran = Arc::new(Mutex::new(false));
    let polled = Arc::new(Mutex::new(false));
    let task = {
        let spawner = SpawnOnDrop {
            runtime: runtime.clone(),
            ran: ran.clone(),
        };
        let polled = polled.clone();
        runtime.spawn("a task that never finishes", async move {
            let _spawner = spawner;
            *lock(&polled) = true;
            pending::<()>().await;
        })
    };

    // Cancelled once the helper has taken the task up, so that the drop of its future is this
    // thread's to make. A cancellation racing that poll would leave the drop to the helper
    // instead, and the spawn the drop makes is served the same either way.
    assert!(within_a_second(|| *lock(&polled)));
    drop(task);

    assert!(within_a_second(|| *lock(&ran)));
}

#[test]
#[timeout(15000)]
fn a_helper_that_dies_is_replaced() {
    /// Panics where it is woken, which is on the helper that fires the timer it waits for.
    struct PanickingWaker;

    impl Wake for PanickingWaker {
        fn wake(self: Arc<Self>) {
            panic!("the waker panicked on purpose");
        }
    }

    let runtime = runtime();
    // A task that never finishes, so that retiring is out of the question and an unwind is the
    // one thing that can clear the running flag.
    let _held = runtime.spawn("a task that never finishes", pending::<()>());
    let waker = Waker::from(Arc::new(PanickingWaker));
    let mut sleep = pin!(runtime.sleep(Duration::from_millis(1)));
    let polled = sleep.as_mut().poll(&mut Context::from_waker(&waker));
    assert!(polled.is_pending());

    // The helper fires the timer, the wake panics, and the thread unwinds out of its loop.
    assert!(helper_gone(&runtime));

    let task = runtime.spawn("the task after it", async { 7 });
    assert_eq!(block_on(task).unwrap(), 7);
}

#[test]
#[timeout(15000)]
fn block_on_runs_the_tasks_on_its_own_thread() {
    let runtime = runtime();

    // Spawned from inside, so that the runtime is handed its first piece of work by the thread
    // in the seat, which asks for no helper and runs that work itself.
    let ran_on = drive(&runtime, async {
        runtime
            .spawn("a task that names the thread it runs on", async {
                thread::current().id()
            })
            .await
    })
    .unwrap();

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
    // Written from another thread once the read below is waiting, so that the byte is one the
    // wait reports rather than one the first attempt to read finds for itself.
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        peer.write_all(&[7]).unwrap();
    });

    let read = drive(&runtime, async {
        // Registered from inside, so that the runtime is handed its first piece of work by the
        // thread in the seat, which asks for no helper and does the waiting itself. The
        // registration goes with this future, and so is gone before the seat is given up.
        let registration = runtime.register_io_source(source.clone()).unwrap();

        poll_fn(|cx| {
            let mut byte = [MaybeUninit::<u8>::uninit(); 1];

            registration.poll_io(cx, Interest::Readable, || {
                SockRef::from(&source).recv(&mut byte)
            })
        })
        .await
    });

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

    // Kept in a place of its own rather than handed out as the future's output: a task handle is
    // a future itself, and an async block that yields one reads as though it were to be awaited.
    let mut task = None;
    drive(&runtime, async {
        task = Some(runtime.spawn("a task that never finishes", pending::<()>()));
    });

    assert!(within_a_second(|| runtime.helper_running()));
    drop(task);
    assert!(helper_gone(&runtime));
}

#[test]
#[timeout(15000)]
fn work_left_by_a_block_on_that_took_no_seat_goes_to_a_helper() {
    let runtime = runtime();
    let resolve = resolve_on_the_second_ask(&runtime);
    let mut task = None;

    driver::block_on(resolve, async {
        task = Some(runtime.spawn("a task that never finishes", pending::<()>()));
    });

    assert!(within_a_second(|| runtime.helper_running()));
    drop(task);
    assert!(helper_gone(&runtime));
}

/// A panic out of a seat-less `block_on` hands the work on all the same.
///
/// The future that panics may have built a connection which outlives it, a clone of it stashed
/// somewhere the panic does not reach. That connection's reader task was spawned while this
/// thread stood surety for running it, and has nobody to run it once the thread has unwound.
#[test]
#[timeout(15000)]
fn work_left_by_a_block_on_that_panicked_without_the_seat_goes_to_a_helper() {
    let runtime = runtime();
    let resolve = resolve_on_the_second_ask(&runtime);
    let mut task = None;

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        driver::block_on(resolve, async {
            task = Some(runtime.spawn("a task that never finishes", pending::<()>()));
            panic!("a future that panics once it has spawned");
        });
    }));
    assert!(panicked.is_err());

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

/// A `block_on` that waited for the seat leaves nothing of itself on the runtime.
///
/// A thread that finds the seat taken is put down as waiting for it, with a handle on itself for
/// the unpark. A thread-per-request program calling `zbus::block_on`, where a helper keeps the
/// seat for a connection's whole life, would gather one such handle per thread it ever ran and hold
/// them for the life of the process, were the entry not taken back where its call returns.
#[test]
#[timeout(15000)]
fn a_block_on_that_waited_for_the_seat_leaves_nothing_behind() {
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
    // Long enough for the thread above to take the seat, which it keeps until it is released.
    thread::sleep(Duration::from_millis(50));
    let task = runtime.spawn("a task that names the thread it runs on", async {
        thread::current().id()
    });

    let ran_on = drive(&runtime, task).unwrap();

    // The task ran on the other thread, which says the seat was that thread's throughout and
    // this one waited for it rather than took it.
    assert_eq!(ran_on, driver.thread().id());
    assert_eq!(waiting(&runtime), 0);
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

/// A runtime of this test's own, with nothing to do and no thread until it is given something.
fn runtime() -> Builtin {
    Builtin::from_inner(Inner::new().unwrap())
}

/// A `block_on` on `runtime`, resolving to that runtime and no other.
fn drive<F>(runtime: &Builtin, future: F) -> F::Output
where
    F: Future,
{
    let inner = runtime.inner().clone();

    driver::block_on(Arc::new(move || Some(inner.clone())), future)
}

/// A resolver that finds no runtime the first time it is asked and `runtime` every time after.
///
/// Which is what a `block_on` whose very first poll builds the connection comes to: it polls
/// without the seat, and the work that poll leaves has nobody to run it.
fn resolve_on_the_second_ask(runtime: &Builtin) -> driver::Resolve {
    let inner = runtime.inner().clone();
    let looked = Mutex::new(false);

    Arc::new(move || {
        let mut looked = lock(&looked);
        if *looked {
            return Some(inner.clone());
        }
        *looked = true;

        None
    })
}

/// How many threads are down as waiting for `runtime`'s seat.
fn waiting(runtime: &Builtin) -> usize {
    lock(&runtime.inner().seat).waiting()
}

/// Whether `runtime`'s helper has gone within a second.
fn helper_gone(runtime: &Builtin) -> bool {
    within_a_second(|| !runtime.helper_running())
}

/// Whether `condition` holds within a second, looked at every 10 ms.
fn within_a_second(condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// A connected pair: the source to register, and the far end to drive it from.
fn pair() -> (IoSource, Socket) {
    let (near, far) = connected();
    near.set_nonblocking(true).unwrap();
    far.set_nonblocking(true).unwrap();

    (IoSource::from_socket(near), far)
}

/// Two sockets connected to one another.
#[cfg(unix)]
fn connected() -> (Socket, Socket) {
    Socket::pair(socket2::Domain::UNIX, socket2::Type::STREAM, None).unwrap()
}

/// Two sockets connected to one another.
///
/// Winsock has no socket pair, so this is a loopback connection which a listener of its own
/// accepts and then has no further use for.
#[cfg(windows)]
fn connected() -> (Socket, Socket) {
    use std::net::{TcpListener, TcpStream};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let far = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let near = loop {
        let (accepted, _) = listener.accept().unwrap();
        // A loopback listener is reachable by anything else on the machine, so a connection that
        // is not the one made just above is turned away rather than taken for it.
        if accepted.peer_addr().unwrap() == far.local_addr().unwrap() {
            break accepted;
        }
    };

    // One-byte messages travel this pair, as they do the poller's wake pair, so neither end
    // holds a send back for the peer's acknowledgement of the one before it.
    near.set_nodelay(true).unwrap();
    far.set_nodelay(true).unwrap();

    (near.into(), far.into())
}

#[test]
#[timeout(15000)]
fn every_handle_in_a_registry_shares_one_runtime() {
    let registry = Mutex::new(Weak::new());
    let first = Builtin::from_inner(Inner::shared_in(&registry).unwrap());
    let second = Builtin::from_inner(Inner::shared_in(&registry).unwrap());

    assert!(Arc::ptr_eq(first.inner(), second.inner()));
}

#[test]
#[timeout(15000)]
fn a_shared_runtime_goes_with_its_last_handle() {
    let registry = Mutex::new(Weak::new());
    let handle = Builtin::from_inner(Inner::shared_in(&registry).unwrap());
    let inner = Arc::downgrade(handle.inner());

    drop(handle);

    // Nothing was spawned or registered, so no thread holds the runtime either.
    assert!(inner.upgrade().is_none());
    // And the next handle brings a fresh one into being.
    let next = Builtin::from_inner(Inner::shared_in(&registry).unwrap());
    assert!(inner.upgrade().is_none());
    drop(next);
}
