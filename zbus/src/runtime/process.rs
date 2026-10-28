//! The helper process behind a transport.
//!
//! `unixexec:` runs a program and speaks D-Bus over its standard input and output, while `ibus:`
//! and, on macOS, `launchd:` run one to ask it where the bus is. Either way the connection makes
//! the pipes to the program itself and hands the program's ends of them over as its standard
//! streams, along with the command, to the connection's runtime: the runtime spawns the program
//! and gives back the future of its exit status. The connection's own ends of the pipes are
//! watched by that runtime the way its sockets are.
//!
//! The wait for a `unixexec:` program is a task of the runtime, spawned along with the program and
//! left to run on its own, so the program is collected as soon as it exits. The call that runs an
//! `ibus:` or `launchd:` program awaits its exit status itself. Closing a `unixexec:` connection
//! closes the program's input, which is what tells the program to exit. What the runtime does to
//! wait for the program, and what that costs, is the runtime's own: see
//! [`Runtime::spawn_process`].
//!
//! [`Runtime::spawn_process`]: crate::runtime::traits::Runtime::spawn_process

#[cfg(feature = "unixexec")]
use std::os::fd::BorrowedFd;
use std::{
    io,
    os::fd::OwnedFd,
    process::{Command, Stdio},
    sync::Arc,
};

#[cfg(feature = "unixexec")]
use super::io::Reader;
use super::{
    Runtime,
    io::{PipeOps, RegisteredIo, registered},
};
#[cfg(feature = "unixexec")]
use crate::{
    Message,
    connection::socket::{BoxedSplit, ReadHalf, Split, WriteHalf},
    fdo::ConnectionCredentials,
};

/// Runs `command` and hands back its standard input and output, watched by `runtime`.
///
/// The child keeps the caller's standard error. The pipes are made and registered before the
/// program is spawned, so that nothing can fail once there is a program to collect. A task of
/// `runtime` awaits the future of the program's exit status from then on, so the program is
/// collected as soon as it exits, whatever has become of the [`Child`] by then.
#[cfg(feature = "unixexec")]
pub(crate) fn spawn(runtime: &Runtime, command: Command) -> io::Result<Child> {
    let (stdin, child_stdin) = pipe_to(runtime)?;
    let (stdout, child_stdout) = pipe_from(runtime)?;
    let exit = runtime.spawn_process(command, child_stdin, child_stdout, Stdio::inherit())?;
    // A task of the runtime waits for the program, so that it is collected as soon as it exits.
    // How it ended has no one to be reported to, so the task lets the status go.
    runtime
        .spawn("helper process", async move {
            let _ = exit.await;
        })
        .detach();

    Ok(Child { stdin, stdout })
}

/// Runs `command` to completion on `runtime` and hands back what it printed.
///
/// A program asked where a bus is has nothing to read and nothing to say beyond that address, so
/// its standard output is the only pipe here. Its standard input is the null device — the end of
/// its input is there from the start, and there is no pipe of its own to register — and so is its
/// standard error, whose contents nobody asking for an address has ever been shown.
///
/// A program that ends with a failing status is an error naming that status, so the bytes a
/// caller gets back are always those of a program that succeeded.
///
/// This call awaits the exit status itself. A read that fails, or a call that is given up on while
/// it reads, drops the future of the exit status unresolved, which leaves the program to the
/// runtime to collect as it sees fit.
#[cfg(any(feature = "ibus", target_os = "macos"))]
pub(crate) async fn stdout(runtime: &Runtime, command: Command) -> io::Result<Vec<u8>> {
    let (pipe, child_stdout) = pipe_from(runtime)?;
    let exit = runtime.spawn_process(command, Stdio::null(), child_stdout, Stdio::null())?;

    let mut collected = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let (read, _) = pipe.recv(&mut buffer).await?;
        if read == 0 {
            break;
        }
        collected.extend_from_slice(&buffer[..read]);
    }
    // Nothing more can come out of the pipe, so the program has exited or is about to.
    drop(pipe);
    let status = exit.await?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "the helper process ended with {status}"
        )));
    }

    Ok(collected)
}

/// A running helper process, with both of its pipes registered on a runtime.
#[cfg(feature = "unixexec")]
#[derive(Debug)]
pub(crate) struct Child {
    stdin: Arc<RegisteredIo<PipeOps>>,
    stdout: Arc<RegisteredIo<PipeOps>>,
}

#[cfg(feature = "unixexec")]
impl Child {
    /// The child's standard output and input, as the halves a connection reads and writes.
    ///
    /// [`Connection::close`] closes the helper's standard input, which is what tells the program
    /// to exit; a pipe has no shutdown of its own, so closing here is letting go of the
    /// descriptor.
    ///
    /// [`Connection::close`]: crate::Connection::close
    pub(crate) fn into_split(self) -> BoxedSplit {
        let Self { stdin, stdout } = self;

        Split::new(
            Box::new(Reader::new(stdout)) as Box<dyn ReadHalf>,
            Box::new(Stdin { pipe: Some(stdin) }) as Box<dyn WriteHalf>,
        )
    }
}

/// The standard input of a helper process, as the half a connection writes to.
///
/// A pipe has no shutdown of its own, so closing this half is letting go of the descriptor: that
/// is what the program at the other end sees as the end of its input, and what a `unixexec:`
/// program takes as the word to exit.
#[cfg(feature = "unixexec")]
#[derive(Debug)]
struct Stdin {
    pipe: Option<Arc<RegisteredIo<PipeOps>>>,
}

#[cfg(feature = "unixexec")]
#[async_trait::async_trait]
impl WriteHalf for Stdin {
    async fn send_messages(&mut self, msgs: &[Message]) -> crate::Result<()> {
        WriteHalf::send_messages(self.pipe()?, msgs).await
    }

    async fn sendmsg(&mut self, buffer: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
        WriteHalf::sendmsg(self.pipe()?, buffer, fds).await
    }

    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    async fn send_zero_byte(&mut self) -> io::Result<Option<usize>> {
        WriteHalf::send_zero_byte(self.pipe()?).await
    }

    async fn close(&mut self) -> io::Result<()> {
        self.pipe.take();

        Ok(())
    }

    fn can_pass_unix_fd(&self) -> bool {
        self.pipe.as_ref().is_some_and(WriteHalf::can_pass_unix_fd)
    }

    async fn peer_credentials(&mut self) -> io::Result<ConnectionCredentials> {
        WriteHalf::peer_credentials(self.pipe()?).await
    }
}

#[cfg(feature = "unixexec")]
impl Stdin {
    /// The pipe, or the error for a half whose descriptor has been let go of.
    fn pipe(&mut self) -> io::Result<&mut Arc<RegisteredIo<PipeOps>>> {
        self.pipe.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "the standard input of the helper process is closed",
            )
        })
    }
}

/// A pipe to a helper process, as the end `runtime` watches for writing, and the end the process
/// reads its standard input from.
///
/// The ends are the ones [`pipe_from`] explains, the other way round.
#[cfg(feature = "unixexec")]
fn pipe_to(runtime: &Runtime) -> io::Result<(Arc<RegisteredIo<PipeOps>>, Stdio)> {
    let (reader, writer) = io::pipe()?;
    let ours = registered(runtime, OwnedFd::from(writer), PipeOps).map(Arc::new)?;

    Ok((ours, Stdio::from(reader)))
}

/// A pipe from a helper process, as the end `runtime` watches for reading, and the end the process
/// writes its standard output to.
///
/// Both ends of a pipe from [`io::pipe`] are close-on-exec, which keeps the process from holding
/// its own pipes open: the end it is handed is duplicated onto its standard stream, and no other
/// end survives into the program. The reading side comes to the end of the pipe once every copy of
/// the write end is closed, and the only one left on this side is the [`Stdio`] returned here,
/// which the caller hands to the runtime to spawn with.
fn pipe_from(runtime: &Runtime) -> io::Result<(Arc<RegisteredIo<PipeOps>>, Stdio)> {
    let (reader, writer) = io::pipe()?;
    let ours = registered(runtime, OwnedFd::from(reader), PipeOps).map(Arc::new)?;

    Ok((ours, Stdio::from(writer)))
}

// Every test here runs a program and reads what it printed, which is what `stdout` is.
#[cfg(all(test, any(feature = "ibus", target_os = "macos")))]
mod tests {
    use ntest::timeout;

    use super::*;
    use crate::runtime::test_runtime::under_every_runtime;

    /// A helper process is waited for, so it leaves no zombie behind.
    #[test]
    #[timeout(15000)]
    fn a_helper_process_is_reaped() {
        under_every_runtime(|runtime| async move {
            let printed = stdout(&runtime, Command::new("true")).await.unwrap();

            assert!(printed.is_empty(), "got {printed:?}");
        });
    }

    /// A helper process that closes its output and then runs on is waited for, and how it ended
    /// is what the call reports.
    ///
    /// A program that had exited by the time its output ends is looked at once and collected. This
    /// one closes its output at once and then runs for a moment, so the read ends with the program
    /// still there to be waited for, and the status the wait comes back with is the one it exits
    /// with.
    #[test]
    #[timeout(15000)]
    fn a_helper_process_that_runs_on_after_its_output_ends_is_waited_for() {
        under_every_runtime(|runtime| async move {
            let mut command = Command::new("sh");
            command.args(["-c", "exec >&-; sleep 0.2; exit 3"]);

            let failed = stdout(&runtime, command).await.unwrap_err();

            assert!(
                failed.to_string().contains("exit status: 3"),
                "got `{failed}`",
            );
        });
    }

    /// What a helper process writes to its standard output is read to the end of it.
    #[test]
    #[timeout(15000)]
    fn stdout_is_collected() {
        under_every_runtime(|runtime| async move {
            let mut command = Command::new("echo");
            command.arg("hello");

            let printed = stdout(&runtime, command).await.unwrap();

            assert_eq!(printed, b"hello\n");
        });
    }

    /// A helper process asked for its output gets no standard input of its own to wait for.
    ///
    /// `cat` reads its input to the end before the `echo` after it runs, so the program only
    /// finishes at all if its input has an end: the null device has one from the start, an open
    /// pipe has none.
    #[test]
    #[timeout(15000)]
    fn a_helper_process_asked_for_its_output_reads_no_input() {
        under_every_runtime(|runtime| async move {
            let mut command = Command::new("sh");
            command.args(["-c", "cat; echo done"]);

            let printed = stdout(&runtime, command).await.unwrap();

            assert_eq!(printed, b"done\n");
        });
    }

    /// A helper process that ends badly is an error, rather than empty output.
    #[test]
    #[timeout(15000)]
    fn a_failing_helper_process_is_an_error() {
        under_every_runtime(|runtime| async move {
            let mut command = Command::new("sh");
            command.args(["-c", "exit 3"]);

            let failed = stdout(&runtime, command).await.unwrap_err();

            assert!(
                failed.to_string().contains("exit status: 3"),
                "got `{failed}`",
            );
        });
    }

    /// A program that cannot be started is an error from the spawn, with nothing to wait for.
    #[test]
    #[timeout(15000)]
    fn a_program_that_cannot_be_started_is_an_error() {
        under_every_runtime(|runtime| async move {
            let failed = stdout(&runtime, Command::new("/nonexistent/zbus-helper"))
                .await
                .unwrap_err();

            assert_eq!(failed.kind(), io::ErrorKind::NotFound);
        });
    }
}

// The halves below are what a `unixexec:` connection is handed, so they are only built, and only
// tested, with that transport.
#[cfg(all(test, feature = "unixexec"))]
mod split_tests {
    use ntest::timeout;

    use super::*;
    #[cfg(target_os = "linux")]
    use crate::runtime::process_table::{is_in_the_process_table, wait_until_collected};
    use crate::runtime::test_runtime::under_every_runtime;

    /// A helper process that exits is collected while the connection holds both of its halves.
    ///
    /// The program announces its process id and exits of its own accord. Neither half is closed
    /// or let go of, so what collects the program is the task that waits for it.
    #[cfg(target_os = "linux")]
    #[test]
    #[timeout(15000)]
    fn a_helper_process_that_exits_is_collected_while_the_connection_holds_both_halves() {
        under_every_runtime(|runtime| async move {
            let (pid, mut stdout, stdin) = start(&runtime, "echo $$").await;

            wait_until_collected(pid);

            // The program is gone from the process table, and its output ends all the same.
            assert_eq!(read_to_the_end(&mut stdout).await, b"");
            drop(stdin);
        });
    }

    /// A helper process is collected as soon as it exits, whether or not what it wrote has been
    /// read.
    ///
    /// `cat` stands in for a `unixexec:` helper: it writes back what it is given and only exits
    /// once its input ends. Its input is closed here while what it wrote back is still to be read.
    /// The program exits and is collected with the read half still held, and what it wrote is
    /// there to be read afterwards.
    #[cfg(target_os = "linux")]
    #[test]
    #[timeout(15000)]
    fn a_helper_process_is_collected_as_it_exits_with_its_output_unread() {
        under_every_runtime(|runtime| async move {
            let (pid, mut stdout, mut stdin) = start(&runtime, "echo $$; exec cat").await;

            WriteHalf::sendmsg(&mut stdin, b"hello", &[]).await.unwrap();
            WriteHalf::close(&mut stdin).await.unwrap();
            wait_until_collected(pid);

            assert_eq!(read_to_the_end(&mut stdout).await, b"hello");
        });
    }

    /// A helper process that is still running when the connection stops reading it is left
    /// running, and is collected once it exits.
    ///
    /// `cat` blocks reading its input and the write half holds that open, so the program is
    /// running when the read half goes. Letting go of the read half neither kills the program nor
    /// stops the wait for it: closing the write half is what lets the program go, and the entry in
    /// the process table is gone once the task that waits for it has collected the program.
    #[cfg(target_os = "linux")]
    #[test]
    #[timeout(15000)]
    fn a_helper_process_still_running_is_collected_once_it_exits() {
        under_every_runtime(|runtime| async move {
            let (pid, stdout, mut stdin) = start(&runtime, "echo $$; exec cat").await;

            drop(stdout);
            // Letting go of the read half did not kill the program.
            assert!(is_in_the_process_table(pid));

            WriteHalf::close(&mut stdin).await.unwrap();
            wait_until_collected(pid);
        });
    }

    /// The helper process itself is gone once both halves are, rather than left behind.
    ///
    /// A program that announces its own process id and then becomes `cat` is one this test can
    /// watch from the outside: the entry under `/proc` outlives a child that was told to exit
    /// but never waited for, so its disappearance is the wait having run.
    #[cfg(target_os = "linux")]
    #[test]
    #[timeout(15000)]
    fn a_helper_process_leaves_the_process_table() {
        under_every_runtime(|runtime| async move {
            let (pid, mut stdout, mut stdin) = start(&runtime, "echo $$; exec cat").await;

            WriteHalf::close(&mut stdin).await.unwrap();
            read_to_the_end(&mut stdout).await;
            drop(stdout);
            drop(stdin);

            wait_until_collected(pid);
        });
    }

    /// A half that has been closed has no descriptor left to write to.
    #[test]
    #[timeout(15000)]
    fn a_write_to_a_closed_half_fails() {
        under_every_runtime(|runtime| async move {
            let (_stdout, mut stdin) = spawn(&runtime, Command::new("cat"))
                .unwrap()
                .into_split()
                .take();

            WriteHalf::close(&mut stdin).await.unwrap();
            let refused = WriteHalf::sendmsg(&mut stdin, b"hello", &[])
                .await
                .unwrap_err();

            assert_eq!(refused.kind(), io::ErrorKind::NotConnected);
        });
    }

    /// Runs `script` as the helper process, and reads the process id its first line announces.
    ///
    /// Nothing but that line is read, so that what the program writes after it is still there.
    #[cfg(target_os = "linux")]
    async fn start(
        runtime: &Runtime,
        script: &str,
    ) -> (u32, Box<dyn ReadHalf>, Box<dyn WriteHalf>) {
        let mut command = Command::new("sh");
        command.args(["-c", script]);
        let (mut stdout, stdin) = spawn(runtime, command).unwrap().into_split().take();

        let mut announced = Vec::new();
        while !announced.ends_with(b"\n") {
            let mut byte = [0];
            let (read, _) = ReadHalf::recvmsg(&mut stdout, &mut byte).await.unwrap();
            assert_ne!(read, 0, "the helper ended before it said what it was");
            announced.push(byte[0]);
        }
        let pid = String::from_utf8_lossy(&announced).trim().parse().unwrap();

        (pid, stdout, stdin)
    }

    /// Everything `stdout` has left to read, up to the end of the stream.
    #[cfg(target_os = "linux")]
    async fn read_to_the_end(stdout: &mut Box<dyn ReadHalf>) -> Vec<u8> {
        let mut read_so_far = Vec::new();
        let mut buffer = [0; 64];
        loop {
            let (read, _) = ReadHalf::recvmsg(stdout, &mut buffer).await.unwrap();
            if read == 0 {
                return read_so_far;
            }
            read_so_far.extend_from_slice(&buffer[..read]);
        }
    }
}
