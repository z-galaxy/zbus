#![cfg(feature = "comms")]

//! Compile-time checks that enabling a runtime feature does not change what a stream builder
//! takes: every one of them takes the socket the platform owns, whichever runtime drives it.

use zbus::connection::Builder;

#[cfg(unix)]
type PlatformUnixStream = std::os::unix::net::UnixStream;
#[cfg(windows)]
type PlatformUnixStream = uds_windows::UnixStream;

#[test]
fn a_unix_stream_builder_takes_the_platform_stream() {
    #[cfg(any(unix, windows))]
    {
        let _: fn(PlatformUnixStream) -> Builder<'static> = Builder::unix_stream;
    }
}

#[test]
fn a_tcp_stream_builder_takes_the_std_stream() {
    let _: fn(std::net::TcpStream) -> Builder<'static> = Builder::tcp_stream;
}

#[test]
fn a_vsock_stream_builder_takes_the_vsock_stream() {
    #[cfg(feature = "vsock")]
    let _: fn(vsock::VsockStream) -> Builder<'static> = Builder::vsock_stream;
}
