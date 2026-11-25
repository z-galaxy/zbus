//! The ID of the machine, as `org.freedesktop.DBus.Peer.GetMachineId` replies with it.

use super::{Error, Result};

/// The ID of the machine that the process runs on, as `org.freedesktop.DBus.Peer.GetMachineId`
/// replies with it.
///
/// The object server replies to `GetMachineId` with this ID. A program that answers the call
/// itself, such as a bus, can reply with it too. The ID comes from:
///
/// * Linux: `/var/lib/dbus/machine-id`, or `/etc/machine-id` if that can't be read.
/// * macOS: `gethostuuid`.
/// * FreeBSD and DragonFly BSD: the first D-Bus machine ID file that holds an ID
///   (`/var/lib/dbus/machine-id`, `/etc/machine-id` or `/var/db/dbus/machine-id`), or else the
///   `kern.hostuuid` sysctl.
/// * OpenBSD and NetBSD: the first D-Bus machine ID file that holds an ID.
/// * Windows: the GUID of the current hardware profile, without its braces and hyphens.
///
/// # Errors
///
/// [`Error::IOError`] if the platform has no ID for the machine, or it can't be read.
/// [`Error::NotSupported`] on other platforms, such as Android and WASI, where zbus has no way to
/// find the ID.
pub fn machine_id() -> Result<String> {
    // On *BSD platforms, first try standard D-Bus machine-id locations
    #[cfg(any(
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    if let Some(id) = read_dbus_machine_id() {
        return Ok(id);
    }

    get_platform_machine_id()
}

#[cfg(target_os = "linux")]
fn get_platform_machine_id() -> Result<String> {
    let mut id = match std::fs::read_to_string("/var/lib/dbus/machine-id") {
        Ok(id) => id,
        Err(e) => {
            if let Ok(id) = std::fs::read_to_string("/etc/machine-id") {
                id
            } else {
                return Err(Error::IOError(format!(
                    "Failed to read from /var/lib/dbus/machine-id or /etc/machine-id: {e}"
                )));
            }
        }
    };

    let len = id.trim_end().len();
    id.truncate(len);
    Ok(id)
}

#[cfg(target_os = "macos")]
fn get_platform_machine_id() -> Result<String> {
    unsafe extern "C" {
        fn gethostuuid(id: *mut u8, wait: *const libc::timespec) -> libc::c_int;
    }

    let mut uuid = [0u8; 16];
    let timeout = libc::timespec {
        tv_sec: 1,
        tv_nsec: 0,
    };

    let ret = unsafe { gethostuuid(uuid.as_mut_ptr(), &timeout) };
    if ret != 0 {
        return Err(Error::IOError(format!(
            "gethostuuid failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    Ok(uuid.iter().map(|b| format!("{b:02x}")).collect())
}

/// Get the machine ID on FreeBSD or DragonFlyBSD using the kern.hostuuid sysctl.
/// This returns a UUID that is typically generated at install time and persists across reboots.
#[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
fn get_platform_machine_id() -> Result<String> {
    use std::ffi::CStr;

    let mut buf = [0u8; 64];
    let mut len = buf.len();

    let mib_name = c"kern.hostuuid";
    let ret = unsafe {
        libc::sysctlbyname(
            mib_name.as_ptr(),
            buf.as_mut_ptr() as *mut libc::c_void,
            &mut len,
            std::ptr::null(),
            0,
        )
    };

    if ret != 0 {
        return Err(Error::IOError(format!(
            "sysctlbyname(kern.hostuuid) failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    // The sysctl returns a null-terminated UUID string (e.g.
    // "01234567-89ab-cdef-0123-456789abcdef"). Remove hyphens to convert to the expected
    // 32-character hex format.
    let uuid_str = CStr::from_bytes_until_nul(&buf[..len])
        .map_err(|e| Error::IOError(format!("Invalid UTF-8 in hostuuid: {e}")))?
        .to_str()
        .map_err(|e| Error::IOError(format!("Invalid UTF-8 in hostuuid: {e}")))?;

    let machine_id: String = uuid_str.chars().filter(|c| *c != '-').collect();

    if machine_id.len() != 32 || !machine_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::IOError(format!(
            "Invalid hostuuid format: {uuid_str}"
        )));
    }

    Ok(machine_id)
}

/// Try to read machine ID from standard D-Bus locations.
/// Used on *BSD platforms as the primary method before falling back to platform-specific
/// mechanisms.
#[cfg(any(
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd"
))]
fn read_dbus_machine_id() -> Option<String> {
    const MACHINE_ID_PATHS: &[&str] = &[
        "/var/lib/dbus/machine-id",
        "/etc/machine-id",
        "/var/db/dbus/machine-id",
    ];

    for path in MACHINE_ID_PATHS {
        if let Ok(mut id) = std::fs::read_to_string(path) {
            let len = id.trim_end().len();
            id.truncate(len);
            if !id.is_empty() {
                return Some(id);
            }
        }
    }
    None
}

#[cfg(any(target_os = "openbsd", target_os = "netbsd"))]
fn get_platform_machine_id() -> Result<String> {
    // OpenBSD and NetBSD don't have a built-in machine UUID mechanism.
    // The D-Bus package typically creates /var/db/dbus/machine-id on installation.
    Err(Error::IOError(
        "No machine-id found. Please ensure D-Bus is properly installed and \
         /var/db/dbus/machine-id or /etc/machine-id exists."
            .to_string(),
    ))
}

/// Fallback for other platforms not explicitly supported: the other Unix platforms, and WASI.
#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "macos",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd"
)))]
fn get_platform_machine_id() -> Result<String> {
    Err(Error::NotSupported(
        "get_machine_id is not yet implemented on this platform".to_string(),
    ))
}

#[cfg(windows)]
fn get_platform_machine_id() -> Result<String> {
    hw_profile_guid().map_err(|e| Error::IOError(e.to_string()))
}

/// Get the machine ID using the hardware profile GUID.
///
/// As per the D-Bus specification, on Windows we use the `GetCurrentHwProfile` API.
#[cfg(windows)]
fn hw_profile_guid() -> std::io::Result<String> {
    use std::ffi::CStr;

    use windows_sys::Win32::System::WindowsProgramming::{GetCurrentHwProfileA, HW_PROFILE_INFOA};

    let mut hw_profile: HW_PROFILE_INFOA = unsafe { std::mem::zeroed() };

    let ret = unsafe { GetCurrentHwProfileA(&mut hw_profile) };
    if ret == 0 {
        return Err(std::io::Error::last_os_error());
    }

    let guid = unsafe { CStr::from_ptr(hw_profile.szHwProfileGuid.as_ptr().cast()) };
    let guid = guid
        .to_str()
        .map_err(|_| std::io::Error::other("Invalid hardware profile GUID"))?;

    Ok(guid.replace(['{', '}', '-'], ""))
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(target_os = "linux")]
    fn linux_machine_id() {
        if let Ok(id) = super::get_platform_machine_id() {
            assert_eq!(id.len(), 32, "machine ID should be 32 hex characters");
            assert!(
                id.chars().all(|c| c.is_ascii_hexdigit()),
                "machine ID should only contain hex characters"
            );
        }
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_machine_id() {
        let id = super::get_platform_machine_id().expect("gethostuuid should succeed on macOS");
        assert_eq!(id.len(), 32, "machine ID should be 32 hex characters");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "machine ID should only contain hex characters"
        );
    }

    #[test]
    #[cfg(any(target_os = "freebsd", target_os = "dragonfly"))]
    fn freebsd_machine_id() {
        // On FreeBSD/DragonFlyBSD, we should always be able to get a machine ID
        // either from D-Bus machine-id files or from kern.hostuuid sysctl.
        // Test the combined logic through `machine_id`.
        let id = super::machine_id().expect("should get machine ID on FreeBSD/DragonFlyBSD");
        assert_eq!(id.len(), 32, "machine ID should be 32 hex characters");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "machine ID should only contain hex characters"
        );
    }

    #[test]
    #[cfg(any(target_os = "openbsd", target_os = "netbsd"))]
    fn openbsd_netbsd_machine_id() {
        // On OpenBSD/NetBSD, machine ID is only available if D-Bus is installed
        // and has created the machine-id file.
        // Test the combined logic through `machine_id`.
        if let Ok(id) = super::machine_id() {
            assert_eq!(id.len(), 32, "machine ID should be 32 hex characters");
            assert!(
                id.chars().all(|c| c.is_ascii_hexdigit()),
                "machine ID should only contain hex characters"
            );
        }
    }

    #[test]
    #[cfg(windows)]
    fn windows_machine_id() {
        let id = super::machine_id().expect("machine_id should succeed");
        assert_eq!(id.len(), 32, "machine ID should be 32 hex characters");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "machine ID should only contain hex characters"
        );
    }
}
