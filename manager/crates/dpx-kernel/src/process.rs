//! OS process helpers: liveness checks and force-kill by pid.

/// Whether a process with `pid` exists on this machine (owned by anyone).
pub fn pid_alive(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    imp::pid_alive(pid)
}

/// SIGKILL / TerminateProcess. `Ok(false)` when the process does not exist.
pub fn kill_pid(pid: u32) -> std::io::Result<bool> {
    if pid == 0 || pid > i32::MAX as u32 {
        return Ok(false);
    }
    imp::kill_pid(pid)
}

#[cfg(unix)]
mod imp {
    use std::io;

    pub fn pid_alive(pid: u32) -> bool {
        // SAFETY: kill with signal 0 only checks for existence and permission.
        let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if rc == 0 {
            return true;
        }
        io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    pub fn kill_pid(pid: u32) -> io::Result<bool> {
        // SAFETY: plain kill(2) on a pid we were asked to terminate.
        let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        if rc == 0 {
            return Ok(true);
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(err)
        }
    }
}

#[cfg(windows)]
mod imp {
    use std::io;

    use windows_sys::Win32::Foundation::{
        CloseHandle, GetLastError, ERROR_ACCESS_DENIED, STILL_ACTIVE,
    };
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, TerminateProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        PROCESS_TERMINATE,
    };

    pub fn pid_alive(pid: u32) -> bool {
        // SAFETY: Win32 calls on a handle we open and close here.
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return GetLastError() == ERROR_ACCESS_DENIED;
            }
            let mut code = 0u32;
            let ok = GetExitCodeProcess(handle, &mut code);
            CloseHandle(handle);
            ok == 0 || code == STILL_ACTIVE as u32
        }
    }

    pub fn kill_pid(pid: u32) -> io::Result<bool> {
        // SAFETY: Win32 calls on a handle we open and close here.
        unsafe {
            let handle = OpenProcess(
                PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION,
                0,
                pid,
            );
            if handle.is_null() {
                return if pid_alive(pid) {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(false)
                };
            }
            let ok = TerminateProcess(handle, 1);
            let err = io::Error::last_os_error();
            CloseHandle(handle);
            if ok == 0 {
                Err(err)
            } else {
                Ok(true)
            }
        }
    }
}
