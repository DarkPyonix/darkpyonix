//! Windows: keep an editor server's whole process tree tied to the manager (SPEC FR-M9).
//!
//! An editor launcher on Windows is a `.cmd` whose node (or python) child is a grandchild of the
//! manager. Killing the direct child leaves that grandchild running, still holding every
//! inheritable handle it got, the manager's own stdout among them. The child is therefore started
//! suspended, put in a job object that kills every process in it when its last handle closes,
//! and only then resumed: no grandchild can start outside the job. Dropping the [`Tree`] (the run
//! ended, the task was cancelled, or the manager process died) ends the whole tree.

use std::ffi::c_void;
use std::io;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

/// The creation flag the child must be started with before [`Tree::adopt`].
pub use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;

/// A kill-on-close job holding one child and everything it starts.
pub struct Tree(HANDLE);

// SAFETY: a job handle may be used and closed from any thread.
unsafe impl Send for Tree {}
unsafe impl Sync for Tree {}

impl Tree {
    /// Puts the suspended process `pid` (handle `process`) in a new kill-on-close job, then
    /// resumes it. The process is resumed even when the job cannot be made, so it never stays
    /// suspended; the error then says the tree is not contained.
    pub fn adopt(pid: u32, process: *mut c_void) -> io::Result<Tree> {
        let job = kill_on_close_job().and_then(|job| {
            // SAFETY: both handles are valid; a failed call is reported by its return value.
            if unsafe { AssignProcessToJobObject(job.0, process) } == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(job)
            }
        });
        resume(pid)?;
        job
    }

    /// Ends every process in the tree now.
    pub fn kill(&self) {
        // SAFETY: the handle is ours until drop.
        unsafe {
            TerminateJobObject(self.0, 1);
        }
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        // SAFETY: the handle is ours; closing the last handle kills the job's processes.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

fn kill_on_close_job() -> io::Result<Tree> {
    // SAFETY: plain Win32 calls; the struct is plain data and zero is a valid start.
    unsafe {
        let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Tree(job);
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let ok = SetInformationJobObject(
            job.0,
            JobObjectExtendedLimitInformation,
            &info as *const _ as *const c_void,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        );
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }
}

/// Resumes every thread of the suspended process `pid` (a new process has exactly one).
fn resume(pid: u32) -> io::Result<()> {
    // SAFETY: plain Win32 calls; every handle opened here is closed here.
    unsafe {
        let snap = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
        if snap == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let mut entry: THREADENTRY32 = std::mem::zeroed();
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
        let mut resumed = 0;
        let mut more = Thread32First(snap, &mut entry) != 0;
        while more {
            if entry.th32OwnerProcessID == pid {
                let thread = OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID);
                if !thread.is_null() {
                    if ResumeThread(thread) != u32::MAX {
                        resumed += 1;
                    }
                    CloseHandle(thread);
                }
            }
            more = Thread32Next(snap, &mut entry) != 0;
        }
        CloseHandle(snap);
        if resumed == 0 {
            return Err(io::Error::other(format!(
                "no thread of process {pid} could be resumed"
            )));
        }
        Ok(())
    }
}
