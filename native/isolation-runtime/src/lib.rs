use std::ffi::{c_void, OsStr, OsString};
use std::fmt::{Display, Formatter};
use std::fs;
use std::mem::{size_of, zeroed};
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, LocalFree, FALSE, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, GetNamedSecurityInfoW, SetEntriesInAclW, SetNamedSecurityInfoW,
    DENY_ACCESS, EXPLICIT_ACCESS_W, GRANT_ACCESS, REVOKE_ACCESS, SE_FILE_OBJECT, TRUSTEE_IS_SID,
    TRUSTEE_W,
};
use windows_sys::Win32::Security::Isolation::{
    CreateAppContainerProfile, DeleteAppContainerProfile, DeriveAppContainerSidFromAppContainerName,
};
use windows_sys::Win32::Security::{
    FreeSid, GetFileSecurityW, GetTokenInformation, SetFileSecurityW, TokenAppContainerSid,
    TokenIsAppContainer, ACL, NO_INHERITANCE, PSECURITY_DESCRIPTOR, PSID, SECURITY_CAPABILITIES,
    TOKEN_APPCONTAINER_INFORMATION, TOKEN_QUERY,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, GetFileAttributesW, GetFileInformationByHandle, GetFinalPathNameByHandleW,
    BY_HANDLE_FILE_INFORMATION, FILE_ALL_ACCESS, FILE_ATTRIBUTE_NORMAL,
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_GENERIC_EXECUTE,
    FILE_GENERIC_READ, FILE_SHARE_READ, FILE_SHARE_WRITE, INVALID_FILE_ATTRIBUTES, OPEN_EXISTING,
    WRITE_DAC, WRITE_OWNER,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_ACTIVE_PROCESS,
    JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};
use windows_sys::Win32::System::Threading::{
    CreateProcessW, DeleteProcThreadAttributeList, GetExitCodeProcess,
    InitializeProcThreadAttributeList, OpenProcessToken, ResumeThread, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT,
    EXTENDED_STARTUPINFO_PRESENT, PROCESS_INFORMATION, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
    STARTUPINFOEXW,
};

const ERROR_INVALID_PARAMETER: u32 = 87;
const ERROR_INSUFFICIENT_BUFFER: u32 = 122;
const DACL_SECURITY_INFORMATION: u32 = 0x0000_0004;

const WAIT_FAILED: u32 = 0xFFFF_FFFF;
const PROFILE_HRESULT_OK: i32 = 0;
const FILE_NAME_NORMALIZED: u32 = 0;
const SUB_CONTAINERS_AND_OBJECTS_INHERIT: u32 = 0x0000_0003;
const FILE_DATABASE_ACCESS: u32 = FILE_ALL_ACCESS & !(WRITE_DAC | WRITE_OWNER);
const JOB_ACTIVE_PROCESS_LIMIT: u32 = 32;
const TIMEOUT_EXIT_CODE: u32 = 0xE001_0001;
static UNIQUE_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
pub struct RuntimeError {
    pub operation: &'static str,
    pub code: u32,
}

impl RuntimeError {
    fn win(operation: &'static str) -> Self {
        Self {
            operation,
            code: unsafe { GetLastError() },
        }
    }

    fn code(operation: &'static str, code: u32) -> Self {
        Self { operation, code }
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (win32={})", self.operation, self.code)
    }
}

impl std::error::Error for RuntimeError {}

type Result<T> = std::result::Result<T, RuntimeError>;

fn wide(value: impl AsRef<OsStr>) -> Vec<u16> {
    value
        .as_ref()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}

fn handle_is_invalid(handle: HANDLE) -> bool {
    handle.is_null() || handle == (-1isize as HANDLE)
}

fn hard_link_free(handle: HANDLE) -> Result<bool> {
    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    let ok = unsafe { GetFileInformationByHandle(handle, &mut information) };
    if ok == FALSE {
        return Err(RuntimeError::win("GET_FILE_IDENTITY"));
    }
    Ok(information.nNumberOfLinks == 1)
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = UNIQUE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}-{}", std::process::id(), nanos, counter)
}

fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn normalize_for_compare(path: &str) -> String {
    let mut value = path.replace('/', "\\");
    if let Some(rest) = value.strip_prefix("\\\\?\\") {
        value = rest.to_string();
    }
    if let Some(rest) = value.strip_prefix("UNC\\") {
        value = format!("\\\\{}", rest);
    }
    value.trim_end_matches('\\').to_ascii_lowercase()
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map_err(|_| RuntimeError::code("GET_CURRENT_DIRECTORY", ERROR_INVALID_PARAMETER))
            .map(|root| root.join(path))
    }
}

fn canonical(path: &Path, operation: &'static str) -> Result<PathBuf> {
    fs::canonicalize(path).map_err(|_| RuntimeError::code(operation, ERROR_INVALID_PARAMETER))
}

fn scoped_path(workspace: &Path, path: &Path, operation: &'static str) -> Result<PathBuf> {
    let workspace_absolute = absolute(workspace)?;
    let workspace_canonical = canonical(&workspace_absolute, "CANONICALIZE_WORKSPACE")?;
    let path = absolute(path)?;
    reject_reparse_components(&path)?;
    let path_canonical = canonical(&path, operation)?;
    if !path_canonical.starts_with(&workspace_canonical) {
        return Err(RuntimeError::code(operation, ERROR_INVALID_PARAMETER));
    }
    Ok(path)
}

fn reject_reparse_components(path: &Path) -> Result<()> {
    let absolute = absolute(path)?;
    let mut current = PathBuf::new();
    for component in absolute.components() {
        current.push(component);
        let attributes = unsafe { GetFileAttributesW(wide(&current).as_ptr()) };
        if attributes == INVALID_FILE_ATTRIBUTES {
            return Err(RuntimeError::win("GET_FILE_ATTRIBUTES"));
        }
        if attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(RuntimeError::code(
                "REPARSE_POINT_PATH_REJECTED",
                ERROR_INVALID_PARAMETER,
            ));
        }
    }
    Ok(())
}

fn final_path_for_handle(handle: HANDLE) -> Result<String> {
    let mut buffer = vec![0u16; 32_768];
    let length = unsafe {
        GetFinalPathNameByHandleW(
            handle,
            buffer.as_mut_ptr(),
            buffer.len() as u32,
            FILE_NAME_NORMALIZED,
        )
    };
    if length == 0 {
        return Err(RuntimeError::win("GET_FINAL_PATH_NAME"));
    }
    if length as usize >= buffer.len() {
        return Err(RuntimeError::code(
            "FINAL_PATH_TOO_LONG",
            ERROR_INSUFFICIENT_BUFFER,
        ));
    }
    String::from_utf16(&buffer[..length as usize])
        .map_err(|_| RuntimeError::code("FINAL_PATH_NOT_UTF16", ERROR_INVALID_PARAMETER))
}

#[derive(Debug, Clone)]
pub struct PathIdentity {
    pub requested: String,
    pub final_path: String,
    pub identity_match: bool,
    pub reparse_free: bool,
    pub hard_link_free: bool,
}

impl PathIdentity {
    fn json(&self) -> String {
        format!(
            "{{\"requested\":{},\"final_path\":{},\"identity_match\":{},\"reparse_free\":{},\"hard_link_free\":{}}}",
            json_string(&self.requested),
            json_string(&self.final_path),
            self.identity_match,
            self.reparse_free,
            self.hard_link_free,
        )
    }
}

struct HandleLedger {
    opened: AtomicUsize,
    closed: AtomicUsize,
    close_failures: AtomicUsize,
}

impl HandleLedger {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            opened: AtomicUsize::new(0),
            closed: AtomicUsize::new(0),
            close_failures: AtomicUsize::new(0),
        })
    }

    fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.opened.load(Ordering::SeqCst),
            self.closed.load(Ordering::SeqCst),
            self.close_failures.load(Ordering::SeqCst),
        )
    }
}

struct OwnedHandle {
    raw: HANDLE,
    ledger: Arc<HandleLedger>,
}

impl OwnedHandle {
    unsafe fn from_raw(raw: HANDLE, ledger: &Arc<HandleLedger>) -> Result<Self> {
        if handle_is_invalid(raw) {
            return Err(RuntimeError::win("OWN_HANDLE"));
        }
        ledger.opened.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            raw,
            ledger: Arc::clone(ledger),
        })
    }

    fn raw(&self) -> HANDLE {
        self.raw
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        let closed = unsafe { CloseHandle(self.raw) };
        if closed == FALSE {
            self.ledger.close_failures.fetch_add(1, Ordering::SeqCst);
        }
        self.ledger.closed.fetch_add(1, Ordering::SeqCst);
    }
}

fn open_trusted_program(
    path: &Path,
    ledger: &Arc<HandleLedger>,
) -> Result<(OwnedHandle, PathIdentity)> {
    let absolute = absolute(path)?;
    reject_reparse_components(&absolute)?;
    let raw = unsafe {
        CreateFileW(
            wide(&absolute).as_ptr(),
            FILE_GENERIC_READ,
            FILE_SHARE_READ,
            null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            null_mut(),
        )
    };
    if handle_is_invalid(raw) {
        return Err(RuntimeError::win("OPEN_TRUSTED_PROGRAM"));
    }
    let final_path = match final_path_for_handle(raw) {
        Ok(path) => path,
        Err(error) => {
            unsafe { CloseHandle(raw) };
            return Err(error);
        }
    };
    let identity_match =
        normalize_for_compare(&final_path) == normalize_for_compare(&absolute.to_string_lossy());
    if !identity_match {
        unsafe { CloseHandle(raw) };
        return Err(RuntimeError::code(
            "TRUSTED_PROGRAM_IDENTITY_MISMATCH",
            ERROR_INVALID_PARAMETER,
        ));
    }
    let hard_link_free = match hard_link_free(raw) {
        Ok(value) => value,
        Err(error) => {
            unsafe { CloseHandle(raw) };
            return Err(error);
        }
    };
    if !hard_link_free {
        unsafe { CloseHandle(raw) };
        return Err(RuntimeError::code(
            "HARD_LINK_PATH_REJECTED",
            ERROR_INVALID_PARAMETER,
        ));
    }
    let handle = unsafe { OwnedHandle::from_raw(raw, ledger)? };
    let identity = PathIdentity {
        requested: absolute.to_string_lossy().into_owned(),
        final_path,
        identity_match: true,
        reparse_free: true,
        hard_link_free,
    };
    Ok((handle, identity))
}

struct Profile {
    name: Vec<u16>,
    sid: PSID,
    sid_string: String,
    deleted: bool,
    sid_freed: bool,
}

impl Profile {
    fn create() -> Result<Self> {
        let name_text = format!("CivilizationTheatreRuntime-{}", unique_suffix());
        let name = wide(OsStr::new(&name_text));
        let display = wide(OsStr::new("Civilization Theatre isolation fixture"));
        let description = wide(OsStr::new(
            "Ephemeral AppContainer for bounded native runtime checks",
        ));
        let mut sid: PSID = null_mut();
        let result = unsafe {
            CreateAppContainerProfile(
                name.as_ptr(),
                display.as_ptr(),
                description.as_ptr(),
                null(),
                0,
                &mut sid,
            )
        };
        if result != PROFILE_HRESULT_OK {
            return Err(RuntimeError::code(
                "CREATE_APPCONTAINER_PROFILE",
                result as u32,
            ));
        }
        if sid.is_null() {
            unsafe {
                DeleteAppContainerProfile(name.as_ptr());
            }
            return Err(RuntimeError::code(
                "CREATE_APPCONTAINER_PROFILE_NO_SID",
                ERROR_INVALID_PARAMETER,
            ));
        }
        let mut derived_sid: PSID = null_mut();
        let derive_result =
            unsafe { DeriveAppContainerSidFromAppContainerName(name.as_ptr(), &mut derived_sid) };
        if derive_result != PROFILE_HRESULT_OK || derived_sid.is_null() {
            unsafe {
                DeleteAppContainerProfile(name.as_ptr());
                FreeSid(sid);
            }
            return Err(RuntimeError::code(
                "DERIVE_APPCONTAINER_SID",
                derive_result as u32,
            ));
        }
        unsafe {
            FreeSid(sid);
        }
        let sid = derived_sid;
        let sid_string = match sid_to_string(sid) {
            Ok(value) => value,
            Err(error) => {
                unsafe {
                    DeleteAppContainerProfile(name.as_ptr());
                    FreeSid(sid);
                }
                return Err(error);
            }
        };
        Ok(Self {
            name,
            sid,
            sid_string,
            deleted: false,
            sid_freed: false,
        })
    }

    fn delete(&mut self) -> Result<()> {
        if !self.deleted {
            let result = unsafe { DeleteAppContainerProfile(self.name.as_ptr()) };
            if result != PROFILE_HRESULT_OK {
                return Err(RuntimeError::code(
                    "DELETE_APPCONTAINER_PROFILE",
                    result as u32,
                ));
            }
            self.deleted = true;
        }
        self.free_sid();
        Ok(())
    }

    fn free_sid(&mut self) {
        if !self.sid_freed && !self.sid.is_null() {
            unsafe { FreeSid(self.sid) };
            self.sid_freed = true;
        }
    }
}

impl Drop for Profile {
    fn drop(&mut self) {
        if !self.deleted {
            unsafe { DeleteAppContainerProfile(self.name.as_ptr()) };
        }
        self.free_sid();
    }
}

fn sid_to_string(sid: PSID) -> Result<String> {
    let mut pointer = null_mut();
    let ok = unsafe { ConvertSidToStringSidW(sid, &mut pointer) };
    if ok == FALSE || pointer.is_null() {
        return Err(RuntimeError::win("CONVERT_SID_TO_STRING"));
    }
    let mut length = 0usize;
    unsafe {
        while *pointer.add(length) != 0 {
            length += 1;
        }
    }
    let value = unsafe { String::from_utf16(std::slice::from_raw_parts(pointer, length)) }
        .map_err(|_| RuntimeError::code("SID_STRING_NOT_UTF16", ERROR_INVALID_PARAMETER));
    unsafe {
        LocalFree(pointer as *mut c_void);
    }
    value
}

struct AclSnapshot {
    path: PathBuf,
    descriptor: Vec<u8>,
}

struct LockedPath {
    identity: PathIdentity,
    _handle: OwnedHandle,
}

struct AclLedger {
    snapshots: Vec<AclSnapshot>,
    locks: Vec<LockedPath>,
}

impl AclLedger {
    fn new() -> Self {
        Self {
            snapshots: Vec::new(),
            locks: Vec::new(),
        }
    }

    fn lock_path(
        &mut self,
        path: &Path,
        handle_ledger: &Arc<HandleLedger>,
    ) -> Result<PathIdentity> {
        let absolute = absolute(path)?;
        reject_reparse_components(&absolute)?;
        let key = normalize_for_compare(&absolute.to_string_lossy());
        if let Some(existing) = self
            .locks
            .iter()
            .find(|locked| normalize_for_compare(&locked.identity.requested) == key)
        {
            return Ok(existing.identity.clone());
        }
        let raw = unsafe {
            CreateFileW(
                wide(&absolute).as_ptr(),
                FILE_GENERIC_READ | WRITE_DAC,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                null(),
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS,
                null_mut(),
            )
        };
        if handle_is_invalid(raw) {
            return Err(RuntimeError::win("LOCK_TRUSTED_PATH"));
        }
        let final_path = match final_path_for_handle(raw) {
            Ok(value) => value,
            Err(error) => {
                unsafe { CloseHandle(raw) };
                return Err(error);
            }
        };
        let identity_match = normalize_for_compare(&final_path)
            == normalize_for_compare(&absolute.to_string_lossy());
        if !identity_match {
            unsafe { CloseHandle(raw) };
            return Err(RuntimeError::code(
                "TRUSTED_PATH_IDENTITY_MISMATCH",
                ERROR_INVALID_PARAMETER,
            ));
        }
        let hard_link_free = match hard_link_free(raw) {
            Ok(value) => value,
            Err(error) => {
                unsafe { CloseHandle(raw) };
                return Err(error);
            }
        };
        if !hard_link_free {
            unsafe { CloseHandle(raw) };
            return Err(RuntimeError::code(
                "HARD_LINK_PATH_REJECTED",
                ERROR_INVALID_PARAMETER,
            ));
        }
        let handle = unsafe { OwnedHandle::from_raw(raw, handle_ledger)? };
        let identity = PathIdentity {
            requested: absolute.to_string_lossy().into_owned(),
            final_path,
            identity_match: true,
            reparse_free: true,
            hard_link_free,
        };
        self.locks.push(LockedPath {
            identity: identity.clone(),
            _handle: handle,
        });
        Ok(identity)
    }

    fn release_locks(&mut self) {
        self.locks.clear();
    }

    fn capture(&mut self, path: &Path) -> Result<()> {
        let path = absolute(path)?;
        let key = normalize_for_compare(&path.to_string_lossy());
        if self
            .snapshots
            .iter()
            .any(|snapshot| normalize_for_compare(&snapshot.path.to_string_lossy()) == key)
        {
            return Ok(());
        }

        let object_name = wide(&path);
        let mut required = 0u32;
        let ok = unsafe {
            GetFileSecurityW(
                object_name.as_ptr(),
                DACL_SECURITY_INFORMATION,
                null_mut(),
                0,
                &mut required,
            )
        };
        if ok != FALSE || required == 0 {
            return Err(RuntimeError::code("GET_PATH_DACL_SIZE", unsafe {
                GetLastError()
            }));
        }
        let mut descriptor = vec![0u8; required as usize];
        let mut returned = 0u32;
        let ok = unsafe {
            GetFileSecurityW(
                object_name.as_ptr(),
                DACL_SECURITY_INFORMATION,
                descriptor.as_mut_ptr() as PSECURITY_DESCRIPTOR,
                descriptor.len() as u32,
                &mut returned,
            )
        };
        if ok == FALSE {
            return Err(RuntimeError::win("GET_PATH_DACL"));
        }
        if returned == 0 || returned as usize > descriptor.len() {
            return Err(RuntimeError::code(
                "GET_PATH_DACL_INVALID_SIZE",
                ERROR_INVALID_PARAMETER,
            ));
        }
        descriptor.truncate(returned as usize);
        self.snapshots.push(AclSnapshot { path, descriptor });
        Ok(())
    }

    fn restore(&self) -> Result<()> {
        let mut first_error = None;
        for snapshot in &self.snapshots {
            let object_name = wide(&snapshot.path);
            let ok = unsafe {
                SetFileSecurityW(
                    object_name.as_ptr(),
                    DACL_SECURITY_INFORMATION,
                    snapshot.descriptor.as_ptr() as PSECURITY_DESCRIPTOR,
                )
            };
            if ok == FALSE && first_error.is_none() {
                first_error = Some(RuntimeError::win("RESTORE_PATH_DACL"));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn capture_path_chain(
    acl_ledger: &mut AclLedger,
    handle_ledger: &Arc<HandleLedger>,
    root: &Path,
    target: &Path,
) -> Result<()> {
    let root = absolute(root)?;
    let target = absolute(target)?;
    if !target.starts_with(&root) {
        return Err(RuntimeError::code(
            "PATH_OUTSIDE_WORKSPACE",
            ERROR_INVALID_PARAMETER,
        ));
    }
    let mut current = target;
    let mut chain = Vec::new();
    loop {
        chain.push(current.clone());
        if current == root {
            break;
        }
        current = current
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| RuntimeError::code("PATH_OUTSIDE_WORKSPACE", ERROR_INVALID_PARAMETER))?;
    }
    for path in chain.into_iter().rev() {
        acl_ledger.lock_path(&path, handle_ledger)?;
        acl_ledger.capture(&path)?;
    }
    Ok(())
}

unsafe fn update_acl(
    path: &Path,
    sid: PSID,
    access: u32,
    mode: i32,
    inheritance: u32,
) -> Result<()> {
    let object_name = wide(path);
    let mut old_dacl: *mut ACL = null_mut();
    let mut security_descriptor: PSECURITY_DESCRIPTOR = null_mut();
    let result = GetNamedSecurityInfoW(
        object_name.as_ptr(),
        SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION,
        null_mut(),
        null_mut(),
        &mut old_dacl,
        null_mut(),
        &mut security_descriptor,
    );
    if result != 0 {
        return Err(RuntimeError::code("GET_PATH_DACL", result));
    }

    let mut trustee = TRUSTEE_W::default();
    trustee.TrusteeForm = TRUSTEE_IS_SID;
    trustee.ptstrName = sid as *mut u16;
    let mut entry = EXPLICIT_ACCESS_W::default();
    entry.grfAccessPermissions = access;
    entry.grfAccessMode = mode;
    entry.grfInheritance = inheritance;
    entry.Trustee = trustee;
    let mut new_dacl: *mut ACL = null_mut();
    let result = SetEntriesInAclW(1, &mut entry, old_dacl, &mut new_dacl);
    if result != 0 {
        if !security_descriptor.is_null() {
            LocalFree(security_descriptor as *mut c_void);
        }
        return Err(RuntimeError::code("BUILD_PATH_DACL", result));
    }
    let result = SetNamedSecurityInfoW(
        object_name.as_ptr(),
        SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION,
        null_mut(),
        null_mut(),
        new_dacl,
        null_mut(),
    );
    if !new_dacl.is_null() {
        LocalFree(new_dacl as *mut c_void);
    }
    if !security_descriptor.is_null() {
        LocalFree(security_descriptor as *mut c_void);
    }
    if result != 0 {
        return Err(RuntimeError::code("SET_PATH_DACL", result));
    }
    Ok(())
}

unsafe fn replace_with_deny_acl(path: &Path, sid: PSID) -> Result<()> {
    let object_name = wide(path);
    let mut old_dacl: *mut ACL = null_mut();
    let mut security_descriptor: PSECURITY_DESCRIPTOR = null_mut();
    let result = GetNamedSecurityInfoW(
        object_name.as_ptr(),
        SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION,
        null_mut(),
        null_mut(),
        &mut old_dacl,
        null_mut(),
        &mut security_descriptor,
    );
    if result != 0 {
        return Err(RuntimeError::code("GET_DENY_PATH_DACL", result));
    }
    let mut trustee = TRUSTEE_W::default();
    trustee.TrusteeForm = TRUSTEE_IS_SID;
    trustee.ptstrName = sid as *mut u16;

    let mut revoke = EXPLICIT_ACCESS_W::default();
    revoke.grfAccessMode = REVOKE_ACCESS;
    revoke.Trustee = trustee;
    let mut without_sid: *mut ACL = null_mut();
    let result = SetEntriesInAclW(1, &mut revoke, old_dacl, &mut without_sid);
    if result != 0 {
        if !security_descriptor.is_null() {
            LocalFree(security_descriptor as *mut c_void);
        }
        return Err(RuntimeError::code("REVOKE_DENY_PATH_SID", result));
    }

    let mut deny = EXPLICIT_ACCESS_W::default();
    deny.grfAccessPermissions = FILE_ALL_ACCESS;
    deny.grfAccessMode = DENY_ACCESS;
    deny.Trustee = trustee;
    let mut new_dacl: *mut ACL = null_mut();
    let result = SetEntriesInAclW(1, &mut deny, without_sid, &mut new_dacl);
    if !without_sid.is_null() {
        LocalFree(without_sid as *mut c_void);
    }
    if result != 0 {
        if !security_descriptor.is_null() {
            LocalFree(security_descriptor as *mut c_void);
        }
        return Err(RuntimeError::code("BUILD_DENY_PATH_DACL", result));
    }
    let result = SetNamedSecurityInfoW(
        object_name.as_ptr(),
        SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION,
        null_mut(),
        null_mut(),
        new_dacl,
        null_mut(),
    );
    if !new_dacl.is_null() {
        LocalFree(new_dacl as *mut c_void);
    }
    if !security_descriptor.is_null() {
        LocalFree(security_descriptor as *mut c_void);
    }
    if result != 0 {
        return Err(RuntimeError::code("SET_DENY_PATH_DACL", result));
    }
    Ok(())
}

fn allow_path(
    acl_ledger: &mut AclLedger,
    sid: PSID,
    path: &Path,
    directory_access: bool,
) -> Result<()> {
    let path = absolute(path)?;
    if !path.exists() {
        return Err(RuntimeError::code(
            "ALLOW_PATH_MISSING",
            ERROR_INVALID_PARAMETER,
        ));
    }
    acl_ledger.capture(&path)?;
    let access = FILE_DATABASE_ACCESS;
    unsafe {
        update_acl(
            &path,
            sid,
            access,
            GRANT_ACCESS,
            if directory_access {
                SUB_CONTAINERS_AND_OBJECTS_INHERIT
            } else {
                NO_INHERITANCE
            },
        )
    }
}

fn deny_path(acl_ledger: &mut AclLedger, sid: PSID, path: &Path) -> Result<()> {
    let path = absolute(path)?;
    if !path.exists() {
        return Err(RuntimeError::code(
            "DENY_PATH_MISSING",
            ERROR_INVALID_PARAMETER,
        ));
    }
    acl_ledger.capture(&path)?;
    unsafe { replace_with_deny_acl(&path, sid) }
}

fn allow_traverse(acl_ledger: &mut AclLedger, sid: PSID, path: &Path) -> Result<()> {
    let path = absolute(path)?;
    if !path.exists() {
        return Err(RuntimeError::code(
            "TRAVERSE_PATH_MISSING",
            ERROR_INVALID_PARAMETER,
        ));
    }
    acl_ledger.capture(&path)?;
    unsafe {
        update_acl(
            &path,
            sid,
            FILE_GENERIC_EXECUTE,
            GRANT_ACCESS,
            NO_INHERITANCE,
        )
    }
}

fn grant_parent_traversal(
    acl_ledger: &mut AclLedger,
    sid: PSID,
    root: &Path,
    target: &Path,
) -> Result<()> {
    let root = absolute(root)?;
    let target = absolute(target)?;
    if !target.starts_with(&root) {
        return Err(RuntimeError::code(
            "PATH_OUTSIDE_WORKSPACE",
            ERROR_INVALID_PARAMETER,
        ));
    }
    let mut current = target
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| root.clone());
    let mut chain = Vec::new();
    loop {
        if !current.starts_with(&root) {
            break;
        }
        chain.push(current.clone());
        if current == root {
            break;
        }
        if let Some(parent) = current.parent() {
            current = parent.to_path_buf();
        } else {
            break;
        }
    }
    for directory in chain.into_iter().rev() {
        allow_traverse(acl_ledger, sid, &directory)?;
    }
    Ok(())
}

fn grant_program_tree(
    acl_ledger: &mut AclLedger,
    sid: PSID,
    workspace: &Path,
    root: &Path,
    program: &Path,
) -> Result<()> {
    let root = absolute(root)?;
    reject_reparse_components(&root)?;
    grant_parent_traversal(acl_ledger, sid, workspace, &root)?;
    acl_ledger.capture(&root)?;
    unsafe {
        update_acl(
            &root,
            sid,
            FILE_GENERIC_READ | FILE_GENERIC_EXECUTE,
            GRANT_ACCESS,
            SUB_CONTAINERS_AND_OBJECTS_INHERIT,
        )
    }?;
    let program = absolute(program)?;
    acl_ledger.capture(&program)?;
    unsafe {
        update_acl(
            &program,
            sid,
            FILE_GENERIC_READ | FILE_GENERIC_EXECUTE,
            GRANT_ACCESS,
            NO_INHERITANCE,
        )
    }
}

#[derive(Debug, Clone)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub is_appcontainer: bool,
    pub appcontainer_sid: String,
    pub expected_sid: String,
}

impl ProcessIdentity {
    fn json(&self) -> String {
        format!(
            "{{\"pid\":{},\"is_appcontainer\":{},\"appcontainer_sid\":{},\"expected_sid\":{},\"sid_match\":{}}}",
            self.pid,
            self.is_appcontainer,
            json_string(&self.appcontainer_sid),
            json_string(&self.expected_sid),
            self.appcontainer_sid.eq_ignore_ascii_case(&self.expected_sid),
        )
    }
}

fn token_identity(token: HANDLE, expected_sid: &str, pid: u32) -> Result<ProcessIdentity> {
    let mut is_appcontainer = 0u32;
    let mut returned = 0u32;
    let ok = unsafe {
        GetTokenInformation(
            token,
            TokenIsAppContainer,
            &mut is_appcontainer as *mut _ as *mut c_void,
            size_of::<u32>() as u32,
            &mut returned,
        )
    };
    if ok == FALSE || is_appcontainer == 0 {
        return Err(RuntimeError::code("TOKEN_NOT_APPCONTAINER", unsafe {
            GetLastError()
        }));
    }

    let mut required = 0u32;
    unsafe {
        GetTokenInformation(token, TokenAppContainerSid, null_mut(), 0, &mut required);
    }
    if required == 0 {
        return Err(RuntimeError::code(
            "TOKEN_APPCONTAINER_SID_MISSING",
            unsafe { GetLastError() },
        ));
    }
    let words = (required as usize + size_of::<usize>() - 1) / size_of::<usize>();
    let mut storage = vec![0usize; words.max(1)];
    let ok = unsafe {
        GetTokenInformation(
            token,
            TokenAppContainerSid,
            storage.as_mut_ptr() as *mut c_void,
            (storage.len() * size_of::<usize>()) as u32,
            &mut returned,
        )
    };
    if ok == FALSE {
        return Err(RuntimeError::code(
            "TOKEN_APPCONTAINER_SID_MISSING",
            unsafe { GetLastError() },
        ));
    }
    let app_info = unsafe { *(storage.as_ptr() as *const TOKEN_APPCONTAINER_INFORMATION) };
    if app_info.TokenAppContainer.is_null() {
        return Err(RuntimeError::code(
            "TOKEN_APPCONTAINER_SID_MISSING",
            ERROR_INVALID_PARAMETER,
        ));
    }
    let appcontainer_sid = sid_to_string(app_info.TokenAppContainer)?;
    if !appcontainer_sid.eq_ignore_ascii_case(expected_sid) {
        return Err(RuntimeError::code(
            "TOKEN_APPCONTAINER_SID_MISMATCH",
            ERROR_INVALID_PARAMETER,
        ));
    }
    Ok(ProcessIdentity {
        pid,
        is_appcontainer: true,
        appcontainer_sid,
        expected_sid: expected_sid.to_string(),
    })
}

fn quote_windows_arg(value: &OsStr) -> String {
    let text = value.to_string_lossy();
    if !text.is_empty() && !text.chars().any(|c| c.is_whitespace() || c == '"') {
        return text.into_owned();
    }
    let mut output = String::from("\"");
    let mut backslashes = 0usize;
    for ch in text.chars() {
        if ch == '\\' {
            backslashes += 1;
        } else if ch == '"' {
            output.push_str(&"\\".repeat(backslashes * 2 + 1));
            output.push('"');
            backslashes = 0;
        } else {
            if backslashes != 0 {
                output.push_str(&"\\".repeat(backslashes));
                backslashes = 0;
            }
            output.push(ch);
        }
    }
    if backslashes != 0 {
        output.push_str(&"\\".repeat(backslashes * 2));
    }
    output.push('"');
    output
}

fn expand(value: &OsStr, workspace: &Path, custody: &Path) -> OsString {
    let value = value.to_string_lossy();
    OsString::from(
        value
            .replace("{workspace}", &workspace.to_string_lossy())
            .replace("{custody}", &custody.to_string_lossy()),
    )
}

struct StartupAttributes {
    _storage: Vec<usize>,
    _capabilities: Box<SECURITY_CAPABILITIES>,
    list: *mut c_void,
}

impl StartupAttributes {
    fn new(sid: PSID) -> Result<Self> {
        let attribute_count = 1;
        let mut size = 0usize;
        unsafe {
            InitializeProcThreadAttributeList(null_mut(), attribute_count, 0, &mut size);
        }
        if size == 0 {
            return Err(RuntimeError::code("ATTRIBUTE_LIST_SIZE", unsafe {
                GetLastError()
            }));
        }
        let words = (size + size_of::<usize>() - 1) / size_of::<usize>();
        let mut storage = vec![0usize; words];
        let list = storage.as_mut_ptr() as *mut c_void;
        let ok =
            unsafe { InitializeProcThreadAttributeList(list as _, attribute_count, 0, &mut size) };
        if ok == FALSE {
            return Err(RuntimeError::win("INITIALIZE_ATTRIBUTE_LIST"));
        }
        let capabilities = Box::new(SECURITY_CAPABILITIES {
            AppContainerSid: sid,
            Capabilities: null_mut(),
            CapabilityCount: 0,
            Reserved: 0,
        });
        let ok = unsafe {
            UpdateProcThreadAttribute(
                list as _,
                0,
                PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                (&*capabilities) as *const _ as *const c_void,
                size_of::<SECURITY_CAPABILITIES>(),
                null_mut(),
                null_mut(),
            )
        };
        if ok == FALSE {
            unsafe {
                DeleteProcThreadAttributeList(list as _);
            }
            return Err(RuntimeError::win("SET_SECURITY_CAPABILITIES"));
        }
        Ok(Self {
            _storage: storage,
            _capabilities: capabilities,
            list,
        })
    }
}

impl Drop for StartupAttributes {
    fn drop(&mut self) {
        if !self.list.is_null() {
            unsafe {
                DeleteProcThreadAttributeList(self.list as _);
            }
        }
    }
}

const CLEANUP_EXIT_CODE: u32 = 0xE001_0002;

struct ProcessCleanupGuard {
    job: HANDLE,
    process: HANDLE,
    assigned: bool,
    armed: bool,
}

impl ProcessCleanupGuard {
    fn new(job: HANDLE, process: HANDLE) -> Self {
        Self {
            job,
            process,
            assigned: false,
            armed: true,
        }
    }

    unsafe fn terminate_and_wait(&mut self, exit_code: u32) -> Result<()> {
        if !self.armed {
            return Ok(());
        }
        let target = if self.assigned {
            if TerminateJobObject(self.job, exit_code) == FALSE {
                let error = GetLastError();
                if WaitForSingleObject(self.job, 0) != WAIT_OBJECT_0 {
                    return Err(RuntimeError::code("TERMINATE_JOB", error));
                }
            }
            self.job
        } else {
            if TerminateProcess(self.process, exit_code) == FALSE {
                let error = GetLastError();
                if WaitForSingleObject(self.process, 0) != WAIT_OBJECT_0 {
                    return Err(RuntimeError::code("TERMINATE_PROCESS", error));
                }
            }
            self.process
        };
        if WaitForSingleObject(target, 10_000) != WAIT_OBJECT_0 {
            return Err(RuntimeError::win(if self.assigned {
                "WAIT_JOB_CLEANUP"
            } else {
                "WAIT_PROCESS_CLEANUP"
            }));
        }
        if self.assigned && WaitForSingleObject(self.process, 10_000) != WAIT_OBJECT_0 {
            return Err(RuntimeError::win("WAIT_PROCESS_CLEANUP"));
        }
        self.armed = false;
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProcessCleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            unsafe {
                let _ = self.terminate_and_wait(CLEANUP_EXIT_CODE);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobReceipt {
    pub assigned: bool,
    pub total_processes: u64,
    pub active_processes: u64,
    pub peak_active_processes: u64,
    pub kill_on_job_close: bool,
    pub tree_quiescent: bool,
}

impl JobReceipt {
    fn json(&self) -> String {
        format!(
            "{{\"assigned\":{},\"total_processes\":{},\"active_processes\":{},\"peak_active_processes\":{},\"kill_on_job_close\":{},\"tree_quiescent\":{}}}",
            self.assigned,
            self.total_processes,
            self.active_processes,
            self.peak_active_processes,
            self.kill_on_job_close,
            self.tree_quiescent,
        )
    }
}

unsafe fn query_job(job: HANDLE, peak_active_processes: u64) -> Result<JobReceipt> {
    let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = zeroed();
    let mut returned = 0u32;
    let ok = QueryInformationJobObject(
        job,
        JobObjectBasicAccountingInformation,
        &mut accounting as *mut _ as *mut c_void,
        size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
        &mut returned,
    );
    if ok == FALSE {
        return Err(RuntimeError::win("QUERY_JOB_ACCOUNTING"));
    }
    Ok(JobReceipt {
        assigned: true,
        total_processes: accounting.TotalProcesses as u64,
        active_processes: accounting.ActiveProcesses as u64,
        peak_active_processes: peak_active_processes.max(accounting.ActiveProcesses as u64),
        kill_on_job_close: true,
        tree_quiescent: accounting.ActiveProcesses == 0,
    })
}

unsafe fn configure_job(job: HANDLE) -> Result<()> {
    let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_ACTIVE_PROCESS
        | JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
    limits.BasicLimitInformation.ActiveProcessLimit = JOB_ACTIVE_PROCESS_LIMIT;
    let ok = SetInformationJobObject(
        job,
        JobObjectExtendedLimitInformation,
        &mut limits as *mut _ as *mut c_void,
        size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
    );
    if ok == FALSE {
        return Err(RuntimeError::win("CONFIGURE_JOB"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownKind {
    Normal,
    Timeout,
    Abnormal,
}

impl ShutdownKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Timeout => "timeout",
            Self::Abnormal => "abnormal",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunReceipt {
    pub schema: &'static str,
    pub status: &'static str,
    pub shutdown: ShutdownKind,
    pub process_identity: ProcessIdentity,
    pub exit_code: u32,
    pub job: JobReceipt,
    pub paths: Vec<PathIdentity>,
    pub profile_name: String,
    pub profile_sid: String,
    pub profile_deleted: bool,
    pub custody_path: String,
    pub custody_removed: bool,
    pub acl_restored: bool,
    pub handles_opened: usize,
    pub handles_closed: usize,
    pub handles_outstanding: usize,
    pub handle_close_failures: usize,
    pub child_report: Option<String>,
    pub errors: Vec<String>,
}

impl RunReceipt {
    pub fn to_json(&self) -> String {
        let paths = self
            .paths
            .iter()
            .map(PathIdentity::json)
            .collect::<Vec<_>>()
            .join(",");
        let child_report = self
            .child_report
            .as_deref()
            .map(json_string)
            .unwrap_or_else(|| "null".to_string());
        let errors = self
            .errors
            .iter()
            .map(|error| json_string(error))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{"schema":{},"status":{},"shutdown":{},"process_identity":{},"exit_code":{},"job":{},"paths":[{}],"profile_name":{},"profile_sid":{},"profile_deleted":{},"custody_path":{},"custody_removed":{},"acl_restored":{},"handles_opened":{},"handles_closed":{},"handles_outstanding":{},"handle_close_failures":{},"child_report":{},"errors":[{}]}}"#,
            json_string(self.schema),
            json_string(self.status),
            json_string(self.shutdown.as_str()),
            self.process_identity.json(),
            self.exit_code,
            self.job.json(),
            paths,
            json_string(&self.profile_name),
            json_string(&self.profile_sid),
            self.profile_deleted,
            json_string(&self.custody_path),
            self.custody_removed,
            self.acl_restored,
            self.handles_opened,
            self.handles_closed,
            self.handles_outstanding,
            self.handle_close_failures,
            child_report,
            errors,
        )
    }
}

pub struct RunConfig {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub workspace: PathBuf,
    pub working_directory: Option<PathBuf>,
    pub program_access_root: Option<PathBuf>,
    pub allow_paths: Vec<PathBuf>,
    pub deny_paths: Vec<PathBuf>,
    pub report_path: Option<PathBuf>,
    pub timeout: Duration,
}

impl RunConfig {
    fn validate(&self) -> Result<()> {
        if self.timeout.is_zero() || self.timeout > Duration::from_secs(120) {
            return Err(RuntimeError::code(
                "BOUNDED_TIMEOUT_REQUIRED",
                ERROR_INVALID_PARAMETER,
            ));
        }
        let workspace = absolute(&self.workspace)?;
        if !workspace.is_dir() || workspace.is_symlink() {
            return Err(RuntimeError::code(
                "OWNED_WORKSPACE_REQUIRED",
                ERROR_INVALID_PARAMETER,
            ));
        }
        reject_reparse_components(&workspace)?;
        canonical(&workspace, "CANONICALIZE_WORKSPACE")?;

        let program = absolute(&self.program)?;
        if !program.is_file() || program.is_symlink() {
            return Err(RuntimeError::code(
                "TRUSTED_PROGRAM_REQUIRED",
                ERROR_INVALID_PARAMETER,
            ));
        }
        reject_reparse_components(&program)?;
        let program_canonical = canonical(&program, "CANONICALIZE_PROGRAM")?;

        let Some(root) = &self.program_access_root else {
            return Err(RuntimeError::code(
                "PROGRAM_ACCESS_ROOT_REQUIRED",
                ERROR_INVALID_PARAMETER,
            ));
        };
        let root_path = scoped_path(&workspace, root, "PROGRAM_ACCESS_ROOT_OUTSIDE_WORKSPACE")?;
        if !root_path.is_dir() || root_path.is_symlink() {
            return Err(RuntimeError::code(
                "PROGRAM_ACCESS_ROOT_REQUIRED",
                ERROR_INVALID_PARAMETER,
            ));
        }
        let root = canonical(&root_path, "CANONICALIZE_PROGRAM_ACCESS_ROOT")?;
        if !program_canonical.starts_with(&root) {
            return Err(RuntimeError::code(
                "PROGRAM_OUTSIDE_ACCESS_ROOT",
                ERROR_INVALID_PARAMETER,
            ));
        }

        if let Some(directory) = &self.working_directory {
            let directory =
                scoped_path(&workspace, directory, "WORKING_DIRECTORY_OUTSIDE_WORKSPACE")?;
            if !directory.is_dir() || directory.is_symlink() {
                return Err(RuntimeError::code(
                    "WORKING_DIRECTORY_REQUIRED",
                    ERROR_INVALID_PARAMETER,
                ));
            }
        }
        for path in self.allow_paths.iter().chain(self.deny_paths.iter()) {
            scoped_path(&workspace, path, "PATH_OUTSIDE_WORKSPACE")?;
        }
        if let Some(report) = &self.report_path {
            let report = scoped_path(&workspace, report, "REPORT_PATH_OUTSIDE_WORKSPACE")?;
            if !report.is_file() || report.is_symlink() {
                return Err(RuntimeError::code(
                    "REPORT_PATH_REQUIRED",
                    ERROR_INVALID_PARAMETER,
                ));
            }
        }
        Ok(())
    }
}

pub struct IsolationRuntime;

impl IsolationRuntime {
    pub fn run(config: RunConfig) -> Result<RunReceipt> {
        config.validate()?;
        let workspace = absolute(&config.workspace)?;
        let custody = workspace.join(format!(".isolation-custody-{}", unique_suffix()));
        fs::create_dir(&custody).map_err(|_| RuntimeError::win("CREATE_CUSTODY_DIRECTORY"))?;
        let marker = custody.join("OWNER");
        if let Err(error) = fs::write(&marker, b"civilization-theatre-isolation-runtime-v1\n")
            .map_err(|_| RuntimeError::win("WRITE_CUSTODY_MARKER"))
        {
            let custody_removed = fs::remove_dir_all(&custody).is_ok();
            if !custody_removed {
                return Err(RuntimeError::code("RUNTIME_CLEANUP_FAILED", error.code));
            }
            return Err(error);
        }

        let mut profile = match Profile::create() {
            Ok(profile) => profile,
            Err(error) => {
                let custody_removed = fs::remove_dir_all(&custody).is_ok();
                if !custody_removed {
                    return Err(RuntimeError::code("RUNTIME_CLEANUP_FAILED", error.code));
                }
                return Err(error);
            }
        };
        let handle_ledger = HandleLedger::new();
        let mut acl_ledger = AclLedger::new();
        let result = Self::run_inner(
            &config,
            &workspace,
            &custody,
            &mut profile,
            &handle_ledger,
            &mut acl_ledger,
        );
        let acl_restored = acl_ledger.restore().is_ok();
        acl_ledger.release_locks();
        let profile_deleted = profile.delete().is_ok();
        let custody_removed = fs::remove_dir_all(&custody).is_ok();
        match result {
            Ok(mut receipt) => {
                receipt.profile_deleted = profile_deleted;
                receipt.custody_removed = custody_removed;
                receipt.acl_restored = acl_restored;
                let (opened, closed, failures) = handle_ledger.snapshot();
                receipt.handles_opened = opened;
                receipt.handles_closed = closed;
                receipt.handles_outstanding = opened.saturating_sub(closed);
                receipt.handle_close_failures = failures;
                if !acl_restored {
                    receipt.status = "degraded";
                    receipt.errors.push("PATH_DACL_RESTORE_FAILED".to_string());
                }
                if !profile_deleted {
                    receipt.status = "degraded";
                    receipt
                        .errors
                        .push("APPCONTAINER_PROFILE_CLEANUP_FAILED".to_string());
                }
                if !custody_removed {
                    receipt.status = "degraded";
                    receipt
                        .errors
                        .push("CUSTODY_DIRECTORY_CLEANUP_FAILED".to_string());
                }
                if failures != 0 || opened != closed {
                    receipt.status = "degraded";
                }
                Ok(receipt)
            }
            Err(error) => {
                if !acl_restored || !profile_deleted || !custody_removed {
                    return Err(RuntimeError::code("RUNTIME_CLEANUP_FAILED", error.code));
                }
                Err(error)
            }
        }
    }

    fn run_inner(
        config: &RunConfig,
        workspace: &Path,
        custody: &Path,
        profile: &mut Profile,
        handle_ledger: &Arc<HandleLedger>,
        acl_ledger: &mut AclLedger,
    ) -> Result<RunReceipt> {
        let program = absolute(&config.program)?;
        let working_directory = scoped_path(
            workspace,
            config.working_directory.as_deref().unwrap_or(workspace),
            "WORKING_DIRECTORY_OUTSIDE_WORKSPACE",
        )?;
        let mut paths = Vec::new();

        capture_path_chain(acl_ledger, handle_ledger, workspace, workspace)?;
        if let Some(root) = &config.program_access_root {
            capture_path_chain(acl_ledger, handle_ledger, workspace, root)?;
        }
        for path in config.allow_paths.iter().chain(config.deny_paths.iter()) {
            let absolute_path = scoped_path(workspace, path, "PATH_OUTSIDE_WORKSPACE")?;
            capture_path_chain(acl_ledger, handle_ledger, workspace, &absolute_path)?;
        }
        capture_path_chain(acl_ledger, handle_ledger, workspace, &working_directory)?;
        if let Some(report) = &config.report_path {
            let report = scoped_path(workspace, report, "REPORT_PATH_OUTSIDE_WORKSPACE")?;
            capture_path_chain(acl_ledger, handle_ledger, workspace, &report)?;
        }

        let (_trusted_program, program_identity) = open_trusted_program(&program, handle_ledger)?;
        paths.push(program_identity);
        if let Some(root) = &config.program_access_root {
            grant_program_tree(acl_ledger, profile.sid, workspace, root, &program)?;
        }

        for path in &config.allow_paths {
            let absolute_path = scoped_path(workspace, path, "PATH_OUTSIDE_WORKSPACE")?;
            if let Some(parent) = absolute_path.parent() {
                grant_parent_traversal(acl_ledger, profile.sid, workspace, parent)?;
            }
            let identity = acl_ledger.lock_path(&absolute_path, handle_ledger)?;
            if absolute_path.is_dir() {
                allow_path(acl_ledger, profile.sid, &absolute_path, true)?;
            } else {
                allow_path(acl_ledger, profile.sid, &absolute_path, false)?;
            }
            paths.push(identity);
        }
        for path in &config.deny_paths {
            let absolute_path = scoped_path(workspace, path, "PATH_OUTSIDE_WORKSPACE")?;
            if let Some(parent) = absolute_path.parent() {
                grant_parent_traversal(acl_ledger, profile.sid, workspace, parent)?;
            }
            let identity = acl_ledger.lock_path(&absolute_path, handle_ledger)?;
            deny_path(acl_ledger, profile.sid, &absolute_path)?;
            paths.push(identity);
        }

        paths.push(acl_ledger.lock_path(workspace, handle_ledger)?);
        allow_traverse(acl_ledger, profile.sid, workspace)?;
        allow_traverse(acl_ledger, profile.sid, &working_directory)?;
        if let Some(report) = &config.report_path {
            let report = scoped_path(workspace, report, "REPORT_PATH_OUTSIDE_WORKSPACE")?;
            paths.push(acl_ledger.lock_path(&report, handle_ledger)?);
            allow_path(acl_ledger, profile.sid, &report, false)?;
        }

        let mut args = Vec::with_capacity(config.args.len() + 1);
        let separator = std::path::MAIN_SEPARATOR.to_string();
        let command_program = program.to_string_lossy().replace('/', &separator);
        args.push(OsString::from(command_program));
        for arg in &config.args {
            args.push(expand(arg, workspace, custody));
        }
        let command_line = args
            .iter()
            .map(|arg| quote_windows_arg(arg))
            .collect::<Vec<_>>()
            .join(" ");
        let mut command_line = wide(OsStr::new(&command_line));
        let program_wide = wide(&program);
        let cwd_wide = wide(&working_directory);

        let attributes = StartupAttributes::new(profile.sid)?;
        let job_raw = unsafe { CreateJobObjectW(null(), null()) };
        let job = unsafe { OwnedHandle::from_raw(job_raw, handle_ledger)? };
        unsafe {
            configure_job(job.raw())?;
        }

        let mut startup = STARTUPINFOEXW::default();
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        startup.lpAttributeList = attributes.list as _;
        let mut process_information = PROCESS_INFORMATION::default();
        let flags = CREATE_SUSPENDED | EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT;
        let created = unsafe {
            CreateProcessW(
                program_wide.as_ptr(),
                command_line.as_mut_ptr(),
                null_mut(),
                null_mut(),
                FALSE,
                flags,
                null_mut(),
                cwd_wide.as_ptr(),
                &mut startup.StartupInfo,
                &mut process_information,
            )
        };
        if created == FALSE {
            return Err(RuntimeError::win("CREATE_APPCONTAINER_PROCESS"));
        }
        let process =
            unsafe { OwnedHandle::from_raw(process_information.hProcess, handle_ledger)? };
        let mut process_cleanup = ProcessCleanupGuard::new(job.raw(), process.raw());
        let thread = unsafe { OwnedHandle::from_raw(process_information.hThread, handle_ledger)? };
        unsafe {
            if AssignProcessToJobObject(job.raw(), process.raw()) == FALSE {
                let error = GetLastError();
                return Err(RuntimeError::code("ASSIGN_PROCESS_TO_JOB", error));
            }
            process_cleanup.assigned = true;
        }

        let token_raw = unsafe {
            let mut token = null_mut();
            if OpenProcessToken(process.raw(), TOKEN_QUERY, &mut token) == FALSE {
                return Err(RuntimeError::win("OPEN_PROCESS_TOKEN"));
            }
            token
        };
        let token = unsafe { OwnedHandle::from_raw(token_raw, handle_ledger)? };
        let identity = token_identity(
            token.raw(),
            &profile.sid_string,
            process_information.dwProcessId,
        )?;
        unsafe {
            if ResumeThread(thread.raw()) == WAIT_FAILED {
                return Err(RuntimeError::win("RESUME_APPCONTAINER_PROCESS"));
            }
        }

        let mut peak_active_processes = unsafe { query_job(job.raw(), 0)? }.active_processes;
        let deadline = Instant::now() + config.timeout;
        let wait_result = loop {
            let now = Instant::now();
            if now >= deadline {
                break WAIT_TIMEOUT;
            }
            let remaining = deadline.duration_since(now);
            let wait_ms = remaining.as_millis().min(50).max(1) as u32;
            let result = unsafe { WaitForSingleObject(process.raw(), wait_ms) };
            let snapshot = unsafe { query_job(job.raw(), peak_active_processes)? };
            peak_active_processes = peak_active_processes.max(snapshot.active_processes);
            if result != WAIT_TIMEOUT {
                break result;
            }
        };
        let mut shutdown;
        let mut exit_code = 0u32;
        if wait_result == WAIT_TIMEOUT {
            unsafe {
                process_cleanup.terminate_and_wait(TIMEOUT_EXIT_CODE)?;
                if GetExitCodeProcess(process.raw(), &mut exit_code) == FALSE {
                    return Err(RuntimeError::win("GET_TIMEOUT_EXIT_CODE"));
                }
            }
            shutdown = ShutdownKind::Timeout;
        } else if wait_result == WAIT_OBJECT_0 {
            unsafe {
                if GetExitCodeProcess(process.raw(), &mut exit_code) == FALSE {
                    return Err(RuntimeError::win("GET_PROCESS_EXIT_CODE"));
                }
            }
            shutdown = if exit_code == 0 {
                ShutdownKind::Normal
            } else {
                ShutdownKind::Abnormal
            };
        } else {
            return Err(RuntimeError::win("WAIT_APPCONTAINER_PROCESS"));
        }

        let mut job_receipt = unsafe { query_job(job.raw(), peak_active_processes)? };
        if job_receipt.active_processes != 0 {
            unsafe {
                process_cleanup.terminate_and_wait(CLEANUP_EXIT_CODE)?;
                job_receipt = query_job(job.raw(), peak_active_processes)?;
            }
            shutdown = ShutdownKind::Abnormal;
        }

        let child_report = config.report_path.as_ref().and_then(|path| {
            scoped_path(workspace, path, "REPORT_PATH_OUTSIDE_WORKSPACE")
                .ok()
                .and_then(|path| fs::read_to_string(path).ok())
        });
        drop(token);
        drop(thread);
        drop(process);
        drop(attributes);
        drop(job);
        process_cleanup.disarm();

        let mut errors = Vec::new();
        if !job_receipt.tree_quiescent {
            errors.push("JOB_TREE_NOT_QUIESCENT".to_string());
        }
        Ok(RunReceipt {
            schema: "civilization-theatre-isolation-receipt-v1",
            status: if errors.is_empty() { "ok" } else { "degraded" },
            shutdown,
            process_identity: identity,
            exit_code,
            job: job_receipt,
            paths,
            profile_name: String::from_utf16_lossy(
                &profile.name[..profile.name.len().saturating_sub(1)],
            ),
            profile_sid: profile.sid_string.clone(),
            profile_deleted: false,
            custody_path: custody.to_string_lossy().into_owned(),
            custody_removed: false,
            acl_restored: false,
            handles_opened: 0,
            handles_closed: 0,
            handles_outstanding: 0,
            handle_close_failures: 0,
            child_report,
            errors,
        })
    }
}

pub fn prepare_workspace(path: &Path) -> Result<()> {
    let path = absolute(path)?;
    if path.exists() {
        if !path.is_dir() || path.is_symlink() {
            return Err(RuntimeError::code(
                "WORKSPACE_NOT_EMPTY_OR_LINK",
                ERROR_INVALID_PARAMETER,
            ));
        }
        return Ok(());
    }
    fs::create_dir_all(&path).map_err(|_| RuntimeError::win("CREATE_WORKSPACE"))
}
