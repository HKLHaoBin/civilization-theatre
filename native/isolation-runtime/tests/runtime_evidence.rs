#![cfg(windows)]

use std::fs::{self, File};
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::ptr::null_mut;
use std::time::{SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{CloseHandle, FALSE, WAIT_TIMEOUT};
use windows_sys::Win32::Security::{GetFileSecurityW, PSECURITY_DESCRIPTOR};
use windows_sys::Win32::System::Threading::{
    OpenProcess, WaitForSingleObject, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
};

fn unique_workspace(label: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("native-evidence")
        .join(format!("{label}-{}-{suffix}", std::process::id()))
}

struct WorkspaceGuard(PathBuf);

impl Drop for WorkspaceGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn copy_fixture(workspace: &Path) -> (WorkspaceGuard, PathBuf, PathBuf) {
    let bin = workspace.join("bin");
    fs::create_dir_all(&bin).expect("create fixture directory");
    let source = PathBuf::from(env!("CARGO_BIN_EXE_sqlite-fixture"));
    let fixture = bin.join("sqlite-fixture.exe");
    fs::copy(source, &fixture).expect("copy sqlite fixture");
    (WorkspaceGuard(workspace.to_path_buf()), bin, fixture)
}

fn run_fixture_case(
    label: &str,
    fixture_args: &[(&str, Option<&Path>)],
    timeout_ms: u64,
) -> (WorkspaceGuard, String, PathBuf) {
    let workspace = unique_workspace(label);
    fs::create_dir_all(&workspace).expect("create workspace");
    let (guard, bin, fixture) = copy_fixture(&workspace);
    let mut command = Command::new(env!("CARGO_BIN_EXE_isolation-runtime"));
    command
        .arg("--program")
        .arg(&fixture)
        .arg("--workspace")
        .arg(&workspace)
        .arg("--program-access-root")
        .arg(&bin)
        .arg("--timeout-ms")
        .arg(timeout_ms.to_string());
    for (flag, path) in fixture_args {
        command.arg("--arg").arg(flag);
        if let Some(path) = path {
            command.arg("--arg").arg(path);
        }
    }
    let output = command.output().expect("run fixture case");
    let stdout = String::from_utf8(output.stdout).expect("runtime stdout utf8");
    assert!(
        output.status.success(),
        "runtime failed: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt = stdout
        .lines()
        .rev()
        .find(|line| line.starts_with('{'))
        .expect("receipt JSON")
        .to_string();
    (guard, receipt, workspace)
}

fn json_number(json: &str, key: &str) -> u64 {
    let marker = format!("\"{key}\":");
    let start = json.find(&marker).expect("JSON number key") + marker.len();
    let end = json[start..]
        .find(|ch: char| !ch.is_ascii_digit())
        .map(|offset| start + offset)
        .unwrap_or(json.len());
    json[start..end].parse().expect("JSON number")
}

fn assert_receipt_cleanup(receipt: &str) {
    assert!(receipt.contains("\"status\":\"ok\""), "{receipt}");
    assert!(receipt.contains("\"is_appcontainer\":true"), "{receipt}");

    assert!(receipt.contains("\"sid_match\":true"), "{receipt}");
    assert!(receipt.contains("\"assigned\":true"), "{receipt}");
    assert!(receipt.contains("\"kill_on_job_close\":true"), "{receipt}");
    assert!(receipt.contains("\"tree_quiescent\":true"), "{receipt}");
    assert!(receipt.contains("\"profile_deleted\":true"), "{receipt}");
    assert!(receipt.contains("\"custody_removed\":true"), "{receipt}");
    assert!(receipt.contains("\"acl_restored\":true"), "{receipt}");
    assert!(receipt.contains("\"handles_outstanding\":0"), "{receipt}");
    assert!(receipt.contains("\"handle_close_failures\":0"), "{receipt}");
    assert!(receipt.contains("\"identity_match\":true"), "{receipt}");
    assert!(receipt.contains("\"reparse_free\":true"), "{receipt}");
    assert!(receipt.contains("\"hard_link_free\":true"), "{receipt}");
    assert!(json_number(receipt, "total_processes") >= 1, "{receipt}");
    assert!(
        json_number(receipt, "peak_active_processes") >= 1,
        "{receipt}"
    );
}

fn security_descriptor(path: &Path) -> Vec<u8> {
    const DACL_SECURITY_INFORMATION: u32 = 0x0000_0004;
    const SE_DACL_AUTO_INHERITED: u16 = 0x0400;
    let name = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut required = 0u32;
    let ok = unsafe {
        GetFileSecurityW(
            name.as_ptr(),
            DACL_SECURITY_INFORMATION,
            null_mut(),
            0,
            &mut required,
        )
    };
    assert_eq!(
        ok, FALSE,
        "security descriptor size query unexpectedly succeeded"
    );
    assert!(required > 0, "security descriptor size missing");
    let mut descriptor = vec![0u8; required as usize];
    let mut returned = 0u32;
    let ok = unsafe {
        GetFileSecurityW(
            name.as_ptr(),
            DACL_SECURITY_INFORMATION,
            descriptor.as_mut_ptr() as PSECURITY_DESCRIPTOR,
            descriptor.len() as u32,
            &mut returned,
        )
    };
    assert_ne!(ok, FALSE, "security descriptor read failed");
    descriptor.truncate(returned as usize);
    if descriptor.len() >= 4 {
        let control = u16::from_le_bytes([descriptor[2], descriptor[3]]) & !SE_DACL_AUTO_INHERITED;
        let bytes = control.to_le_bytes();
        descriptor[2] = bytes[0];
        descriptor[3] = bytes[1];
    }
    descriptor
}

fn process_is_running(pid: u32) -> bool {
    let handle = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            FALSE,
            pid,
        )
    };
    if handle.is_null() {
        return false;
    }
    let result = unsafe { WaitForSingleObject(handle, 0) };
    unsafe {
        CloseHandle(handle);
    }
    result == WAIT_TIMEOUT
}

fn run_child_case(
    timeout_ms: u64,
    child_sleep_ms: u64,
) -> (WorkspaceGuard, String, PathBuf, PathBuf) {
    let workspace = unique_workspace("job-child");
    fs::create_dir_all(&workspace).expect("create child workspace");
    let (guard, bin, fixture) = copy_fixture(&workspace);
    let child_report = workspace.join("child-pid.json");
    let parent_report = workspace.join("parent-report.json");
    File::create(&child_report).expect("create child pid report");
    File::create(&parent_report).expect("create parent report");
    let output = Command::new(env!("CARGO_BIN_EXE_isolation-runtime"))
        .arg("--program")
        .arg(&fixture)
        .arg("--workspace")
        .arg(&workspace)
        .arg("--program-access-root")
        .arg(&bin)
        .arg("--allow")
        .arg(&child_report)
        .arg("--allow")
        .arg(&parent_report)
        .arg("--report")
        .arg(&parent_report)
        .arg("--arg")
        .arg("--spawn-child-report")
        .arg("--arg")
        .arg(&child_report)
        .arg("--arg")
        .arg("--child-sleep-ms")
        .arg("--arg")
        .arg(child_sleep_ms.to_string())
        .arg("--timeout-ms")
        .arg(timeout_ms.to_string())
        .output()
        .expect("run child job evidence");
    let stdout = String::from_utf8(output.stdout).expect("child receipt stdout utf8");
    assert!(
        output.status.success(),
        "runtime failed: stdout={stdout} stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt = stdout
        .lines()
        .rev()
        .find(|line| line.starts_with('{'))
        .expect("child receipt JSON")
        .to_string();
    (guard, receipt, workspace, child_report)
}

#[test]
fn native_sqlite_allow_and_deny_share_appcontainer_identity() {
    let workspace = unique_workspace("sqlite");
    let outside_root = unique_workspace("sqlite-outside");
    fs::create_dir_all(&workspace).expect("create sqlite workspace");
    fs::create_dir_all(&outside_root).expect("create outside sqlite workspace");
    let (guard, bin, fixture) = copy_fixture(&workspace);
    let outside_guard = WorkspaceGuard(outside_root.clone());
    let data = workspace.join("data");
    fs::create_dir_all(&data).expect("create sqlite data directory");
    let allowed = data.join("allowed.sqlite");
    let denied = outside_root.join("denied.sqlite");
    let report = workspace.join("child-report.json");
    File::create(&allowed).expect("create allowed database");
    File::create(&denied).expect("create denied database");
    File::create(&report).expect("create child report");
    let allowed_descriptor = security_descriptor(&allowed);
    let workspace_descriptor = security_descriptor(&workspace);

    let output = Command::new(env!("CARGO_BIN_EXE_isolation-runtime"))
        .arg("--program")
        .arg(&fixture)
        .arg("--workspace")
        .arg(&workspace)
        .arg("--program-access-root")
        .arg(&bin)
        .arg("--allow")
        .arg(&data)
        .arg("--allow")
        .arg(&allowed)
        .arg("--allow")
        .arg(&report)
        .arg("--report")
        .arg(&report)
        .arg("--arg")
        .arg("--allow")
        .arg("--arg")
        .arg(&allowed)
        .arg("--arg")
        .arg("--deny")
        .arg("--arg")
        .arg(&denied)
        .arg("--arg")
        .arg("--report")
        .arg("--arg")
        .arg(&report)
        .output()
        .expect("run sqlite evidence");
    let stdout = String::from_utf8(output.stdout).expect("receipt stdout utf8");
    let receipt = stdout
        .lines()
        .rev()
        .find(|line| line.starts_with('{'))
        .unwrap_or_else(|| {
            panic!(
                "sqlite receipt missing: stdout={stdout} stderr={}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
    assert!(output.status.success(), "{stdout}");
    assert!(receipt.contains("\"shutdown\":\"normal\""), "{receipt}");
    assert_receipt_cleanup(receipt);

    let child_report = fs::read_to_string(&report).expect("read child report");
    assert!(
        child_report.contains("\"allow_opened\":true"),
        "{child_report}"
    );
    assert!(
        child_report.contains("\"deny_blocked\":true"),
        "{child_report}"
    );
    assert_eq!(
        json_number(receipt, "pid"),
        json_number(&child_report, "pid")
    );
    assert!(allowed.is_file(), "allowed SQLite database missing");
    assert_eq!(allowed_descriptor, security_descriptor(&allowed));
    assert_eq!(workspace_descriptor, security_descriptor(&workspace));
    drop(outside_guard);
    drop(guard);
}

#[test]
fn trusted_program_must_be_inside_declared_access_root() {
    let workspace = unique_workspace("trusted-root");
    fs::create_dir_all(&workspace).expect("create trusted-root workspace");
    let (guard, _bin, fixture) = copy_fixture(&workspace);
    let wrong_root = workspace.join("wrong-root");
    fs::create_dir_all(&wrong_root).expect("create wrong access root");
    let output = Command::new(env!("CARGO_BIN_EXE_isolation-runtime"))
        .arg("--program")
        .arg(&fixture)
        .arg("--workspace")
        .arg(&workspace)
        .arg("--program-access-root")
        .arg(&wrong_root)
        .arg("--timeout-ms")
        .arg("100")
        .output()
        .expect("run trusted-root rejection");
    assert!(
        !output.status.success(),
        "trusted root rejection unexpectedly passed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("PROGRAM_OUTSIDE_ACCESS_ROOT"), "{stderr}");
    drop(guard);
}

#[test]
fn native_shutdown_receipts_cover_normal_abnormal_and_timeout() {
    let (_normal_guard, normal, _normal_workspace) = run_fixture_case("normal", &[], 5_000);
    assert!(normal.contains("\"shutdown\":\"normal\""));
    assert!(normal.contains("\"exit_code\":0"));
    assert_receipt_cleanup(&normal);

    let (_abnormal_guard, abnormal, _abnormal_workspace) =
        run_fixture_case("abnormal", &[("--exit-code", None), ("7", None)], 5_000);
    assert!(abnormal.contains("\"shutdown\":\"abnormal\""));
    assert!(abnormal.contains("\"exit_code\":7"));
    assert_receipt_cleanup(&abnormal);

    let (_timeout_guard, timeout, _timeout_workspace) =
        run_fixture_case("timeout", &[("--sleep-ms", None), ("1000", None)], 100);
    assert!(timeout.contains("\"shutdown\":\"timeout\""));
    assert_receipt_cleanup(&timeout);
}

#[test]
fn outer_job_contains_and_kills_inherited_child() {
    let (_guard, receipt, _workspace, report) = run_child_case(1_000, 5_000);
    assert!(receipt.contains("\"shutdown\":\"timeout\""), "{receipt}");
    assert_receipt_cleanup(&receipt);
    assert!(json_number(&receipt, "total_processes") >= 2, "{receipt}");
    assert!(json_number(&receipt, "active_processes") == 0, "{receipt}");
    assert!(
        json_number(&receipt, "peak_active_processes") >= 2,
        "{receipt}"
    );
    assert!(receipt.contains("\"tree_quiescent\":true"), "{receipt}");
    assert!(receipt.contains("\"profile_deleted\":true"), "{receipt}");
    assert!(receipt.contains("\"custody_removed\":true"), "{receipt}");
    assert!(receipt.contains("\"acl_restored\":true"), "{receipt}");
    assert!(receipt.contains("\"handles_outstanding\":0"), "{receipt}");

    let child_report = fs::read_to_string(&report).expect("read child pid report");
    let child_pid = json_number(&child_report, "pid") as u32;
    assert_ne!(
        child_pid as u64,
        json_number(&receipt, "pid"),
        "child report was overwritten"
    );
    assert!(
        !process_is_running(child_pid),
        "child process {child_pid} escaped Job"
    );
}
