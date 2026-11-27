use std::ffi::{CStr, CString, OsString};
use std::os::raw::{c_char, c_void};
use std::path::PathBuf;
use std::process::Command;
use std::ptr::null_mut;
use std::thread;
use std::time::Duration;

use libsqlite3_sys::{
    sqlite3, sqlite3_close, sqlite3_errcode, sqlite3_errmsg, sqlite3_exec,
    sqlite3_extended_errcode, sqlite3_free, sqlite3_open_v2, SQLITE_OK, SQLITE_OPEN_CREATE,
    SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE,
};
use windows_sys::Win32::System::Threading::GetCurrentProcessId;

fn take_value<I>(args: &mut I, flag: &str) -> Result<OsString, String>
where
    I: Iterator<Item = OsString>,
{
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn json_string(value: &str) -> String {
    let mut output = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            ch if ch.is_control() => output.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => output.push(ch),
        }
    }
    output.push('"');
    output
}

fn sqlite_message(db: *mut sqlite3, code: i32) -> String {
    if db.is_null() {
        return format!("sqlite rc={code}");
    }
    let extended = unsafe { sqlite3_extended_errcode(db) };
    let primary = unsafe { sqlite3_errcode(db) };
    let message = unsafe { CStr::from_ptr(sqlite3_errmsg(db)) }
        .to_string_lossy()
        .into_owned();
    format!("{message} (primary={primary}, extended={extended})")
}

fn sqlite_open(path: &PathBuf, create: bool, write: bool) -> Result<*mut sqlite3, String> {
    let filename = CString::new(path.to_string_lossy().as_bytes())
        .map_err(|_| "sqlite path contains NUL".to_string())?;
    let flags = if write {
        SQLITE_OPEN_READWRITE
    } else {
        SQLITE_OPEN_READONLY
    } | if create { SQLITE_OPEN_CREATE } else { 0 };
    let mut db = null_mut();
    let code = unsafe { sqlite3_open_v2(filename.as_ptr(), &mut db, flags, std::ptr::null()) };
    if code != SQLITE_OK {
        let message = sqlite_message(db, code);
        if !db.is_null() {
            unsafe {
                sqlite3_close(db);
            }
        }
        return Err(message);
    }
    Ok(db)
}

fn sqlite_exec(db: *mut sqlite3, sql: &str) -> Result<(), String> {
    let sql = CString::new(sql).map_err(|_| "sqlite SQL contains NUL".to_string())?;
    let mut error_message: *mut c_char = null_mut();
    let code = unsafe {
        sqlite3_exec(
            db,
            sql.as_ptr(),
            None,
            null_mut::<c_void>(),
            &mut error_message,
        )
    };
    if code == SQLITE_OK {
        return Ok(());
    }
    let message = if error_message.is_null() {
        sqlite_message(db, code)
    } else {
        let message = unsafe { CStr::from_ptr(error_message) }
            .to_string_lossy()
            .into_owned();
        unsafe {
            sqlite3_free(error_message as *mut c_void);
        }
        format!("{message} ({})", sqlite_message(db, code))
    };
    Err(message)
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mut allow = None;
    let mut deny = None;
    let mut report = None;
    let mut sleep_ms = 0u64;
    let mut exit_code = 0i32;
    let mut spawn_child_report = None;
    let mut child_report = None;
    let mut child_sleep_ms = 0u64;

    let parsed = (|| -> Result<(), String> {
        while let Some(flag) = args.next() {
            match flag.to_string_lossy().as_ref() {
                "--allow" => allow = Some(PathBuf::from(take_value(&mut args, "--allow")?)),
                "--deny" => deny = Some(PathBuf::from(take_value(&mut args, "--deny")?)),
                "--report" => report = Some(PathBuf::from(take_value(&mut args, "--report")?)),
                "--spawn-child-report" => {
                    spawn_child_report = Some(PathBuf::from(take_value(
                        &mut args,
                        "--spawn-child-report",
                    )?));
                }
                "--child-report" => {
                    child_report = Some(PathBuf::from(take_value(&mut args, "--child-report")?));
                }
                "--child-sleep-ms" => {
                    child_sleep_ms = take_value(&mut args, "--child-sleep-ms")?
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "invalid --child-sleep-ms".to_string())?;
                }
                "--sleep-ms" => {
                    sleep_ms = take_value(&mut args, "--sleep-ms")?
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "invalid --sleep-ms".to_string())?;
                }
                "--exit-code" => {
                    exit_code = take_value(&mut args, "--exit-code")?
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "invalid --exit-code".to_string())?;
                }
                "--help" | "-h" => {
                    println!("usage: sqlite-fixture --allow PATH --deny PATH --report PATH [--sleep-ms N] [--exit-code N] [--spawn-child-report PATH --child-sleep-ms N]");
                    std::process::exit(0);
                }
                other => return Err(format!("unknown option: {other}")),
            }
        }
        Ok(())
    })();

    if let Err(error) = parsed {
        eprintln!("sqlite fixture error: {error}");
        std::process::exit(2);
    }

    if let Some(path) = child_report {
        let report_body = format!(r#"{{"pid":{}}}"#, unsafe { GetCurrentProcessId() });
        if let Err(error) = std::fs::write(path, report_body) {
            eprintln!("sqlite fixture child report error: {error}");
            std::process::exit(3);
        }
    }

    if let Some(path) = spawn_child_report {
        let executable = std::env::current_exe().unwrap_or_else(|error| {
            eprintln!("sqlite fixture current executable error: {error}");
            std::process::exit(4);
        });
        let status = Command::new(executable)
            .arg("--child-report")
            .arg(path)
            .arg("--sleep-ms")
            .arg(child_sleep_ms.to_string())
            .status()
            .unwrap_or_else(|error| {
                eprintln!("sqlite fixture child launch error: {error}");
                std::process::exit(4);
            });
        if !status.success() {
            eprintln!("sqlite fixture child exited with {status}");
            std::process::exit(4);
        }
    }

    if sleep_ms != 0 {
        thread::sleep(Duration::from_millis(sleep_ms));
    }

    let mut allow_opened = false;
    let mut deny_blocked = false;
    let mut allow_error = None;
    let mut deny_error = None;
    let expect_allow = allow.is_some();
    let expect_deny = deny.is_some();

    if let Some(path) = allow {
        match sqlite_open(&path, true, true).and_then(|db| {
            let result = sqlite_exec(
                db,
                "CREATE TABLE IF NOT EXISTS isolation_evidence (value TEXT); INSERT INTO isolation_evidence(value) VALUES ('allowed');",
            );
            unsafe { sqlite3_close(db); }
            result
        }) {
            Ok(()) => allow_opened = true,
            Err(error) => allow_error = Some(error),
        }
    }

    if let Some(path) = deny {
        match sqlite_open(&path, false, false) {
            Ok(db) => {
                let result = sqlite_exec(db, "SELECT name FROM sqlite_master;");
                unsafe {
                    sqlite3_close(db);
                }
                match result {
                    Ok(()) => {
                        deny_error = Some(format!("denied database opened: {}", path.display()))
                    }
                    Err(error) => {
                        deny_blocked = true;
                        deny_error = Some(error);
                    }
                }
            }
            Err(error) => {
                deny_blocked = true;
                deny_error = Some(error);
            }
        }
    }

    let successful =
        (!expect_allow || allow_opened) && (!expect_deny || deny_blocked) && exit_code == 0;
    if let Some(path) = report {
        let report_body = format!(
            "{{\"pid\":{},\"allow_opened\":{},\"deny_blocked\":{},\"allow_error\":{},\"deny_error\":{},\"sleep_ms\":{}}}",
            unsafe { GetCurrentProcessId() },
            allow_opened,
            deny_blocked,
            allow_error.as_deref().map(json_string).unwrap_or_else(|| "null".to_string()),
            deny_error.as_deref().map(json_string).unwrap_or_else(|| "null".to_string()),
            sleep_ms,
        );
        if let Err(error) = std::fs::write(path, report_body) {
            eprintln!("sqlite fixture report error: {error}");
            std::process::exit(3);
        }
    }
    if !successful {
        std::process::exit(if exit_code == 0 { 1 } else { exit_code });
    }
}
