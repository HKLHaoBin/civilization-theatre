use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use civilization_theatre_isolation_runtime::{IsolationRuntime, RunConfig};

fn usage() -> &'static str {
    "usage: isolation-runtime --program PATH --workspace PATH --program-access-root PATH [--arg VALUE]* [--allow PATH]* [--deny PATH]* [--working-directory PATH] [--report PATH] [--timeout-ms N]"
}

fn take_value<I>(args: &mut I, flag: &str) -> Result<OsString, String>
where
    I: Iterator<Item = OsString>,
{
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mut program = None;
    let mut workspace = None;
    let mut working_directory = None;
    let mut program_access_root = None;
    let mut report = None;
    let mut command_args = Vec::new();
    let mut allow_paths = Vec::new();
    let mut deny_paths = Vec::new();
    let mut timeout_ms = 10_000u64;

    let parsed = (|| -> Result<(), String> {
        while let Some(flag) = args.next() {
            match flag.to_string_lossy().as_ref() {
                "--program" => program = Some(PathBuf::from(take_value(&mut args, "--program")?)),
                "--workspace" => {
                    workspace = Some(PathBuf::from(take_value(&mut args, "--workspace")?))
                }
                "--working-directory" => {
                    working_directory =
                        Some(PathBuf::from(take_value(&mut args, "--working-directory")?));
                }
                "--program-access-root" => {
                    program_access_root = Some(PathBuf::from(take_value(
                        &mut args,
                        "--program-access-root",
                    )?));
                }
                "--report" => report = Some(PathBuf::from(take_value(&mut args, "--report")?)),
                "--arg" => command_args.push(take_value(&mut args, "--arg")?),
                "--allow" => allow_paths.push(PathBuf::from(take_value(&mut args, "--allow")?)),
                "--deny" => deny_paths.push(PathBuf::from(take_value(&mut args, "--deny")?)),
                "--timeout-ms" => {
                    let value = take_value(&mut args, "--timeout-ms")?;
                    timeout_ms = value
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "invalid --timeout-ms".to_string())?;
                }
                "--help" | "-h" => {
                    println!("{}", usage());
                    std::process::exit(0);
                }
                other => return Err(format!("unknown option: {other}")),
            }
        }
        Ok(())
    })();

    if let Err(error) = parsed {
        eprintln!("{}\nerror: {}", usage(), error);
        std::process::exit(2);
    }
    let Some(program) = program else {
        eprintln!("{}\nerror: --program is required", usage());
        std::process::exit(2);
    };
    let Some(workspace) = workspace else {
        eprintln!("{}\nerror: --workspace is required", usage());
        std::process::exit(2);
    };

    let config = RunConfig {
        program,
        args: command_args,
        workspace,
        working_directory,
        program_access_root,
        allow_paths,
        deny_paths,
        report_path: report,
        timeout: Duration::from_millis(timeout_ms),
    };
    match IsolationRuntime::run(config) {
        Ok(receipt) => println!("{}", receipt.to_json()),
        Err(error) => {
            eprintln!("isolation runtime failed: {}", error);
            std::process::exit(1);
        }
    }
}
