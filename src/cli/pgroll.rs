use std::ffi::OsString;
use std::process::Command;

use anyhow::{Context, Result, bail};
use clap::Parser;

#[derive(Parser)]
#[command(
    about = "Run pgroll through tidx",
    trailing_var_arg = true,
    allow_hyphen_values = true
)]
pub struct Args {
    /// Arguments to pass through to pgroll.
    ///
    /// The pgroll executable is resolved from TIDX_PGROLL_BIN, then PGROLL_BIN,
    /// then PATH as `pgroll`.
    #[arg(value_name = "PGROLL_ARGS", trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<OsString>,
}

pub fn run(args: Args) -> Result<()> {
    let pgroll_bin = std::env::var_os("TIDX_PGROLL_BIN")
        .or_else(|| std::env::var_os("PGROLL_BIN"))
        .unwrap_or_else(|| OsString::from("pgroll"));

    let status = Command::new(&pgroll_bin)
        .args(args.args)
        .status()
        .with_context(|| {
            format!(
                "failed to execute `{}`; install pgroll or set TIDX_PGROLL_BIN",
                pgroll_bin.to_string_lossy()
            )
        })?;

    if let Some(code) = status.code() {
        if code == 0 {
            return Ok(());
        }
        bail!("pgroll exited with status code {code}");
    }

    bail!("pgroll was terminated by a signal");
}
