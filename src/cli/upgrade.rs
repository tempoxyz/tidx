use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::process::Command;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub fn run() -> Result<()> {
    let os = detect_os()?;
    let arch = detect_arch()?;
    let asset = format!("tidx-{os}-{arch}");
    let url = format!("https://github.com/tempoxyz/tidx/releases/download/latest/{asset}");

    println!("Updating tidx...");
    println!("Downloading from {url}...");

    let current_exe = env::current_exe().context("Failed to get current executable path")?;

    // Download to a sibling temp file, then atomically rename over the running
    // binary. Writing straight to `current_exe` is unsafe: on Linux `curl -o`
    // fails with ETXTBSY on the executing file, and on macOS it truncates the
    // binary *before* the download finishes, so any interrupted transfer
    // (curl's `-f` guards HTTP errors, not dropped connections) leaves a
    // corrupt, unrunnable `tidx` with no backup. A sibling temp keeps the
    // rename on the same filesystem so it is atomic.
    let tmp = current_exe.with_extension("new");
    let tmp_str = tmp
        .to_str()
        .context("Executable path is not valid UTF-8")?;

    let output = Command::new("curl")
        .args(["-fsSL", &url, "-o", tmp_str])
        .output()
        .context("Failed to download update")?;

    if !output.status.success() {
        let _ = fs::remove_file(&tmp);
        anyhow::bail!(
            "Failed to download update: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Guard against a "successful" but empty/partial download.
    match fs::metadata(&tmp) {
        Ok(meta) if meta.len() > 0 => {}
        _ => {
            let _ = fs::remove_file(&tmp);
            anyhow::bail!("Downloaded update is missing or empty; leaving current binary untouched");
        }
    }

    #[cfg(unix)]
    if let Err(e) = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o755)) {
        let _ = fs::remove_file(&tmp);
        return Err(e).context("Failed to set executable permissions");
    }

    fs::rename(&tmp, &current_exe).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })?;

    println!("Updated tidx successfully!");

    let version_output = Command::new(&current_exe).arg("--version").output()?;
    print!("{}", String::from_utf8_lossy(&version_output.stdout));

    Ok(())
}

fn detect_os() -> Result<&'static str> {
    match env::consts::OS {
        "linux" => Ok("linux"),
        "macos" => Ok("darwin"),
        os => anyhow::bail!("Unsupported OS: {os}"),
    }
}

fn detect_arch() -> Result<&'static str> {
    match env::consts::ARCH {
        "x86_64" => Ok("amd64"),
        "aarch64" => Ok("arm64"),
        arch => anyhow::bail!("Unsupported architecture: {arch}"),
    }
}
