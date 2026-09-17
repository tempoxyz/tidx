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

    let output = Command::new("curl")
        .args(["-fsSL", &url, "-o", current_exe.to_str().unwrap()])
        .output()
        .context("Failed to download update")?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to download update: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(unix)]
    fs::set_permissions(&current_exe, fs::Permissions::from_mode(0o755))
        .context("Failed to set executable permissions")?;

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
