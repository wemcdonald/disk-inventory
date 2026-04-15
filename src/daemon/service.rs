//! OS service management: install/uninstall the daemon as a background service.
//! - macOS: launchd plist in ~/Library/LaunchAgents/
//! - Linux: systemd user unit in ~/.config/systemd/user/

use crate::config;
use anyhow::{Context, Result};
use std::path::PathBuf;

const LABEL: &str = "com.disk-inventory.daemon";
const SERVICE_NAME: &str = "disk-inventory";

/// Install the daemon as an OS service.
pub fn install() -> Result<()> {
    let binary = std::env::current_exe().context("cannot determine binary path")?;

    #[cfg(target_os = "macos")]
    install_launchd(&binary)?;

    #[cfg(target_os = "linux")]
    install_systemd(&binary)?;

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    anyhow::bail!("Service installation is not supported on this platform");

    Ok(())
}

/// Uninstall the daemon OS service.
pub fn uninstall() -> Result<()> {
    #[cfg(target_os = "macos")]
    uninstall_launchd()?;

    #[cfg(target_os = "linux")]
    uninstall_systemd()?;

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    anyhow::bail!("Service uninstallation is not supported on this platform");

    Ok(())
}

// --- macOS: launchd ---

#[cfg(target_os = "macos")]
fn plist_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("Library/LaunchAgents")
        .join(format!("{}.plist", LABEL))
}

#[cfg(target_os = "macos")]
fn install_launchd(binary: &PathBuf) -> Result<()> {
    let plist_dir = plist_path().parent().unwrap().to_path_buf();
    std::fs::create_dir_all(&plist_dir)?;

    let log_path = config::config_dir().join("daemon.log");
    let plist_content = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{binary}</string>
        <string>daemon</string>
        <string>run</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{log}</string>
    <key>StandardErrorPath</key>
    <string>{log}</string>
    <key>ProcessType</key>
    <string>Background</string>
    <key>LowPriorityIO</key>
    <true/>
    <key>Nice</key>
    <integer>10</integer>
</dict>
</plist>"#,
        label = LABEL,
        binary = binary.display(),
        log = log_path.display(),
    );

    let path = plist_path();
    std::fs::write(&path, plist_content)?;

    // Load the service
    let status = std::process::Command::new("launchctl")
        .args(["load", &path.to_string_lossy()])
        .status()
        .context("failed to run launchctl load")?;

    if status.success() {
        println!("Service installed and started.");
        println!("  Plist: {}", path.display());
        println!("  Log:   {}", log_path.display());
        println!("\nTo check status: disk-inventory daemon status");
        println!("To stop:         disk-inventory daemon uninstall");
    } else {
        anyhow::bail!(
            "launchctl load failed with exit code: {:?}",
            status.code()
        );
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn uninstall_launchd() -> Result<()> {
    let path = plist_path();

    if path.exists() {
        let _ = std::process::Command::new("launchctl")
            .args(["unload", &path.to_string_lossy()])
            .status();

        std::fs::remove_file(&path)?;
        println!("Service uninstalled.");
        println!("  Removed: {}", path.display());
    } else {
        println!(
            "No service installed (plist not found at {})",
            path.display()
        );
    }

    // Also clean up the socket
    let socket_path = config::config_dir().join("daemon.sock");
    let _ = std::fs::remove_file(&socket_path);

    Ok(())
}

// --- Linux: systemd ---

#[cfg(target_os = "linux")]
fn unit_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config/systemd/user")
        .join(format!("{}.service", SERVICE_NAME))
}

#[cfg(target_os = "linux")]
fn install_systemd(binary: &PathBuf) -> Result<()> {
    let unit_dir = unit_path().parent().unwrap().to_path_buf();
    std::fs::create_dir_all(&unit_dir)?;

    let unit_content = format!(
        r#"[Unit]
Description=Disk Inventory Daemon
After=default.target

[Service]
Type=simple
ExecStart={binary} daemon run
Restart=on-failure
RestartSec=10
Nice=10
IOSchedulingClass=idle

[Install]
WantedBy=default.target
"#,
        binary = binary.display(),
    );

    let path = unit_path();
    std::fs::write(&path, unit_content)?;

    // Reload and enable
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();

    let status = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", SERVICE_NAME])
        .status()
        .context("failed to run systemctl")?;

    if status.success() {
        println!("Service installed and started.");
        println!("  Unit: {}", path.display());
        println!(
            "\nTo check status: systemctl --user status {}",
            SERVICE_NAME
        );
        println!("To stop:         disk-inventory daemon uninstall");
    } else {
        anyhow::bail!("systemctl enable failed");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn uninstall_systemd() -> Result<()> {
    let path = unit_path();

    let _ = std::process::Command::new("systemctl")
        .args(["--user", "disable", "--now", SERVICE_NAME])
        .status();

    if path.exists() {
        std::fs::remove_file(&path)?;
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        println!("Service uninstalled.");
        println!("  Removed: {}", path.display());
    } else {
        println!(
            "No service installed (unit not found at {})",
            path.display()
        );
    }

    // Clean up socket
    let socket_path = config::config_dir().join("daemon.sock");
    let _ = std::fs::remove_file(&socket_path);

    Ok(())
}
