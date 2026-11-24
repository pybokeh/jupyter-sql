# Common WSL Commands

Run these from PowerShell or Command Prompt (not inside Linux).

## Setup and installing

```powershell
wsl --install                      # Enable WSL and install the default distro (Ubuntu)
wsl --install -d Ubuntu-24.04      # Install a specific distro
wsl --list --online                # List distros available to install (short: wsl -l -o)
wsl --update                       # Update the WSL kernel/components
wsl --set-default-version 2        # Make new distros use WSL 2
```

## Listing and checking

```powershell
wsl --list --verbose               # Show installed distros, state, and WSL version (short: wsl -l -v)
wsl --status                       # Show default distro and WSL version
wsl --version                      # Show WSL, kernel, and WSLg versions
```

## Running and switching

```powershell
wsl                                # Open the default distro
wsl -d Ubuntu-24.04                # Open a specific distro
wsl -d Ubuntu-24.04 -u root        # Open as a specific user (handy for password resets)
wsl ls -la                         # Run a single Linux command from Windows
wsl --cd ~                         # Start in a specific directory (~ = Linux home)
wsl --set-default Ubuntu-24.04     # Change which distro `wsl` opens by default
wsl --set-version Ubuntu-24.04 2   # Convert a distro between WSL 1 and WSL 2
```

## Stopping

```powershell
wsl --terminate Ubuntu-24.04       # Stop one distro (short: wsl -t)
wsl --shutdown                     # Stop all distros and the WSL VM (use after editing .wslconfig)
```

## Backing up, moving, and restoring

```powershell
wsl --export Ubuntu-24.04 D:\backup\ubuntu.tar                # Back up a distro to a tar file
wsl --import MyUbuntu D:\WSL\MyUbuntu D:\backup\ubuntu.tar    # Restore/clone it under a new name and location
wsl --export Ubuntu-24.04 D:\backup\ubuntu.vhdx --vhd         # Export as a VHDX instead
```

Export and import is also the standard way to move a distro to another drive.

## Deleting a distro

```powershell
wsl --unregister Ubuntu-24.04
```

This permanently deletes the distro and all its files, so export a backup first if you might want anything. After unregistering, the Store app is still installed; you can also uninstall it from Windows Settings > Apps if you want it gone entirely. Reinstalling with `wsl --install -d Ubuntu-24.04` gives you a fresh copy.

## Disk and file access

```powershell
wsl --manage Ubuntu-24.04 --set-sparse true   # Enable sparse VHD so the disk can shrink
wsl --mount \\.\PHYSICALDRIVE1                # Mount a physical disk in WSL
```

Inside Linux, your Windows drives are under `/mnt/c`, `/mnt/d`, and so on. From Windows, Linux files are reachable at `\\wsl$\Ubuntu-24.04` (or `\\wsl.localhost\Ubuntu-24.04`). Running `explorer.exe .` from a Linux shell opens the current Linux folder in File Explorer.

## Handy extras

- **Config files:** `%UserProfile%\.wslconfig` sets global options like memory and CPU limits; `/etc/wsl.conf` inside a distro sets per-distro options like systemd or default user. Run `wsl --shutdown` for changes to take effect.
- **Help:** `wsl --help` lists everything, including newer flags.
