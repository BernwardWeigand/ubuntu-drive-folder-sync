# Drive Sync
## Requirements
See `Description` in `poject/DEBIAN/control` 

## Configuration
Here is a small example for the configuration file
```json
{
    "drive_user": "email.of.the.google.user.account@xyz.org",
    "source_folder": "~/path/to/the/source/folder",
    "destination_folder": "My Drive/MyTargetFolder"
}
```
The file shall be placed under `~/.config/drive-sync`

## Development
### Dependencies
- [GNOME Python bindings (don't forget the stubs for IDE support)](https://pygobject.gnome.org/getting_started.html#ubuntu-getting-started)
- [Watchdog for checking if changes occur](https://pypi.org/project/watchdog/)

## Versions
### 1
First version of the tool tested from to Ubuntu 24.10 up to 25.10.

### 2
[Unfortunately the native support for Google Drive was dropped with GNOME 50.](https://discourse.gnome.org/t/google-drive-in-gnome-50/34417)
Thus, this new version is necessary to run on Ubuntu 26.04 onwards.
