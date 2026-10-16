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