# Drive Sync

## Setup
### Google Drive access via rclone
The tool uses [rclone](https://rclone.org/drive/) to upload the files to Google Drive.
The login to Google Drive is done once per user while creating an rclone remote:
```shell
rclone config
```
Choose `Google Drive` as storage type and give the remote a name, e.g. `gdrive`.
In the browser that opens, log in with the Google account and grant access.
rclone stores the token in `~/.config/rclone/rclone.conf` and renews it automatically.

Notes:
- rclone's default client ID is shared by all rclone users and thus rate limited.
  [Creating an own client ID](https://rclone.org/drive/#making-your-own-client-id) is recommended.
  Set its publishing status to `In production` (or use the user type `Internal` for a Google Workspace organisation),
  otherwise Google revokes the login after 7 days.
- With the scope `drive.file` rclone only sees files it uploaded itself.
  Files uploaded by version 1 of this tool would then be uploaded a second time, so prefer the scope `drive`.
- If the login expires anyway, a desktop notification is shown. Log in again with
  ```shell
  rclone config reconnect gdrive:
  ```

### Configuration
Here is a small example for the configuration file
```json
{
    "rclone_remote": "gdrive",
    "source_folder": "~/path/to/the/source/folder",
    "destination_folder": "MyTargetFolder"
}
```
The file shall be placed under `~/.config/drive-sync` as `config.json`.
The `destination_folder` is relative to `My Drive`.



## Development
### Requirements
See `Description` in `project/DEBIAN/control`

### Dependencies
- [GNOME Python bindings (don't forget the stubs for IDE support)](https://pygobject.gnome.org/getting_started.html#ubuntu-getting-started)
- [Watchdog for checking if changes occur](https://pypi.org/project/watchdog/)
- [rclone for the upload to Google Drive](https://rclone.org/)

### Build
The Debian package is built from the folder `project`, which mirrors the file system of the target machine
(`DEBIAN` holds the package metadata and the `postinst` script).
1. Set the new version in `project/DEBIAN/control`.
2. Run the build script from the root of the repository (requires `dpkg-deb`, which is part of Ubuntu):
   ```shell
   bash build-deb.sh
   ```
   It removes Python bytecode (`__pycache__`) from `project` and creates `<Package>-<Version>.deb`, e.g. `drive-sync-2.0.0.deb`.
3. Optionally check the package metadata and contents:
   ```shell
   dpkg-deb --info drive-sync-2.0.0.deb
   dpkg-deb --contents drive-sync-2.0.0.deb
   ```
4. Install it including its dependencies:
   ```shell
   sudo apt install ./drive-sync-2.0.0.deb
   ```

## Versions
### 1
First version of the tool tested from to Ubuntu 24.10 up to 25.10.

### 2
[Unfortunately the native support for Google Drive was dropped with GNOME 50.](https://discourse.gnome.org/t/google-drive-in-gnome-50/34417)
Thus, this new version is necessary to run on Ubuntu 26.04 onwards.
It uploads the files via rclone instead of the Google Drive mount of GNOME.

Migrating from version 1:
1. Create a rclone remote as described in the setup.
2. Replace `drive_user` with `rclone_remote` in the configuration file
   and remove the `My Drive/` prefix from `destination_folder` (a remaining prefix is ignored).
