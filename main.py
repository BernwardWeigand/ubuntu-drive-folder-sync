#!/usr/bin/python3
from hashlib import sha256
from json import JSONDecodeError, load
from logging import basicConfig, warning, INFO, error, info
from os import path, walk
from signal import signal, SIGTERM, Signals
from types import FrameType
from typing import Tuple

from gi.repository import Gio, GLib  # GNOME APIs for file operations and DBus integration
from watchdog.events import FileSystemEvent, FileSystemEventHandler, FileSystemMovedEvent
from watchdog.observers import Observer

# Constants
GOOGLE_DRIVE_PREFIX = "google-drive://"

# Load configuration
CONFIG_DIR = path.expanduser("~/.drive-sync/")
CONFIG_PATH = path.join(CONFIG_DIR, "config.json")
LOG_FILE = path.join(CONFIG_DIR, "sync.log")

# Set up logging
basicConfig(
    filename=LOG_FILE,
    level=INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_config() -> Tuple[str, str, str]:
    try:
        with open(CONFIG_PATH, "r") as config_file:
            config = load(config_file)
            source_folder: str = config.get("source_folder")
            destination_folder: str = config.get("destination_folder")
            drive_user: str = config.get("drive_user")
            if not source_folder or not destination_folder or not drive_user:
                raise ValueError(
                    "Missing required configuration values: source_folder, destination_folder, and drive_user")
            return path.expanduser(source_folder), path.expanduser(destination_folder), drive_user
    except (FileNotFoundError, JSONDecodeError, ValueError) as e:
        error(f"Configuration error: {e}")
        exit(1)


# Set paths from config
LOCAL_FOLDER, GOOGLE_DRIVE_FOLDER, DRIVE_USER = load_config()


def is_drive_available() -> bool:
    """Check if Google Drive is mounted using GNOME's Gio.VolumeMonitor."""
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/class-VolumeMonitor.html#gi.repository.Gio.VolumeMonitor.get
    volume_monitor = Gio.VolumeMonitor.get()
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/class-VolumeMonitor.html#gi.repository.Gio.VolumeMonitor.get_mounts
    mounts = volume_monitor.get_mounts()

    for mount in mounts:
        # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-Mount.html#gi.repository.Gio.Mount.get_root
        # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.get_uri
        mount_uri = mount.get_root().get_uri()
        if mount_uri.startswith(GOOGLE_DRIVE_PREFIX) and DRIVE_USER in mount_uri:
            return True
    return False


def mount_google_drive() -> None:
    """Attempt to manually mount Google Drive using Gio.VolumeMonitor."""
    info("Attempting to mount Google Drive...")
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/class-VolumeMonitor.html#gi.repository.Gio.VolumeMonitor.get
    volume_monitor = Gio.VolumeMonitor.get()
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/class-VolumeMonitor.html#gi.repository.Gio.VolumeMonitor.get_volumes
    volumes = volume_monitor.get_volumes()
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/GLib-2.0/structure-MainLoop.html#gi.repository.GLib.MainLoop
    loop = GLib.MainLoop()  # Create a main loop to handle async calls

    for volume in volumes:
        for identifier in volume.enumerate_identifiers():
            # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-AsyncResult.html#gi.repository.Gio.AsyncResult
            def on_mount_done(user_args, result: Gio.AsyncResult) -> None:
                """Callback function to resolve the event when mounting is complete."""
                try:
                    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-Volume.html#gi.repository.Gio.Volume.mount_finish
                    res = volume.mount_finish(result)
                    if res:
                        info("Google Drive mounted successfully.")
                    else:
                        error("Failed to mount Google Drive.")
                except Exception as e:
                    error(f"Failed to mount Google Drive: {e}")
                finally:
                    loop.quit()  # Stop the event loop once done

            identifier_value = volume.get_identifier(identifier)
            if identifier_value.startswith(GOOGLE_DRIVE_PREFIX) and DRIVE_USER in identifier_value:
                try:
                    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-Volume.html#gi.repository.Gio.Volume.mount
                    volume.mount(Gio.MountMountFlags.NONE, callback=on_mount_done)
                    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/GLib-2.0/structure-MainLoop.html#gi.repository.GLib.MainLoop.run
                    loop.run()  # Run the main loop until `on_mount_done` quits it to block the main thread to avoid race conditions
                    return
                except Exception as e:
                    error(f"Failed to mount Google Drive: {e}")
                    return

    warning("No Google Drive volume found to mount.")


def get_remote_file_hash(dest_file: Gio.File) -> str:
    """Compute the SHA256 hash of a remote Google Drive file."""
    try:
        stream = dest_file.read(None)
        hasher = sha256()

        while True:
            buffer = stream.read_bytes(4096)
            if buffer.get_size() == 0:  # Proper EOF check
                break
            hasher.update(buffer.get_data())  # Convert GLib.Bytes to raw bytes

        stream.close()  # Ensure the stream is closed
        return hasher.hexdigest()
    except Exception as e:
        error(f"Failed to compute remote file hash: {e}")
        return ""


def compute_file_hash(file_path: str) -> str:
    """Compute the SHA256 hash of a local file."""
    try:
        hasher = sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(4096):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        error(f"Failed to compute local file hash: {e}")
        return ""


def sync_file(file_path: str) -> None:
    """Ensure the destination file exists in Google Drive before copying and syncing changes."""
    """Sync a single file to Google Drive using Gio.File. Uses SHA256 hashes for change detection."""
    if not is_drive_available():
        info("Google Drive not found. Attempting to mount...")
        mount_google_drive()
        if not is_drive_available():
            warning("Mounting failed. Skipping sync.")
            return

    rel_path = path.relpath(file_path, LOCAL_FOLDER)
    drive_file_path = f"{GOOGLE_DRIVE_PREFIX}{DRIVE_USER}/{path.join(GOOGLE_DRIVE_FOLDER, rel_path).lstrip('/')}"

    # Create Gio file objects for source and destination
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.new_for_path
    src_file = Gio.File.new_for_path(file_path)
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.new_for_uri
    dest_file = Gio.File.new_for_uri(drive_file_path)
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.get_parent
    parent_dir = dest_file.get_parent()
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.query_exists
    if parent_dir and not parent_dir.query_exists():
        try:
            # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.make_directory_with_parents
            parent_dir.make_directory_with_parents()
            info(f"Created parent directory: {parent_dir.get_uri()}")
        except Exception as e:
            warning(f"Failed to create parent directory {parent_dir.get_uri()}: {e}")
            return

    # Check if file exists and compare hashes before deleting and copying
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.query_exists
    if dest_file.query_exists():
        remote_hash = get_remote_file_hash(dest_file)
        local_hash = compute_file_hash(file_path)  # Attempt to fetch an ETag for the local file
        if remote_hash and remote_hash == local_hash:
            info(f"Skipping {file_path}: No changes detected.")
            return
        try:
            # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.delete
            dest_file.delete()
            info(f"Deleted existing file: {drive_file_path}")
            # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/Gio-2.0/interface-File.html#gi.repository.Gio.File.new_for_uri
            dest_file = Gio.File.new_for_uri(drive_file_path)
        except Exception as e:
            error(f"Failed to delete existing file {drive_file_path}: {e}")
            return

    try:
        src_file.copy(dest_file, Gio.FileCopyFlags.OVERWRITE)
    except Exception as e:
        warning(f"Failed to copy {file_path} to {drive_file_path}: {e}")
        return
    info(f"Synced: {file_path} -> {drive_file_path}")


def sync_all_files() -> None:
    """Perform a full sync of all files from the local folder to Google Drive."""
    info("Performing initial sync of all files...")
    if not is_drive_available():
        info("Google Drive not found. Attempting to mount...")
        mount_google_drive()
        if not is_drive_available():
            warning("Mounting failed. Skipping initial sync.")
            return

    for root, _, files in walk(LOCAL_FOLDER):
        for file in files:
            sync_file(path.join(root, file))
    info("Initial sync complete.")


# noinspection PyUnusedLocal
def on_user_logout(proxy, changed_properties, invalidated_properties, observer: Observer) -> None:
    """Handle user logout event using DBus and stop the sync service."""
    # The "Active" property in login1.Session represents whether the session is active.
    # If it changes to False, the user has logged out, so we stop the sync service.
    if "Active" in changed_properties and not changed_properties["Active"]:
        info("User logged out. Stopping sync service...")
        observer.stop()
        observer.join()
        info("Sync service stopped.")
        exit(0)


# noinspection PyUnusedLocal
def on_screen_locked(proxy, changed_properties, invalidated_properties) -> None:
    """Detect screen lock using GNOME's DBus ScreenSaver service and trigger a full sync."""
    # The "Active" property in ScreenSaver represents whether the screen is locked.
    # If it changes to True, the screen has been locked, so we trigger a full sync.
    if "Active" in changed_properties and changed_properties["Active"]:
        info("Screen locked. Performing a full sync...")
        sync_all_files()


class SyncHandler(FileSystemEventHandler):
    """Monitor file system events and trigger sync operations."""

    def on_moved(self, event: FileSystemMovedEvent) -> None:
        """Handle moved files (common with text editors doing atomic saves)."""
        if not event.is_directory:
            sync_file(event.dest_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle file modifications and trigger a sync."""
        if not event.is_directory:
            sync_file(event.src_path)

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle new file creations and trigger a sync."""
        if not event.is_directory:
            sync_file(event.src_path)


def start_syncing():
    """Initialize the sync service, perform an initial sync, set up DBus listeners, and monitor file changes."""
    sync_all_files()  # Perform a full sync before watching for changes

    observer = Observer()
    event_handler = SyncHandler()
    observer.schedule(event_handler, LOCAL_FOLDER, recursive=True)
    observer.start()
    info("Sync service started.")

    try:
        login_proxy = Gio.DBusProxy.new_for_bus_sync(
            Gio.BusType.SYSTEM,
            Gio.DBusProxyFlags.NONE,
            None,
            "org.freedesktop.login1",
            "/org/freedesktop/login1/session/self",
            "org.freedesktop.login1.Session",
            None
        )
        login_proxy.connect("g-properties-changed", lambda p, c, i: on_user_logout(p, c, i, observer))

        screen_proxy = Gio.DBusProxy.new_for_bus_sync(
            Gio.BusType.SESSION,
            Gio.DBusProxyFlags.NONE,
            None,
            "org.freedesktop.ScreenSaver",
            "/org/freedesktop/ScreenSaver",
            "org.freedesktop.ScreenSaver",
            None
        )
        screen_proxy.connect("g-properties-changed", on_screen_locked)
    except Exception as e:
        error(f"Failed to set up system event detection: {e}")

    def stop_app(signum: Signals | int, frame: FrameType | None = None):
        observer.stop()
        observer.join()
        info(f"Sync service stopped: {Signals(signum).name}")
        exit(0)

    signal(SIGTERM, stop_app)

    try:
        observer.join()
    except KeyboardInterrupt:
        info(f"Sync manually stopped")
        stop_app(SIGTERM)


if __name__ == "__main__":
    start_syncing()
