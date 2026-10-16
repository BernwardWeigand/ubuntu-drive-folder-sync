#!/usr/bin/python3
from json import JSONDecodeError, load
from logging import basicConfig, warning, INFO, error, info
from os import path
from posixpath import join as remote_join
from shutil import which
from signal import SIGINT, SIGTERM
from subprocess import run, CompletedProcess
from threading import Condition, Thread
from time import monotonic
from typing import Tuple

from gi.repository import Gio, GLib  # GNOME APIs for DBus integration and the main loop

try:
    from gi.repository.GLibUnix import signal_add  # GLib >= 2.80
except ImportError:
    from gi.repository.GLib import unix_signal_add as signal_add
from watchdog.events import FileSystemEvent, FileSystemEventHandler, FileSystemMovedEvent
from watchdog.observers import Observer

# Constants
RCLONE = "rclone"
# Seconds to wait after the last change of a file before uploading it (editors fire several events per save)
DEBOUNCE_SECONDS = 2.0
# Fragments of rclone's error output which indicate that the Google login has to be renewed
AUTH_ERROR_MARKERS = ("invalid_grant", "token expired", "cannot fetch token", "couldn't fetch token")

# Load configuration
CONFIG_DIR = path.expanduser("~/.config/drive-sync/")
CONFIG_PATH = path.join(CONFIG_DIR, "config.json")
LOG_FILE = path.join(CONFIG_DIR, "sync.log")

# Set up logging
basicConfig(
    filename=LOG_FILE,
    level=INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def notify(summary: str, body: str) -> None:
    """Show a desktop notification via the freedesktop notification service on the session bus."""
    try:
        # https://specifications.freedesktop.org/notification-spec/latest/protocol.html#command-notify
        Gio.bus_get_sync(Gio.BusType.SESSION, None).call_sync(
            "org.freedesktop.Notifications",
            "/org/freedesktop/Notifications",
            "org.freedesktop.Notifications",
            "Notify",
            GLib.Variant("(susssasa{sv}i)", ("Drive Sync", 0, "dialog-warning", summary, body, [], {}, -1)),
            None,
            Gio.DBusCallFlags.NONE,
            -1,
            None
        )
    except Exception as e:
        error(f"Failed to show notification '{summary}': {e}")


def fail(message: str) -> None:
    """Log and show a fatal error, then stop the service."""
    error(message)
    notify("Drive Sync stopped", message)
    exit(1)


def load_config() -> Tuple[str, str, str]:
    try:
        with open(CONFIG_PATH, "r") as config_file:
            config = load(config_file)
            source_folder: str = config.get("source_folder")
            destination_folder: str = config.get("destination_folder")
            rclone_remote: str = config.get("rclone_remote")
            if not rclone_remote and config.get("drive_user"):
                raise ValueError(
                    "The configuration is still in the format of version 1: "
                    "replace drive_user with rclone_remote (see the README)"
                )
            if not source_folder or not destination_folder or not rclone_remote:
                raise ValueError(
                    "Missing some of the required configuration values: source_folder, destination_folder, and rclone_remote"
                )
            # rclone's root of a Google Drive remote already is "My Drive"
            if destination_folder == "My Drive" or destination_folder.startswith("My Drive/"):
                destination_folder = destination_folder.removeprefix("My Drive")
            destination_folder = destination_folder.strip("/")
            return path.expanduser(source_folder), destination_folder, rclone_remote.rstrip(":")
    except (JSONDecodeError, ValueError) as e:
        fail(f"Error in configuration file: {e}")
    except FileNotFoundError as e:
        fail(f"No configuration file found: {e}")


# Set paths from config
LOCAL_FOLDER, DRIVE_FOLDER, RCLONE_REMOTE = load_config()

# Whether the user was already told to renew the login, to avoid a notification for every file
auth_error_notified = False


def run_rclone(*args: str) -> bool:
    """Run an rclone command, log its outcome and report login problems to the user."""
    global auth_error_notified
    # https://rclone.org/docs/
    result: CompletedProcess = run([RCLONE, *args], capture_output=True, text=True)
    if result.returncode == 0:
        auth_error_notified = False
        return True

    error(f"rclone {' '.join(args)} failed ({result.returncode}): {result.stderr.strip()}")
    if any(marker in result.stderr for marker in AUTH_ERROR_MARKERS) and not auth_error_notified:
        auth_error_notified = True
        notify(
            "Google Drive login expired",
            f"Run 'rclone config reconnect {RCLONE_REMOTE}:' in a terminal to log in again."
        )
    return False


def check_rclone() -> None:
    """Ensure rclone is installed and the configured remote exists."""
    if not which(RCLONE):
        fail("rclone is not installed.")
    # https://rclone.org/commands/rclone_listremotes/
    remotes = run([RCLONE, "listremotes"], capture_output=True, text=True).stdout.split()
    if f"{RCLONE_REMOTE}:" not in remotes:
        fail(f"The rclone remote '{RCLONE_REMOTE}' does not exist. Create it with 'rclone config'.")


def sync_file(file_path: str) -> None:
    """Upload a single file to Google Drive. rclone skips it if the checksums are equal."""
    if not path.isfile(file_path):
        return  # The file was removed or renamed again before it was synced
    rel_path = path.relpath(file_path, LOCAL_FOLDER)
    drive_file_path = f"{RCLONE_REMOTE}:{remote_join(DRIVE_FOLDER, *rel_path.split(path.sep))}"
    # https://rclone.org/commands/rclone_copyto/
    if run_rclone("copyto", "--checksum", file_path, drive_file_path):
        info(f"Synced: {file_path} -> {drive_file_path}")


def sync_all_files() -> None:
    """Perform a full sync of all files from the local folder to Google Drive (without deleting remote files)."""
    info("Performing full sync of all files...")
    # https://rclone.org/commands/rclone_copy/
    if run_rclone("copy", "--checksum", LOCAL_FOLDER, f"{RCLONE_REMOTE}:{DRIVE_FOLDER}"):
        info("Full sync complete.")


class SyncQueue:
    """Collects sync requests and processes them one after another in a background thread.
    File changes are debounced, so a file is uploaded once even if it changes several times in a row."""

    def __init__(self) -> None:
        self._condition = Condition()
        self._pending: dict[str, float] = {}  # file path -> point in time when it is due
        self._full_sync_requested = False
        Thread(target=self._run, daemon=True).start()

    def add_file(self, file_path: str) -> None:
        with self._condition:
            self._pending[file_path] = monotonic() + DEBOUNCE_SECONDS
            self._condition.notify()

    def add_full_sync(self) -> None:
        with self._condition:
            self._full_sync_requested = True
            self._condition.notify()

    def _run(self) -> None:
        while True:
            with self._condition:
                while not self._full_sync_requested and not self._pending:
                    self._condition.wait()
                if self._full_sync_requested:
                    # A full sync covers all pending files as well
                    self._full_sync_requested = False
                    self._pending.clear()
                    due_files = None
                else:
                    now = monotonic()
                    due_files = [file for file, due in self._pending.items() if due <= now]
                    if not due_files:
                        self._condition.wait(min(self._pending.values()) - now)
                        continue
                    for file in due_files:
                        del self._pending[file]

            try:
                if due_files is None:
                    sync_all_files()
                else:
                    for file in due_files:
                        sync_file(file)
            except Exception as e:
                error(f"Sync failed: {e}")


class SyncHandler(FileSystemEventHandler):
    """Monitor file system events and queue sync operations."""

    def __init__(self, queue: SyncQueue) -> None:
        self._queue = queue

    def on_moved(self, event: FileSystemMovedEvent) -> None:
        """Handle moved files (common with text editors doing atomic saves)."""
        if not event.is_directory:
            self._queue.add_file(event.dest_path)

    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle file modifications and queue a sync."""
        if not event.is_directory:
            self._queue.add_file(event.src_path)

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle new file creations and queue a sync."""
        if not event.is_directory:
            self._queue.add_file(event.src_path)


def start_syncing():
    """Initialize the sync service, perform an initial sync, set up DBus listeners, and monitor file changes."""
    check_rclone()

    queue = SyncQueue()
    queue.add_full_sync()  # Perform a full sync before handling single changes

    observer = Observer()
    observer.schedule(SyncHandler(queue), LOCAL_FOLDER, recursive=True)
    observer.start()
    info("Sync service started.")

    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/GLib-2.0/structure-MainLoop.html#gi.repository.GLib.MainLoop
    loop = GLib.MainLoop()  # Required to receive DBus signals

    # noinspection PyUnusedLocal
    def on_screen_saver_signal(proxy, sender_name: str, signal_name: str, parameters: GLib.Variant) -> None:
        """Trigger a full sync when the screen gets locked."""
        if signal_name == "ActiveChanged" and parameters.unpack()[0]:
            info("Screen locked. Performing a full sync...")
            queue.add_full_sync()

    try:
        screen_proxy = Gio.DBusProxy.new_for_bus_sync(
            Gio.BusType.SESSION,
            Gio.DBusProxyFlags.DO_NOT_LOAD_PROPERTIES,
            None,
            "org.gnome.ScreenSaver",
            "/org/gnome/ScreenSaver",
            "org.gnome.ScreenSaver",
            None
        )
        screen_proxy.connect("g-signal", on_screen_saver_signal)
    except Exception as e:
        error(f"Failed to set up screen lock detection: {e}")

    def stop_app(signal_name: str) -> bool:
        info(f"Sync service stopped: {signal_name}")
        loop.quit()
        return GLib.SOURCE_REMOVE

    # The session sends SIGTERM to its processes when the user logs out
    # https://amolenaar.pages.gitlab.gnome.org/pygobject-docs/GLibUnix-2.0/functions.html#gi.repository.GLibUnix.signal_add
    signal_add(GLib.PRIORITY_DEFAULT, SIGTERM, stop_app, SIGTERM.name)
    signal_add(GLib.PRIORITY_DEFAULT, SIGINT, stop_app, SIGINT.name)  # Ctrl+C

    try:
        loop.run()
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    start_syncing()
