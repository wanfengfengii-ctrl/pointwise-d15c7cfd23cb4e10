"""User-session service: LaunchAgent on macOS, resident supervisor elsewhere.

A single supervisor process holds ``ROOT/service.lock`` and keeps one worker
child alive.  Killing the worker (even with SIGKILL) makes the supervisor start
a fresh one, which mirrors LaunchAgent ``KeepAlive`` semantics for recovery
tests on hosts without ``launchctl``.
"""
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

BOOTSTRAP = "import sys; sys.path.insert(0, sys.argv[1]); import agent; agent.supervise(sys.argv[2])"


def worker_argv(root):
    return [sys.executable, str(Path(root) / "app.pyz"), "worker", str(root)]


def _alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except (OSError, ValueError):
        return False
    return True


def _read_pid(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def _lock_held(path):
    """True when another process currently owns the service lock."""
    try:
        handle = Path(path).open("a+b")
    except OSError:
        return False
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return True
    else:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()
        return False


def supervise(root):
    root = Path(root)
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    lock = (root / "service.lock").open("a+b")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return 0
    (root / "service.pid").write_text(str(os.getpid()))
    stopping = {"flag": False}

    def terminate(signum, frame):
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    child = None
    while not stopping["flag"]:
        try:
            with (control / "service.stdout").open("ab") as out, \
                    (control / "service.stderr").open("ab") as err:
                child = subprocess.Popen(worker_argv(root), cwd=str(root), stdin=subprocess.DEVNULL,
                                         stdout=out, stderr=err)
                (root / "worker.pid").write_text(str(child.pid))
                while child.poll() is None and not stopping["flag"]:
                    time.sleep(0.1)
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
        finally:
            (root / "worker.pid").unlink(missing_ok=True)
        if not stopping["flag"]:
            time.sleep(0.5)
    (root / "service.pid").unlink(missing_ok=True)
    return 0


def spawn(root):
    root = Path(root)
    control = Path(json.loads((root / "config.json").read_bytes())["control"])
    control.mkdir(parents=True, exist_ok=True)
    with (control / "service.stdout").open("ab") as out, (control / "service.stderr").open("ab") as err:
        process = subprocess.Popen([sys.executable, "-c", BOOTSTRAP, str(root / "app.pyz"), str(root)],
                                   cwd=str(root), stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                   start_new_session=True)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _alive(_read_pid(root / "worker.pid")):
            return process.pid
        if process.poll() is not None:
            raise RuntimeError("SERVICE_EXITED")
        time.sleep(0.05)
    raise RuntimeError("SERVICE_TIMEOUT")


def halt(root, timeout=5):
    root = Path(root)
    pid = _read_pid(root / "service.pid")
    if _alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and _alive(pid):
            time.sleep(0.05)
        if _alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
    worker = _read_pid(root / "worker.pid")
    if _alive(worker):
        try:
            os.kill(worker, signal.SIGTERM)
        except OSError:
            pass
    for name in ("service.pid", "worker.pid"):
        (root / name).unlink(missing_ok=True)


def pid(root):
    return _read_pid(Path(root) / "worker.pid")


def service_pid(root):
    return _read_pid(Path(root) / "service.pid")


def running(root):
    root = Path(root)
    return _alive(_read_pid(root / "worker.pid")) or _lock_held(root / "service.lock")
