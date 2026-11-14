"""Installed-artifact verification, demo and recovery evidence.

Nothing in here is imported by the shipped runtime.  Every collection and
recovery scenario drives ``ROOT/app.pyz`` produced by ``ops.py build`` and
installed through ``ops.py install``; the extracted development copy is moved
away before any traffic starts so the evidence cannot be satisfied by host
imports.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile

import ops
import pipeline


def command(argv, **kwargs):
    return subprocess.run([str(x) for x in argv], check=True, capture_output=True,
                          text=True, **kwargs).stdout


def wait_for(predicate, message, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(message)


def service_pid(root):
    if sys.platform == "darwin":
        target = "gui/%d/%s" % (os.getuid(), ops.identity(root))
        result = subprocess.run(["launchctl", "print", target], capture_output=True, text=True)
        match = re.search(r"\bpid = ([0-9]+)", result.stdout)
        return int(match[1]) if match else None
    import agent
    return agent.pid(root)


# ---------------------------------------------------------------------------
# Environment, packaging and installation helpers


def environment():
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["PYTHONNOUSERSITE"] = "1"
    return env


def reproducible(release, directory):
    """Rebuild the package from its own extracted source, compare, move it away."""
    release = Path(release).resolve()
    directory = Path(directory).resolve()
    application = ops.unpack(release)
    development = directory / "development"
    development.mkdir(parents=True)
    with zipfile.ZipFile(io.BytesIO(application)) as package:
        for name in ops.CODE:
            (development / name).write_bytes(package.read(name))
    rebuilt = directory / "rebuilt.zip"
    command([sys.executable, "-B", development / "ops.py", "build", rebuilt], cwd=directory)
    assert rebuilt.read_bytes() == release.read_bytes(), "NONREPRODUCIBLE_PACKAGE"
    development.rename(directory / "development-moved")
    return ops.unpack(rebuilt)


def install(bootstrap, release, root, control, cwd, env, commands):
    argv = [sys.executable, bootstrap, "install", release, root, "--control", control]
    commands.append(" ".join(str(x) for x in argv))
    command(argv, cwd=cwd, env=env)


def call(app, *args, cwd, env, commands):
    argv = [str(sys.executable), str(app), *[str(x) for x in args]]
    commands.append(" ".join(argv))
    return json.loads(command(argv, cwd=cwd, env=env))


class Source:
    """One recorded source process running from the installed package."""

    def __init__(self, app, cwd, env):
        self.process = subprocess.Popen(
            [sys.executable, str(app), "source", "--port", "0"], cwd=cwd, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line = self.process.stdout.readline()
        if not line:
            raise AssertionError("SOURCE_NOT_STARTED")
        self.url = json.loads(line)["url"]

    def _target(self, path):
        return self.url[: self.url.rindex("/")] + path

    def control(self, value):
        request = urllib.request.Request(self._target("/control"), method="POST",
                                         data=json.dumps(value).encode())
        urllib.request.urlopen(request, timeout=5).read()

    def requests(self):
        return json.loads(urllib.request.urlopen(self._target("/requests"), timeout=5).read())

    def stop(self):
        self.process.terminate()
        try:
            self.process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()


# ---------------------------------------------------------------------------
# Scenario helpers


def read_pid(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def clear_hooks(control):
    for name in ("chunk", "verified", "accepted", "accounted"):
        for suffix in (".ready", ".go", ".spent"):
            (control / (name + suffix)).unlink(missing_ok=True)


def wait_phase(control, phase, seconds=30):
    return wait_for(lambda: (control / (phase + ".ready")).exists(), "HOOK_NOT_REACHED:" + phase,
                    seconds)


def wait_new_pid(root, old, seconds=30):
    def fresh():
        pid = service_pid(root)
        return pid if pid and pid != old else None
    return wait_for(fresh, "WORKER_NOT_RESTARTED", seconds)


def wait_state(app, root, state, cwd, env, commands, extra=None, seconds=30):
    def match():
        rows = call(app, "downloads", root, cwd=cwd, env=env, commands=commands)
        if not rows:
            return None
        row = rows[0]
        if row["state"] != state:
            return None
        if extra and not extra(row):
            return None
        return row
    return wait_for(match, "DOWNLOAD_NOT_" + state.upper(), seconds)


def download_state(app, root, cwd, env, commands):
    rows = call(app, "downloads", root, cwd=cwd, env=env, commands=commands)
    return rows[0] if rows else None


def ledger_count(root):
    connection = sqlite3.connect(str(Path(root) / "state.sqlite"))
    try:
        return connection.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
    finally:
        connection.close()


def snapshot(app, root, cwd, env, commands):
    state = download_state(app, root, cwd, env, commands)
    return {"download": state, "ledger_rows": ledger_count(root)}


class Scenario:
    def __init__(self, directory, name, release, bootstrap, app, env):
        self.base = Path(directory) / name
        self.base.mkdir(parents=True)
        self.root = self.base / "root"
        self.control = self.base / "control"
        self.app = self.root / "app.pyz"
        self.env = env
        self.commands = []
        self.name = name
        install(bootstrap, release, self.root, self.control, self.base, env, self.commands)
        self.source = None

    def start_source(self):
        self.source = Source(self.app, self.base, self.env)
        return self.source

    def call(self, *args):
        return call(self.app, *args, cwd=self.base, env=self.env, commands=self.commands)

    def state(self):
        return download_state(self.app, self.root, self.base, self.env, self.commands)

    def snapshot(self):
        return snapshot(self.app, self.root, self.base, self.env, self.commands)

    def wait_completed(self):
        return wait_state(self.app, self.root, "completed", self.base, self.env,
                          self.commands, extra=lambda row: row["received"] == row["length"])

    def hook(self, phase):
        clear_hooks(self.control)
        (self.control / "hook.json").write_text(json.dumps({"phase": phase}))

    def release_hook(self, phase):
        (self.control / (phase + ".go")).write_text("go")

    def finish(self):
        try:
            self.call("stop", self.root)
        except subprocess.CalledProcessError:
            pass
        if self.source is not None:
            self.source.stop()
        clear_hooks(self.control)
        (self.control / "hook.json").unlink(missing_ok=True)


def _evidence(scenario, expected_raw, result=None, extra=None):
    state = scenario.state()
    raw_path = scenario.root / "downloads" / (state["task_id"] + ".raw")
    raw = raw_path.read_bytes() if raw_path.exists() else b""
    business = scenario.call("read", scenario.root, "sample")
    record = {
        "scenario": scenario.name,
        "mechanism": "launchd" if sys.platform == "darwin" else "supervisor",
        "root": str(scenario.root),
        "commands": scenario.commands,
        "requests": scenario.source.requests() if scenario.source else [],
        "final_download": state,
        "raw": {"length": len(raw), "sha256": hashlib.sha256(raw).hexdigest()},
        "raw_matches_expected": raw == expected_raw,
        "business": business,
        "receipt_equals_business": state["receipt"] == business.get("receipt"),
        "ledger_rows": ledger_count(scenario.root),
    }
    assert result is None or business.get("result") == result, "RESULT_MISMATCH"
    assert record["ledger_rows"] == 1, "LEDGER_NOT_UNIQUE"
    assert record["raw_matches_expected"], "RAW_MISMATCH"
    assert state["received"] == state["length"], "INCOMPLETE_DOWNLOAD"
    assert record["receipt_equals_business"], "RECEIPT_MISMATCH"
    if extra:
        record.update(extra)
    return record


# ---------------------------------------------------------------------------
# Recovery scenarios


def scenario_truncation(directory, release, bootstrap, app, env, expected):
    case = Scenario(directory, "truncation", release, bootstrap, app, env)
    try:
        source = case.start_source()
        source.control({"cut_after": 20000})
        case.hook("chunk")
        case.call("enqueue", case.root, "trunc", source.url)
        case.call("start", case.root)
        wait_phase(case.control, "chunk")
        paused = case.snapshot()
        case.release_hook("chunk")
        final = case.wait_completed()
        requests = source.requests()
        resumed = [item for item in requests if item["status"] == 206]
        assert paused["download"]["received"] > 0, "NO_DURABLE_PROGRESS"
        assert paused["download"]["received"] < paused["download"]["length"], "TOO_EARLY"
        # The cut can fall inside the chunk whose confirmation is still in
        # flight, so the durable checkpoint at failure is at least what the
        # pause observed and never beyond the bytes the source actually sent.
        assert resumed, "NO_RESUME_REQUEST"
        offset = int(resumed[0]["range"].split("=")[1].rstrip("-"))
        assert paused["download"]["received"] <= offset <= 20000, "WRONG_RESUME_OFFSET"
        assert resumed[0]["body_bytes"] == final["length"] - offset, "REFETCHED_PREFIX"
        assert any(item["method"] == "GET" and item["status"] == 200
                   and item["body_bytes"] == 20000 for item in requests), "CUT_NOT_OBSERVED"
        return _evidence(case, expected, _expected_result(),
                         {"paused": paused, "resumed_at": offset})
    finally:
        case.finish()


def scenario_kill_resume(directory, release, bootstrap, app, env, expected):
    case = Scenario(directory, "kill-resume", release, bootstrap, app, env)
    try:
        source = case.start_source()
        case.hook("chunk")
        case.call("enqueue", case.root, "kill", source.url)
        case.call("start", case.root)
        wait_phase(case.control, "chunk")
        paused = case.snapshot()
        old = read_pid(case.control / "chunk.ready")
        os.kill(old, signal.SIGKILL)
        new = wait_new_pid(case.root, old)
        case.release_hook("chunk")
        final = case.wait_completed()
        requests = source.requests()
        resumed = [item for item in requests if item["status"] == 206]
        assert new != old, "PID_DID_NOT_CHANGE"
        assert resumed and resumed[0]["range"] == "bytes=%d-" % paused["download"]["received"], \
            "PREFIX_REFETCHED"
        assert resumed[0]["body_bytes"] == final["length"] - paused["download"]["received"], \
            "PREFIX_SIZE_MISMATCH"
        return _evidence(case, expected, _expected_result(), {
            "paused": paused, "pid_before_kill": old, "pid_after_restart": new,
            "resumed_at": paused["download"]["received"], "final_download": final})
    finally:
        case.finish()


def scenario_version_change(directory, release, bootstrap, app, env, expected_v2):
    case = Scenario(directory, "version-change", release, bootstrap, app, env)
    try:
        source = case.start_source()
        case.hook("chunk")
        case.call("enqueue", case.root, "switch", source.url)
        case.call("start", case.root)
        wait_phase(case.control, "chunk")
        paused = case.snapshot()
        old = read_pid(case.control / "chunk.ready")
        os.kill(old, signal.SIGKILL)
        source.control({"version": 2})
        new = wait_new_pid(case.root, old)
        case.release_hook("chunk")
        final = case.wait_completed()
        requests = source.requests()
        assert paused["download"]["etag"] == '"portal-1"', "UNEXPECTED_START_VERSION"
        assert final["etag"] == '"portal-2"', "VERSION_NOT_ADOPTED"
        assert not any(item["status"] == 206 for item in requests), "APPENDED_TO_OLD_PREFIX"
        full_v2 = [item for item in requests
                   if item["method"] == "GET" and item["status"] == 200
                   and item["version"] == 2 and item["body_bytes"] == final["length"]]
        assert full_v2, "NO_FULL_V2_GET"
        evidence = _evidence(case, expected_v2, [{"station": "east", "minute": 0, "count": 2, "sum": 0},
                                                  {"station": "west", "minute": 1, "count": 1, "sum": 110}], {
            "paused": paused, "pid_before_kill": old, "pid_after_restart": new,
            "final_download": final})
        return evidence
    finally:
        case.finish()


def scenario_bad_digest(directory, release, bootstrap, app, env):
    case = Scenario(directory, "bad-digest", release, bootstrap, app, env)
    try:
        source = case.start_source()
        source.control({"bad_digest": True})
        case.call("enqueue", case.root, "bad", source.url)
        case.call("start", case.root)
        final = wait_state(case.app, case.root, "failed", case.base, env, case.commands)
        raw = case.root / "downloads" / "bad.raw"
        business = case.call("read", case.root, "sample")
        assert "DIGEST" in (final["error"] or ""), "WRONG_FAILURE"
        assert not raw.exists(), "CORRUPT_RAW_PUBLISHED"
        assert not business["available"], "CORRUPT_INPUT_ACCEPTED"
        assert ledger_count(case.root) == 0, "CORRUPT_ACCOUNTED"
        return {
            "scenario": case.name,
            "mechanism": "launchd" if sys.platform == "darwin" else "supervisor",
            "root": str(case.root),
            "commands": case.commands,
            "requests": source.requests(),
            "final_download": final,
            "raw_published": False,
            "business": business,
            "ledger_rows": ledger_count(case.root),
        }
    finally:
        case.finish()


HANDOFF = ("verified", "accepted", "accounted")


def scenario_handoff(directory, release, bootstrap, app, env, expected, phase):
    case = Scenario(directory, "handoff-" + phase, release, bootstrap, app, env)
    try:
        source = case.start_source()
        case.hook(phase)
        case.call("enqueue", case.root, "handoff", source.url)
        case.call("start", case.root)
        wait_phase(case.control, phase)
        paused = case.snapshot()
        old = read_pid(case.control / (phase + ".ready"))
        jobs_before = case.call("status", case.root)
        os.kill(old, signal.SIGKILL)
        new = wait_new_pid(case.root, old)
        case.release_hook(phase)
        final = case.wait_completed()
        assert new != old, "PID_DID_NOT_CHANGE"
        assert paused["ledger_rows"] in (0, 1), "LEDGER_AHEAD"
        evidence = _evidence(case, expected, _expected_result(), {
            "phase": phase, "paused": paused, "jobs_before_kill": jobs_before,
            "pid_before_kill": old, "pid_after_restart": new, "final_download": final})
        return evidence
    finally:
        case.finish()


# ---------------------------------------------------------------------------
# Normal installed collection and demo


def installed_regression(app, root, cwd, env):
    """Run the frozen checks.installed() oracle from inside the installed package."""
    script = ("import sys, json; sys.path.insert(0, sys.argv[1]); import checks; "
              "print(json.dumps(checks.installed(sys.argv[2], "
              "json.loads(sys.argv[3]))))")
    fixture = json.dumps(checks_small())
    return json.loads(command([sys.executable, "-c", script, app, root, fixture], cwd=cwd, env=env))


def checks_small():
    import checks
    return checks.small()


def installed_collection(directory, release, bootstrap, app, env, expected):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    root = directory / "service"
    control = directory / "control"
    commands = []
    install(bootstrap, release, root, control, directory, env, commands)
    installed_app = root / "app.pyz"
    source = Source(installed_app, directory, env)
    evidence = {"commands": commands, "source_moved": True}
    try:
        evidence["accepted"] = call(installed_app, "enqueue", root, "normal", source.url,
                                    cwd=directory, env=env, commands=commands)
        evidence["agent"] = call(installed_app, "start", root,
                                 cwd=directory, env=env, commands=commands)
        download = wait_state(installed_app, root, "completed", directory, env, commands,
                              extra=lambda row: row["received"] == row["length"])
        evidence["download"] = download
        evidence["selftest"] = call(installed_app, "selftest",
                                    cwd=directory, env=env, commands=commands)
        business = call(installed_app, "read", root, "sample",
                        cwd=directory, env=env, commands=commands)
        evidence["business"] = business
        raw = (root / "downloads" / "normal.raw").read_bytes()
        assert raw == expected, "RAW_MISMATCH"
        assert download["etag"] == '"portal-1"', "WRONG_VERSION"
        assert download["length"] == download["received"] == 262144, "LENGTH"
        assert download["sha256"] == hashlib.sha256(raw).hexdigest(), "RAW_DIGEST"
        assert business["available"] and business["receipt"] == download["receipt"], "RECEIPT"
        before = len(source.requests())
        again = call(installed_app, "enqueue", root, "normal", source.url,
                     cwd=directory, env=env, commands=commands)
        assert again == evidence["accepted"], "REENQUEUE_CHANGED"
        time.sleep(0.6)
        assert len(source.requests()) == before, "COMPLETED_DOWNLOAD_REFETCHED"
        assert ledger_count(root) == 1, "LEDGER_NOT_UNIQUE"
        evidence["installed_regression"] = installed_regression(installed_app, root, directory, env)
        evidence["requests"] = source.requests()
        evidence["ledger_rows"] = ledger_count(root)
        evidence["raw"] = {"length": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        evidence["re_enqueue"] = again
        return evidence
    finally:
        try:
            call(installed_app, "stop", root, cwd=directory, env=env, commands=commands)
        except subprocess.CalledProcessError:
            pass
        source.stop()


# ---------------------------------------------------------------------------
# Entry points


def build_release(directory):
    directory = Path(directory)
    release = directory / "release.zip"
    command([sys.executable, "-B", Path(__file__).resolve().with_name("ops.py"), "build", release],
            cwd=directory)
    return release


def _expected_versions():
    import portal
    versions = portal.Portal()
    return versions.versions[1], versions.versions[2]


def _expected_result():
    import checks
    return checks.expected(checks.small())


def _prepare(release, directory):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if release is None:
        packaged = Path(__file__).resolve().parent / "artifacts" / "release.zip"
        if packaged.exists():
            release = packaged
        else:
            release = build_release(directory)
    release = Path(release).resolve()
    application = reproducible(release, directory)
    bootstrap = directory / "bootstrap.pyz"
    bootstrap.write_bytes(application)
    return release, application, bootstrap, environment()


def recovery(release, directory):
    release, application, bootstrap, env = _prepare(release, directory)
    expected_v1, expected_v2 = _expected_versions()
    evidence = {"kind": "recovery", "app_sha256": hashlib.sha256(application).hexdigest(),
                "scenarios": {}}
    evidence["scenarios"]["truncation"] = scenario_truncation(
        directory, release, bootstrap, application, env, expected_v1)
    evidence["scenarios"]["kill_resume"] = scenario_kill_resume(
        directory, release, bootstrap, application, env, expected_v1)
    evidence["scenarios"]["version_change"] = scenario_version_change(
        directory, release, bootstrap, application, env, expected_v2)
    evidence["scenarios"]["bad_digest"] = scenario_bad_digest(
        directory, release, bootstrap, application, env)
    for phase in HANDOFF:
        evidence["scenarios"]["handoff_" + phase] = scenario_handoff(
            directory, release, bootstrap, application, env, expected_v1, phase)
    (Path(directory) / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def verify(release=None, directory=None):
    if directory is None:
        directory = Path(tempfile.mkdtemp(prefix="pointwise-verify-"))
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    release, application, bootstrap, env = _prepare(release, directory)
    expected_v1, _ = _expected_versions()
    normal = installed_collection(directory / "normal", release, bootstrap, application, env,
                                  expected_v1)
    recovered = recovery(release, directory / "recovery")
    return {"installed": True, "normal": normal, "recovery": recovered,
            "app_sha256": hashlib.sha256(application).hexdigest()}


def evidence(release=None, directory=None):
    """Collect the full delivery evidence set from installed artifacts."""
    if directory is None:
        directory = Path(tempfile.mkdtemp(prefix="pointwise-evidence-"))
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if release is None:
        packaged = Path(__file__).resolve().parent / "artifacts" / "release.zip"
        release = packaged if packaged.exists() else build_release(directory)
    release = Path(release).resolve()
    digest = hashlib.sha256(release.read_bytes()).hexdigest()
    verified = verify(release, directory / "verify")
    shown = demo(release, directory / "demo")
    live = live_evidence(release, directory / "live")
    payload = {"release": {"path": str(release), "sha256": digest},
               "verify": verified, "demo": shown, "live": live}
    (directory / "evidence.json").write_bytes(pipeline.encode(payload))
    (directory / "RECOVERY.md").write_bytes(
        Path(__file__).resolve().with_name("RECOVERY.md").read_bytes())
    shutil.copyfile(release, directory / "release.zip")
    (directory / "release.sha256").write_text(digest + "  release.zip\n")
    return payload


def live_evidence(release, directory):
    """Install from the delivered package, then drive a real kill/restart run."""
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    root = directory / "service root"
    control = directory / "control dir"
    control.mkdir()
    commands = []
    bootstrap = directory / "bootstrap.pyz"
    bootstrap.write_bytes(ops.unpack(release))
    install(bootstrap, release, root, control, directory, environment(), commands)
    app = root / "app.pyz"
    env = environment()

    def invoke(*args):
        return call(app, *args, cwd=directory, env=env, commands=commands)

    source = Source(app, directory, env)
    old = None
    try:
        (control / "hook.json").write_text(json.dumps({"phase": "chunk"}))
        accepted = invoke("enqueue", root, "live", source.url)
        agent = invoke("start", root)
        wait_phase(control, "chunk")
        paused = invoke("downloads", root)[0]
        old = read_pid(control / "chunk.ready")
        os.kill(old, signal.SIGKILL)
        new = wait_new_pid(root, old)
        (control / "chunk.go").write_text("go")
        final = wait_state(app, root, "completed", directory, env, commands,
                           extra=lambda row: row["received"] == row["length"])
        business = invoke("read", root, "sample")
        raw = (root / "downloads" / "live.raw").read_bytes()
        state = {"commands": commands, "installed_version": invoke("version", root),
                 "accepted": accepted, "agent": agent, "url": source.url,
                 "pid_before_kill": old, "pid_after_restart": new,
                 "progress_at_pause": paused, "final_download": final,
                 "raw": {"length": len(raw), "sha256": hashlib.sha256(raw).hexdigest()},
                 "business": business, "jobs": invoke("status", root)}
        assert new != old, "PID_DID_NOT_CHANGE"
        assert paused["received"] > 0, "NO_DURABLE_PROGRESS"
        assert final["received"] == final["length"] == 262144, "INCOMPLETE"
        assert business["available"] and business["receipt"] == final["receipt"], "RECEIPT"
        assert ledger_count(root) == 1, "LEDGER_NOT_UNIQUE"
        resumed = [item for item in source.requests() if item["status"] == 206]
        assert resumed and int(resumed[0]["range"].split("=")[1].rstrip("-")) == paused["received"], \
            "PREFIX_REFETCHED"
        state["source_requests"] = source.requests()
        return state
    finally:
        try:
            invoke("stop", root)
        except subprocess.CalledProcessError:
            pass
        source.stop()
        clear_hooks(control)
        (control / "hook.json").unlink(missing_ok=True)


def demo(release=None, directory=None):
    if directory is None:
        directory = Path(tempfile.mkdtemp(prefix="pointwise-demo-"))
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    release, application, bootstrap, env = _prepare(release, directory)
    expected_v1, _ = _expected_versions()
    normal = installed_collection(directory / "demo", release, bootstrap, application, env,
                                  expected_v1)
    return {
        "kind": "demo",
        "app_sha256": hashlib.sha256(application).hexdigest(),
        "source": {"url_etag": normal["download"]["etag"],
                   "length": normal["download"]["length"],
                   "sha256": normal["download"]["sha256"]},
        "raw_sha256": normal["raw"]["sha256"],
        "result": normal["business"]["result"],
        "receipt": normal["business"]["receipt"],
        "requests": normal["requests"],
        "ledger_rows": normal["ledger_rows"],
    }


def setup():
    directory = Path(__file__).resolve().parent / "artifacts"
    directory.mkdir(exist_ok=True)
    release = build_release(directory)
    digest = hashlib.sha256(release.read_bytes()).hexdigest()
    (directory / "release.sha256").write_text(digest + "  release.zip\n")
    return {"release": str(release), "sha256": digest}


def normal(release, directory):
    """Original normal-path lab entry (installation and collection only)."""
    release = Path(release).resolve()
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    application = reproducible(release, directory)
    bootstrap = directory / "bootstrap.pyz"
    bootstrap.write_bytes(application)
    env = environment()
    expected_v1, _ = _expected_versions()
    evidence = installed_collection(directory / "service", release, bootstrap, application, env,
                                    expected_v1)
    (directory / "evidence.json").write_bytes(pipeline.encode(evidence))
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation",
                        choices=("normal", "verify", "demo", "recovery", "evidence", "setup"))
    parser.add_argument("release", nargs="?")
    parser.add_argument("directory", nargs="?")
    args = parser.parse_args()
    if args.operation == "normal":
        if not args.release or not args.directory:
            parser.error("normal requires RELEASE DIRECTORY")
        value = normal(args.release, args.directory)
    elif args.operation == "verify":
        value = verify(args.release, args.directory)
    elif args.operation == "demo":
        value = demo(args.release, args.directory)
    elif args.operation == "recovery":
        if not args.directory:
            parser.error("recovery requires DIRECTORY")
        value = recovery(args.release, args.directory)
    elif args.operation == "evidence":
        value = evidence(args.release, args.directory)
    else:
        value = setup()
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
