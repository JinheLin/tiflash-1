#!/usr/bin/env python3
"""Keep TiUP's child alive while explicitly restarting the real TiFlash process.

SIGUSR1 requests a graceful restart; SIGUSR2 requests a crash restart.
The binary, arguments and working directory remain identical across restarts.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main():
    binary = os.environ["SCHEMA_WATERMARK_TIFLASH_BINARY"]
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = str(Path(binary).parent) + ":" + env.get("LD_LIBRARY_PATH", "")
    arguments = [binary, *sys.argv[1:]]
    if "server" not in sys.argv[1:]:
        os.execve(binary, arguments, env)
    state = Path(os.environ["SCHEMA_WATERMARK_RESTART_STATE"])
    state.mkdir(parents=True, exist_ok=True)
    (state / "wrapper.pid").write_text(str(os.getpid()))
    stopping = False
    requested_signal = None
    child = None

    def event(kind, **fields):
        with (state / "events.jsonl").open("a") as stream:
            stream.write(json.dumps({"event": kind, "time": time.time(), **fields}) + "\n")

    def handle(signum, _frame):
        nonlocal stopping, requested_signal
        if signum in (signal.SIGINT, signal.SIGTERM):
            stopping = True
            requested_signal = signal.SIGTERM
        else:
            requested_signal = signal.SIGTERM if signum == signal.SIGUSR1 else signal.SIGKILL

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGUSR1, signal.SIGUSR2):
        signal.signal(signum, handle)
    try:
        while not stopping:
            gate_logged = False
            while (state / "restart.gate").exists() and not stopping:
                if not gate_logged:
                    event("restart_gated")
                    gate_logged = True
                time.sleep(0.1)
            if stopping:
                break
            child = subprocess.Popen(arguments, env=env)
            event("start", pid=child.pid, arguments=arguments, cwd=os.getcwd())
            requested_restart = False
            deadline = None
            while child.poll() is None:
                if requested_signal is not None:
                    requested_restart = not stopping
                    event("signal", pid=child.pid, signal=requested_signal, restart=requested_restart)
                    child.send_signal(requested_signal)
                    requested_signal = None
                    deadline = time.monotonic() + 45
                if deadline is not None and time.monotonic() >= deadline:
                    event("shutdown_timeout", pid=child.pid)
                    child.kill()
                    stopping = True
                time.sleep(0.1)
            event("exit", pid=child.pid, returncode=child.returncode)
            if not requested_restart:
                return child.returncode
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=45)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == "__main__":
    sys.exit(main())
