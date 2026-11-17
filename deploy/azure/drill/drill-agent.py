#!/usr/bin/env python3
"""Fault agent for the live drill. Runs as root on a node VM, started by
drill-open.sh under systemd-run and stopped by drill-close.sh.

It listens on loopback only (127.0.0.1:9102 on a VM). The drill runner
reaches it through the drill user's SSH forward, which may open nothing
else but the node's stream and health ports. One line in, one line out:

    ping                  -> ok
    kill9                 -> ok <wall ms>      SIGKILL; systemd restarts the node 1 s later
    freeze <ms>           -> ok <wall ms>      SIGSTOP now, SIGCONT after <ms>
    freeze_leader <ms>    -> ok <wall ms> | skip not_leader
                             the same, only if the node's own /ha says it leads

The agent sends SIGCONT itself, so a runner that disappears mid-freeze
never leaves the node stopped, and it wakes any node it froze when it is
stopped. A kill is refused while a freeze is in progress. Every action is
appended to the log as one JSON line. Nothing here reads or prints venue
data, addresses or keys.

The node's main pid comes from systemd (poly-node.service), or from
--pidfile when a supervisor other than systemd runs it (the loopback
rehearsal in CI).
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request

MAX_FREEZE_MS = 120_000


def wall_ms():
    return int(time.time() * 1000)


class Agent:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.frozen = None

    def log(self, ev, **kw):
        line = dict(wall_ms=wall_ms(), ev=ev, **kw)
        with open(self.args.log, "a", encoding="utf-8") as f:
            f.write(json.dumps(line, sort_keys=True) + "\n")

    def main_pid(self):
        if self.args.pidfile:
            with open(self.args.pidfile, encoding="utf-8") as f:
                pid = int(f.read().strip())
        else:
            out = subprocess.run(
                ["systemctl", "show", "-P", "MainPID", self.args.unit],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            pid = int(out or "0")
        if pid <= 0:
            raise RuntimeError("node not running")
        return pid

    def leads(self):
        url = "http://%s/ha" % self.args.http
        with urllib.request.urlopen(url, timeout=2) as r:
            return bool(json.load(r).get("leader"))

    def thaw(self, pid, ms):
        time.sleep(ms / 1000.0)
        with self.lock:
            self.cont(pid)
            self.log("thaw", pid_ok=True)

    def cont(self, pid):
        try:
            os.kill(pid, signal.SIGCONT)
        except ProcessLookupError:
            pass
        if self.frozen == pid:
            self.frozen = None

    def wake_all(self):
        with self.lock:
            if self.frozen is not None:
                self.cont(self.frozen)
                self.log("thaw", on_exit=True)

    def handle(self, line):
        parts = line.split()
        if not parts:
            return "error empty"
        cmd = parts[0]
        if cmd == "ping":
            return "ok"
        with self.lock:
            if cmd == "kill9" and len(parts) == 1:
                if self.frozen is not None:
                    return "skip frozen"
                pid = self.main_pid()
                at = wall_ms()
                os.kill(pid, signal.SIGKILL)
                self.log("kill9", at_ms=at)
                return "ok %d" % at
            if cmd in ("freeze", "freeze_leader") and len(parts) == 2:
                if not parts[1].isdigit():
                    return "error bad duration"
                ms = int(parts[1])
                if not 0 < ms <= MAX_FREEZE_MS:
                    return "error duration out of range"
                if self.frozen is not None:
                    return "skip frozen"
                if cmd == "freeze_leader":
                    try:
                        leads = self.leads()
                    except Exception:  # noqa: BLE001 - any failure is "not known to lead"
                        leads = False
                    if not leads:
                        self.log("skip", why="not_leader")
                        return "skip not_leader"
                pid = self.main_pid()
                at = wall_ms()
                os.kill(pid, signal.SIGSTOP)
                self.frozen = pid
                self.log(cmd, at_ms=at, ms=ms)
                threading.Thread(target=self.thaw, args=(pid, ms), daemon=True).start()
                return "ok %d" % at
        return "error unknown command"


def serve(agent, listen, stop_at):
    host, port = listen.rsplit(":", 1)
    if host not in ("127.0.0.1", "localhost"):
        sys.exit("drill-agent: listens on loopback only")
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", int(port)))
    srv.listen(8)
    srv.settimeout(1.0)
    agent.log("start")
    while stop_at is None or time.time() < stop_at:
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        with conn:
            conn.settimeout(5.0)
            try:
                data = b""
                while b"\n" not in data and len(data) < 256:
                    chunk = conn.recv(256)
                    if not chunk:
                        break
                    data += chunk
                try:
                    answer = agent.handle(data.decode("ascii", "replace").strip())
                except Exception as e:  # noqa: BLE001 - reported to the caller
                    answer = "error %s" % type(e).__name__
                conn.sendall((answer + "\n").encode())
            except OSError:
                pass
    agent.log("stop", why="max_seconds")


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--listen", default="127.0.0.1:9102")
    p.add_argument("--http", default="127.0.0.1:9101")
    p.add_argument("--unit", default="poly-node.service")
    p.add_argument("--pidfile")
    p.add_argument("--log", required=True)
    p.add_argument("--max-seconds", type=int, default=4 * 3600)
    args = p.parse_args()
    agent = Agent(args)

    def stop(_sig, _frame):
        agent.wake_all()
        agent.log("stop", why="signal")
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        serve(agent, args.listen, time.time() + args.max_seconds)
    finally:
        agent.wake_all()


if __name__ == "__main__":
    main()
