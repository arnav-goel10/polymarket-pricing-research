#!/usr/bin/env python3
"""Stop tests for the drill agent (deploy/azure/drill/drill-agent.py).

The agent runs as a real process with --pidfile naming a `sleep` that
stands in for the node. Each test sends SIGTERM at a chosen moment and
checks that the agent exits within a few seconds and that the stand-in is
not left stopped. The first sends it while the agent is answering a
freeze_leader, that is while it holds its lock, which is where a handler
that took the lock itself would hang for ever.

    python3 ops/azure/test_drill_agent.py

Run from ops/azure/test_dry_run.sh. Needs nothing but Python and `ps`.
"""

import http.server
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT = os.path.join(HERE, "..", "..", "deploy", "azure", "drill", "drill-agent.py")
EXIT_WITHIN_S = 8


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def stopped(pid):
    out = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout
    return out.strip().startswith("T")


class SlowHa(http.server.BaseHTTPRequestHandler):
    """/ha that says it leads, a second after the agent asked."""

    on_request = None

    def do_GET(self):
        if SlowHa.on_request:
            SlowHa.on_request()
        time.sleep(1.0)
        body = json.dumps({"leader": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


class AgentStop(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.node = subprocess.Popen(["sleep", "300"])
        pidfile = os.path.join(self.tmp.name, "node.pid")
        with open(pidfile, "w", encoding="utf-8") as f:
            f.write("%d\n" % self.node.pid)
        self.ha = http.server.ThreadingHTTPServer(("127.0.0.1", 0), SlowHa)
        threading.Thread(target=self.ha.serve_forever, daemon=True).start()
        self.port = free_port()
        self.log = os.path.join(self.tmp.name, "faults.jsonl")
        self.agent = subprocess.Popen(
            [sys.executable, "-I", AGENT, "--listen", "127.0.0.1:%d" % self.port,
             "--http", "127.0.0.1:%d" % self.ha.server_address[1],
             "--pidfile", pidfile, "--log", self.log, "--max-seconds", "120"],
        )
        for _ in range(100):
            try:
                if self.ask("ping", timeout=1) == "ok":
                    break
            except OSError:
                time.sleep(0.05)
        else:
            self.fail("the agent did not answer ping")

    def tearDown(self):
        SlowHa.on_request = None
        if self.agent.poll() is None:
            self.agent.kill()
            self.agent.wait()
        os.kill(self.node.pid, signal.SIGCONT)
        self.node.kill()
        self.node.wait()
        self.ha.shutdown()
        self.ha.server_close()
        self.tmp.cleanup()

    def ask(self, line, timeout=5):
        with socket.create_connection(("127.0.0.1", self.port), timeout=timeout) as s:
            s.sendall((line + "\n").encode())
            return s.recv(64).decode().strip()

    def events(self):
        with open(self.log, encoding="utf-8") as f:
            return [json.loads(x) for x in f if x.strip()]

    def assert_exits_and_node_runs(self):
        try:
            code = self.agent.wait(timeout=EXIT_WITHIN_S)
        except subprocess.TimeoutExpired:
            self.fail("the agent did not exit within %d s of SIGTERM" % EXIT_WITHIN_S)
        self.assertEqual(code, 0)
        self.assertFalse(stopped(self.node.pid), "the node was left stopped")
        stops = [e for e in self.events() if e["ev"] == "stop"]
        self.assertEqual([e.get("why") for e in stops], ["signal"])

    def test_sigterm_while_the_lock_is_held(self):
        # freeze_leader asks /ha while it holds the lock; the signal lands
        # in that window, and the freeze still happens after it.
        SlowHa.on_request = lambda: os.kill(self.agent.pid, signal.SIGTERM)
        answer = self.ask("freeze_leader 60000")
        self.assertTrue(answer.startswith("ok "), answer)
        self.assert_exits_and_node_runs()
        evs = [e["ev"] for e in self.events()]
        self.assertIn("freeze_leader", evs)
        self.assertTrue(any(e["ev"] == "thaw" and e.get("on_exit") for e in self.events()))

    def test_sigterm_during_a_freeze(self):
        self.assertTrue(self.ask("freeze 60000").startswith("ok "))
        for _ in range(50):
            if stopped(self.node.pid):
                break
            time.sleep(0.02)
        self.assertTrue(stopped(self.node.pid), "the freeze did not stop the node")
        self.agent.send_signal(signal.SIGTERM)
        self.assert_exits_and_node_runs()

    def test_sigterm_when_idle(self):
        self.agent.send_signal(signal.SIGTERM)
        self.assert_exits_and_node_runs()


if __name__ == "__main__":
    unittest.main(verbosity=1)
