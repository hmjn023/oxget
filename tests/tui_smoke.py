"""Exercise the real terminal UI with only Python's standard library (Unix).

Run after `cargo build`: python3 tests/tui_smoke.py
"""

import fcntl
import http.server
import json
import os
from pathlib import Path
import pty
import select
import struct
import subprocess
import tempfile
import termios
import threading
import time


DATA = bytes(range(251)) * 8192
ROOT = Path(__file__).resolve().parents[1]


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        start, end = 0, len(DATA) - 1
        partial = "Range" in self.headers
        if partial:
            start, end = map(int, self.headers["Range"][6:].split("-"))
        self.send_response(206 if partial else 200)
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("ETag", '"smoke-v1"')
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(DATA)}")
        self.end_headers()
        try:
            for offset in range(start, end + 1, 4096):
                self.wfile.write(DATA[offset : min(offset + 4096, end + 1)])
                self.wfile.flush()
                time.sleep(0.005)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_args):
        pass


class Terminal:
    def __init__(self, directory, *args):
        self.fd, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 26, 100, 0, 0))
        self.output = bytearray()
        self.process = subprocess.Popen(
            [str(ROOT / "target/debug/oxget"), "-d", str(directory), "-r", "0", *args],
            stdin=slave, stdout=slave, stderr=slave,
            env={**os.environ, "TERM": "xterm-256color"},
        )
        os.close(slave)

    def send(self, text):
        os.write(self.fd, text.encode())

    def drain(self):
        while select.select([self.fd], [], [], 0)[0]:
            try:
                chunk = os.read(self.fd, 65536)
            except OSError:
                break
            if not chunk:
                break
            self.output.extend(chunk)

    def wait(self, predicate, description):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            self.drain()
            if predicate():
                return
            if self.process.poll() is not None:
                raise AssertionError(f"Exited waiting for {description}: {self.output[-3000:]!r}")
            time.sleep(0.01)
        raise AssertionError(f"Timeout waiting for {description}: {self.output[-3000:]!r}")

    def close(self):
        if self.process.poll() is None:
            self.send("q")
            self.wait(lambda: self.process.poll() is not None, "clean exit")
        self.drain()
        os.close(self.fd)
        assert self.process.returncode == 0, self.output[-3000:]


def main():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    terminal = None
    try:
        with tempfile.TemporaryDirectory(prefix="oxget-tui-") as directory:
            output = Path(directory)
            base = f"http://127.0.0.1:{server.server_port}"
            state = output / ".oxget/queue.json"

            def queue():
                return json.loads(state.read_text())

            def cli(*args):
                result = subprocess.run(
                    [str(ROOT / "target/debug/oxget"), "-d", str(output), "--json", *args],
                    capture_output=True, text=True, timeout=15,
                )
                assert result.returncode == 0, (result.stdout, result.stderr)
                return json.loads(result.stdout)

            terminal = Terminal(output, "--paused", base + "/alpha.bin", base + "/beta.bin")
            terminal.wait(lambda: state.exists(), "saved initial queue")
            terminal.wait(lambda: b"oxget" in terminal.output and b"alpha.bin" in terminal.output, "rendered queue")
            terminal.send(" ")
            terminal.wait(lambda: queue()["jobs"][0]["status"] == "Paused", "Space pause")
            terminal.send("J")
            terminal.wait(lambda: queue()["jobs"][0]["file_name"] == "beta.bin", "reordered queue")
            urls = output / "more urls.txt"
            urls.write_text(base + "/gamma.bin\n")
            terminal.send("a")
            time.sleep(0.05)
            terminal.send(str(urls) + "\r")
            terminal.wait(lambda: len(queue()["jobs"]) == 3, "runtime list addition")
            # An independent AI/CLI client can operate the queue while the TUI owns it.
            assert cli("status")["data"]["running"] is True
            assert cli("add", base + "/delta.bin", "--paused")["data"]["ids"] == [4]
            assert cli("pause", "2")["data"]["jobs"][0]["state"] == "paused"
            assert cli("resume", "2")["data"]["jobs"][0]["state"] == "queued"
            assert cli("move", "4", "--last")["data"]["jobs"][0]["position"] == 4
            terminal.send("p")
            terminal.wait(lambda: queue()["jobs"][0]["status"] == "Downloading", "global resume")
            terminal.wait(lambda: any(p.stat().st_size > 0 for p in (output / ".oxget/parts/2").glob("*.part")), "received partial bytes")
            terminal.send("k ")
            terminal.wait(lambda: queue()["jobs"][0]["status"] == "Paused", "active transfer pause")
            terminal.close()
            assert queue()["jobs"][0]["status"] == "Paused"
            assert queue()["jobs"][1]["status"] == "Paused"
            assert not (output / "beta.bin").exists()

            terminal = Terminal(output)
            terminal.wait(lambda: b"oxget" in terminal.output and b"beta.bin" in terminal.output, "restored queue")
            terminal.send(" ")
            terminal.wait(lambda: queue()["jobs"][0]["status"] == "Done", "resume to completion")
            assert (output / "beta.bin").read_bytes() == DATA
            assert cli("stop")["data"]["running"] is False
            terminal.close()
            terminal = None
        print("TUI smoke passed: keys, CLI/IPC control, background additions, stop, restore, exact bytes")
    finally:
        if terminal is not None and terminal.process.poll() is None:
            terminal.process.terminate()
            terminal.process.wait(timeout=5)
            os.close(terminal.fd)
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
