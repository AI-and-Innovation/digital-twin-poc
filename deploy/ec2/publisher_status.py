#!/usr/bin/env python3
"""Local page for the Hyderabad publisher: on or off, and the latest log lines.

Listens on 127.0.0.1 only. On the server it reads the systemd journal. On a
Mac it follows the running process. The proxy password is never shown.

    python3 deploy/ec2/publisher_status.py
    open http://127.0.0.1:8787
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

UNIT = "airport-eye-pax.service"
PROCESS_MARK = "airport-eye pax-stream --rest-proxy"
SECRET = re.compile(r"(PASSWORD|authorization|Basic )[=: ].+", re.I)
BATCH = re.compile(r"batch (\d+): .*delivered (\d+), failed (\d+)")


def run(command: list[str], timeout: float = 5) -> str:
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return completed.stdout


def publisher_process() -> dict | None:
    listing = run(["ps", "-axo", "pid=,etime=,rss=,command="])
    for line in listing.splitlines():
        if PROCESS_MARK not in line or "publisher_status.py" in line:
            continue
        pid, etime, rss, command = line.split(None, 3)
        return {"pid": int(pid), "uptime": etime, "memory_mb": int(rss) // 1024, "command": command.strip()}
    return None


def systemd_active() -> bool | None:
    if not run(["systemctl", "show", UNIT, "--property=LoadState"]):
        return None
    state = run(["systemctl", "is-active", UNIT]).strip()
    if not state:
        return None
    return state == "active"


def log_lines(limit: int, log_file: str | None) -> list[str]:
    if log_file:
        text = Path(log_file).read_text(encoding="utf-8", errors="replace")
    else:
        text = run(["journalctl", "-u", UNIT, "-n", str(limit), "--no-pager", "-o", "cat"], timeout=8)
        if not text:
            text = process_log(limit)
    lines = []
    for line in text.splitlines():
        if SECRET.search(line) or line.startswith("command:") or "KAFKA_REST_PASSWORD" in line:
            continue
        if line.startswith("[Unit]") or "def read(" in line:
            continue
        lines.append(line)
    return lines[-limit:]


def ancestor_pids(pid: int) -> set[str]:
    found = {str(pid)}
    current = str(pid)
    for _ in range(6):
        parent = run(["ps", "-o", "ppid=", "-p", current]).strip()
        if not parent or parent in ("0", "1") or parent in found:
            break
        found.add(parent)
        current = parent
    return found


def output_after_header(path: Path) -> str:
    """Drop the session header. The command block can span many lines."""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    fences = [index for index, line in enumerate(lines) if line.strip() == "---"]
    if len(fences) >= 2:
        lines = lines[fences[1] + 1:]
    useful = [line for line in lines if line.startswith((
        "KEEP ALIVE", "START ", "EXIT ", "STALL ", "WINDOW ", "STOPPED",
        "Kafka REST", "Topics ready", "Feeds:", "Terminal simulation", "[",
    )) or "urlopen error" in line or line.startswith("Traceback")]
    return "\n".join(useful or lines[-80:])


def process_log(limit: int) -> str:
    """Read the publisher session log. stdout is often a pipe, so also match the session file by pid."""
    process = publisher_process()
    if not process:
        return ""
    pids = ancestor_pids(process["pid"])
    transcripts = sorted(Path.home().glob(".cursor/projects/*/terminals/*.txt"),
                         key=lambda path: path.stat().st_mtime, reverse=True)
    for path in transcripts[:40]:
        head = path.read_text(encoding="utf-8", errors="replace")[:2500]
        match = re.search(r"(?m)^pid:\s*(\d+)", head)
        if match and match.group(1) in pids:
            return output_after_header(path)
    return ""


def snapshot(log_file: str | None) -> dict:
    process = publisher_process()
    service = systemd_active()
    running = bool(process) or service is True
    logs = log_lines(80, log_file)
    delivered = failed = last_batch = None
    for line in logs:
        match = BATCH.search(line)
        if match:
            last_batch = line.strip()
            delivered = int(match.group(2))
            failed = int(match.group(3))
    return {
        "running": running,
        "source": "process" if process else ("systemd" if service is not None else "none"),
        "pid": process["pid"] if process else None,
        "uptime": process["uptime"] if process else None,
        "memory_mb": process["memory_mb"] if process else None,
        "delivered": delivered,
        "failed": failed,
        "last_batch": last_batch,
        "logs": logs,
    }


PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hyderabad publisher</title>
<style>
  :root { color-scheme: light; --ink:#18202b; --muted:#5c6b7a; --line:#d9e0e7; --bg:#f4f7f8; --card:#fff; }
  body { margin:0; font:15px/1.45 ui-sans-serif, system-ui, sans-serif; color:var(--ink); background:var(--bg); }
  main { max-width:920px; margin:0 auto; padding:28px 20px 48px; }
  h1 { font-size:22px; margin:0 0 4px; }
  .sub { color:var(--muted); margin:0 0 20px; }
  .row { display:grid; grid-template-columns:180px 1fr 1fr 1fr; gap:12px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
  .label { color:var(--muted); font-size:12px; letter-spacing:.04em; text-transform:uppercase; }
  .value { font-size:28px; font-weight:650; margin-top:4px; }
  .on { color:#0d7a45; } .off { color:#b42318; }
  pre { background:#12181f; color:#e7eef5; border-radius:10px; padding:14px 16px; overflow:auto;
        max-height:520px; font:12px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; white-space:pre-wrap; }
  @media (max-width:720px) { .row { grid-template-columns:1fr 1fr; } }
</style>
</head>
<body>
<main>
  <h1>Hyderabad publisher</h1>
  <p class="sub" id="updated">Checking the local process…</p>
  <section class="row">
    <div class="card"><div class="label">Status</div><div class="value" id="status">—</div></div>
    <div class="card"><div class="label">Uptime</div><div class="value" id="uptime">—</div></div>
    <div class="card"><div class="label">Delivered</div><div class="value" id="delivered">—</div></div>
    <div class="card"><div class="label">Failed</div><div class="value" id="failed">—</div></div>
  </section>
  <p class="sub" id="batch"></p>
  <pre id="logs">Waiting for logs…</pre>
</main>
<script>
async function refresh() {
  const data = await fetch("/api/status").then(r => r.json());
  const status = document.getElementById("status");
  status.textContent = data.running ? "On" : "Off";
  status.className = "value " + (data.running ? "on" : "off");
  document.getElementById("uptime").textContent = data.uptime || "—";
  document.getElementById("delivered").textContent = data.delivered == null ? "—" : data.delivered.toLocaleString();
  document.getElementById("failed").textContent = data.failed == null ? "—" : data.failed.toLocaleString();
  document.getElementById("batch").textContent = data.last_batch || "";
  const logs = document.getElementById("logs");
  const stick = logs.scrollHeight - logs.scrollTop - logs.clientHeight < 40;
  logs.textContent = (data.logs || []).join("\\n") || "No log lines yet.";
  if (stick) logs.scrollTop = logs.scrollHeight;
  document.getElementById("updated").textContent = data.running
    ? "Process " + data.pid + " · " + (data.memory_mb || "?") + " MB · refreshes every 5 seconds"
    : "No publisher process. Refreshes every 5 seconds.";
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    log_file: str | None = None

    def do_GET(self):  # noqa: N802
        if self.path.split("?", 1)[0] == "/api/status":
            body = json.dumps(snapshot(self.log_file)).encode()
            self._send(200, "application/json", body)
            return
        if self.path.split("?", 1)[0] in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", PAGE.encode())
            return
        self._send(404, "text/plain", b"not found")

    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Status page for the Hyderabad Kafka publisher")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--log", help="Log file to show instead of the process output or journal")
    args = parser.parse_args()
    Handler.log_file = args.log
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Publisher status: http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
