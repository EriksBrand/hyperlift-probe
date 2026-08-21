#!/usr/bin/env python3
"""
Hyperlift trial probe app.

Covers: test 1 (request duration ceiling + SSE buffering),
        test 2 (persistence sentinel),
        test 3 (locale / encoding),
        test 6 (env vars),
        early read on test 4 (header pass-through).

Stdlib only. No requirements.txt, no pip install.
"""

import http.server
import json
import locale
import os
import socketserver
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("PORT", "8080"))

# Candidate paths to probe for persistent storage. The real one is unknown;
# this reports which are writable so test 2 has somewhere to aim.
CANDIDATE_DIRS = [
    "/data",
    "/var/data",
    "/mnt/data",
    "/storage",
    "/persistent",
    "/app/data",
    "/tmp",
    os.getcwd(),
]

SENTINEL_NAME = "hyperlift-sentinel.txt"
BOOT_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def log(msg):
    """Timestamped stderr log — shows up in Hyperlift runtime logs."""
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", file=sys.stderr, flush=True)


class Handler(http.server.BaseHTTPRequestHandler):
    # CRITICAL: stdlib defaults to HTTP/1.0, which has no chunked transfer
    # encoding. Without this, the SSE test measures Python's limitation
    # rather than Hyperlift's proxy.
    protocol_version = "HTTP/1.1"
    server_version = "HyperliftProbe/1.0"
    sys_version = ""

    # ---------- helpers ----------

    def _send_json(self, obj, status=200, extra_headers=None):
        body = json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text, status=200, extra_headers=None):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _chunk(self, data: bytes):
        """Write one HTTP chunked-transfer frame and flush immediately."""
        self.wfile.write(b"%X\r\n" % len(data))
        self.wfile.write(data)
        self.wfile.write(b"\r\n")
        self.wfile.flush()

    def _end_chunks(self):
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def log_message(self, fmt, *args):
        log("req " + (fmt % args))

    # ---------- routes ----------

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        q = parse_qs(parsed.query)

        try:
            if path == "/":
                return self.route_index()
            if path == "/info":
                return self.route_info()
            if path == "/sleep":
                return self.route_sleep(q)
            if path == "/stream":
                return self.route_stream(q)
            if path == "/locale":
                return self.route_locale()
            if path == "/sentinel":
                return self.route_sentinel(q)
            if path == "/env":
                return self.route_env(q)
            if path == "/headers":
                return self.route_headers()
            if path == "/redirect":
                return self.route_redirect()
            if path == "/refuse":
                return self.route_refuse()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as exc:
            # The client (or the proxy) went away mid-response. Nothing can be
            # sent on a dead socket, so log one line and stop — do NOT try to
            # send a 500, which is what produced the noisy double-traceback.
            log(f"peer gone during {path}: {type(exc).__name__}")
            return
        except Exception as exc:  # noqa: BLE001
            log(f"ERROR {path}: {type(exc).__name__}: {exc}")
            return self._send_json(
                {"error": type(exc).__name__, "detail": str(exc)}, status=500
            )

        self._send_json({"error": "not found", "path": path}, status=404)

    def route_index(self):
        self._send_json({
            "app": "hyperlift-probe",
            "version": "1.1",
            "boot_id": BOOT_ID,
            "now": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "endpoints": {
                "/info": "runtime facts: python, platform, cwd, port",
                "/sleep?s=N": "TEST 1 - silent sleep N seconds, then respond",
                "/stream?s=N&hb=N": "TEST 1 - SSE, N seconds, heartbeat every hb",
                "/locale": "TEST 3 - read a file with em-dash and middot",
                "/sentinel": "TEST 2 - read sentinels from candidate paths",
                "/sentinel?write=1": "TEST 2 - write a sentinel to every writable path",
                "/env": "TEST 6 - report PROBE_SENTINEL and env var names",
                "/headers": "TEST 4 - emit ETag / Cache-Control / X-Provenance",
                "/redirect": "TEST 4 - 302, does the proxy preserve it",
                "/refuse": "TEST 4 - 422 with a body",
            },
        })

    def route_info(self):
        self._send_json({
            "boot_id": BOOT_ID,
            "python": sys.version,
            "python_short": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "platform": sys.platform,
            "cwd": os.getcwd(),
            "port_listening": PORT,
            "port_env_var_present": "PORT" in os.environ,
            "port_env_value": os.environ.get("PORT"),
            "pid": os.getpid(),
            "active_threads": threading.active_count(),
            "protocol_version": Handler.protocol_version,
        })

    # --- TEST 1 ---

    def route_sleep(self, q):
        secs = float(q.get("s", ["30"])[0])
        started = time.time()
        log(f"/sleep start s={secs}")
        time.sleep(secs)
        elapsed = time.time() - started
        log(f"/sleep done s={secs} elapsed={elapsed:.1f}")
        self._send_json({
            "test": "1 - silent request duration",
            "requested_seconds": secs,
            "elapsed_seconds": round(elapsed, 2),
            "note": "If this never arrives, the total-request cap is below this value.",
        })

    def route_stream(self, q):
        secs = float(q.get("s", ["60"])[0])
        hb = float(q.get("hb", ["5"])[0])
        log(f"/stream start s={secs} hb={hb}")

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")  # nginx: disable buffering
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        started = time.time()
        n = 0
        # 2KB of padding defeats small proxy buffers that would otherwise
        # hold the first bytes back and look like a hang.
        pad = "x" * 2048
        self._chunk(f": priming {pad}\n\n".encode("utf-8"))

        while True:
            elapsed = time.time() - started
            if elapsed >= secs:
                break
            n += 1
            payload = json.dumps({
                "n": n,
                "elapsed": round(elapsed, 2),
                "wall": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            })
            self._chunk(f"event: tick\ndata: {payload}\n\n".encode("utf-8"))
            time.sleep(hb)

        total = round(time.time() - started, 2)
        self._chunk(
            f"event: done\ndata: {json.dumps({'ticks': n, 'elapsed': total})}\n\n".encode("utf-8")
        )
        self._end_chunks()
        log(f"/stream done ticks={n} elapsed={total}")

    # --- TEST 3 ---

    def route_locale(self):
        data_file = Path(__file__).parent / "probe-data.txt"
        result = {
            "test": "3 - locale / encoding",
            "file": str(data_file),
            "locale_getpreferredencoding": locale.getpreferredencoding(False),
            "sys_getdefaultencoding": sys.getdefaultencoding(),
            "sys_getfilesystemencoding": sys.getfilesystemencoding(),
            "stdout_encoding": getattr(sys.stdout, "encoding", None),
            "env_LANG": os.environ.get("LANG"),
            "env_LC_ALL": os.environ.get("LC_ALL"),
            "env_PYTHONUTF8": os.environ.get("PYTHONUTF8"),
            "env_PYTHONIOENCODING": os.environ.get("PYTHONIOENCODING"),
        }
        try:
            # Deliberately no encoding= argument. This is the exact call
            # that 500s on cPanel under an ASCII locale.
            text = data_file.read_text()
            result["read_ok"] = True
            result["content"] = text
            result["has_em_dash"] = "\u2014" in text
            result["has_middot"] = "\u00b7" in text
        except Exception as exc:  # noqa: BLE001
            result["read_ok"] = False
            result["error"] = f"{type(exc).__name__}: {exc}"
            log(f"/locale FAILED: {type(exc).__name__}: {exc}")
            return self._send_json(result, status=500)

        log("/locale ok")
        self._send_json(result)

    # --- TEST 2 ---

    def route_sentinel(self, q):
        do_write = q.get("write", ["0"])[0] not in ("0", "", "false")
        report = {
            "test": "2 - persistence",
            "boot_id": BOOT_ID,
            "wrote_this_call": do_write,
            "paths": {},
        }

        for d in CANDIDATE_DIRS:
            entry = {}
            p = Path(d)
            entry["exists"] = p.is_dir()

            if do_write and not p.is_dir():
                try:
                    p.mkdir(parents=True, exist_ok=True)
                    entry["created"] = True
                    entry["exists"] = True
                except Exception as exc:  # noqa: BLE001
                    entry["created"] = False
                    entry["mkdir_error"] = f"{type(exc).__name__}"

            if entry["exists"]:
                sfile = p / SENTINEL_NAME
                if do_write:
                    stamp = (
                        f"boot={BOOT_ID} "
                        f"written={datetime.now(timezone.utc).isoformat(timespec='seconds')}\n"
                    )
                    try:
                        with sfile.open("a", encoding="utf-8") as fh:
                            fh.write(stamp)
                        entry["writable"] = True
                    except Exception as exc:  # noqa: BLE001
                        entry["writable"] = False
                        entry["write_error"] = f"{type(exc).__name__}"
                try:
                    if sfile.exists():
                        lines = sfile.read_text(encoding="utf-8").strip().splitlines()
                        entry["sentinel_lines"] = len(lines)
                        entry["first"] = lines[0] if lines else None
                        entry["last"] = lines[-1] if lines else None
                    else:
                        entry["sentinel_lines"] = 0
                except Exception as exc:  # noqa: BLE001
                    entry["read_error"] = f"{type(exc).__name__}"

            report["paths"][d] = entry

        report["how_to_read"] = (
            "Survival is proven when 'first' predates the current boot_id. "
            "Append one line per deploy: restart, then new image, then scale 0->1."
        )
        log(f"/sentinel write={do_write}")
        self._send_json(report)

    # --- TEST 6 ---

    def route_env(self, q):
        sentinel = os.environ.get("PROBE_SENTINEL")
        report = {
            "test": "6 - env vars",
            "PROBE_SENTINEL_present": sentinel is not None,
            "PROBE_SENTINEL_value": sentinel,
            "env_var_count": len(os.environ),
            "env_var_names": sorted(os.environ.keys()),
        }
        if q.get("log", ["0"])[0] not in ("0", "", "false"):
            # Deliberate: prove whether a value printed by the app appears
            # unmasked in the runtime log stream.
            log(f"DELIBERATE LOG LEAK TEST: PROBE_SENTINEL={sentinel}")
            report["logged_to_stderr"] = True
        self._send_json(report)

    # --- TEST 4 (early read) ---

    def route_headers(self):
        self._send_text(
            "header probe body\n",
            extra_headers={
                "ETag": '"probe-etag-v1"',
                "Cache-Control": "public, max-age=31536000, immutable",
                "X-Provenance": "hyperlift-probe/test4",
                "X-Custom-Nonstandard": "should-survive",
                "Vary": "Accept-Encoding",
                "Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT",
            },
        )

    def route_redirect(self):
        self.send_response(302)
        self.send_header("Location", "/headers")
        self.send_header("X-Provenance", "hyperlift-probe/redirect")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def route_refuse(self):
        self._send_json(
            {"error": "rule text refused", "code": 422},
            status=422,
            extra_headers={"X-Provenance": "hyperlift-probe/refusal"},
        )


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    # The Hyperlift proxy RSTs idle keep-alive connections after ~15s rather
    # than closing them gracefully. Stock socketserver prints a full traceback
    # for each one, which buries real errors. Collapse those to a single line.
    QUIET_ERRORS = (
        ConnectionResetError,
        BrokenPipeError,
        ConnectionAbortedError,
        TimeoutError,
    )

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, self.QUIET_ERRORS):
            log(f"conn closed by peer {client_address[0]} ({type(exc).__name__})")
            return
        super().handle_error(request, client_address)


if __name__ == "__main__":
    log(f"probe v1.1 starting boot_id={BOOT_ID} port={PORT} python={sys.version_info[:3]}")
    log(f"preferred encoding={locale.getpreferredencoding(False)} LANG={os.environ.get('LANG')}")
    ThreadedServer(("0.0.0.0", PORT), Handler).serve_forever()
