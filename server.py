import http.server
import json
import threading
import sys
import webbrowser
import urllib.parse
from datetime import date
from contextlib import redirect_stdout

# ── shared update state ──────────────────────────────────────────────────────
_update_state = {
    "running": False, "log": [], "done": False, "error": None,
    "progress": {"current": 0, "total": 0, "phase": ""},
}
_state_lock = threading.Lock()


class _LiveStream:
    """Writes to _update_state["log"] in real-time and parses [PROGRESS] lines."""
    def write(self, text):
        for line in text.split('\n'):
            line = line.rstrip()
            if not line:
                continue
            with _state_lock:
                _update_state["log"].append(line)
                if line.startswith("[PROGRESS]"):
                    # format: [PROGRESS] done/total phase code
                    try:
                        parts = line[10:].strip().split()
                        nums = parts[0].split('/')
                        _update_state["progress"]["current"] = int(nums[0])
                        _update_state["progress"]["total"] = int(nums[1])
                        _update_state["progress"]["phase"] = parts[1] if len(parts) > 1 else ""
                    except Exception:
                        pass

    def flush(self):
        pass


def _run_update(force_financial=False):
    from db import init_db
    from fetcher import update_stock_list, fetch_incremental_data, fetch_financial_data

    stream = _LiveStream()
    try:
        with redirect_stdout(stream):
            init_db()
            update_stock_list()
            fetch_incremental_data()
            if force_financial:
                fetch_financial_data(date.today().year - 1, force=True)
        with _state_lock:
            _update_state["done"] = True
            _update_state["running"] = False
    except Exception as e:
        with _state_lock:
            _update_state["log"].append(f"[ERROR] 更新中断: {e}")
            _update_state["error"] = str(e)
            _update_state["done"] = True
            _update_state["running"] = False


# ── request handler ──────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # suppress access logs

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path, content_type):
        try:
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_error(404)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self._send_file("index.html", "text/html; charset=utf-8")

        elif path == "/kline.html":
            self._send_file("kline.html", "text/html; charset=utf-8")

        elif path == "/api/screen":
            try:
                from screener import screen
                last_year = date.today().year - 1
                results = screen(last_year)
                # sqlite3.Row → dict
                out = []
                for r in results:
                    out.append({k: r[k] for k in r.keys()} if hasattr(r, "keys") else r)
                self._send_json({"ok": True, "data": out})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)

        elif path == "/api/update/status":
            with _state_lock:
                self._send_json({
                    "running": _update_state["running"],
                    "done": _update_state["done"],
                    "log": _update_state["log"],
                    "error": _update_state["error"],
                    "progress": _update_state["progress"],
                })

        elif path == "/api/kline":
            code = params.get("code", [None])[0]
            if not code:
                self._send_json({"ok": False, "error": "missing code"}, 400)
                return
            try:
                from db import get_daily_prices, get_conn
                rows = get_daily_prices(code)
                conn = get_conn()
                name_row = conn.execute(
                    "SELECT name FROM stock_list WHERE code = ?", (code,)
                ).fetchone()
                conn.close()
                name = name_row["name"] if name_row else code
                data = [
                    {"date": r["date"], "open": r["open"], "close": r["close"],
                     "high": r["high"], "low": r["low"]}
                    for r in rows
                ]
                self._send_json({"ok": True, "code": code, "name": name, "data": data})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)

        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/api/update":
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len).decode("utf-8") if content_len else ""
            params = urllib.parse.parse_qs(body)
            force_financial = params.get("force_financial", ["false"])[0] == "true"

            with _state_lock:
                if _update_state["running"]:
                    self._send_json({"status": "already_running"})
                    return
                _update_state["running"] = True
                _update_state["done"] = False
                _update_state["log"] = []
                _update_state["error"] = None
                _update_state["progress"] = {"current": 0, "total": 0, "phase": ""}
            t = threading.Thread(target=_run_update, args=(force_financial,), daemon=True)
            t.start()
            self._send_json({"status": "started"})
        else:
            self.send_error(404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()


# ── main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    port = 8080
    server = http.server.HTTPServer(("localhost", port), Handler)
    url = f"http://localhost:{port}"
    print(f"A股筛选工具已启动: {url}")
    print("按 Ctrl+C 停止服务")
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止")
