from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import time
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from new_poly.dashboard.logs import delete_log_runs, list_log_runs
from new_poly.dashboard.paths import DashboardPaths, resolve_dashboard_paths
from new_poly.dashboard.process_control import DashboardProcessController

STATIC_DIR = Path(__file__).resolve().parent / "static"


class DashboardHandler(SimpleHTTPRequestHandler):
    server_version = "NewPolyDashboard/0.1"

    def __init__(
        self,
        *args: Any,
        paths: DashboardPaths,
        controller: DashboardProcessController,
        user: str,
        password: str,
        session_secret: str,
        **kwargs: Any,
    ) -> None:
        self.paths = paths
        self.controller = controller
        self.auth_user = user
        self.auth_password = password
        self.session_secret = session_secret
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/login", "/login.html"}:
            self.path = "/login.html"
            return super().do_GET()
        if not self._authorized():
            if parsed.path.startswith("/api/"):
                return self._send_json({"ok": False, "error": "login required"}, status=HTTPStatus.UNAUTHORIZED)
            return self._redirect("/login")
        if parsed.path == "/api/status":
            try:
                mode = _mode_from_query(parsed.query)
                log_stem = _stem_from_query(parsed.query)
                return self._send_json(self.controller.status(mode, log_stem=log_stem))
            except ValueError as exc:
                return self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        if parsed.path == "/api/logs":
            try:
                mode = _mode_filter_from_query(parsed.query)
                return self._send_json(list_log_runs(self.paths.log_dir, mode=mode, running_stems=self._running_log_stems()))
            except ValueError as exc:
                return self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_HEAD(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/login", "/login.html"}:
            self.path = "/login.html"
            return super().do_HEAD()
        if not self._authorized():
            if parsed.path.startswith("/api/"):
                self.send_response(HTTPStatus.UNAUTHORIZED)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            return self._redirect("/login")
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_HEAD()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/login":
            return self._login()
        if parsed.path == "/api/logout":
            return self._logout()
        if not self._authorized():
            return self._send_json({"ok": False, "error": "login required"}, status=HTTPStatus.UNAUTHORIZED)
        try:
            payload = self._read_json()
            if parsed.path == "/api/logs/delete":
                return self._send_json(
                    delete_log_runs(
                        self.paths.log_dir,
                        payload.get("stems"),
                        running_stems=self._running_log_stems(),
                    )
                )
            mode = _validate_mode(str(payload.get("mode") or ""))
            if parsed.path == "/api/stop":
                return self._send_json(self.controller.stop(mode))
            if parsed.path == "/api/restart":
                return self._send_json(self.controller.restart(mode, payload.get("windows")))
        except ValueError as exc:
            return self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        self.send_error(HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _authorized(self) -> bool:
        token = _cookie_value(self.headers.get("Cookie", ""), "npdash")
        if not token:
            return False
        user, expires_raw, signature = _split_token(token)
        if user != self.auth_user:
            return False
        try:
            expires = int(expires_raw)
        except ValueError:
            return False
        if expires < int(time.time()):
            return False
        expected = _sign_session(user, expires, self.session_secret)
        return hmac.compare_digest(signature, expected)

    def _login(self) -> None:
        try:
            payload = self._read_json()
        except (ValueError, json.JSONDecodeError) as exc:
            return self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        user = str(payload.get("user") or "")
        password = str(payload.get("password") or "")
        if not (hmac.compare_digest(user, self.auth_user) and hmac.compare_digest(password, self.auth_password)):
            return self._send_json({"ok": False, "error": "invalid username or password"}, status=HTTPStatus.UNAUTHORIZED)
        expires = int(time.time()) + 12 * 60 * 60
        token = f"{user}:{expires}:{_sign_session(user, expires, self.session_secret)}"
        self._send_json({"ok": True}, headers={"Set-Cookie": _session_cookie(token, max_age=12 * 60 * 60)})

    def _logout(self) -> None:
        self._send_json({"ok": True}, headers={"Set-Cookie": _session_cookie("", max_age=0)})

    def _running_log_stems(self) -> set[str]:
        stems: set[str] = set()
        for mode in ("live", "paper"):
            try:
                status = self.controller.status(mode)
            except Exception:
                continue
            if status.get("run_status") != "running":
                continue
            path = status.get("log_path")
            if isinstance(path, str) and path.endswith(".jsonl"):
                stems.add(Path(path).name[: -len(".jsonl")])
        return stems

    def _redirect(self, location: str) -> None:
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("JSON body must be an object")
        return data

    def _send_json(
        self,
        payload: dict[str, Any],
        *,
        status: HTTPStatus = HTTPStatus.OK,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def create_server(
    host: str,
    port: int,
    *,
    paths: DashboardPaths,
    controller: DashboardProcessController,
    user: str,
    password: str,
    session_secret: str | None = None,
) -> ThreadingHTTPServer:
    if not user or not password:
        raise ValueError("dashboard user and password are required")
    if not session_secret:
        session_secret = hashlib.sha256(f"{user}:{password}".encode("utf-8")).hexdigest()
    handler = partial(
        DashboardHandler,
        paths=paths,
        controller=controller,
        user=user,
        password=password,
        session_secret=session_secret,
    )
    return ThreadingHTTPServer((host, port), handler)


def _mode_from_query(query: str) -> str:
    values = parse_qs(query).get("mode", [])
    return _validate_mode(values[0] if values else "")


def _stem_from_query(query: str) -> str | None:
    values = parse_qs(query).get("stem", [])
    if not values:
        return None
    value = values[0].strip()
    return value or None


def _mode_filter_from_query(query: str) -> str:
    values = parse_qs(query).get("mode", [])
    mode = values[0] if values else "all"
    if mode not in {"all", "live", "paper"}:
        raise ValueError("mode must be all, live, or paper")
    return mode


def _validate_mode(mode: str) -> str:
    if mode not in {"live", "paper"}:
        raise ValueError("mode must be live or paper")
    return mode


def _cookie_value(header: str, name: str) -> str | None:
    prefix = f"{name}="
    for item in header.split(";"):
        value = item.strip()
        if value.startswith(prefix):
            return value[len(prefix):]
    return None


def _split_token(token: str) -> tuple[str, str, str]:
    parts = token.split(":", 2)
    if len(parts) != 3:
        return "", "", ""
    return parts[0], parts[1], parts[2]


def _sign_session(user: str, expires: int, secret: str) -> str:
    message = f"{user}:{expires}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def _session_cookie(token: str, *, max_age: int) -> str:
    return f"npdash={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the New Poly dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args(argv)

    paths = resolve_dashboard_paths()
    user = os.environ.get("DASHBOARD_USER", "")
    password = os.environ.get("DASHBOARD_PASSWORD", "")
    session_secret = os.environ.get("DASHBOARD_SESSION_SECRET")
    server = create_server(
        args.host,
        args.port,
        paths=paths,
        controller=DashboardProcessController(paths),
        user=user,
        password=password,
        session_secret=session_secret,
    )
    print(f"dashboard listening on http://{args.host}:{server.server_address[1]} env={paths.env}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
