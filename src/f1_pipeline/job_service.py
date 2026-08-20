from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any, cast
from urllib.parse import urlparse

from f1_pipeline.job_runner import (
    JOB_MANIFEST_DIR,
    JobAlreadyRunningError,
    JobRunnerError,
    WeekendJobIntent,
    job_id,
    read_job_status,
    run_weekend_job,
)

MAX_REQUEST_BYTES = 64 * 1024


def _run_job(intent: WeekendJobIntent, job_dir: Path) -> None:
    try:
        run_weekend_job(intent, job_dir=job_dir)
    except (JobAlreadyRunningError, JobRunnerError):
        return


def handler_class(job_dir: Path) -> type[BaseHTTPRequestHandler]:
    class JobHandler(BaseHTTPRequestHandler):
        def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            content = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/health":
                self._json(HTTPStatus.OK, {"status": "available"})
                return
            if not path.startswith("/jobs/"):
                self._json(HTTPStatus.NOT_FOUND, {"error": "Not found."})
                return
            identifier = path.rsplit("/", 1)[-1]
            try:
                status = read_job_status(identifier, job_dir)
            except JobRunnerError as exc:
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
                return
            if status is None:
                self._json(HTTPStatus.NOT_FOUND, {"error": "Job not found."})
                return
            self._json(HTTPStatus.OK, status)

        def do_POST(self) -> None:
            if urlparse(self.path).path != "/jobs":
                self._json(HTTPStatus.NOT_FOUND, {"error": "Not found."})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = 0
            if length <= 0 or length > MAX_REQUEST_BYTES:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "Invalid request size."})
                return
            try:
                payload = json.loads(self.rfile.read(length))
                intent = WeekendJobIntent(**payload).normalized()
                identifier = job_id(intent)
            except (json.JSONDecodeError, TypeError, JobRunnerError) as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            existing = read_job_status(identifier, job_dir)
            if existing and existing.get("status") == "running":
                self._json(HTTPStatus.ACCEPTED, existing)
                return
            Thread(target=_run_job, args=(intent, job_dir), daemon=True).start()
            self._json(
                HTTPStatus.ACCEPTED,
                {"job_id": identifier, "status": "queued", "intent": payload},
            )

        def log_message(self, message_format: str, *args: Any) -> None:
            return

    return JobHandler


def create_server(
        host: str = "127.0.0.1",
        port: int = 8765,
        *,
        job_dir: Path = JOB_MANIFEST_DIR,
) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), cast(Any, handler_class(job_dir)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve controlled Dashboard V1 data jobs.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    server = create_server(args.host, args.port)
    print(f"Job service listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
