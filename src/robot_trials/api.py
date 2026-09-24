"""无第三方依赖的 HTTP JSON 接口。"""

from __future__ import annotations

import argparse
import contextlib
import json
import threading
import traceback
from collections.abc import Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from .errors import ServiceError, ValidationFailed
from .service import TrialService
from .storage import connect, initialize


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: Mapping[str, Any]


class DatabaseGateway:
    """拥有 SQLite 文件，并为每个请求在使用它的线程内开启独立连接。

    启动线程只负责建立一次数据库模式，随后立即关闭自己的连接，
    绝不把连接借给请求线程；请求结束或异常时连接都会被可靠关闭。
    """

    def __init__(self, database: str | Path, *, clock=None) -> None:
        self._database = str(database)
        if self._database == ":memory:":
            raise ValueError("按请求分配连接需要文件数据库，不支持 :memory:")
        self._clock = clock
        self._lock = threading.Lock()
        self._open_connections = 0
        self._leased_total = 0
        self._closed = False
        connection = connect(self._database)
        try:
            initialize(connection)
        finally:
            connection.close()

    @property
    def open_connections(self) -> int:
        """当前尚未关闭的租约连接数，用于检测泄漏。"""

        with self._lock:
            return self._open_connections

    @property
    def leased_total(self) -> int:
        """累计租约次数；纯健康检查不会使其增加。"""

        with self._lock:
            return self._leased_total

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    @contextlib.contextmanager
    def lease(self) -> Iterator[TrialService]:
        """在当前线程开启独立连接并构造领域服务，退出时必定关闭连接。"""

        with self._lock:
            if self._closed:
                raise RuntimeError("数据库网关已关闭")
        connection = connect(self._database)
        with self._lock:
            self._open_connections += 1
            self._leased_total += 1
        try:
            yield TrialService(connection, self._clock, initialize_schema=False)
        finally:
            connection.close()
            with self._lock:
                self._open_connections -= 1

    def close(self) -> None:
        """拒绝后续租约；已存在的租约仍由各自的请求线程负责关闭。"""

        with self._lock:
            self._closed = True


class JsonApplication:
    """将 HTTP 路由映射到领域服务，便于无网络单元测试。

    构造参数可以是固定的 `TrialService`（单线程测试场景，连接所有权归调用方），
    也可以是 `DatabaseGateway`（每个请求在使用线程内租约独立连接）。
    """

    def __init__(self, service: TrialService | DatabaseGateway) -> None:
        self._source = service

    @contextlib.contextmanager
    def _lease(self) -> Iterator[TrialService]:
        if isinstance(self._source, DatabaseGateway):
            with self._source.lease() as service:
                yield service
        else:
            yield self._source

    @staticmethod
    def _actor(headers: Mapping[str, str]) -> str:
        actor = headers.get("x-actor-id", "").strip()
        if not actor:
            raise ValidationFailed("缺少 X-Actor-Id")
        return actor

    @staticmethod
    def _json(body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationFailed("请求体必须是 UTF-8 JSON 对象") from exc
        if not isinstance(value, dict):
            raise ValidationFailed("请求体必须是 JSON 对象")
        return value

    def handle(
        self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b""
    ) -> Response:
        normalized_headers = {key.lower(): value for key, value in (headers or {}).items()}
        path = urlparse(target).path.rstrip("/") or "/"
        parts = [part for part in path.split("/") if part]
        try:
            if method == "GET" and path == "/health":
                # 纯健康检查不租约连接，避免制造无意义的数据库写入。
                return Response(200, {"status": "ok"})
            payload = self._json(body) if method in {"POST", "PUT", "PATCH"} else {}
            with self._lease() as service:
                return self._route(service, method, path, parts, normalized_headers, payload)
        except ServiceError as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})

    def _route(
        self,
        service: TrialService,
        method: str,
        path: str,
        parts: list[str],
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
    ) -> Response:
        if method == "POST" and path == "/users":
            result = service.create_user(payload["user_id"], payload["display_name"], payload["role"])
            return Response(201, result)
        if method == "POST" and path == "/robots":
            result = service.register_robot(
                self._actor(headers), payload["robot_id"], payload["model_name"], payload["vendor"]
            )
            return Response(201, result)
        if method == "POST" and path == "/builds":
            result = service.register_build(
                self._actor(headers), payload["build_id"], payload["robot_id"],
                payload["version"], payload["content_sha256"],
            )
            return Response(201, result)
        if method == "POST" and path == "/protocols":
            return Response(201, service.publish_protocol(self._actor(headers), payload))
        if method == "POST" and path == "/batches":
            result = service.create_batch(
                self._actor(headers), payload["batch_id"], payload["protocol_id"],
                int(payload["protocol_version"]), payload["build_id"],
            )
            return Response(201, result)
        if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "start":
            result = service.start_batch(
                self._actor(headers), parts[1], int(payload["expected_revision"])
            )
            return Response(200, result)
        if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "observations":
            key = headers.get("idempotency-key", "").strip()
            if not key:
                raise ValidationFailed("缺少 Idempotency-Key")
            result = service.import_observations(
                self._actor(headers), parts[1], key, payload.get("observations", [])
            )
            return Response(200, result)
        if method == "POST" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "seal":
            result = service.seal_batch(
                self._actor(headers), parts[1], int(payload["expected_revision"])
            )
            return Response(200, result)
        if method == "GET" and len(parts) == 3 and parts[0] == "batches" and parts[2] == "report":
            return Response(200, service.report(self._actor(headers), parts[1]))
        if method == "POST" and path == "/exclusions":
            result = service.request_exclusion(
                self._actor(headers), int(payload["observation_id"]), payload["reason"]
            )
            return Response(201, result)
        if method == "POST" and len(parts) == 3 and parts[0] == "exclusions" and parts[2] == "review":
            result = service.review_exclusion(
                self._actor(headers), int(parts[1]), bool(payload["approve"]), payload.get("note", "")
            )
            return Response(200, result)
        if method == "POST" and len(parts) == 3 and parts[0] == "exclusions" and parts[2] == "revoke":
            result = service.revoke_exclusion(
                self._actor(headers), int(parts[1]), payload["reason"]
            )
            return Response(200, result)
        if method == "POST" and path == "/jobs/claim":
            result = service.claim_job(payload["worker_id"], int(payload.get("lease_seconds", 60)))
            return Response(200, {"job": result})
        if method == "POST" and len(parts) == 3 and parts[0] == "jobs" and parts[2] == "complete":
            result = service.complete_job(
                payload["worker_id"], int(parts[1]), self._actor(headers)
            )
            return Response(200, result)
        if method == "POST" and len(parts) == 3 and parts[0] == "jobs" and parts[2] == "fail":
            result = service.fail_job(
                payload["worker_id"], int(parts[1]), payload["error"], int(payload.get("retry_seconds", 0))
            )
            return Response(200, result)
        if method == "POST" and path == "/decisions":
            result = service.decide(
                self._actor(headers), payload["batch_id"], int(payload["analysis_id"]),
                payload["decision"], payload["reason"],
            )
            return Response(201, result)
        return Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})


def make_handler(application: JsonApplication):
    class Handler(BaseHTTPRequestHandler):
        server_version = "RobotTrials/1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def _dispatch(self) -> None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length) if length else b""
                response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            except Exception:  # 兜底：未预期错误返回 500，而不是直接断开客户端连接
                traceback.print_exc()
                response = Response(500, {"error": {"code": "internal_error", "message": "服务内部错误"}})
            encoded = json.dumps(response.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def make_server(gateway: DatabaseGateway, host: str, port: int) -> ThreadingHTTPServer:
    """按生产接线方式创建 HTTP 服务：每个请求线程租约独立连接。"""

    return ThreadingHTTPServer((host, port), make_handler(JsonApplication(gateway)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动人形机器人试验统计准入 HTTP 服务")
    parser.add_argument("--database", type=Path, default=Path("robot_trials.sqlite3"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    gateway = DatabaseGateway(args.database)
    server = make_server(gateway, args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        gateway.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
