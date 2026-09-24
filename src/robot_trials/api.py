"""无第三方依赖的 HTTP JSON 接口。"""

from __future__ import annotations

import argparse
import contextlib
import json
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

from .errors import ServiceError, ValidationFailed
from .service import TrialService
from .storage import Database


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: Mapping[str, Any]


class JsonApplication:
    """将 HTTP 路由映射到领域服务，便于无网络单元测试。

    每个非健康检查请求都会在处理它的线程上取得独立 SQLite 连接，请求结束
    （含异常）后立即关闭，因此并发请求之间不共享连接，也不会残留事务。
    """

    def __init__(
        self,
        database: Database,
        *,
        service_cls: Callable[..., TrialService] = TrialService,
        clock=None,
    ) -> None:
        self.database = database
        self._service_cls = service_cls
        self._clock = clock

    @contextlib.contextmanager
    def _service_scope(self) -> TrialService:
        # 连接在当前请求线程内创建、使用并关闭，满足 SQLite 的线程亲和性。
        with self.database.connection() as connection:
            yield self._service_cls(connection, self._clock, initialize_schema=False)

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
            # 纯健康检查不打开数据库连接，避免无意义的连接获取与任何写入。
            if method == "GET" and path == "/health":
                return Response(200, {"status": "ok"})
            payload = self._json(body) if method in {"POST", "PUT", "PATCH"} else {}
            with self._service_scope() as service:
                response = self._route(service, method, path, parts, normalized_headers, payload)
            if response is None:
                return Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})
            return response
        except ServiceError as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})
        except Exception as exc:  # noqa: BLE001 - 兜底 500；连接与事务已由上下文管理器释放
            return Response(500, {"error": {"code": "internal_error", "message": str(exc) or "内部错误"}})

    def _route(
        self,
        service: TrialService,
        method: str,
        path: str,
        parts: list[str],
        headers: Mapping[str, str],
        payload: dict[str, Any],
    ) -> Response | None:
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
        return None


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
            except Exception:  # noqa: BLE001 - 请求级兜底，绝不拖垮工作线程
                response = Response(500, {"error": {"code": "internal_error", "message": "内部错误"}})
            encoded = json.dumps(response.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            try:
                self.send_response(response.status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
            except (BrokenPipeError, ConnectionResetError):
                # 客户端已断开；请求连接在 application.handle 退出时即已释放。
                pass

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


class RequestTrackingHTTPServer(ThreadingHTTPServer):
    """记录在途请求线程，关闭时等待其连接作用域退出。"""

    drain_timeout = 10.0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._active_lock = threading.Lock()
        self._active_threads: set[threading.Thread] = set()

    def process_request_thread(self, request: Any, client_address: Any) -> None:  # type: ignore[override]
        thread = threading.current_thread()
        with self._active_lock:
            self._active_threads.add(thread)
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._active_lock:
                self._active_threads.discard(thread)

    def server_close(self) -> None:
        super().server_close()
        deadline = time.monotonic() + self.drain_timeout
        while True:
            with self._active_lock:
                pending = [thread for thread in self._active_threads if thread.is_alive()]
            if not pending:
                break
            if time.monotonic() >= deadline:
                # 兜底：正常关闭不应到达此处；即便到达，连接对象析构也会关闭句柄。
                break
            time.sleep(0.01)


def create_server(host: str, port: int, database: Database, *, clock=None) -> RequestTrackingHTTPServer:
    """创建已绑定的线程化 HTTP 服务；调用方负责 shutdown/server_close。"""

    application = JsonApplication(database, clock=clock)
    server = RequestTrackingHTTPServer((host, port), make_handler(application))
    server.daemon_threads = True
    return server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动人形机器人试验统计准入 HTTP 服务")
    parser.add_argument("--database", type=Path, default=Path("robot_trials.sqlite3"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    database = Database(args.database)
    database.initialize()
    server = create_server(args.host, args.port, database)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # 不能在此线程调用 server.shutdown()：它要求从其它线程停止 serve_forever，
        # 否则会死锁。工作线程均为 daemon，关闭监听套接字后进程可立即退出。
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
