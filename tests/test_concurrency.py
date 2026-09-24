"""真正启动本地 HTTP 服务的并发回归场景。

这些测试在线程化 HTTP 服务上通过真实 socket 并发执行读写，覆盖：

* SQLite 连接的线程亲和性——每个请求在自己的线程创建/使用/关闭独立连接；
* 跨请求事务隔离——失败事务整体回滚，不污染其它请求；
* 请求结束、异常和服务器关闭后连接计数归零，无资源泄漏；
* 纯健康检查不获取数据库连接；
* 命令行入口仍可启动、优雅关闭并保留数据。
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from robot_trials.api import JsonApplication, RequestTrackingHTTPServer, create_server, make_handler
from robot_trials.service import TrialService
from robot_trials.storage import Database, connect

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

WRITERS = 6
READERS = 4
ITERATIONS = 3
ROWS_PER_IMPORT = 4


class CountingFactory:
    """记录连接的创建线程、存活数量与峰值，并校验关闭线程归属。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.created = 0
        self.active = 0
        self.peak = 0
        self.thread_violations: list[str] = []

    def __call__(self, path):
        owner = threading.get_ident()
        recorder = self

        class TrackedConnection(sqlite3.Connection):
            def close(self):
                if threading.get_ident() != owner:
                    with recorder._lock:
                        recorder.thread_violations.append(
                            f"连接在线程 {owner} 创建，却在线程 {threading.get_ident()} 关闭"
                        )
                with recorder._lock:
                    recorder.active -= 1
                return super().close()

        connection = connect(path, connection_factory=TrackedConnection)
        with self._lock:
            self.created += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
        return connection


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def http_request(port: int, method: str, path: str, body: dict | None = None, headers=None):
    encoded = json.dumps(body).encode("utf-8") if body is not None else None
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(method, path, body=encoded, headers=request_headers)
        response = connection.getresponse()
        data = response.read()
        return response.status, json.loads(data.decode("utf-8"))
    finally:
        connection.close()


def wait_for_health(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, body = http_request(port, "GET", "/health")
            if status == 200 and body["status"] == "ok":
                return
        except OSError:
            pass
        time.sleep(0.02)
    raise RuntimeError("服务未在限定时间内就绪")


class HttpConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self._directory.name) / "concurrent.sqlite3"
        self.factory = CountingFactory()
        self.database = Database(self.db_path, connection_factory=self.factory)
        self.database.initialize()
        self.server = create_server("127.0.0.1", 0, self.database)
        self.port = self.server.server_address[1]
        self.server_thread = threading.Thread(target=self.server.serve_forever, name="http-server", daemon=True)
        self.server_thread.start()
        wait_for_health(self.port)
        self._seed_catalog()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=5)
        self._directory.cleanup()

    def _seed_catalog(self) -> None:
        post = lambda path, body, actor=None: http_request(  # noqa: E731
            self.port, "POST", path, body, {"X-Actor-Id": actor} if actor else None
        )
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
            ("auditor", "auditor"),
        ):
            status, _ = post("/users", {"user_id": user_id, "display_name": user_id, "role": role})
            self.assertEqual(status, 201)
        protocol = json.loads((ROOT / "fixtures" / "demo_protocol.json").read_text(encoding="utf-8"))
        self.assertEqual(post("/robots", {"robot_id": "robot-a", "model_name": "A", "vendor": "V"}, "operator")[0], 201)
        self.assertEqual(
            post(
                "/builds",
                {"build_id": "build-a", "robot_id": "robot-a", "version": "1.0", "content_sha256": "b" * 64},
                "operator",
            )[0],
            201,
        )
        self.assertEqual(post("/protocols", protocol, "stat")[0], 201)
        self.assertEqual(
            post(
                "/batches",
                {"batch_id": "batch-a", "protocol_id": "demo-delivery-v1", "protocol_version": 1, "build_id": "build-a"},
                "operator",
            )[0],
            201,
        )
        self.assertEqual(post("/batches/batch-a/start", {"expected_revision": 1}, "operator")[0], 200)

    @staticmethod
    def _row(source_row: str) -> dict:
        return {
            "source_batch": "hall-a-20260921",
            "source_row": source_row,
            "robot_id": "robot-a",
            "protocol_id": "demo-delivery-v1",
            "protocol_version": 1,
            "stratum_key": "clear-aisle",
            "observed_at": "2026-09-21T09:00:00+08:00",
            "metrics": {"completed": 1, "completion_seconds": "42.8", "interventions": 0},
            "excluded_reason": None,
        }

    def test_concurrent_reads_and_writes(self) -> None:
        barrier = threading.Barrier(WRITERS + READERS)
        failures: list[str] = []
        writer_committed = WRITERS * ITERATIONS * ROWS_PER_IMPORT
        poisoned_rows = {f"t{w}-poison-new" for w in range(WRITERS)}

        def writer(writer_id: int) -> None:
            try:
                for iteration in range(ITERATIONS):
                    barrier.wait()
                    key = f"w{writer_id}-i{iteration}"
                    rows = [
                        self._row(f"t{writer_id}-i{iteration}-r{r}") for r in range(ROWS_PER_IMPORT)
                    ]
                    status, first = http_request(
                        self.port, "POST", "/batches/batch-a/observations",
                        {"observations": rows}, {"X-Actor-Id": "operator", "Idempotency-Key": key},
                    )
                    assert status == 200, (status, first)
                    assert first["inserted"] == ROWS_PER_IMPORT, first
                    # 同键同体重放：返回一致结果且不重复写入。
                    status, replay = http_request(
                        self.port, "POST", "/batches/batch-a/observations",
                        {"observations": rows}, {"X-Actor-Id": "operator", "Idempotency-Key": key},
                    )
                    assert status == 200 and replay == first, (status, replay, first)
                    # 同键不同体：冲突，不产生部分提交。
                    changed = [dict(rows[0])]
                    changed[0]["metrics"] = {**rows[0]["metrics"], "completion_seconds": "99"}
                    status, _ = http_request(
                        self.port, "POST", "/batches/batch-a/observations",
                        {"observations": changed}, {"X-Actor-Id": "operator", "Idempotency-Key": key},
                    )
                    assert status == 409, status
                # 毒事务：一条已提交行 + 一条新行，必须整体回滚。
                barrier.wait()
                poison = [self._row("t0-i0-r0"), self._row(f"t{writer_id}-poison-new")]
                status, _ = http_request(
                    self.port, "POST", "/batches/batch-a/observations",
                    {"observations": poison},
                    {"X-Actor-Id": "operator", "Idempotency-Key": f"poison-{writer_id}"},
                )
                assert status == 409, status
            except Exception as exc:  # noqa: BLE001
                failures.append(f"writer-{writer_id}: {exc!r}")

        def reader(reader_id: int) -> None:
            try:
                for _ in range(ITERATIONS + 1):
                    barrier.wait()
                    if reader_id % 2 == 0:
                        status, body = http_request(
                            self.port, "GET", "/batches/batch-a/report", headers={"X-Actor-Id": "auditor"}
                        )
                        assert status == 200, (status, body)
                        assert body["batch"]["batch_id"] == "batch-a"
                    else:
                        status, body = http_request(
                            self.port, "POST", "/jobs/claim", {"worker_id": f"reader-{reader_id}", "lease_seconds": 30}
                        )
                        assert status == 200 and body["job"] is None, (status, body)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"reader-{reader_id}: {exc!r}")

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(WRITERS)]
        threads += [threading.Thread(target=reader, args=(i,)) for i in range(READERS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
            self.assertFalse(thread.is_alive(), "并发请求线程超时未结束")
        self.assertEqual(failures, [])

        # 跨请求事务隔离：只有幂等提交的行存在，毒事务中的新行一律未写入。
        with self.database.connection() as checker:
            count = checker.execute("SELECT count(*) FROM observations").fetchone()[0]
            self.assertEqual(count, writer_committed)
            poison_count = checker.execute(
                "SELECT count(*) FROM observations WHERE source_row IN ({})".format(
                    ",".join("?" for _ in poisoned_rows)
                ),
                tuple(sorted(poisoned_rows)),
            ).fetchone()[0]
            self.assertEqual(poison_count, 0)

        # 真正发生了连接级并发，且每个连接都在创建它的线程关闭。
        self.assertGreaterEqual(self.factory.peak, 2)
        self.assertEqual(self.factory.thread_violations, [])
        # 所有请求结束后连接立即归零，无需等待服务器关闭。
        self.assertEqual(self.factory.active, 0)

        # 并发结束后业务状态机仍可推进完整工作流。
        status, sealed = http_request(
            self.port, "POST", "/batches/batch-a/seal", {"expected_revision": 2}, {"X-Actor-Id": "stat"}
        )
        self.assertEqual(status, 200, sealed)
        status, claimed = http_request(self.port, "POST", "/jobs/claim", {"worker_id": "worker", "lease_seconds": 60})
        self.assertEqual(status, 200)
        job_id = claimed["job"]["job_id"]
        status, analysis = http_request(
            self.port, "POST", f"/jobs/{job_id}/complete", {"worker_id": "worker"}, {"X-Actor-Id": "stat"}
        )
        self.assertEqual(status, 200, analysis)
        status, decision = http_request(
            self.port, "POST", "/decisions",
            {"batch_id": "batch-a", "analysis_id": analysis["analysis_id"], "decision": "approved", "reason": "ok"},
            {"X-Actor-Id": "approver"},
        )
        self.assertEqual(status, 201, decision)
        status, report = http_request(
            self.port, "GET", "/batches/batch-a/report", headers={"X-Actor-Id": "auditor"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(report["batch"]["state"], "decided")

    def test_health_never_opens_connection(self) -> None:
        before = self.factory.created
        for _ in range(READERS):
            status, body = http_request(self.port, "GET", "/health")
            self.assertEqual((status, body["status"]), (200, "ok"))
        self.assertEqual(self.factory.created, before)


class SlowUserService(TrialService):
    """让写请求在持有连接期间短暂停留，制造关闭时确定的在途请求。"""

    def create_user(self, user_id, display_name, role):  # type: ignore[override]
        time.sleep(0.4)
        return super().create_user(user_id, display_name, role)


class ShutdownDrainTests(unittest.TestCase):
    def test_inflight_connections_released_on_shutdown(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        factory = CountingFactory()
        database = Database(Path(directory.name) / "shutdown.sqlite3", connection_factory=factory)
        database.initialize()
        application = JsonApplication(database, service_cls=SlowUserService)
        server = RequestTrackingHTTPServer(("127.0.0.1", 0), make_handler(application))
        server.daemon_threads = True
        port = server.server_address[1]
        threading.Thread(target=server.serve_forever, name="http-server", daemon=True).start()
        wait_for_health(port)

        in_flight = 5
        results: list[tuple[int, dict]] = []

        def slow_write(index: int) -> None:
            results.append(http_request(
                port, "POST", "/users",
                {"user_id": f"u{index}", "display_name": str(index), "role": "operator"},
            ))

        threads = [threading.Thread(target=slow_write, args=(i,)) for i in range(in_flight)]
        for thread in threads:
            thread.start()
        # 等待全部请求确定进入持有连接的在途状态。
        deadline = time.monotonic() + 5
        while factory.active < in_flight and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertGreaterEqual(factory.active, in_flight)

        server.shutdown()
        server.server_close()
        for thread in threads:
            thread.join(timeout=5)

        # 关闭后无连接泄漏，且在途请求都正常提交（不是被强杀成断连）。
        self.assertEqual(factory.active, 0)
        self.assertEqual(len(results), in_flight)
        self.assertEqual({status for status, _ in results}, {201})
        with database.connection() as checker:
            count = checker.execute("SELECT count(*) FROM users").fetchone()[0]
        self.assertEqual(count, in_flight)



class CommandLineServerTests(unittest.TestCase):
    """用与文档一致的命令行方式真正起停子进程服务。"""

    def test_cli_serves_graceful_shutdown_and_persists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "cli.sqlite3"
            port = free_port()
            env = {**os.environ, "PYTHONPATH": str(SRC)}
            process = subprocess.Popen(
                [sys.executable, "-m", "robot_trials.api",
                 "--database", str(db_path), "--host", "127.0.0.1", "--port", str(port)],
                cwd=str(ROOT), env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                wait_for_health(port)
                status, _ = http_request(
                    port, "POST", "/users",
                    {"user_id": "u1", "display_name": "操作员", "role": "operator"},
                )
                self.assertEqual(status, 201)
                process.send_signal(signal.SIGINT)
                stdout, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 0, stderr.decode("utf-8", "replace"))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)

            # 数据落盘：用新进程重新打开同一数据库应看到既有用户并能继续写入。
            port = free_port()
            restarted = subprocess.Popen(
                [sys.executable, "-m", "robot_trials.api",
                 "--database", str(db_path), "--host", "127.0.0.1", "--port", str(port)],
                cwd=str(ROOT), env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                wait_for_health(port)
                status, _ = http_request(
                    port, "POST", "/users",
                    {"user_id": "u2", "display_name": "统计", "role": "statistician"},
                )
                self.assertEqual(status, 201)
                # 重复用户返回冲突而不是 5xx，说明模式与约束完整保留。
                status, body = http_request(
                    port, "POST", "/users",
                    {"user_id": "u1", "display_name": "操作员", "role": "operator"},
                )
                self.assertEqual(status, 409, body)
            finally:
                restarted.send_signal(signal.SIGINT)
                try:
                    restarted.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    restarted.kill()
                    restarted.wait(timeout=10)

            with sqlite3.connect(db_path) as direct:
                users = {row[0] for row in direct.execute("SELECT user_id FROM users")}
            self.assertEqual(users, {"u1", "u2"})


if __name__ == "__main__":
    unittest.main()
