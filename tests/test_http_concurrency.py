"""真实 HTTP 服务上的并发读写回归测试。

复现并防止以下故障回归：请求处理线程共用启动线程创建的 SQLite 连接，
导致线程亲和错误、跨请求事务污染以及关闭后的资源泄漏。
"""

from __future__ import annotations

import hashlib
import http.client
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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from robot_trials.api import DatabaseGateway, make_server
from robot_trials.jsonio import load_json
from robot_trials.storage import connect


ROOT = Path(__file__).resolve().parents[1]

BATCH_COUNT = 4
IMPORTS_PER_BATCH = 4
OBSERVATIONS_PER_IMPORT = 3
STORM_REPORTS_PER_BATCH = 2
CLAIM_WORKERS = 3
STORM_HEALTH_CHECKS = 6


def _observation(batch_index: int, task_index: int, row_index: int) -> dict[str, object]:
    return {
        "source_batch": f"src-{batch_index}",
        "source_row": f"{task_index:02d}-{row_index:02d}",
        "robot_id": "robot-a",
        "protocol_id": "demo-delivery-v1",
        "protocol_version": 1,
        "stratum_key": "clear-aisle" if row_index % 2 == 0 else "cross-traffic",
        "observed_at": "2026-09-22T09:00:00+08:00",
        "metrics": {"completed": 1, "completion_seconds": "41.5", "interventions": 0},
        "excluded_reason": None,
    }


class HttpServerFixture(unittest.TestCase):
    """按生产接线方式启动真实 HTTP 服务。"""

    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory(prefix="robot-trials-http-")
        self.database = Path(self._temporary.name) / "server.sqlite3"
        self.gateway = DatabaseGateway(self.database)
        self.server = make_server(self.gateway, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self._stop_server()
        self._temporary.cleanup()

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.gateway.close()

    def _request(
        self, method: str, path: str, payload: dict | None = None, headers: dict | None = None
    ) -> tuple[int, dict]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            body = json.dumps(payload).encode("utf-8") if payload is not None else None
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            raw = response.read()
            return response.status, json.loads(raw.decode("utf-8"))
        finally:
            connection.close()

    def _seed_catalog(self) -> None:
        for user_id, role in (("op", "operator"), ("stat", "statistician"), ("appr", "approver"), ("aud", "auditor")):
            status, body = self._request("POST", "/users", {"user_id": user_id, "display_name": user_id, "role": role})
            self.assertEqual((status, body), (201, {"user_id": user_id, "role": role}))
        status, _ = self._request(
            "POST", "/robots", {"robot_id": "robot-a", "model_name": "A 型", "vendor": "厂商"}, {"X-Actor-Id": "op"}
        )
        self.assertEqual(status, 201)
        status, _ = self._request(
            "POST",
            "/builds",
            {"build_id": "build-a", "robot_id": "robot-a", "version": "1.0", "content_sha256": "c" * 64},
            {"X-Actor-Id": "op"},
        )
        self.assertEqual(status, 201)
        protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        status, _ = self._request("POST", "/protocols", protocol, {"X-Actor-Id": "stat"})
        self.assertEqual(status, 201)

    def _create_running_batch(self, batch_id: str) -> None:
        status, _ = self._request(
            "POST",
            "/batches",
            {"batch_id": batch_id, "protocol_id": "demo-delivery-v1", "protocol_version": 1, "build_id": "build-a"},
            {"X-Actor-Id": "op"},
        )
        self.assertEqual(status, 201)
        status, _ = self._request(
            "POST", f"/batches/{batch_id}/start", {"expected_revision": 1}, {"X-Actor-Id": "op"}
        )
        self.assertEqual(status, 200)


class ConcurrentReadWriteTests(HttpServerFixture):
    """并发上传观测、查询报告和领取分析任务的确定性回归场景。"""

    def test_concurrent_storm_has_no_thread_pollution_or_leak(self) -> None:
        self._seed_catalog()
        batch_ids = [f"batch-c-{index}" for index in range(BATCH_COUNT)]
        for batch_id in batch_ids:
            self._create_running_batch(batch_id)

        # 准备一个已封存批次，让并发领取任务有确定的对象。
        self._create_running_batch("batch-jobs")
        fixture_rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        status, body = self._request(
            "POST",
            "/batches/batch-jobs/observations",
            {"observations": fixture_rows},
            {"X-Actor-Id": "op", "Idempotency-Key": "jobs-import-1"},
        )
        self.assertEqual(status, 200, body)
        status, _ = self._request(
            "POST", "/batches/batch-jobs/seal", {"expected_revision": 2}, {"X-Actor-Id": "stat"}
        )
        self.assertEqual(status, 200)

        tasks: list[tuple[str, object]] = []
        for batch_index, batch_id in enumerate(batch_ids):
            for task_index in range(IMPORTS_PER_BATCH):
                rows = [
                    _observation(batch_index, task_index, row_index)
                    for row_index in range(OBSERVATIONS_PER_IMPORT)
                ]
                tasks.append((
                    "import",
                    (batch_id, f"storm-{batch_index}-{task_index}", rows),
                ))
            for _ in range(STORM_REPORTS_PER_BATCH):
                tasks.append(("report", batch_id))
        for worker_index in range(CLAIM_WORKERS):
            tasks.append(("claim", f"worker-{worker_index}"))
        for _ in range(STORM_HEALTH_CHECKS):
            tasks.append(("health", None))

        def run(task: tuple[str, object]) -> tuple[str, object]:
            kind, argument = task
            if kind == "import":
                batch_id, key, rows = argument  # type: ignore[misc]
                return kind, (batch_id, self._request(
                    "POST",
                    f"/batches/{batch_id}/observations",
                    {"observations": rows},
                    {"X-Actor-Id": "op", "Idempotency-Key": key},
                ))
            if kind == "report":
                return kind, (argument, self._request("GET", f"/batches/{argument}/report", headers={"X-Actor-Id": "aud"}))
            if kind == "claim":
                return kind, (argument, self._request("POST", "/jobs/claim", {"worker_id": argument, "lease_seconds": 60}))
            return kind, self._request("GET", "/health")

        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(run, tasks))

        imports = [result for kind, result in results if kind == "import"]
        reports = [result for kind, result in results if kind == "report"]
        claims = [result for kind, result in results if kind == "claim"]
        healths = [result for kind, result in results if kind == "health"]

        # 无线程亲和错误：每个请求都拿到结构完整的 JSON 响应，而不是空连接或中断。
        self.assertEqual(len(imports), BATCH_COUNT * IMPORTS_PER_BATCH)
        for batch_id, (status, body) in imports:
            self.assertEqual(status, 200, body)
            self.assertEqual(body["batch_id"], batch_id)
            self.assertEqual(body["inserted"], OBSERVATIONS_PER_IMPORT)
            self.assertEqual(len(body["request_sha256"]), 64)

        for batch_id, (status, body) in reports:
            self.assertEqual(status, 200, body)
            self.assertEqual(body["batch"]["batch_id"], batch_id)
            self.assertEqual(body["batch"]["state"], "running")

        self.assertEqual(healths, [(200, {"status": "ok"})] * STORM_HEALTH_CHECKS)

        # 并发领取租约：恰好一个工作进程拿到任务，其余得到空结果。
        leased = [body["job"] for _, (status, body) in claims if status == 200 and body["job"] is not None]
        for _, (status, _) in claims:
            self.assertEqual(status, 200)
        self.assertEqual(len(leased), 1, claims)
        job = leased[0]
        self.assertEqual(job["batch_id"], "batch-jobs")
        self.assertEqual(job["state"], "leased")
        winner = job["lease_owner"]

        # 全部请求结束后，租约连接必须全部释放。
        self.assertEqual(self.gateway.open_connections, 0)

        # 持有租约的工作进程完成分析，随后审批并读取报告，全链路走 HTTP。
        status, body = self._request(
            "POST", f"/jobs/{job['job_id']}/complete", {"worker_id": winner}, {"X-Actor-Id": "stat"}
        )
        self.assertEqual(status, 200, body)
        analysis_id = body["analysis_id"]
        self.assertEqual(body["result"]["conclusion"], "pass")
        status, body = self._request(
            "POST",
            "/decisions",
            {"batch_id": "batch-jobs", "analysis_id": analysis_id, "decision": "approved", "reason": "满足规则"},
            {"X-Actor-Id": "appr"},
        )
        self.assertEqual(status, 201, body)
        status, body = self._request("GET", "/batches/batch-jobs/report", headers={"X-Actor-Id": "aud"})
        self.assertEqual(status, 200)
        self.assertEqual(body["batch"]["state"], "decided")
        self.assertEqual(body["decision"]["decision"], "approved")

        # 安静期内的纯健康检查：不租约连接，数据库文件字节完全不变。
        self.assertEqual(self.gateway.open_connections, 0)
        digest_before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        leased_before = self.gateway.leased_total
        for _ in range(20):
            status, body = self._request("GET", "/health")
            self.assertEqual((status, body), (200, {"status": "ok"}))
        self.assertEqual(self.gateway.leased_total, leased_before)
        self.assertEqual(hashlib.sha256(self.database.read_bytes()).hexdigest(), digest_before)

        # 关闭服务器与网关后：连接计数归零，无泄漏，数据库内容证明无跨请求事务污染。
        self._stop_server()
        self.assertEqual(self.gateway.open_connections, 0)
        self.assertTrue(self.gateway.closed)
        with self.assertRaises(RuntimeError):
            with self.gateway.lease():
                pass
        self.assertEqual(self.gateway.open_connections, 0)
        for suffix in ("-journal", "-wal", "-shm"):
            self.assertFalse(Path(str(self.database) + suffix).exists())

        verification = connect(self.database)
        try:
            counts = dict(
                verification.execute("SELECT batch_id, COUNT(*) FROM observations GROUP BY batch_id").fetchall()
            )
            expected = {batch_id: IMPORTS_PER_BATCH * OBSERVATIONS_PER_IMPORT for batch_id in batch_ids}
            expected["batch-jobs"] = len(fixture_rows)
            self.assertEqual(counts, expected)
            keys = verification.execute("SELECT COUNT(*) FROM idempotency_keys").fetchone()[0]
            self.assertEqual(keys, BATCH_COUNT * IMPORTS_PER_BATCH + 1)
            events = verification.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
            self.assertGreater(events, 0)
        finally:
            verification.close()


class LifecycleTests(HttpServerFixture):
    """健康检查、关闭路径与连接生命周期的边界行为。"""

    def test_health_checks_do_not_touch_database(self) -> None:
        digest_before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        for _ in range(10):
            status, body = self._request("GET", "/health")
            self.assertEqual((status, body), (200, {"status": "ok"}))
        self.assertEqual(self.gateway.leased_total, 0)
        self.assertEqual(self.gateway.open_connections, 0)
        self.assertEqual(hashlib.sha256(self.database.read_bytes()).hexdigest(), digest_before)

    def test_request_after_gateway_close_returns_500_without_leak(self) -> None:
        self.gateway.close()
        status, body = self._request("POST", "/users", {"user_id": "u1", "display_name": "x", "role": "operator"})
        self.assertEqual(status, 500)
        self.assertEqual(body["error"]["code"], "internal_error")
        self.assertEqual(self.gateway.open_connections, 0)

    def test_memory_database_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            DatabaseGateway(":memory:")


class CommandLineTests(unittest.TestCase):
    """命令行启动方式保持兼容，且退出时资源可靠释放。"""

    def test_cli_starts_serves_and_shuts_down_cleanly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="robot-trials-cli-") as temporary:
            database = Path(temporary) / "cli.sqlite3"
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            environment = dict(os.environ)
            environment["PYTHONPATH"] = str(ROOT / "src")
            process = subprocess.Popen(
                [
                    sys.executable, "-m", "robot_trials.api",
                    "--database", str(database), "--host", "127.0.0.1", "--port", str(port),
                ],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 15
                while True:
                    try:
                        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
                        connection.request("GET", "/health")
                        response = connection.getresponse()
                        response.read()
                        connection.close()
                        if response.status == 200:
                            break
                    except OSError:
                        if time.monotonic() > deadline:
                            raise
                    time.sleep(0.1)
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                connection.request(
                    "POST",
                    "/users",
                    body=json.dumps({"user_id": "cli-op", "display_name": "操作员", "role": "operator"}),
                )
                response = connection.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                connection.close()
                self.assertEqual(response.status, 201, payload)
                self.assertEqual(payload["role"], "operator")
            finally:
                process.send_signal(signal.SIGINT)
                stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertTrue(database.exists())
            for suffix in ("-journal", "-wal", "-shm"):
                self.assertFalse(Path(str(database) + suffix).exists())
            verification = connect(database)
            try:
                row = verification.execute(
                    "SELECT role FROM users WHERE user_id='cli-op'"
                ).fetchone()
                self.assertEqual(row["role"], "operator")
            finally:
                verification.close()


if __name__ == "__main__":
    unittest.main()
