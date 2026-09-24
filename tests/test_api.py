from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from robot_trials.api import JsonApplication
from robot_trials.storage import Database


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = tempfile.TemporaryDirectory()
        database = Database(Path(self._directory.name) / "api.sqlite3")
        database.initialize()
        self.app = JsonApplication(database)

    def tearDown(self) -> None:
        self._directory.cleanup()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_health_opens_no_database_connection(self) -> None:
        def factory(path):  # pragma: no cover - 健康检查绝不应该真正取连接
            raise AssertionError("健康检查不能打开数据库连接")

        app = JsonApplication(Database(Path("unused.sqlite3"), connection_factory=factory))
        response = app.handle("GET", "/health")
        self.assertEqual((response.status, response.body["status"]), (200, "ok"))

    def test_json_error_shape(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_user_route(self) -> None:
        payload = json.dumps({"user_id": "u1", "display_name": "操作员", "role": "operator"}).encode()
        response = self.app.handle("POST", "/users", body=payload)
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["role"], "operator")

    def test_request_scope_releases_connection_after_error(self) -> None:
        # 首个请求正常提交；随后同一线程的错误请求必须独立回滚且不污染前一个请求。
        payload = json.dumps({"user_id": "u1", "display_name": "操作员", "role": "operator"}).encode()
        self.assertEqual(self.app.handle("POST", "/users", body=payload).status, 201)
        conflict = self.app.handle("POST", "/users", body=payload)
        self.assertEqual(conflict.status, 409)
        missing = self.app.handle(
            "POST", "/robots",
            headers={"X-Actor-Id": "u1"},
            body=json.dumps({"robot_id": "r1", "model_name": "M", "vendor": "V"}).encode(),
        )
        self.assertEqual(missing.status, 201)


if __name__ == "__main__":
    unittest.main()
