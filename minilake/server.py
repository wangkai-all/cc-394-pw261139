"""HTTP API：网页端对三个服务的操作全部经此转发（JSON）。"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import urlparse

from .catalog import Catalog
from .daemon import DemoRuntime
from .paths import Warehouse
from .planner import QueryPlanError, QueryService
from .services import RebuildPlanner, ServiceError, Writer
from .filter import FilterParseError

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
PARTITION_COL = "dt"
TABLE = "orders"


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "minilake/1.0"

    # ------------------------------------------------------------ 基础
    def _send_json(self, obj: Any, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ServiceError(f"请求体不是合法 JSON: {e}") from e

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静日志
        pass

    @property
    def cat(self) -> Catalog:
        return self.server.cat  # type: ignore[attr-defined]

    @property
    def wh(self) -> Warehouse:
        return self.server.wh  # type: ignore[attr-defined]

    @property
    def runtime(self) -> Optional[DemoRuntime]:
        return getattr(self.server, "runtime", None)

    # ------------------------------------------------------------ 路由
    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/" or path == "/index.html":
                return self._serve_static("index.html", "text/html; charset=utf-8")
            if path == "/api/overview":
                ov = self.cat.overview()
                if self.runtime:
                    ov["workers"] = self.runtime.status()
                else:
                    ov["workers"] = [
                        {"worker": "external", "alive": "n/a",
                         "last": "独立进程部署，请查看各 worker 日志"}
                    ]
                return self._send_json(ov)
            if path == "/api/rules":
                return self._send_json({"rules": self.cat.list_rules()})
            if path == "/api/files":
                return self._send_json({"files": self.cat.list_files(
                    TABLE, status=None)})
            if path == "/api/stats":
                return self._send_json({"stats": self.cat.list_stats()})
            if path == "/api/jobs":
                return self._send_json({"jobs": self.cat.list_jobs()})
            if path == "/api/queries":
                return self._send_json({"queries": self.cat.list_queries()})
            if path.startswith("/api/queries/"):
                qid = path.rsplit("/", 1)[-1]
                detail = self.cat.get_query_detail(qid)
                if detail is None:
                    return self._send_json({"error": "查询不存在"}, 404)
                return self._send_json(detail)
            if path == "/api/hotspots":
                return self._send_json({
                    "hotspots": self.cat.hotspot_files(TABLE)
                })
            return self._send_json({"error": "not found"}, 404)
        except Exception as e:
            return self._send_json({"error": repr(e)}, 500)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            body = self._body()
            if path == "/api/rules":
                cols = body.get("tracked_columns") or []
                if not isinstance(cols, list) or not cols:
                    raise ServiceError("tracked_columns 必须是非空数组")
                rule = self.cat.publish_rule(cols, body.get("description", ""))
                return self._send_json({
                    "rule": rule,
                    "warning": "新版本已发布，所有旧版本统计立即失可信",
                })
            if path == "/api/ingest":
                from .demo_data import generate_orders
                rows = generate_orders(
                    partitions=int(body.get("partitions", 6)),
                    files_per_partition=int(body.get("files_per_partition", 2)),
                    rows_per_file=int(body.get("rows_per_file", 800)),
                    seed=int(body.get("seed", 42)),
                )
                job_id = Writer(self.wh, self.cat, PARTITION_COL).enqueue_ingest(
                    TABLE, rows,
                    rows_per_file=int(body.get("rows_per_file", 800)),
                )
                return self._send_json({"job_id": job_id, "rows": len(rows)})
            if path == "/api/late-update":
                from .demo_data import generate_late_rows
                pval = body.get("partition_val")
                if not pval:
                    raise ServiceError("partition_val 必填")
                extra = generate_late_rows(
                    pval, n=int(body.get("n", 100)),
                    seed=int(body.get("seed", 7)),
                )
                job_id = Writer(self.wh, self.cat, PARTITION_COL).enqueue_late_update(
                    TABLE, pval, extra
                )
                return self._send_json({"job_id": job_id, "late_rows": len(extra)})
            if path == "/api/compact":
                job_id = Writer(self.wh, self.cat, PARTITION_COL).enqueue_compact(
                    TABLE
                )
                return self._send_json({"job_id": job_id})
            if path == "/api/stats/pause":
                if self.runtime:
                    self.runtime.stats.pause()
                else:
                    self.cat.set_paused(True)
                return self._send_json({"paused": True})
            if path == "/api/stats/resume":
                if self.runtime:
                    self.runtime.stats.resume()
                else:
                    self.cat.set_paused(False)
                return self._send_json({"paused": False})
            if path == "/api/rebuild/preview":
                mode = body.get("mode", "full")
                rp = RebuildPlanner(self.wh, self.cat, PARTITION_COL)
                return self._send_json(rp.preview(
                    TABLE, mode=mode, limit=int(body.get("limit", 20))))
            if path == "/api/rebuild":
                mode = body.get("mode", "full")
                rp = RebuildPlanner(self.wh, self.cat, PARTITION_COL)
                job_id = rp.enqueue(
                    TABLE, mode=mode, limit=int(body.get("limit", 20))
                )
                return self._send_json({"job_id": job_id, "mode": mode})
            if path == "/api/query":
                qs = QueryService(self.wh, self.cat, PARTITION_COL)
                ftext = (body.get("filter") or "").strip()
                if body.get("dry_run"):
                    return self._send_json(qs.plan(TABLE, ftext))
                result = qs.run(TABLE, ftext)
                return self._send_json(result)
            if path == "/api/reset":
                return self._reset()
            return self._send_json({"error": "not found"}, 404)
        except (FilterParseError, QueryPlanError, ServiceError) as e:
            return self._send_json({"error": str(e)}, 400)
        except Exception as e:
            return self._send_json({"error": repr(e)}, 500)

    # ------------------------------------------------------------ 静态资源
    def _serve_static(self, name: str, ctype: str) -> None:
        fp = os.path.join(WEB_DIR, name)
        if not os.path.isfile(fp):
            self.send_response(404)
            self.end_headers()
            return
        with open(fp, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _reset(self) -> None:
        rt = self.runtime
        if rt:
            rt.stop()
        self.cat.close()
        self.wh.wipe()
        new_cat = Catalog(self.wh.catalog_path)
        self.server.cat = new_cat  # type: ignore[attr-defined]
        if rt:
            new_rt = DemoRuntime(self.wh, new_cat, PARTITION_COL)
            new_rt.start()
            self.server.runtime = new_rt  # type: ignore[attr-defined]
        self._send_json({"reset": True})


def build_server(
    warehouse: Warehouse,
    catalog: Catalog,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    runtime: Optional[DemoRuntime] = None,
) -> ThreadingHTTPServer:
    warehouse.ensure()
    httpd = ThreadingHTTPServer((host, port), ApiHandler)
    httpd.wh = warehouse  # type: ignore[attr-defined]
    httpd.cat = catalog  # type: ignore[attr-defined]
    httpd.runtime = runtime  # type: ignore[attr-defined]
    return httpd
