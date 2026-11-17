"""SQLite 目录：规则版本、文件元数据、列统计、查询审计、作业队列。

目录是三个服务共享的唯一协调面：

* 写入器只写 files 表（active -> superseded/compacted）和 jobs 表；
* 统计服务只写 file_stats 表（以及 stats_paused 标志）；
* 查询服务只读，并写 queries / query_scans 审计表。

统计可信判定（``is_trusted``）同时要求：

1. 统计的 rule_version == 当前已发布规则版本；
2. 统计的 generation == 文件当前 generation；
3. 文件处于 active 状态。

任一不满足即不可信，查询服务必须把该文件计入扫描清单。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version         INTEGER NOT NULL UNIQUE,
    tracked_columns TEXT NOT NULL,          -- JSON array[str]
    description     TEXT NOT NULL DEFAULT '',
    created_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id        TEXT NOT NULL UNIQUE,
    table_name     TEXT NOT NULL,
    partition_name TEXT NOT NULL,
    partition_val  TEXT,                    -- 分区列标量值（用于分区裁剪）
    rel_path       TEXT NOT NULL,
    row_count      INTEGER NOT NULL,
    byte_size      INTEGER NOT NULL,
    status         TEXT NOT NULL,           -- active | superseded | compacted
    generation     INTEGER NOT NULL,
    created_at     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_lookup
    ON files(table_name, status, partition_name);

CREATE TABLE IF NOT EXISTS file_stats (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id      TEXT NOT NULL,
    generation   INTEGER NOT NULL,
    rule_version INTEGER NOT NULL,
    columns      TEXT NOT NULL,             -- JSON: {col: {min,max,distinct,nnull}}
    row_count    INTEGER NOT NULL,
    build_ms     REAL NOT NULL,
    built_at     REAL NOT NULL,
    UNIQUE(file_id, generation, rule_version)
);

CREATE TABLE IF NOT EXISTS queries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id        TEXT NOT NULL UNIQUE,
    table_name      TEXT NOT NULL,
    filter_text     TEXT NOT NULL,
    result_rows     INTEGER NOT NULL,
    planned_files   INTEGER NOT NULL,
    baseline_files  INTEGER NOT NULL,
    total_files     INTEGER NOT NULL,
    planned_bytes   INTEGER NOT NULL,
    baseline_bytes  INTEGER NOT NULL,
    result_matches  INTEGER NOT NULL,       -- 1 = 与基准结果一致
    created_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS query_scans (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id     TEXT NOT NULL,
    file_id      TEXT NOT NULL,
    partition_name TEXT NOT NULL,
    in_plan      INTEGER NOT NULL,          -- 统计裁剪计划是否包含
    in_baseline  INTEGER NOT NULL,          -- 基准（全量）是否包含
    scanned      INTEGER NOT NULL,          -- 是否真正打开读取
    skip_reason  TEXT,                      -- 未入计划的原因
    scan_reason  TEXT                       -- 入计划的原因
);
CREATE INDEX IF NOT EXISTS idx_query_scans ON query_scans(query_id);

CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL UNIQUE,
    job_type    TEXT NOT NULL,              -- ingest | late_update | compact | rebuild_stats
    payload     TEXT NOT NULL,              -- JSON
    status      TEXT NOT NULL,              -- queued | running | done | failed
    message     TEXT NOT NULL DEFAULT '',
    cost        TEXT NOT NULL DEFAULT '',   -- JSON: 成本/有效性结果
    created_at  REAL NOT NULL,
    started_at  REAL,
    finished_at REAL
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, job_type);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# 文件状态 / 作业状态常量
ACTIVE, SUPERSEDED, COMPACTED = "active", "superseded", "compacted"
QUEUED, RUNNING, DONE, FAILED = "queued", "running", "done", "failed"

MAX_DISTINCT = 200  # distinct 集合上限，超出后只保留 min/max/nnull（裁剪仍安全）


class Catalog:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        first = not os.path.exists(path)
        # isolation_level=None：完全由代码控制事务，避免隐式事务与
        # BEGIN IMMEDIATE 嵌套（多线程 worker 并发访问时必须如此）
        self._conn = sqlite3.connect(
            path, timeout=30, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        # 先设忙等待，再设 WAL；多进程同时建库时 WAL 切换可能短暂撞锁，
        # 重试即可。WAL 是库级持久属性，任一进程设置成功即全局生效。
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._exec_pragma_retry("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        if first:
            self._exec_retry(lambda: self._conn.executescript(SCHEMA))
        else:
            # 兼容已存在库：补建缺失表
            self._exec_retry(lambda: self._conn.executescript(SCHEMA))

    def _exec_pragma_retry(self, sql: str, attempts: int = 10) -> None:
        self._exec_retry(lambda: self._conn.execute(sql), attempts)

    def _exec_retry(self, fn, attempts: int = 10) -> None:
        delay = 0.05
        for i in range(attempts):
            try:
                fn()
                return
            except sqlite3.OperationalError as e:
                # 多进程并发建库/切换 WAL 撞锁可重试；其余错误直接抛出
                if "locked" not in str(e).lower() or i == attempts - 1:
                    raise
                time.sleep(delay)
                delay *= 1.5

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ 基础
    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    @staticmethod
    def new_id(prefix: str = "") -> str:
        return f"{prefix}{uuid.uuid4().hex[:16]}"

    # ------------------------------------------------------------------ 规则
    def publish_rule(
        self, tracked_columns: list[str], description: str = ""
    ) -> dict[str, Any]:
        """发布新规则，版本号自增；旧版本统计自动全部失可信（版本门禁）。"""
        now = time.time()
        with self.tx() as c:
            row = c.execute("SELECT MAX(version) AS v FROM rules").fetchone()
            version = (row["v"] or 0) + 1
            c.execute(
                "INSERT INTO rules(version, tracked_columns, description, created_at)"
                " VALUES (?,?,?,?)",
                (version, json.dumps(tracked_columns), description, now),
            )
        return self.get_current_rule()

    def get_current_rule(self) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM rules ORDER BY version DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["tracked_columns"] = json.loads(d["tracked_columns"])
        return d

    def list_rules(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM rules ORDER BY version DESC"
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["tracked_columns"] = json.loads(d["tracked_columns"])
            out.append(d)
        return out

    # ------------------------------------------------------------------ 文件
    def register_file(
        self,
        *,
        table: str,
        partition_name: str,
        partition_val: Any,
        rel_path: str,
        row_count: int,
        byte_size: int,
        generation: int,
        file_id: Optional[str] = None,
        status: str = ACTIVE,
    ) -> str:
        file_id = file_id or self.new_id("f_")
        with self.tx() as c:
            c.execute(
                "INSERT INTO files(file_id, table_name, partition_name, partition_val,"
                " rel_path, row_count, byte_size, status, generation, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    file_id, table, partition_name,
                    None if partition_val is None else json.dumps(partition_val),
                    rel_path, row_count, byte_size, status, generation, time.time(),
                ),
            )
        return file_id

    def replace_files(
        self, old_file_ids: Iterable[str], new_status: str
    ) -> None:
        ids = list(old_file_ids)
        if not ids:
            return
        with self.tx() as c:
            c.executemany(
                "UPDATE files SET status=? WHERE file_id=?",
                [(new_status, fid) for fid in ids],
            )

    def get_file(self, file_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM files WHERE file_id=?", (file_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_files(
        self,
        table: Optional[str] = None,
        status: Optional[str] = ACTIVE,
        partition_name: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM files WHERE 1=1"
        args: list[Any] = []
        if table is not None:
            sql += " AND table_name=?"
            args.append(table)
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        if partition_name is not None:
            sql += " AND partition_name=?"
            args.append(partition_name)
        sql += " ORDER BY partition_name, file_id"
        return [dict(r) for r in self._conn.execute(sql, args).fetchall()]

    def next_generation(self, table: str, partition_name: str) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(generation),0) AS g FROM files"
            " WHERE table_name=? AND partition_name=?",
            (table, partition_name),
        ).fetchone()
        return (row["g"] or 0) + 1

    # ------------------------------------------------------------------ 统计
    def upsert_stat(
        self,
        *,
        file_id: str,
        generation: int,
        rule_version: int,
        columns: dict[str, Any],
        row_count: int,
        build_ms: float,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT INTO file_stats(file_id, generation, rule_version, columns,"
                " row_count, build_ms, built_at) VALUES (?,?,?,?,?,?,?)"
                " ON CONFLICT(file_id, generation, rule_version) DO UPDATE SET"
                " columns=excluded.columns, row_count=excluded.row_count,"
                " build_ms=excluded.build_ms, built_at=excluded.built_at",
                (
                    file_id, generation, rule_version, json.dumps(columns),
                    row_count, build_ms, time.time(),
                ),
            )

    def get_stat(
        self, file_id: str, generation: int, rule_version: int
    ) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM file_stats WHERE file_id=? AND generation=? AND rule_version=?",
            (file_id, generation, rule_version),
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["columns"] = json.loads(d["columns"])
        return d

    def list_stats(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT fs.*, f.status, f.table_name FROM file_stats fs"
            " JOIN files f ON f.file_id = fs.file_id"
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["columns"] = json.loads(d["columns"])
            out.append(d)
        return out

    def trusted_file_ids(self, rule_version: int) -> set[str]:
        rows = self._conn.execute(
            "SELECT fs.file_id FROM file_stats fs JOIN files f ON f.file_id=fs.file_id"
            " WHERE fs.rule_version=? AND fs.generation=f.generation AND f.status=?",
            (rule_version, ACTIVE),
        ).fetchall()
        return {r["file_id"] for r in rows}

    # ------------------------------------------------------------------ 查询审计
    def record_query(
        self,
        *,
        query_id: str,
        table: str,
        filter_text: str,
        result_rows: int,
        planned: list[dict[str, Any]],
        baseline: list[dict[str, Any]],
        scans: list[dict[str, Any]],
        result_matches: bool,
    ) -> None:
        planned_bytes = sum(s["byte_size"] for s in planned)
        baseline_bytes = sum(s["byte_size"] for s in baseline)
        with self.tx() as c:
            c.execute(
                "INSERT INTO queries(query_id, table_name, filter_text, result_rows,"
                " planned_files, baseline_files, total_files, planned_bytes,"
                " baseline_bytes, result_matches, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    query_id, table, filter_text, result_rows,
                    len(planned), len(baseline),
                    len(self.list_files(table, status=None)),
                    planned_bytes, baseline_bytes, 1 if result_matches else 0,
                    time.time(),
                ),
            )
            c.executemany(
                "INSERT INTO query_scans(query_id, file_id, partition_name,"
                " in_plan, in_baseline, scanned, skip_reason, scan_reason)"
                " VALUES (?,?,?,?,?,?,?,?)",
                [
                    (
                        query_id, s["file_id"], s["partition_name"],
                        1 if s["in_plan"] else 0,
                        1 if s["in_baseline"] else 0,
                        1 if s["scanned"] else 0,
                        s.get("skip_reason"), s.get("scan_reason"),
                    )
                    for s in scans
                ],
            )

    def list_queries(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM queries ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_query_detail(self, query_id: str) -> Optional[dict[str, Any]]:
        q = self._conn.execute(
            "SELECT * FROM queries WHERE query_id=?", (query_id,)
        ).fetchone()
        if q is None:
            return None
        scans = self._conn.execute(
            "SELECT * FROM query_scans WHERE query_id=? ORDER BY id", (query_id,)
        ).fetchall()
        return {"query": dict(q), "scans": [dict(s) for s in scans]}

    def hotspot_files(self, table: str, limit: int = 20) -> list[dict[str, Any]]:
        """查询热点：被实际扫描次数最多的文件（用于按热点补建统计）。"""
        rows = self._conn.execute(
            "SELECT qs.file_id, qs.partition_name, COUNT(*) AS scan_count,"
            " SUM(qs.scanned) AS scanned_count FROM query_scans qs"
            " JOIN queries q ON q.query_id = qs.query_id"
            " WHERE q.table_name=? GROUP BY qs.file_id"
            " ORDER BY scan_count DESC, scanned_count DESC LIMIT ?",
            (table, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ 作业
    def enqueue_job(
        self, job_type: str, payload: dict[str, Any], job_id: Optional[str] = None
    ) -> str:
        job_id = job_id or self.new_id("j_")
        with self.tx() as c:
            c.execute(
                "INSERT INTO jobs(job_id, job_type, payload, status, created_at)"
                " VALUES (?,?,?,?,?)",
                (job_id, job_type, json.dumps(payload), QUEUED, time.time()),
            )
        return job_id

    def claim_job(
        self, job_types: Optional[list[str]] = None
    ) -> Optional[dict[str, Any]]:
        """原子领取一个 queued 作业；无则返回 None。"""
        with self.tx() as c:
            sql = "SELECT * FROM jobs WHERE status=?"
            args: list[Any] = [QUEUED]
            if job_types:
                sql += " AND job_type IN (%s)" % ",".join("?" * len(job_types))
                args.extend(job_types)
            sql += " ORDER BY id LIMIT 1"
            row = c.execute(sql, args).fetchone()
            if row is None:
                return None
            c.execute(
                "UPDATE jobs SET status=?, started_at=? WHERE id=?",
                (RUNNING, time.time(), row["id"]),
            )
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        return d

    def finish_job(
        self,
        job_id: str,
        status: str,
        message: str = "",
        cost: Optional[dict[str, Any]] = None,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "UPDATE jobs SET status=?, message=?, cost=?, finished_at=?"
                " WHERE job_id=?",
                (
                    status, message,
                    json.dumps(cost or {}), time.time(), job_id,
                ),
            )

    def list_jobs(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = json.loads(d["payload"])
            d["cost"] = json.loads(d["cost"]) if d["cost"] else {}
            out.append(d)
        return out

    # ------------------------------------------------------------------ 服务控制
    def set_paused(self, paused: bool) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT INTO meta(key,value) VALUES('stats_paused',?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("1" if paused else "0",),
            )

    def is_paused(self) -> bool:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key='stats_paused'"
        ).fetchone()
        return bool(row and row["value"] == "1")

    # ------------------------------------------------------------------ 总览
    def overview(self) -> dict[str, Any]:
        rule = self.get_current_rule()
        rv = rule["version"] if rule else None
        files = self._conn.execute(
            "SELECT status, COUNT(*) AS n, COALESCE(SUM(row_count),0) AS rows,"
            " COALESCE(SUM(byte_size),0) AS bytes FROM files GROUP BY status"
        ).fetchall()
        by_status = {r["status"]: dict(r) for r in files}
        stat_rows = self._conn.execute(
            "SELECT fs.rule_version, fs.generation, f.file_id, f.generation AS fgen,"
            " f.status FROM file_stats fs JOIN files f ON f.file_id=fs.file_id"
        ).fetchall()
        trusted = stale = 0
        for r in stat_rows:
            if (
                rv is not None
                and r["rule_version"] == rv
                and r["generation"] == r["fgen"]
                and r["status"] == ACTIVE
            ):
                trusted += 1
            else:
                stale += 1
        active_n = by_status.get(ACTIVE, {}).get("n", 0)
        queued = self._conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE status=?", (QUEUED,)
        ).fetchone()["n"]
        return {
            "rule": rule,
            "stats_paused": self.is_paused(),
            "files_by_status": by_status,
            "active_files": active_n,
            "trusted_stats": trusted,
            "stale_stats": stale,
            "files_without_trusted_stats": active_n - trusted,
            "queued_jobs": queued,
        }
