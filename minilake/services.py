"""三个独立运行的服务实现。

* :class:`Writer`        —— 入仓 / 迟到更新 / 文件整理（写 files 表 + 数据文件）
* :class:`StatsService`  —— 统计构建/重建（写 file_stats 表），可暂停
* :class:`RebuildPlanner`—— 全量 / 热点补建的成本与有效性预估

作业通过 SQLite jobs 队协调，三个服务可分别放在不同进程/主机运行，
也可以用 ``daemon.py`` 的线程模式在一个演示进程里同时拉起。
"""
from __future__ import annotations

import os
import time
from typing import Any, Optional

from . import data as datamod
from .catalog import (
    ACTIVE, COMPACTED, DONE, FAILED, SUPERSEDED, Catalog,
)
from .data import partition_name, write_jsonl
from .paths import Warehouse
from .planner import QueryService

# 统计服务处理的作业类型；其余由写入器处理
STATS_JOB = "rebuild_stats"
WRITER_JOBS = ["ingest", "late_update", "compact"]


class ServiceError(RuntimeError):
    pass


# ===================================================================== 写入器
class Writer:
    def __init__(self, warehouse: Warehouse, catalog: Catalog,
                 partition_col: str = "dt"):
        self.wh = warehouse
        self.cat = catalog
        self.pcol = partition_col

    # ---------------------------------------------------------- 作业入口
    def enqueue_ingest(
        self, table: str, rows: list[dict[str, Any]],
        *, rows_per_file: int = 5000, source: str = "api"
    ) -> str:
        return self.cat.enqueue_job("ingest", {
            "table": table, "rows": rows,
            "rows_per_file": rows_per_file, "source": source,
        })

    def enqueue_late_update(
        self, table: str, partition_val: Any,
        extra_rows: list[dict[str, Any]], *, source: str = "late"
    ) -> str:
        return self.cat.enqueue_job("late_update", {
            "table": table, "partition_val": partition_val,
            "extra_rows": extra_rows, "source": source,
        })

    def enqueue_compact(self, table: str) -> str:
        return self.cat.enqueue_job("compact", {"table": table})

    def process_next(self) -> Optional[dict[str, Any]]:
        job = self.cat.claim_job(WRITER_JOBS)
        if job is None:
            return None
        try:
            if job["job_type"] == "ingest":
                cost = self._do_ingest(job["payload"])
            elif job["job_type"] == "late_update":
                cost = self._do_late_update(job["payload"])
            else:
                cost = self._do_compact(job["payload"])
            self.cat.finish_job(job["job_id"], DONE, "完成", cost)
        except Exception as e:  # 作业失败落库，不拖垮 worker
            self.cat.finish_job(job["job_id"], FAILED, repr(e))
            raise
        return {"job_id": job["job_id"], "job_type": job["job_type"], "cost": cost}

    # ---------------------------------------------------------- 具体动作
    def _write_one_file(
        self, table: str, pval: Any, rows: list[dict[str, Any]],
        generation: int
    ) -> dict[str, Any]:
        pname = partition_name(self.pcol, pval)
        file_id = Catalog.new_id("f_")
        rel = os.path.join(
            "data", table, pname, f"{file_id}.jsonl"
        )
        full = os.path.join(self.wh.root, rel)
        write_jsonl(full, rows)
        byte_size = os.path.getsize(full)
        self.cat.register_file(
            table=table, partition_name=pname, partition_val=pval,
            rel_path=rel, row_count=len(rows), byte_size=byte_size,
            generation=generation, file_id=file_id,
        )
        return {
            "file_id": file_id, "partition_name": pname,
            "partition_val": pval, "rows": len(rows),
            "bytes": byte_size, "generation": generation,
        }

    @staticmethod
    def _group_by_partition(
        rows: list[dict[str, Any]], pcol: str
    ) -> dict[Any, list[dict[str, Any]]]:
        groups: dict[Any, list[dict[str, Any]]] = {}
        for r in rows:
            if pcol not in r:
                raise ServiceError(f"行缺少分区列 {pcol!r}: {r}")
            groups.setdefault(r[pcol], []).append(r)
        return groups

    def _do_ingest(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = payload["table"]
        rows = payload["rows"]
        rpf = int(payload.get("rows_per_file") or 5000)
        t0 = time.perf_counter()
        written: list[dict[str, Any]] = []
        for pval, prows in self._group_by_partition(rows, self.pcol).items():
            pname = partition_name(self.pcol, pval)
            gen = self.cat.next_generation(table, pname)
            for i in range(0, len(prows), rpf):
                written.append(
                    self._write_one_file(table, pval, prows[i:i + rpf], gen)
                )
        return {
            "files_written": len(written),
            "rows_written": len(rows),
            "bytes_written": sum(w["bytes"] for w in written),
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            "files": written,
            "note": "新文件无统计 -> 统计服务补建前查询会保守多读",
        }

    def _do_late_update(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = payload["table"]
        pval = payload["partition_val"]
        extra = payload["extra_rows"]
        pname = partition_name(self.pcol, pval)
        t0 = time.perf_counter()

        old = self.cat.list_files(table, status=ACTIVE, partition_name=pname)
        old_rows: list[dict[str, Any]] = []
        for f in old:
            old_rows.extend(
                datamod.read_jsonl(os.path.join(self.wh.root, f["rel_path"]))
            )
        # 迟到数据：同主键覆盖（若提供 id），否则追加
        no_id_rows = [r for r in old_rows if r.get("id") is None]
        by_id = {r["id"]: r for r in old_rows if r.get("id") is not None}
        appended = 0
        for r in extra:
            rid = r.get("id")
            if rid is not None and rid in by_id:
                by_id[rid] = r          # 同主键迟到覆盖
            else:
                if rid is not None:
                    by_id[rid] = r      # 新主键
                else:
                    no_id_rows.append(r)
                appended += 1
        merged = list(by_id.values()) + no_id_rows

        gen = self.cat.next_generation(table, pname)
        written = self._write_one_file(table, pval, merged, gen)
        self.cat.replace_files((f["file_id"] for f in old), SUPERSEDED)
        return {
            "partition": pname,
            "old_files": [f["file_id"] for f in old],
            "old_rows": len(old_rows) - appended,
            "late_rows": len(extra),
            "appended": appended,
            "new_file": written["file_id"],
            "generation": gen,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            "note": "generation 递增 -> 旧统计自动失可信，查询回退多读直至补建",
        }

    def _do_compact(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = payload["table"]
        t0 = time.perf_counter()
        actives = self.cat.list_files(table, status=ACTIVE)
        partitions = sorted({f["partition_name"] for f in actives})
        replaced: list[dict[str, Any]] = []
        for pname in partitions:
            pfiles = [f for f in actives if f["partition_name"] == pname]
            if len(pfiles) <= 1:
                continue
            rows: list[dict[str, Any]] = []
            for f in pfiles:
                rows.extend(
                    datamod.read_jsonl(os.path.join(self.wh.root, f["rel_path"]))
                )
            pval = pname.split("=", 1)[1] if "=" in pname else None
            gen = self.cat.next_generation(table, pname)
            new_file = self._write_one_file(table, pval, rows, gen)
            self.cat.replace_files((f["file_id"] for f in pfiles), COMPACTED)
            replaced.append({
                "partition": pname,
                "removed_files": [f["file_id"] for f in pfiles],
                "new_file": new_file["file_id"],
                "rows": len(rows),
            })
        return {
            "partitions_compacted": len(replaced),
            "details": replaced,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            "note": "整理后新 generation 无统计 -> 旧统计失效，等待统计服务补建",
        }


# ===================================================================== 统计服务
class StatsService:
    def __init__(self, warehouse: Warehouse, catalog: Catalog):
        self.wh = warehouse
        self.cat = catalog

    def pause(self) -> None:
        self.cat.set_paused(True)

    def resume(self) -> None:
        self.cat.set_paused(False)

    def build_for_file(self, file_id: str) -> dict[str, Any]:
        """为单个 active 文件按当前规则构建统计（真实逐行聚合）。"""
        rule = self.cat.get_current_rule()
        if rule is None:
            raise ServiceError("尚未发布规则，无法构建统计")
        f = self.cat.get_file(file_id)
        if f is None:
            raise ServiceError(f"文件不存在: {file_id}")
        if f["status"] != ACTIVE:
            raise ServiceError(f"文件 {file_id} 非 active（{f['status']}），跳过")
        stats, row_count, build_ms = datamod.build_stats_for_file(
            os.path.join(self.wh.root, f["rel_path"]),
            rule["tracked_columns"],
        )
        if row_count != f["row_count"]:
            raise ServiceError(
                f"文件 {file_id} 行数漂移: 目录 {f['row_count']} vs 实际 {row_count}"
            )
        self.cat.upsert_stat(
            file_id=file_id, generation=f["generation"],
            rule_version=rule["version"], columns=stats,
            row_count=row_count, build_ms=build_ms,
        )
        return {
            "file_id": file_id, "rule_version": rule["version"],
            "generation": f["generation"], "rows": row_count,
            "build_ms": round(build_ms, 2),
        }

    def _select_targets(
        self,
        active: list[dict[str, Any]],
        trusted: set[str],
        table: str,
        mode: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """选择待补建文件。full=全部未可信；hotspot=历史扫描 top-N 中未可信的。"""
        untrusted = [f for f in active if f["file_id"] not in trusted]
        if mode != "hotspot":
            return untrusted
        hot = self.cat.hotspot_files(table, limit=limit)
        if not hot:
            # 无查询历史：退化为任意 N 个未可信文件，保证作业仍有确定产出
            return untrusted[:limit]
        rank = {h["file_id"]: i for i, h in enumerate(hot)}
        picked = [f for f in untrusted if f["file_id"] in rank]
        picked.sort(key=lambda f: rank[f["file_id"]])
        return picked

    def _backfill_one(self) -> Optional[dict[str, Any]]:
        """补建一个缺可信统计的 active 文件。"""
        rule = self.cat.get_current_rule()
        if rule is None:
            return None
        trusted = self.cat.trusted_file_ids(rule["version"])
        for f in self.cat.list_files(status=ACTIVE):
            if f["file_id"] not in trusted:
                return self.build_for_file(f["file_id"])
        return None

    def process_next(self) -> Optional[dict[str, Any]]:
        """处理一个统计作业，或补建一个缺失文件。暂停时直接返回 None。"""
        if self.cat.is_paused():
            return None
        job = self.cat.claim_job([STATS_JOB])
        if job is not None:
            try:
                cost = self._do_rebuild(job["payload"])
                self.cat.finish_job(job["job_id"], DONE, "完成", cost)
            except Exception as e:
                self.cat.finish_job(job["job_id"], FAILED, repr(e))
                raise
            return {"job_id": job["job_id"], "job_type": STATS_JOB, "cost": cost}
        bf = self._backfill_one()
        return {"backfill": bf} if bf else None

    def _do_rebuild(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.cat.is_paused():
            raise ServiceError("统计服务已暂停")
        rule = self.cat.get_current_rule()
        if rule is None:
            raise ServiceError("尚未发布规则")
        table = payload["table"]
        mode = payload.get("mode", "full")
        t0 = time.perf_counter()

        active = self.cat.list_files(table, status=ACTIVE)
        trusted = self.cat.trusted_file_ids(rule["version"])
        targets = self._select_targets(
            active, trusted, table, mode, payload.get("limit", 20))

        built: list[dict[str, Any]] = []
        rows_scanned = 0
        bytes_scanned = 0
        for f in targets:
            r = self.build_for_file(f["file_id"])
            built.append(r)
            rows_scanned += f["row_count"]
            bytes_scanned += f["byte_size"]
        return {
            "mode": mode,
            "rule_version": rule["version"],
            "files_built": len(built),
            "files_skipped_already_trusted": len(active) - len(targets),
            "rows_scanned": rows_scanned,
            "bytes_scanned": bytes_scanned,
            "build_elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            "built": built,
        }


# ===================================================================== 重建规划
class RebuildPlanner:
    """重建前的成本与有效性预估（dry-run，不真正扫数据）。"""

    def __init__(self, warehouse: Warehouse, catalog: Catalog,
                 partition_col: str = "dt"):
        self.qs = QueryService(warehouse, catalog, partition_col)
        self.cat = catalog

    def preview(self, table: str, mode: str = "full",
                limit: int = 20) -> dict[str, Any]:
        rule = self.cat.get_current_rule()
        if rule is None:
            raise ServiceError("尚未发布规则")
        active = self.cat.list_files(table, status=ACTIVE)
        trusted = self.cat.trusted_file_ids(rule["version"])
        untrusted = [f for f in active if f["file_id"] not in trusted]
        # 与实际执行共用同一选择逻辑，预估即结果
        targets = StatsService(self.qs.wh, self.cat)._select_targets(
            active, trusted, table, mode, limit)

        cost = {
            "mode": mode,
            "files_to_scan": len(targets),
            "rows_to_scan": sum(f["row_count"] for f in targets),
            "bytes_to_scan": sum(f["byte_size"] for f in targets),
            "remaining_untrusted_after": len(untrusted) - len(targets),
        }

        # 有效性：用最近查询做"补建后假设"dry-run（临时把目标文件视为有统计
        # 无法真实模拟区域内容，因此有效性依据改为重放历史查询的实际节省区间）
        recent = self.cat.list_queries(limit=10)
        effectiveness = []
        for q in recent:
            if q["table_name"] != table:
                continue
            effectiveness.append({
                "query_id": q["query_id"],
                "filter": q["filter_text"],
                "baseline_files": q["baseline_files"],
                "planned_files_before": q["planned_files"],
                "saved_bytes_before": q["baseline_bytes"] - q["planned_bytes"],
                "result_matches": bool(q["result_matches"]),
            })
        return {
            "cost": cost,
            "effectiveness_basis": effectiveness,
            "note": (
                "成本=目标文件行数/字节数（全量重读聚合）；"
                "有效性依据=历史对照查询的真实扫描节省，补建后下一轮查询即可验证"
            ),
        }

    def enqueue(self, table: str, mode: str = "full", limit: int = 20) -> str:
        return self.cat.enqueue_job(STATS_JOB, {
            "table": table, "mode": mode, "limit": limit,
        })
