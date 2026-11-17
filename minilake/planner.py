"""查询服务：统计裁剪规划 + 实际执行 + 全量基准对照。

核心安全不变量
--------------
文件进入扫描清单的条件（任一满足即扫描，只可能多读）：

* 文件非 active；
* 当前没有已发布规则（无版本，统计版本不明）；
* 文件没有与当前 rule_version / 文件 generation 同时匹配的统计；
* 统计列未覆盖谓词引用列；
* 区域三值判定认为谓词"可能为真"（含信息不足的保守回退）。

每次查询同时执行基准计划（扫描全部 active 文件）并比对结果行，
任何不一致都会在返回值与审计记录中显式标红。
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from . import data as datamod
from .catalog import ACTIVE, Catalog
from .filter import Node, eval_node, node_could_be_true, parse_filter


class QueryPlanError(ValueError):
    pass


# 谓词引用到的列
def _referenced_cols(node: Node, acc: Optional[set[str]] = None) -> set[str]:
    acc = acc if acc is not None else set()
    if node.col is not None:
        acc.add(node.col)
    for c in node.children:
        _referenced_cols(c, acc)
    return acc


# 收集叶子谓词，附带排除原因说明
def _leaves(node: Node) -> list[Node]:
    if node.kind in ("and", "or"):
        out: list[Node] = []
        for c in node.children:
            out.extend(_leaves(c))
        return out
    return [node]


def _leaf_text(node: Node) -> str:
    if node.kind == "cmp":
        return f"{node.col} {node.op} {node.value!r}"
    if node.kind == "between":
        lo, hi = node.values  # type: ignore[misc]
        kw = "NOT BETWEEN" if node.negated else "BETWEEN"
        return f"{node.col} {kw} {lo!r} AND {hi!r}"
    if node.kind == "in":
        kw = "NOT IN" if node.negated else "IN"
        return f"{node.col} {kw} {tuple(node.values or [])!r}"
    if node.kind == "isnull":
        return f"{node.col} IS {'NOT ' if node.negated else ''}NULL"
    return node.kind


class FileZone:
    """单文件区域：分区合成统计 + 该文件列统计；任何缺失都保守回退。"""

    def __init__(
        self,
        *,
        partition_col: Optional[str],
        partition_val: Any,
        stat_columns: Optional[dict[str, dict[str, Any]]],
        stat_row_count: int,
        file_row_count: int,
    ):
        self.pcol = partition_col
        self.pval = partition_val
        self.cols = stat_columns
        self.file_row_count = file_row_count
        # 行数对不上说明统计与文件内容脱节，整体不可信
        self.rowcount_ok = stat_row_count == file_row_count and file_row_count >= 0

    def _stat(self, col: str) -> Optional[dict[str, Any]]:
        if not self.rowcount_ok or self.cols is None:
            return None
        if self.pcol is not None and col == self.pcol:
            # 分区列：所有行同值，合成精确统计
            return {
                "min": self.pval, "max": self.pval,
                "distinct": [self.pval], "nnull": 0 if self.pval is None else 1,
                "complete": True,
            }
        return self.cols.get(col)

    # ---- Zone 接口：信息不足一律 True（除明确的空/NULL 判定） ----
    def has_value(self, col: str) -> bool:
        s = self._stat(col)
        if s is None:
            return True
        return s["nnull"] > 0

    def could_equal(self, col: str, value: Any) -> bool:
        s = self._stat(col)
        if s is None:
            return True
        if value is None:
            return self.could_be_null(col)
        if s["nnull"] == 0:
            return False
        if s["complete"] and s["distinct"] is not None:
            return value in s["distinct"]
        lo, hi = s["min"], s["max"]
        if lo is None or hi is None:
            return True
        try:
            return lo <= value <= hi
        except TypeError:
            return True

    def could_compare(self, col: str, op: str, value: Any) -> bool:
        s = self._stat(col)
        if s is None:
            return True
        if value is None:
            # 与 NULL 做大小比较永不为真（SQL 三值逻辑）
            return False
        if s["nnull"] == 0:
            return False
        lo, hi = s["min"], s["max"]
        if lo is None or hi is None:
            return True
        try:
            if op == "<":
                return lo < value
            if op == "<=":
                return lo <= value
            if op == ">":
                return hi > value
            if op == ">=":
                return hi >= value
        except TypeError:
            return True
        return True

    def in_distinct(self, col: str, values: list[Any]) -> bool:
        s = self._stat(col)
        if s is None:
            return True
        if s["nnull"] == 0:
            return False
        if s["complete"] and s["distinct"] is not None:
            return any(v in s["distinct"] for v in values)
        # 集合不完整：逐个值用 min/max 判断，任一可能即保留
        return any(self.could_equal(col, v) for v in values)

    def has_value_other_than(self, col: str, value: Any) -> bool:
        s = self._stat(col)
        if s is None:
            return True
        if s["nnull"] == 0:
            return False
        if s["complete"] and s["distinct"] is not None:
            return any(v != value for v in s["distinct"])
        lo, hi = s["min"], s["max"]
        if lo is None or hi is None:
            return True
        try:
            return lo != value or hi != value
        except TypeError:
            return True

    def distinct_is_complete(self, col: str) -> bool:
        s = self._stat(col)
        if s is None:
            return False  # 信息不足 -> 调用方按保守处理
        return bool(s["complete"])

    def get_distinct(self, col: str) -> Optional[list[Any]]:
        s = self._stat(col)
        if s is None or not s["complete"]:
            return None
        return list(s["distinct"]) if s["distinct"] is not None else None

    def could_be_null(self, col: str) -> bool:
        if self.pcol is not None and col == self.pcol:
            return self.pval is None
        s = self._stat(col)
        if s is None:
            return True
        # nnull 为非空行数；小于总行数则必然存在 NULL
        return s["nnull"] < self.file_row_count


@dataclass
class PlannedFile:
    file: dict[str, Any]
    scan: bool
    reason: str
    trusted: bool


class QueryService:
    def __init__(self, warehouse, catalog: Catalog, partition_col: str = "dt"):
        self.wh = warehouse
        self.cat = catalog
        self.partition_col = partition_col

    # ------------------------------------------------------------ 规划
    def plan(self, table: str, filter_text: str) -> dict[str, Any]:
        ast = parse_filter(filter_text)
        rule = self.cat.get_current_rule()
        files = self.cat.list_files(table, status=ACTIVE)
        refs = _referenced_cols(ast)
        planned: list[PlannedFile] = []
        for f in files:
            pf = self._plan_file(f, ast, refs, rule)
            planned.append(pf)
        return {
            "table": table,
            "filter": filter_text,
            "rule_version": rule["version"] if rule else None,
            "referenced_columns": sorted(refs),
            "files": [
                {
                    "file_id": p.file["file_id"],
                    "partition_name": p.file["partition_name"],
                    "row_count": p.file["row_count"],
                    "byte_size": p.file["byte_size"],
                    "generation": p.file["generation"],
                    "scan": p.scan,
                    "reason": p.reason,
                    "trusted": p.trusted,
                }
                for p in planned
            ],
            "scan_count": sum(1 for p in planned if p.scan),
            "skip_count": sum(1 for p in planned if not p.scan),
        }

    def _plan_file(
        self,
        f: dict[str, Any],
        ast: Node,
        refs: set[str],
        rule: Optional[dict[str, Any]],
    ) -> PlannedFile:
        if rule is None:
            return PlannedFile(f, True, "无已发布规则，统计版本不明 -> 全量扫描", False)
        stat = self.cat.get_stat(f["file_id"], f["generation"], rule["version"])
        if stat is None:
            return PlannedFile(
                f, True,
                "统计缺失（rule_version=%s 无匹配统计）-> 保守多读" % rule["version"],
                False,
            )
        tracked = set(rule["tracked_columns"])
        missing_cols = {c for c in refs if c != self.partition_col and c not in tracked}
        if missing_cols:
            return PlannedFile(
                f, True,
                f"谓词列 {sorted(missing_cols)} 未被规则跟踪 -> 保守多读",
                False,
            )
        if stat["row_count"] != f["row_count"]:
            return PlannedFile(f, True, "统计行数与文件不一致 -> 统计不可信", False)

        import json as _json
        pval = None
        if f["partition_val"] is not None:
            pval = _json.loads(f["partition_val"])
        zone = FileZone(
            partition_col=self.partition_col,
            partition_val=pval,
            stat_columns=stat["columns"],
            stat_row_count=stat["row_count"],
            file_row_count=f["row_count"],
        )
        if node_could_be_true(ast, zone):
            return PlannedFile(f, True, "区域判定谓词可能为真 -> 扫描", True)
        reasons = [
            _leaf_text(leaf)
            for leaf in _leaves(ast)
            if not node_could_be_true(leaf, zone)
        ]
        return PlannedFile(
            f, False, "统计可证伪: " + " | ".join(reasons[:4]), True
        )

    # ------------------------------------------------------------ 执行 + 对照
    def run(
        self,
        table: str,
        filter_text: str,
        *,
        query_id: Optional[str] = None,
        record: bool = True,
    ) -> dict[str, Any]:
        ast = parse_filter(filter_text)
        plan = self.plan(table, filter_text)
        files = self.cat.list_files(table, status=ACTIVE)
        by_id = {f["file_id"]: f for f in files}

        t0 = time.perf_counter()
        planned_rows: list[dict[str, Any]] = []
        planned_scan_ids = {p["file_id"] for p in plan["files"] if p["scan"]}
        for fid in planned_scan_ids:
            f = by_id[fid]
            for row in datamod.iter_jsonl(
                os.path.join(self.wh.root, f["rel_path"])
            ):
                if eval_node(ast, row):
                    planned_rows.append(row)
        planned_ms = (time.perf_counter() - t0) * 1000

        # 基准：扫描全部 active 文件
        t0 = time.perf_counter()
        baseline_rows: list[dict[str, Any]] = []
        for f in files:
            for row in datamod.iter_jsonl(
                os.path.join(self.wh.root, f["rel_path"])
            ):
                if eval_node(ast, row):
                    baseline_rows.append(row)
        baseline_ms = (time.perf_counter() - t0) * 1000

        # 结果对照（与行序无关；JSON 键排序，兼容 NULL 值）
        def _norm(rows):
            return sorted(
                json.dumps(r, sort_keys=True, ensure_ascii=False, default=str)
                for r in rows
            )
        result_matches = _norm(planned_rows) == _norm(baseline_rows)

        reason_by_id = {p["file_id"]: p["reason"] for p in plan["files"]}
        trusted_by_id = {p["file_id"]: p["trusted"] for p in plan["files"]}
        scans = []
        for f in files:
            in_plan = f["file_id"] in planned_scan_ids
            scans.append({
                "file_id": f["file_id"],
                "partition_name": f["partition_name"],
                "byte_size": f["byte_size"],
                "row_count": f["row_count"],
                "in_plan": in_plan,
                "in_baseline": True,
                "scanned": in_plan,
                "skip_reason": None if in_plan else reason_by_id[f["file_id"]],
                "scan_reason": reason_by_id[f["file_id"]] if in_plan else None,
                "trusted": trusted_by_id[f["file_id"]],
            })

        qid = query_id or Catalog.new_id("q_")
        if record:
            self.cat.record_query(
                query_id=qid,
                table=table,
                filter_text=filter_text,
                result_rows=len(planned_rows),
                planned=[by_id[i] for i in planned_scan_ids],
                baseline=files,
                scans=scans,
                result_matches=result_matches,
            )

        baseline_bytes = sum(f["byte_size"] for f in files)
        planned_bytes = sum(
            f["byte_size"] for f in files if f["file_id"] in planned_scan_ids
        )
        return {
            "query_id": qid,
            "table": table,
            "filter": filter_text,
            "rule_version": plan["rule_version"],
            "result_rows": len(planned_rows),
            "result_matches_baseline": result_matches,
            "plan": plan,
            "planned_files": len(planned_scan_ids),
            "baseline_files": len(files),
            "total_files": len(files),
            "planned_bytes": planned_bytes,
            "baseline_bytes": baseline_bytes,
            "bytes_saved": baseline_bytes - planned_bytes,
            "bytes_pruned_pct": round(
                100 * (baseline_bytes - planned_bytes) / baseline_bytes, 1
            ) if baseline_bytes else 0.0,
            "planned_ms": round(planned_ms, 2),
            "baseline_ms": round(baseline_ms, 2),
            "scans": scans,
            "sample_rows": planned_rows[:20],
        }
