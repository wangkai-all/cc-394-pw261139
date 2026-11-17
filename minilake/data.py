"""数据层：JSONL 分区文件读写与真实列统计构建。

统计由扫描文件中的每一行真实聚合得到（min/max/distinct/非空数），
distinct 超过 ``MAX_DISTINCT`` 后退化为仅 min/max —— 退化只影响裁剪
精度，不影响正确性（精确集合判定会自动回退为保守保留）。
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Iterable, Optional

from .catalog import MAX_DISTINCT

# 常见分区目录名格式：dt=2026-11-01
def partition_name(partition_col: str, partition_val: Any) -> str:
    return f"{partition_col}={partition_val}"


def parse_partition_val(name: str) -> Optional[str]:
    if "=" in name:
        return name.split("=", 1)[1]
    return None


def write_jsonl(path: str, rows: Iterable[dict[str, Any]]) -> int:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    n = 0
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            f.write("\n")
            n += 1
    os.replace(tmp, path)
    return n


def read_jsonl(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def iter_jsonl(path: str) -> Iterable[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def build_column_stats(
    rows: Iterable[dict[str, Any]], columns: list[str]
) -> tuple[dict[str, dict[str, Any]], int]:
    """真实聚合列统计。返回 (stats, row_count)。

    每个列::

        {
          "min": ..., "max": ...,          # 可比类型的最小/最大值
          "distinct": [...] | null,       # null 表示基数超限，集合不完整
          "nnull": int,                    # 非空行数
          "complete": bool                 # distinct 是否精确完整
        }
    """
    agg: dict[str, dict[str, Any]] = {
        c: {"min": None, "max": None, "distinct": set(), "nnull": 0}
        for c in columns
    }
    row_count = 0
    for row in rows:
        row_count += 1
        for c in columns:
            v = row.get(c)
            if v is None:
                continue
            a = agg[c]
            a["nnull"] += 1
            try:
                if a["min"] is None or v < a["min"]:
                    a["min"] = v
                if a["max"] is None or v > a["max"]:
                    a["max"] = v
            except TypeError:
                # 同列类型不可比：放弃 min/max（该列裁剪自动回退保守）
                a["min"] = a["max"] = None
            if a["distinct"] is not None:
                a["distinct"].add(v)
                if len(a["distinct"]) > MAX_DISTINCT:
                    a["distinct"] = None  # 超限：集合不再完整
    out: dict[str, dict[str, Any]] = {}
    for c, a in agg.items():
        distinct = a["distinct"]
        complete = distinct is not None
        out[c] = {
            "min": a["min"],
            "max": a["max"],
            "distinct": sorted(distinct) if complete else None,
            "nnull": a["nnull"],
            "complete": complete,
        }
    return out, row_count


def build_stats_for_file(path: str, columns: list[str]) -> tuple[dict, int, float]:
    t0 = time.perf_counter()
    stats, row_count = build_column_stats(iter_jsonl(path), columns)
    build_ms = (time.perf_counter() - t0) * 1000
    return stats, row_count, build_ms
