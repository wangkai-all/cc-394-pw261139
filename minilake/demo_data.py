"""演示数据生成：确定性的订单行，分区与文件分布带倾斜，便于展示真实裁剪。

不是硬编码"扫描数量"——生成的是真实 JSONL 数据文件，裁剪完全由
查询规划器读取统计后现场计算。
"""
from __future__ import annotations

import random
from typing import Any

REGIONS = ["north", "south", "east", "west"]
CATEGORIES = ["book", "toy", "food", "tech", "cloth"]
STATUS_VALUES = ["paid", "shipped", "done", "refunded"]


def generate_orders(
    *,
    partitions: int = 6,
    files_per_partition: int = 2,
    rows_per_file: int = 800,
    seed: int = 42,
    start_date: str = "2026-11-01",
    skew: bool = True,
) -> list[dict[str, Any]]:
    """生成订单行。

    skew=True 时 region 呈确定性区域分布：每个分区只出现 2 个 region
    （主导 region + 一个固定邻区），因此不同分区文件的 region 统计
    值域真实不同，min/max 与 distinct 裁剪都能观察到跳过效果。
    """
    rng = random.Random(seed)
    rows: list[dict[str, Any]] = []
    oid = 100000
    base = _parse_date(start_date)
    total = partitions * files_per_partition * rows_per_file
    for p in range(partitions):
        dt = _add_days(base, p)
        dominant = REGIONS[p % len(REGIONS)]
        # 该分区允许出现的 region 集合（区域化分布）
        neighbor = REGIONS[(p + 1) % len(REGIONS)]
        local_regions = (
            [dominant, neighbor] if skew else list(REGIONS)
        )
        for _f in range(files_per_partition):
            for _ in range(rows_per_file):
                if skew and rng.random() < 0.85:
                    region = dominant
                else:
                    region = rng.choice(local_regions)
                amount = round(rng.uniform(1.0, 999.0), 2)
                # 少量 NULL，验证 NULL 安全
                category = (
                    None if rng.random() < 0.03
                    else rng.choice(CATEGORIES)
                )
                rows.append({
                    "id": oid,
                    "dt": dt,
                    "region": region,
                    "category": category,
                    "amount": amount,
                    "qty": rng.randint(1, 20),
                    "status": rng.choice(STATUS_VALUES),
                    "seq": total - oid,  # 辅助列
                })
                oid += 1
    return rows


def generate_late_rows(
    partition_val: str, *, n: int = 100, seed: int = 7
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        rows.append({
            "id": 900000 + i,
            "dt": partition_val,
            "region": rng.choice(REGIONS),
            "category": rng.choice(CATEGORIES),
            "amount": round(rng.uniform(1.0, 500.0), 2),
            "qty": rng.randint(1, 9),
            "status": "late",
            "seq": i,
        })
    return rows


def _parse_date(s: str):
    from datetime import date
    y, m, d = map(int, s.split("-"))
    return date(y, m, d)


def _add_days(d, n: int) -> str:
    from datetime import timedelta
    return (d + timedelta(days=n)).isoformat()
