"""端到端：从空目录构建真实分区文件，验证裁剪正确性与统计失效全链路。

关键正确性手段：每次查询都跑全量基准并比对结果行；
另用穷举随机过滤条件做差分测试（计划结果 == 基准结果，永不漏读）。
"""
from __future__ import annotations

import os
import random
import tempfile
import unittest

from minilake.catalog import ACTIVE, Catalog
from minilake.daemon import DemoRuntime
from minilake.demo_data import generate_late_rows, generate_orders
from minilake.paths import Warehouse
from minilake.planner import QueryService
from minilake.services import RebuildPlanner, StatsService, Writer

TABLE = "orders"
PCOL = "dt"


class MinilakeTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="minilake-e2e-")
        self.wh = Warehouse(os.path.join(self.tmp, "wh")).ensure()
        self.cat = Catalog(self.wh.catalog_path)
        self.writer = Writer(self.wh, self.cat, PCOL)
        self.stats = StatsService(self.wh, self.cat)
        self.qs = QueryService(self.wh, self.cat, PCOL)

    def tearDown(self):
        self.cat.close()

    def drain_writer(self):
        while self.writer.process_next() is not None:
            pass

    def drain_stats(self):
        while True:
            r = self.stats.process_next()
            if r is None:
                return
            if isinstance(r, dict) and "backfill" in r and r["backfill"] is None:
                return

    def ingest(self, **kw):
        rows = generate_orders(**kw)
        self.writer.enqueue_ingest(TABLE, rows, rows_per_file=kw.get(
            "rows_per_file", 800))
        self.drain_writer()
        return rows

    def publish(self, cols, desc="t"):
        return self.cat.publish_rule(cols, desc)

    def assertQueryMatches(self, filter_text):
        d = self.qs.run(TABLE, filter_text)
        self.assertTrue(
            d["result_matches_baseline"],
            f"裁剪结果与基准不一致: {filter_text} -> {d}",
        )
        return d


class TestEmptyToPruning(MinilakeTestBase):
    def test_empty_dir_startup_and_conservative_overread(self):
        # 空目录：无规则、无文件
        self.assertEqual(self.cat.list_files(TABLE, status=None), [])
        self.assertIsNone(self.cat.get_current_rule())

        # 先入仓、后发布规则：此时无规则 -> 必须全量保守扫描
        self.ingest(partitions=3, files_per_partition=2, rows_per_file=200)
        files = self.cat.list_files(TABLE)
        self.assertEqual(len(files), 6)
        # 数据文件真实存在且行数正确
        for f in files:
            p = os.path.join(self.wh.root, f["rel_path"])
            self.assertTrue(os.path.isfile(p))
            with open(p) as fh:
                self.assertEqual(f["row_count"], sum(1 for _ in fh))

        d = self.assertQueryMatches("region = 'north'")
        self.assertEqual(d["planned_files"], 6)       # 无规则，全扫
        self.assertEqual(d["baseline_files"], 6)
        self.assertEqual(d["bytes_pruned_pct"], 0.0)

        # 发布规则并补建统计
        self.publish(["region", "category", "amount", "status", "qty"])
        self.drain_stats()
        ov = self.cat.overview()
        self.assertEqual(ov["trusted_stats"], 6)
        self.assertEqual(ov["files_without_trusted_stats"], 0)

        # 有统计后：同一条查询产生真实裁剪，且结果仍与基准一致
        d2 = self.assertQueryMatches("region = 'north'")
        self.assertLess(d2["planned_files"], d2["baseline_files"])
        self.assertGreater(d2["bytes_pruned_pct"], 0.0)
        # 跳过的文件都有明确的可证伪理由
        for s in d2["scans"]:
            if not s["in_plan"]:
                self.assertIn("可证伪", s["skip_reason"])

    def test_real_partition_pruning(self):
        self.ingest(partitions=4, files_per_partition=2, rows_per_file=300)
        self.publish(["region", "amount", "status", "qty", "category"])
        self.drain_stats()
        # 分区等值裁剪：只扫一个分区的文件
        d = self.assertQueryMatches("dt = '2026-11-02' AND amount > 100")
        self.assertEqual(d["planned_files"], 2)
        self.assertEqual(d["baseline_files"], 8)

    def test_skips_are_subset_and_never_miss_rows(self):
        self.ingest(partitions=5, files_per_partition=3, rows_per_file=250)
        self.publish(["region", "category", "amount", "status", "qty"])
        self.drain_stats()
        d = self.assertQueryMatches(
            "region = 'east' AND amount BETWEEN 200 AND 800"
        )
        for s in d["scans"]:
            self.assertTrue(s["in_baseline"])          # 计划 ⊆ 基准
            if s["in_plan"]:
                self.assertIsNotNone(s["scan_reason"])
            else:
                self.assertIsNotNone(s["skip_reason"])


class TestRuleVersionInvalidation(MinilakeTestBase):
    def test_rule_upgrade_invalidates_old_stats(self):
        self.ingest(partitions=3, files_per_partition=2, rows_per_file=200)
        r1 = self.publish(["region", "amount"])
        self.drain_stats()
        self.assertEqual(self.cat.overview()["trusted_stats"], 6)

        # 发布 v2 规则（跟踪列变化）：旧统计全部失可信
        r2 = self.cat.publish_rule(
            ["region", "amount", "status", "qty"], "v2 扩展跟踪列"
        )
        self.assertEqual(r2["version"], r1["version"] + 1)
        ov = self.cat.overview()
        self.assertEqual(ov["trusted_stats"], 0)
        self.assertEqual(ov["files_without_trusted_stats"], 6)

        # 失可信期间：只能多读，不能漏读
        d = self.assertQueryMatches("status = 'paid'")
        self.assertEqual(d["planned_files"], 6)
        for s in d["scans"]:
            if s["in_plan"]:
                self.assertIn("保守多读", s["scan_reason"])

        # 重建后恢复裁剪能力
        self.drain_stats()
        self.assertEqual(self.cat.overview()["trusted_stats"], 6)
        d2 = self.assertQueryMatches("region = 'north'")
        self.assertLess(d2["planned_files"], d2["baseline_files"])

    def test_untracked_predicate_column_forces_scan(self):
        self.ingest(partitions=2, files_per_partition=1, rows_per_file=150)
        self.publish(["region"])
        self.drain_stats()
        # qty 未被规则跟踪 -> 即使统计"可信"也要保守全扫
        d = self.assertQueryMatches("qty > 10")
        self.assertEqual(d["planned_files"], 2)
        self.assertTrue(
            all("未被规则跟踪" in (s["scan_reason"] or "")
                for s in d["scans"] if s["in_plan"])
        )


class TestLateUpdateAndCompaction(MinilakeTestBase):
    def test_late_update_generation_invalidation(self):
        self.ingest(partitions=3, files_per_partition=2, rows_per_file=200)
        self.publish(["region", "amount", "status", "qty", "category"])
        self.drain_stats()
        before = self.assertQueryMatches("region = 'south'")

        # 迟到更新重写 2026-11-02 分区
        extra = generate_late_rows("2026-11-02", n=120)
        self.writer.enqueue_late_update(TABLE, "2026-11-02", extra)
        self.drain_writer()

        active = self.cat.list_files(TABLE, partition_name="dt=2026-11-02")
        self.assertEqual(len(active), 1)                 # 合并为一个新文件
        self.assertEqual(active[0]["generation"], 2)    # 代际递增
        superseded = self.cat.list_files(
            TABLE, status="superseded", partition_name="dt=2026-11-02")
        self.assertEqual(len(superseded), 2)

        # 新文件无统计 -> 该分区保守多读；其余分区仍可裁剪
        ov = self.cat.overview()
        self.assertEqual(ov["files_without_trusted_stats"], 1)
        d = self.assertQueryMatches("region = 'south'")
        new_id = active[0]["file_id"]
        for s in d["scans"]:
            if s["file_id"] == new_id:
                self.assertTrue(s["in_plan"])
                self.assertIn("保守多读", s["scan_reason"])
        # 迟到行确实可读
        all_rows = self.qs.run(TABLE, "status = 'late'")
        self.assertEqual(all_rows["result_rows"], 120)

        # 补建后恢复，且结果与基准一致
        self.drain_stats()
        after = self.assertQueryMatches("region = 'south'")
        self.assertEqual(
            self.cat.overview()["files_without_trusted_stats"], 0
        )
        self.assertLessEqual(after["planned_files"], before["planned_files"])

    def test_compaction_invalidates_stats_and_rebuild(self):
        self.ingest(partitions=3, files_per_partition=3, rows_per_file=150)
        self.publish(["region", "amount", "status", "qty", "category"])
        self.drain_stats()
        total_rows = sum(
            f["row_count"] for f in self.cat.list_files(TABLE))

        self.writer.enqueue_compact(TABLE)
        self.drain_writer()
        actives = self.cat.list_files(TABLE)
        self.assertEqual(len(actives), 3)                # 每分区一个文件
        self.assertTrue(
            all(f["generation"] == 2 for f in actives))
        compacted = self.cat.list_files(TABLE, status="compacted")
        self.assertEqual(len(compacted), 9)

        # 整理后全部失可信 -> 全扫但结果正确，行数不丢失
        d = self.assertQueryMatches("amount >= 500")
        self.assertEqual(d["planned_files"], 3)
        rows_now = sum(f["row_count"] for f in actives)
        self.assertEqual(rows_now, total_rows)

        self.drain_stats()
        d2 = self.assertQueryMatches("region = 'east'")
        self.assertLess(d2["planned_files"], d2["baseline_files"])


class TestPauseAndHotspot(MinilakeTestBase):
    def test_paused_stats_service_never_builds(self):
        self.ingest(partitions=2, files_per_partition=2, rows_per_file=150)
        self.publish(["region", "amount"])
        self.stats.pause()
        self.assertIsNone(self.stats.process_next())    # 暂停时空转
        self.assertEqual(self.cat.overview()["trusted_stats"], 0)

        # 重建作业在暂停状态下入队也不会被处理
        RebuildPlanner(self.wh, self.cat, PCOL).enqueue(TABLE, "full")
        self.assertIsNone(self.stats.process_next())
        jobs = [j for j in self.cat.list_jobs()
                if j["job_type"] == "rebuild_stats"]
        self.assertTrue(jobs and jobs[0]["status"] == "queued")

        self.stats.resume()
        self.drain_stats()
        self.assertEqual(self.cat.overview()["trusted_stats"], 4)

    def test_hotspot_rebuild_cost_and_effectiveness(self):
        self.ingest(partitions=4, files_per_partition=2, rows_per_file=200)
        self.publish(["region", "amount", "status", "qty", "category"])
        # 暂停自动补建，制造"统计缺失 + 查询热点"的初始状态
        self.stats.pause()
        for f in [
            "region = 'north'", "region = 'north' AND amount > 10",
            "status = 'paid'", "region = 'north' OR qty < 3",
        ]:
            self.qs.run(TABLE, f)
        self.assertEqual(self.cat.overview()["trusted_stats"], 0)

        rp = RebuildPlanner(self.wh, self.cat, PCOL)
        preview = rp.preview(TABLE, "hotspot", limit=3)
        self.assertEqual(preview["cost"]["mode"], "hotspot")
        self.assertLessEqual(preview["cost"]["files_to_scan"], 3)
        self.assertGreater(preview["cost"]["bytes_to_scan"], 0)
        # 有效性依据来自真实历史对照查询
        self.assertTrue(preview["effectiveness_basis"])
        self.assertTrue(
            all(e["result_matches"] for e in preview["effectiveness_basis"]))

        # 全量预估覆盖全部 8 个文件
        full = rp.preview(TABLE, "full")
        self.assertEqual(full["cost"]["files_to_scan"], 8)

        # 恢复后执行热点补建：作业只建 ≤3 个文件
        self.stats.resume()
        rp.enqueue(TABLE, "hotspot", limit=3)
        self._drain_jobs_only()
        self.assertLessEqual(self.cat.overview()["trusted_stats"], 3)
        d = self.assertQueryMatches("region = 'north'")
        self.assertTrue(all(s["in_baseline"] for s in d["scans"]))

        # 再全量补齐（作业一次），剩余交给自动补建
        rp.enqueue(TABLE, "full")
        self._drain_jobs_only()
        self.drain_stats()
        self.assertEqual(self.cat.overview()["trusted_stats"], 8)

    def _drain_jobs_only(self):
        """只处理 rebuild_stats 作业，不触发空闲自动补建。"""
        import time
        for _ in range(100):
            job = self.cat.claim_job(["rebuild_stats"])
            if job is None:
                return
            self.stats._do_rebuild(job["payload"])
            self.cat.finish_job(job["job_id"], "done", "完成", {})
            time.sleep(0.01)


class TestDifferentialFuzz(MinilakeTestBase):
    """随机过滤条件差分测试：任何状态下计划结果都必须等于全量基准。"""

    def test_random_filters_never_miss_rows(self):
        self.ingest(partitions=4, files_per_partition=2, rows_per_file=250)
        self.publish(["region", "category", "amount", "status", "qty"])
        rng = random.Random(123)
        regions = ["north", "south", "east", "west", "moon"]   # moon 必不命中
        statuses = ["paid", "shipped", "done", "refunded"]

        def rand_pred():
            kind = rng.randrange(7)
            if kind == 0:
                return f"region = '{rng.choice(regions)}'"
            if kind == 1:
                return f"region != '{rng.choice(regions)}'"
            if kind == 2:
                return f"amount >= {rng.randrange(0, 1200)}"
            if kind == 3:
                return f"amount < {rng.randrange(0, 1200)}"
            if kind == 4:
                vals = rng.sample(statuses, rng.randrange(1, 4))
                return "status IN (" + ",".join(f"'{v}'" for v in vals) + ")"
            if kind == 5:
                return f"qty BETWEEN {rng.randrange(0,10)} AND {rng.randrange(10,25)}"
            return "category IS NULL"

        # 阶段 1：无统计（全保守）
        for _ in range(40):
            f = self._combine(rng, rand_pred)
            self.assertQueryMatches(f)

        # 阶段 2：统计齐全
        self.drain_stats()
        for _ in range(80):
            f = self._combine(rng, rand_pred)
            d = self.assertQueryMatches(f)
            # 至少一部分查询应产生真实裁剪
        self.assertGreater(
            sum(1 for q in self.cat.list_queries(limit=200)
                if q["planned_files"] < q["baseline_files"]),
            0,
        )

        # 阶段 3：规则升级后统计全部失可信，再次随机验证不漏读
        self.cat.publish_rule(["region", "amount", "status"], "v2 收窄")
        for _ in range(40):
            f = self._combine(rng, rand_pred)
            self.assertQueryMatches(f)

    @staticmethod
    def _combine(rng, pred_fn):
        parts = [pred_fn() for _ in range(rng.randrange(1, 4))]
        if len(parts) == 1:
            return parts[0]
        joiner = " AND " if rng.random() < 0.6 else " OR "
        expr = joiner.join(parts)
        if rng.random() < 0.25:
            expr = "NOT (" + expr + ")"
        if rng.random() < 0.3:
            expr = "(" + expr + ") AND " + pred_fn()
        return expr


class TestDemoRuntimeThreading(MinilakeTestBase):
    def test_background_workers_process_jobs(self):
        rt = DemoRuntime(self.wh, self.cat, PCOL, interval=0.05)
        rt.start()
        try:
            rows = generate_orders(partitions=2, files_per_partition=2,
                                   rows_per_file=120)
            self.cat.publish_rule(
                ["region", "amount", "status", "qty", "category"], "r")
            Writer(self.wh, self.cat, PCOL).enqueue_ingest(
                TABLE, rows, rows_per_file=120)
            # 等待异步 worker 完成入仓 + 自动补建
            deadline = 0
            while deadline < 200:
                ov = self.cat.overview()
                if ov["active_files"] == 4 and ov["trusted_stats"] == 4:
                    break
                deadline += 1
                import time; time.sleep(0.05)
            ov = self.cat.overview()
            self.assertEqual(ov["active_files"], 4)
            self.assertEqual(ov["trusted_stats"], 4)
            d = QueryService(self.wh, self.cat, PCOL).run(
                TABLE, "region = 'west'")
            self.assertTrue(d["result_matches_baseline"])
        finally:
            rt.stop()
            # worker 线程全部退出后再由 tearDown 关闭连接，避免跨线程竞态


if __name__ == "__main__":
    unittest.main()
