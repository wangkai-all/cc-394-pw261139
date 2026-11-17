"""独立 worker 循环与一体化演示模式。

三种部署形态共用同一份服务代码：

* ``writer``  —— 只跑写入器作业（ingest/late_update/compact）
* ``stats``   —— 只跑统计服务（可被暂停）
* ``demo``    —— 同进程三个线程，便于网页端完整演示
"""
from __future__ import annotations

import threading
from typing import Optional

from .catalog import Catalog
from .paths import Warehouse
from .services import StatsService, Writer


class WorkerLoop(threading.Thread):
    def __init__(self, name: str, tick_fn, interval: float = 0.25):
        super().__init__(name=name, daemon=True)
        self.tick_fn = tick_fn
        self.interval = interval
        self._stop_event = threading.Event()
        self.last_tick: Optional[str] = None

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                r = self.tick_fn()
                self.last_tick = str(r) if r else "idle"
            except Exception as e:  # 单次失败不退出循环
                self.last_tick = f"error: {e!r}"
            self._stop_event.wait(self.interval)

    def stop(self) -> None:
        self._stop_event.set()


class DemoRuntime:
    def __init__(self, warehouse: Warehouse, catalog: Catalog,
                 partition_col: str = "dt", interval: float = 0.2):
        self.writer = Writer(warehouse, catalog, partition_col)
        self.stats = StatsService(warehouse, catalog)
        self.loops = [
            WorkerLoop("writer-worker", self.writer.process_next, interval),
            WorkerLoop("stats-worker", self.stats.process_next, interval),
        ]

    def start(self) -> None:
        for t in self.loops:
            t.start()

    def stop(self) -> None:
        for t in self.loops:
            t.stop()
        for t in self.loops:
            t.join(timeout=5)
        self.loops = []

    def status(self) -> list[dict[str, str]]:
        return [
            {"worker": t.name, "alive": str(t.is_alive()), "last": t.last_tick or "starting"}
            for t in self.loops
        ]
