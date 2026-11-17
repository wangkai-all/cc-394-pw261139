"""仓库路径布局与目录初始化。

布局::

    <warehouse>/
      catalog.db                # SQLite 目录（规则 / 文件 / 统计 / 查询 / 作业）
      data/<table>/<partition>/<file_id>.jsonl
      staging/<job_id>.jsonl    # 写入器的暂存文件（可选）
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Warehouse:
    root: str

    @property
    def catalog_path(self) -> str:
        return os.path.join(self.root, "catalog.db")

    @property
    def data_dir(self) -> str:
        return os.path.join(self.root, "data")

    @property
    def staging_dir(self) -> str:
        return os.path.join(self.root, "staging")

    def table_dir(self, table: str) -> str:
        return os.path.join(self.data_dir, table)

    def partition_dir(self, table: str, partition_name: str) -> str:
        return os.path.join(self.data_dir, table, partition_name)

    def file_path(self, table: str, partition_name: str, file_id: str) -> str:
        return os.path.join(
            self.data_dir, table, partition_name, f"{file_id}.jsonl"
        )

    def ensure(self) -> "Warehouse":
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.staging_dir, exist_ok=True)
        return self

    def wipe(self) -> None:
        """清空整个仓库（仅用于测试 / 演示重置）。"""
        import shutil

        if os.path.isdir(self.root):
            shutil.rmtree(self.root)
        self.ensure()
