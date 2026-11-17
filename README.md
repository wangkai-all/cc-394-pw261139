# minilake — 带版本化列统计的迷你湖仓

入仓 JSONL 分区文件附带**列统计**供查询跳过不相关文件。规则升级、迟到更新、
文件整理都会让旧统计失效；三个服务独立运行，**统计缺失或版本不明时只能
多读，绝不漏读**。每次查询同时执行全量基准扫描并比对结果，网页可查看实际
扫描清单与基准差异。

纯 Python 3.11 标准库实现（SQLite 目录 + HTTP 服务 + 原生网页），无第三方依赖。

## 运行

```bash
# 一体化演示：写入器 + 统计服务（后台线程）+ 查询/网页服务
python3 -m minilake demo --root ./warehouse --port 8000
# 打开 http://127.0.0.1:8000/

# 三进程独立部署（共享同一个 --root 目录与 SQLite 目录）
python3 -m minilake serve  --root ./warehouse --port 8000
python3 -m minilake worker --root ./warehouse writer
python3 -m minilake worker --root ./warehouse stats

python3 -m minilake reset --root ./warehouse   # 清空
python3 -m unittest discover -s tests -t .     # 测试（23 个）
```

## 架构

```
Writer(写入器)          StatsService(统计服务)        QueryService(查询服务)
 ingest 写 JSONL 文件    build/backfill 列统计          解析过滤 -> 统计裁剪规划
 late_update 重写分区    可暂停(pause/resume)           实际读文件执行谓词
 compact 合并小文件      full / hotspot 重建            同步全量基准 + 结果对照
       \                       |                              /
        \________________  SQLite catalog  ________________/
          rules(规则版本) files(文件/代际/状态) file_stats(统计双版本)
          queries/query_scans(审计) jobs(作业队列) meta(暂停标志)
```

### 统计可信判定（`is_trusted`，三重门禁）

一条统计只有同时满足以下条件才会被用于裁剪，否则文件进入扫描清单：

1. 统计的 `rule_version` == 当前已发布规则版本（**规则升级即整体失可信**）；
2. 统计的 `generation` == 文件当前 `generation`（**迟到更新/整理重写即失效**）；
3. 文件状态为 `active`（superseded/compacted 不参与查询）。

此外：谓词引用了规则未跟踪的列、统计行数与文件不符、distinct 基数超限等
情况一律回退为保守保留。所有"信息不足"的区域接口都返回 `True`（可能为真）。

### 失效场景如何保证不漏读

| 事件 | 目录变化 | 查询行为 |
|---|---|---|
| 发布新规则 | `rules.version` 自增 | 旧 `rule_version` 统计全部失可信 → 全量保守多读 |
| 迟到更新 | 分区重写为新 `generation`，旧文件 `superseded` | 新文件无匹配统计 → 该分区多读，其余仍可裁剪 |
| 文件整理 | 合并文件为新 `generation`，旧文件 `compacted` | 新文件无统计 → 全扫，直到统计服务补建 |
| 统计服务暂停 | `stats_paused=1` | 缺统计文件持续保守多读，正确性不受影响 |

## 过滤与裁剪

- 语法：`AND/OR/NOT`、括号、`= != < <= > >=`、`BETWEEN`、`IN`、`IS [NOT] NULL`。
- 解析后转 **NNF（否定下推）**，裁剪阶段无 NOT 节点。
- 区域三值判定基于 min/max 与精确 distinct（基数超 `MAX_DISTINCT=200` 后
  distinct 退化为不完整，只影响精度不影响正确性）。
- 分区列（`dt`）由目录元数据合成精确统计，支持分区等值裁剪。

## 两种统计重建（均展示成本与有效性依据）

- **全量重建**：扫描所有缺可信统计的 active 文件。
- **热点补建**：按 `query_scans` 审计出的历史扫描 top-N 文件补建。
- `POST /api/rebuild/preview` 在执行前给出成本（文件数/行数/字节数）与
  有效性依据（历史对照查询的真实节省、结果是否一致），dry-run 不扫数据。

## 对照查询与审计

`POST /api/query` 一次请求产出两份计划：

- **统计裁剪计划**：按可信统计跳过文件后实际读取；
- **基准计划**：扫描全部 active 文件。

两者结果行做无序比对（`result_matches_baseline`），并把每个文件的
`in_plan / in_baseline / scanned / skip_reason / scan_reason` 写入
`query_scans`。计划清单恒为基准清单的子集——只允许多读。网页"查询历史"
可查看每次查询的扫描清单与基准差异。

## 验证

- `tests/test_filter.py`：解析、NNF、行求值、区域裁剪（含 NOT IN / NULL 边界）。
- `tests/test_e2e.py`：空目录建真实分区文件、规则升级失效、迟到更新、
  整理、暂停、热点/全量重建、线程化 worker，以及 **160 条随机过滤条件的
  差分测试**（计划结果必须恒等于全量基准）。
- 开发期另跑过 960 条随机查询（8 个种子）差分验证，全部与基准一致。

数据文件为真实 JSONL（`warehouse/data/<table>/<分区>/<file>.jsonl`），
裁剪数量由规划器读统计后现场计算，不存在硬编码的扫描数量。
