"""命令行入口。

用法::

    python -m minilake demo --root ./warehouse --port 8000   # 一体化演示
    python -m minilake serve --root ./warehouse              # 仅查询/网页服务
    python -m minilake worker --root ./warehouse writer      # 写入器进程
    python -m minilake worker --root ./warehouse stats       # 统计服务进程
    python -m minilake reset --root ./warehouse              # 清空重建
"""
from __future__ import annotations

import argparse
import signal
import time

from .catalog import Catalog
from .daemon import DemoRuntime
from .paths import Warehouse
from .server import PARTITION_COL, build_server
from .services import StatsService, Writer


def _open(args: argparse.Namespace):
    wh = Warehouse(args.root).ensure()
    cat = Catalog(wh.catalog_path)
    return wh, cat


def cmd_demo(args: argparse.Namespace) -> None:
    wh, cat = _open(args)
    runtime = DemoRuntime(wh, cat, PARTITION_COL, interval=args.interval)
    runtime.start()
    httpd = build_server(wh, cat, host=args.host, port=args.port,
                         runtime=runtime)
    print(f"[demo] 仓库: {wh.root}")
    print(f"[demo] 写入器 + 统计服务 worker 已启动（同进程线程）")
    print(f"[demo] 网页: http://{args.host}:{args.port}/")
    _serve_until_signal(httpd, runtime)


def cmd_serve(args: argparse.Namespace) -> None:
    wh, cat = _open(args)
    httpd = build_server(wh, cat, host=args.host, port=args.port)
    print(f"[serve] 仅查询/网页服务，worker 需另行启动: http://{args.host}:{args.port}/")
    _serve_until_signal(httpd, None)


def cmd_worker(args: argparse.Namespace) -> None:
    wh, cat = _open(args)
    stop = False

    def _handle(_sig, _frm):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    if args.kind == "writer":
        svc = Writer(wh, cat, PARTITION_COL)
        print("[worker] 写入器启动（ingest/late_update/compact）")
        while not stop:
            r = svc.process_next()
            if r:
                print("[writer]", r)
            else:
                time.sleep(args.interval)
    elif args.kind == "stats":
        svc = StatsService(wh, cat)
        print("[worker] 统计服务启动（可通过网页/API 暂停）")
        while not stop:
            if cat.is_paused():
                print("[stats] 已暂停，等待恢复 ...")
                time.sleep(1.0)
                continue
            r = svc.process_next()
            if r:
                print("[stats]", r)
            else:
                time.sleep(args.interval)
    else:
        raise SystemExit(f"未知 worker 类型: {args.kind}")
    cat.close()


def cmd_reset(args: argparse.Namespace) -> None:
    wh = Warehouse(args.root)
    wh.wipe()
    Catalog(wh.catalog_path).close()
    print(f"[reset] 仓库已清空: {wh.root}")


def _serve_until_signal(httpd, runtime) -> None:
    stop = False

    def _handle(_sig, _frm):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    try:
        while not stop:
            httpd.handle_request()
    finally:
        if runtime:
            runtime.stop()
        httpd.cat.close()
        httpd.server_close()


def main() -> None:
    p = argparse.ArgumentParser(prog="minilake")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--root", default="./warehouse", help="仓库根目录")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("demo", parents=[common], help="一体化演示（worker 线程 + 网页）")
    d.add_argument("--host", default="127.0.0.1")
    d.add_argument("--port", type=int, default=8000)
    d.add_argument("--interval", type=float, default=0.2)
    d.set_defaults(func=cmd_demo)

    s = sub.add_parser("serve", parents=[common], help="仅查询/网页服务")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    s.set_defaults(func=cmd_serve)

    w = sub.add_parser("worker", parents=[common], help="独立 worker 进程")
    w.add_argument("kind", choices=["writer", "stats"])
    w.add_argument("--interval", type=float, default=0.25)
    w.set_defaults(func=cmd_worker)

    r = sub.add_parser("reset", parents=[common], help="清空仓库")
    r.set_defaults(func=cmd_reset)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
