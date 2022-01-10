#!/usr/bin/env python3
import argparse
import os
import signal
import sys
from typing import List, Sequence

from .core import ConcurrentSync

__all__ = [
    "main",
    "parse_args",
    "setup_signals",
]


def setup_signals(concurrent_sync: ConcurrentSync) -> None:
    def handler(_, __):
        concurrent_sync.terminate()

    for s in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
        signal.signal(s, handler)


def _validate_targets(targets: List[str]) -> None:
    for target in targets:
        abs_target = os.path.abspath(target)
        if not os.path.exists(abs_target):
            print(f"'{target}' does not exist!", file=sys.stderr)
            sys.exit(2)


def _prepare_sources(sources: List[str]) -> List[str]:
    result = []
    for src in sources:
        abs_src = os.path.abspath(src)
        if not os.path.exists(abs_src):
            print(f"'{src}' does not exist!", file=sys.stderr)
            sys.exit(2)

        if src.endswith(os.sep):
            if os.path.isdir(abs_src):
                children = os.listdir(abs_src)
                result += (os.path.join(abs_src, c) for c in children)
            else:
                print(f"'{src}' is not a directory!", file=sys.stderr)
                sys.exit(2)
        else:
            result.append(abs_src)

    return result


def parse_args(args: Sequence[str]) -> argparse.Namespace:
    description = ("A concurrent sync tool which works similar to rsync. "
                   "It supports syncing given sources with multiple targets concurrently.")
    epilog = "For more information: https://github.com/simsekhalit/concurrent-sync"
    parser = argparse.ArgumentParser("csync", description=description, epilog=epilog)
    parser.add_argument("--max-memory", type=int, help="specify allowed max memory usage as percent")
    parser.add_argument("sources", nargs="+", help="specify source directories/files", metavar="SOURCE")
    parser.add_argument("targets", action="append", help="specify target directory", metavar="TARGET")
    parser.add_argument("--target", action="append", help="specify additional target directories", metavar="TARGET",
                        dest="targets")
    args = parser.parse_args(args)

    args.sources = _prepare_sources(args.sources)
    _validate_targets(args.targets)

    return args


def main(args: Sequence[str]) -> None:
    args = parse_args(args)
    concurrent_sync = ConcurrentSync(args.sources, args.targets, max_memory=args.max_memory)
    setup_signals(concurrent_sync)
    concurrent_sync.run()


if __name__ == "__main__":
    main(sys.argv[1:])
