#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import shutil
import stat
import sys
import time
import traceback
from abc import ABCMeta
from collections import deque
from itertools import chain
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
from typing import Dict, Iterable, List, Pattern, Set, Union

import psutil

__all__ = [
    "BaseTask",
    "ConcurrentSync",
    "CopyTask",
    "Logger",
    "Metadata",
    "Monitor",
    "Options",
    "PreparationTask",
    "Reader",
    "Stats",
    "TerminationTask",
    "Worker",
    "Writer"
]

BUFFER_SIZE = 1024 ** 2
POLL_TIME = 0.1


class Stats:
    def __init__(self):
        self.data: Dict[int, List[int, int]] = {}
        self.last_time: int = time.monotonic_ns()
        self.total_size: int = 0
        self.total_time: int = 0

    def add_stat(self, size: int) -> None:
        now = time.monotonic_ns()
        took = now - self.last_time
        timestamp = now // 1_000_000_000
        if timestamp not in self.data:
            self.data[timestamp] = [0, 0]

        self.data[timestamp][0] += size
        self.data[timestamp][1] += took
        self.total_size += size
        self.total_time += took
        self.last_time = now

    def get_current_speed(self) -> float:
        now = int(time.monotonic())
        size = 0
        took = 0

        for timestamp in range(now - 4, now):
            if d := self.data.get(timestamp):
                size += d[0]
                took += d[1]

        return size / (took / 1_000_000_000) if took else 0

    def get_overall_speed(self) -> float:
        return self.total_size / (self.total_time / 1_000_000_000) if self.total_time else 0

    def reset(self) -> None:
        self.data.clear()
        self.last_time = time.monotonic_ns()
        self.total_size = 0
        self.total_time = 0


class Options:
    def __init__(self, dry_run: False, excluded_paths: Set[str] = None, excluded_patterns: List[Pattern] = None):
        self.dry_run: bool = dry_run
        self.excluded_paths: Set[str] = excluded_paths
        self.excluded_patterns: List[Pattern] = excluded_patterns

    def is_path_excluded(self, *paths: str) -> bool:
        if any(p in self.excluded_paths for p in paths):
            return True

        if any(pattern.search(path) for pattern in self.excluded_patterns for path in paths):
            return True


class Metadata:
    def __init__(self, path: str, atime: int = 0, ctime: int = 0, dev: int = 0, ino: int = 0, mode: int = 0,
                 mtime: int = 0, size: int = 0):
        self.atime: int = atime
        self.ctime: int = ctime
        self.dev: int = dev
        self.ino: int = ino
        self.mode: int = mode
        self.mtime: int = mtime
        self.path: str = path
        self.size: int = size

    @classmethod
    def from_stat(cls, path: str, stat_: os.stat_result) -> Metadata:
        return cls(path, stat_.st_atime_ns, stat_.st_ctime_ns, stat_.st_dev, stat_.st_ino, stat_.st_mode,
                   stat_.st_mtime_ns, stat_.st_size)

    def is_mtimes_different(self, other_metadata: Metadata) -> bool:
        if other_metadata:
            return abs(self.mtime - other_metadata.mtime) > 1_000_000_000
        else:
            return True


class Logger:
    _instance: Logger = None
    _lock: Lock = Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls, *args, **kwargs)

        return cls._instance

    def __init__(self):
        self.queue: deque[str] = deque()

    def get_logs(self) -> List[str]:
        logs = []
        while True:
            try:
                logs.append(self.queue.popleft())
            except IndexError:
                return logs

    def log(self, log: str) -> None:
        if log:
            self.queue.append(log)


class Worker(Thread, metaclass=ABCMeta):
    def __init__(self, canceled: Event):
        super().__init__()
        self.canceled: Event = canceled
        self.terminated: Event = Event()

    def is_terminated(self, block: bool = False, timeout: float = None) -> bool:
        if block:
            return self.terminated.wait(timeout)
        else:
            return self.terminated.is_set()


class Reader(Worker):
    def __init__(self, sources: List[str], writers: List[Writer], options: Options, canceled: Event):
        super().__init__(canceled)
        self.buffer_size: int = BUFFER_SIZE
        self.logger: Logger = Logger()
        self.metadata: Dict[str, Metadata] = {}
        self.options: Options = options
        self.skipped_size: int = 0
        self.sources: List[str] = sources.copy()
        self.stats: Stats = Stats()
        self.throttled: bool = False
        self.total_size: int = 0
        self.writers: List[Writer] = writers.copy()

    def _terminate_targets(self) -> None:
        for writer in self.writers:
            writer.terminate()

    def _put_data_to_buffer(self, task: CopyTask, data: bytes) -> None:
        while not self.canceled.is_set():
            task.buffer.maxsize = 16 if self.throttled else 0
            try:
                task.buffer.put(data, timeout=POLL_TIME)
            except Full:
                pass
            else:
                break

    @staticmethod
    def _fadvise(fd: int, is_open: bool) -> None:
        if os.name == "posix":
            try:
                if is_open:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_SEQUENTIAL)
                else:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            except Exception:
                pass

    def _copy_path(self, path: str, tasks: List[CopyTask]) -> None:
        with open(self.metadata[path].path, "br", buffering=BUFFER_SIZE) as f:
            self._fadvise(f.fileno(), True)
            while not self.canceled.is_set() and tasks:
                data = f.read(self.buffer_size)
                tasks = [t for t in tasks if not t.is_done()]
                for task in tasks:
                    self._put_data_to_buffer(task, data)
                if not data:
                    break
                self.stats.add_stat(len(data))
                del data
            self._fadvise(f.fileno(), False)

        while self.throttled:
            if all(t.is_done(True, POLL_TIME) for t in tasks):
                break

    def _sync_targets(self) -> None:
        for path, metadata in self.metadata.items():
            if self.canceled.is_set():
                return
            if stat.S_ISDIR(metadata.mode):
                continue

            tasks = []
            for w in self.writers:
                if w.is_terminated() or not w.is_path_missing(path):
                    w.skipped_size += metadata.size
                else:
                    tasks.append(w.copy_path(path))

            if not tasks:
                self.skipped_size += metadata.size
                continue

            try:
                self._copy_path(path, tasks)
            except Exception:
                self.logger.log(traceback.format_exc())

    def _prepare_targets(self) -> None:
        tasks = [w.prepare_target(self.metadata) for w in self.writers]
        while not self.canceled.is_set() and tasks:
            if tasks[0].is_done(True, POLL_TIME):
                tasks.pop(0)

    def _scan_path(self, rel_src: str, src_root: str) -> Iterable[str]:
        abs_src = os.path.join(src_root, rel_src)
        if self.options.is_path_excluded(rel_src, abs_src):
            return ()

        src_stat = os.stat(abs_src, follow_symlinks=False)
        if stat.S_ISDIR(src_stat.st_mode):
            children = sorted(os.listdir(abs_src), reverse=True)
            self.metadata[rel_src] = Metadata.from_stat(abs_src, src_stat)
            return [os.path.join(rel_src, c) for c in children]
        else:
            self.metadata[rel_src] = Metadata.from_stat(abs_src, src_stat)
            self.total_size += src_stat.st_size
            return ()

    def _scan_source(self, source: str) -> None:
        src_root, rel_src = os.path.split(source)
        stack = [rel_src]
        while not self.canceled.is_set() and stack:
            try:
                path = stack.pop()
                stack.extend(self._scan_path(path, src_root))
            except Exception:
                self.logger.log(traceback.format_exc())

    def _scan_sources(self) -> None:
        for src in self.sources:
            self._scan_source(src)

    def run(self) -> None:
        try:
            for m in (self._scan_sources, self._prepare_targets, self._sync_targets):
                if not self.canceled.is_set():
                    m()
        finally:
            self._terminate_targets()
            self.terminated.set()


class BaseTask(metaclass=ABCMeta):
    def __init__(self, worker_terminated: Event):
        self.done: Event = Event()
        self.worker_terminated: Event = worker_terminated

    def is_done(self, block: bool = False, timeout: float = None) -> bool:
        if block:
            return self.worker_terminated.is_set() or self.done.wait(timeout) or self.worker_terminated.is_set()
        else:
            return self.worker_terminated.is_set() or self.done.is_set()


class CopyTask(BaseTask):
    def __init__(self, worker_terminated: Event, path: str):
        super().__init__(worker_terminated)
        self.buffer: Queue[bytes] = Queue(4096)
        self.path: str = path


class PreparationTask(BaseTask):
    pass


class TerminationTask(BaseTask):
    pass


class Writer(Worker):
    def __init__(self, target: str, options: Options, canceled: Event):
        super().__init__(canceled)
        self.logger: Logger = Logger()
        self.missing_paths: Set[str] = set()
        self.options: Options = options
        self.skipped_size: int = 0
        self.src_metadata: Dict[str, Metadata] = {}
        self.stats: Stats = Stats()
        self.target_metadata: Dict[str, Metadata] = {}
        self.target_root = target
        self.task_queue: Queue[BaseTask] = Queue()
        self.total_count: int = 0
        self.unavailable_paths: Set[str] = set()
        self.updated_dirs: Set[str] = set()
        self.updated_files: Set[str] = set()

    def copy_path(self, path: str) -> CopyTask:
        task = CopyTask(self.terminated, path)
        self.task_queue.put(task)
        return task

    @staticmethod
    def _get_parent_paths(path: str) -> List[str]:
        parents = []
        while True:
            parent = os.path.dirname(path)
            if not parent or parent == os.sep:
                break
            parents.append(parent)
            path = parent

        return parents

    def _is_path_excluded(self, *paths: str) -> bool:
        parents = chain.from_iterable(self._get_parent_paths(p) for p in paths)
        if any(p in self.unavailable_paths for p in chain(paths, parents)):
            return True
        abs_paths = (os.path.join(self.target_root, p) for p in paths)
        abs_parents = (os.path.join(self.target_root, p) for p in parents)
        return self.options.is_path_excluded(*paths, *abs_paths, *parents, *abs_parents)

    def is_path_missing(self, path: str) -> bool:
        return path in self.missing_paths and not self._is_path_excluded(path)

    def prepare_target(self, src_metadata: Dict[str, Metadata]) -> PreparationTask:
        self.src_metadata = src_metadata
        task = PreparationTask(self.terminated)
        self.task_queue.put(task)
        return task

    def _update_dirs(self) -> None:
        updated_dirs = sorted((d for d in self.updated_dirs), key=lambda d: d.count(os.sep), reverse=True)
        for d in updated_dirs:
            self.logger.log(f"[U] {os.path.join(self.target_root, d)}")
            self._update_metadata(d)
        self.updated_dirs.clear()

    def _terminate(self) -> None:
        self._update_dirs()
        self.terminated.set()

    def _is_metadata_different(self, path: str) -> bool:
        if path not in self.src_metadata or path not in self.target_metadata:
            return True
        else:
            return self.target_metadata[path].is_mtimes_different(self.src_metadata[path])

    def _is_file_different(self, path: str) -> bool:
        if path not in self.src_metadata or path not in self.target_metadata:
            return True
        src = self.src_metadata[path]
        target = self.target_metadata[path]
        return src.size != target.size or target.mtime < src.mtime - 1_000_000_000

    def _make_dirs(self, path: str) -> None:
        abs_path = os.path.join(self.target_root, path)
        os.makedirs(abs_path, exist_ok=True)
        self.target_metadata[path] = Metadata(abs_path, mode=stat.S_IFDIR)
        self._add_updated_dirs(path)

    def _prepare_missing_paths(self) -> None:
        for rel_path, src_metadata in self.src_metadata.items():
            abs_target = os.path.join(self.target_root, rel_path)
            if self._is_path_excluded(rel_path, abs_target):
                continue
            elif stat.S_ISDIR(src_metadata.mode):
                if rel_path not in self.target_metadata:
                    self.logger.log(f"[M] {abs_target}")
                    self._make_dirs(rel_path)
                elif not stat.S_ISDIR(self.target_metadata[rel_path].mode):
                    os.remove(abs_target)
                    self._make_dirs(rel_path)
                elif self.target_metadata[rel_path].is_mtimes_different(src_metadata):
                    self._add_updated_dirs(rel_path)
            else:
                if rel_path in self.target_metadata:
                    if stat.S_ISDIR(self.target_metadata[rel_path].mode):
                        shutil.rmtree(abs_target)
                        del self.target_metadata[rel_path]
                        self.missing_paths.add(rel_path)
                        self.updated_files.add(rel_path)
                        self._add_updated_dirs(os.path.dirname(rel_path))
                    elif self._is_file_different(rel_path):
                        os.remove(abs_target)
                        del self.target_metadata[rel_path]
                        self.missing_paths.add(rel_path)
                        self.updated_files.add(rel_path)
                    elif self._is_metadata_different(rel_path):
                        self.logger.log(f"[U] {abs_target}")
                        self._update_metadata(rel_path)
                        self._add_updated_dirs(os.path.dirname(rel_path))
                else:
                    self.missing_paths.add(rel_path)

    def _scan_path(self, rel_path: str) -> Iterable[str]:
        abs_target = os.path.join(self.target_root, rel_path)
        try:
            target_stat = os.stat(abs_target, follow_symlinks=False)
        except FileNotFoundError:
            return ()

        if rel_path not in self.src_metadata:
            self.logger.log(f"[D] {abs_target}")
            if stat.S_ISDIR(target_stat.st_mode):
                shutil.rmtree(abs_target)
            else:
                os.remove(abs_target)
            self._add_updated_dirs(os.path.dirname(rel_path))
            return ()

        if stat.S_ISDIR(target_stat.st_mode):
            self.target_metadata[rel_path] = Metadata.from_stat(abs_target, target_stat)
            return [os.path.join(rel_path, c) for c in sorted(os.listdir(abs_target), reverse=True)]
        else:
            self.target_metadata[rel_path] = Metadata.from_stat(abs_target, target_stat)
            return ()

    def _scan_target(self) -> None:
        stack = [p for p in self.src_metadata if p.count(os.sep) == 0]

        while not self.canceled.is_set() and stack:
            path = stack.pop()
            if not self._is_path_excluded(path):
                try:
                    stack.extend(self._scan_path(path))
                except Exception:
                    self.unavailable_paths.add(path)
                    self.logger.log(traceback.format_exc())

    def _prepare_target(self, task: PreparationTask) -> None:
        self._scan_target()
        self._prepare_missing_paths()
        self.total_count = len(self.missing_paths)
        task.done.set()

    def _add_updated_dirs(self, *paths) -> None:
        for path in paths:
            if path:
                self.updated_dirs.update((path, *self._get_parent_paths(path)))

    def _update_metadata(self, path: str) -> None:
        if not (src_metadata := self.src_metadata.get(path)):
            return

        abs_path = os.path.join(self.target_root, path)
        os.utime(abs_path, ns=(src_metadata.atime, src_metadata.mtime), follow_symlinks=False)
        self.target_metadata[path] = Metadata.from_stat(abs_path, os.stat(abs_path, follow_symlinks=False))

    def _copy_path(self, task: CopyTask) -> None:
        abs_path = os.path.join(self.target_root, task.path)
        self.logger.log(f"[{'U' if task.path in self.updated_files else 'C'}] {abs_path} "
                        f"({self.total_count - len(self.missing_paths) + 1}/{self.total_count})")

        with open(os.path.join(self.target_root, task.path), "bw", buffering=BUFFER_SIZE) as f:
            while not self.canceled.is_set():
                try:
                    data = task.buffer.get(timeout=POLL_TIME)
                except Empty:
                    continue

                if not data:
                    break

                f.write(data)
                self.stats.add_stat(len(data))
                del data
            else:
                return

        self.missing_paths.discard(task.path)
        self._update_metadata(task.path)
        self._add_updated_dirs(os.path.dirname(task.path))

    def _process_task(self, task: BaseTask) -> None:
        if isinstance(task, CopyTask):
            self._copy_path(task)
        elif isinstance(task, PreparationTask):
            self._prepare_target(task)
        elif isinstance(task, TerminationTask):
            self._terminate()
        else:
            raise ValueError(f"Task type '{type(task)}' is not supported!")

    def run(self) -> None:
        try:
            while not (self.terminated.is_set() or self.canceled.is_set()):
                try:
                    task = self.task_queue.get(timeout=POLL_TIME)
                    self._process_task(task)
                    task.done.set()
                except Empty:
                    pass
        finally:
            self.terminated.set()

    def terminate(self) -> None:
        self.task_queue.put(TerminationTask(self.terminated))


class Monitor(Thread):
    def __init__(self, canceled: Event, max_memory: int = None):
        super().__init__(name="Monitor")
        self.canceled: Event = canceled
        self.logger: Logger = Logger()
        self.max_memory: int = min(max_memory, 80) if max_memory is not None else 50
        self.readers: List[Reader] = []
        self.start_time: float = time.monotonic()
        self.writers: List[Writer] = []

    def add_worker(self, worker: Union[Reader, Writer]) -> None:
        if isinstance(worker, Reader):
            self.readers.append(worker)
        elif isinstance(worker, Writer):
            self.writers.append(worker)
        else:
            raise ValueError(f"Worker type '{type(worker)}' is not supported!")

    @staticmethod
    def get_pretty_duration(duration: float) -> str:
        result = []
        hours = int(duration / 3600)
        if hours >= 1:
            result.append(f"{hours}h")
        minutes = int((duration // 60) % 60)
        if minutes >= 1:
            result.append(f"{minutes}m")
        seconds = int(duration % 60)
        if seconds >= 1:
            result.append(f"{seconds}s")

        if result:
            return " ".join(result)
        else:
            return f"{duration:.1f}"

    def _print_results(self) -> None:
        if os.isatty(sys.stdout.fileno()):
            print("\r" + " " * os.get_terminal_size().columns + "\r")

        if self.canceled.is_set():
            status = "Canceled."
        elif any(w.missing_paths for w in self.writers):
            status = "Failed."
        else:
            status = "Done."

        total_size = sum(r.total_size for r in self.readers) if self.readers else 0
        skipped_size = sum(r.skipped_size for r in self.readers) if self.readers else 0
        copied_size = sum(w.stats.total_size for w in self.writers) if self.writers else 0
        duration = time.monotonic() - self.start_time
        copy_speed = copied_size / duration if duration else 0

        print(f"{status}\n"
              f"Total   : {self.get_pretty_size(total_size)}\n"
              f"Skipped : {self.get_pretty_size(skipped_size)} ({int(skipped_size * 100 // total_size)}%)\n"
              f"Copied  : {self.get_pretty_size(copied_size)} ({self.get_pretty_size(copy_speed)}/s)\n"
              f"Duration: {self.get_pretty_duration(duration)}")

    @staticmethod
    def get_pretty_size(size: Union[float, int]) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1000:
                break
            else:
                size /= 1024

        if size < 100:
            return f"{size:.1f} {unit}"
        else:
            return f"{size:.0f} {unit}"

    def _print_stats(self) -> None:
        if not os.isatty(sys.stdout.fileno()):
            return

        readers = []
        writers = []
        i = 0
        j = 0
        for reader in self.readers:
            i += 1
            speed = reader.stats.get_current_speed()
            speed = f"{self.get_pretty_size(speed)}/s" if speed else "💤"
            status = (reader.skipped_size + reader.stats.total_size) * 100 // reader.total_size
            readers.append(f"R{i}: {speed} ({status}%)")

            for writer in reader.writers:
                j += 1
                if not writer.is_terminated():
                    speed = writer.stats.get_current_speed()
                    speed = f"{self.get_pretty_size(speed)}/s" if speed else "💤"
                    status = (writer.skipped_size + writer.stats.total_size) * 100 // reader.total_size
                    writers.append(f"W{j}: {speed} ({status}%)")

        t_width = os.get_terminal_size().columns
        output = " | ".join(chain(readers, writers))[:t_width]
        output = output[:t_width - output.count("💤")]
        print(f"\r{' ' * t_width}\r"
              f"{output:<{t_width - output.count('💤')}}", end="", flush=True)

    def _print_logs(self) -> None:
        logs = self.logger.get_logs()
        if logs:
            if os.isatty(sys.stdout.fileno()):
                print("\r" + " " * os.get_terminal_size().columns, end="\r")
            for log in logs:
                print(log)

    def _check_memory(self) -> None:
        memory = psutil.virtual_memory()
        if memory.percent < self.max_memory:
            for reader in self.readers:
                reader.throttled = False
                if not reader.writers:
                    continue
                read_size = reader.skipped_size + reader.stats.total_size
                buffered_size = read_size - min(w.skipped_size + w.stats.total_size for w in reader.writers)
                if buffered_size < 256 * 1024 ** 2:
                    reader.buffer_size = BUFFER_SIZE
                else:
                    speeds = []
                    for w in (reader, *reader.writers):
                        if not w.is_terminated() and (speed := w.stats.get_current_speed()) > 0:
                            speeds.append(speed)

                    speed = sum(speeds) / len(speeds) if speeds else 0
                    reader.buffer_size = max(int(speed * POLL_TIME * 2), BUFFER_SIZE)
        else:
            for reader in self.readers:
                reader.buffer_size = BUFFER_SIZE
                reader.throttled = True

    def _is_finished(self) -> bool:
        for worker in chain(self.readers, self.writers):
            if not worker.is_terminated(True, POLL_TIME):
                return False
        return True

    def run(self) -> None:
        finished = False
        while not finished:
            finished = self._is_finished()
            self._check_memory()
            self._print_logs()
            self._print_stats()

        self._print_results()


class ConcurrentSync:
    def __init__(self, sources: List[str], targets: List[str], /, dry_run: bool = False,
                 excluded_paths: List[str] = None, excluded_patterns: List[str] = None, max_memory: int = None):
        excluded_paths: Set[str] = {os.path.abspath(p) for p in excluded_paths} if excluded_paths else set()
        excluded_patterns: List[Pattern] = [re.compile(p) for p in excluded_patterns] if excluded_patterns else []
        self.options: Options = Options(dry_run, excluded_paths, excluded_patterns)
        self.sources: List[str] = sources
        self.targets: List[str] = targets

        self.canceled: Event = Event()
        self.monitor: Monitor = Monitor(self.canceled, max_memory)

    def _prepare_and_start_workers(self) -> None:
        writers = [Writer(t, self.options, self.canceled) for t in self.targets]
        for w in writers:
            w.start()
            self.monitor.add_worker(w)

        reader = Reader(self.sources, writers, self.options, self.canceled)
        reader.start()
        self.monitor.add_worker(reader)

        self.monitor.start()

    def run(self) -> None:
        self._prepare_and_start_workers()
        self.monitor.join()

    def terminate(self) -> None:
        self.canceled.set()
