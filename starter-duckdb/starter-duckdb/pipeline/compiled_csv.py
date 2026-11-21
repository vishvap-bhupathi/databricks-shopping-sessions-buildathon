"""Bridge the Rust reader's owned buffers to PyArrow without copying strings."""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
import ctypes as c
from functools import lru_cache
import os
from pathlib import Path

import pyarrow as pa


class ColumnView(c.Structure):
    _fields_ = [(name, c.c_void_p) for name in ("validity", "offsets", "data")] + [
        ("data_bytes", c.c_size_t), ("nulls", c.c_size_t),
    ]


@lru_cache(maxsize=1)
def library():
    if os.environ.get("SHOPPING_SESSIONS_PYTHON_CSV") == "1":
        return None
    path = Path(__file__).with_name("libstrict_csv.so")
    if not path.exists():
        return None  # Keep the source-only Python entry point usable.
    lib = c.CDLL(path)
    for name, args, result in (
        ("csv_open", [c.c_char_p, c.c_size_t], c.c_void_p),
        ("csv_close", [c.c_void_p], None),
        ("csv_next", [c.c_void_p, c.POINTER(c.c_void_p)], c.c_int),
        ("csv_batch_rows", [c.c_void_p], c.c_size_t),
        ("csv_column", [c.c_void_p, c.c_size_t], ColumnView),
        ("csv_batch_free", [c.c_void_p], None),
    ):
        fn = getattr(lib, name)
        fn.argtypes = args
        fn.restype = result
    return lib


class Batch:
    def __init__(self, lib, pointer):
        self.lib = lib
        self.pointer = pointer

    def __del__(self):
        self.lib.csv_batch_free(self.pointer)

    def as_arrow(self, schema):
        length = self.lib.csv_batch_rows(self.pointer)
        arrays = []
        for index, field in enumerate(schema):
            column = self.lib.csv_column(self.pointer, index)
            validity = pa.foreign_buffer(column.validity, (length + 7) // 8, base=self) if column.nulls else None
            offsets = pa.foreign_buffer(column.offsets, (length + 1) * 8, base=self)
            data = pa.foreign_buffer(column.data, column.data_bytes, base=self)
            arrays.append(pa.Array.from_buffers(field.type, length, [validity, offsets, data], column.nulls))
        return pa.RecordBatch.from_arrays(arrays, schema=schema)


class Reader:
    def __init__(self, lib, path, width):
        self.lib = lib
        self.path = path
        self.pointer = lib.csv_open(os.fsencode(path), width)
        if not self.pointer:
            raise OSError(f"Cannot open CSV part: {path}")

    def __del__(self):
        if self.pointer:
            self.lib.csv_close(self.pointer)

    def read(self):
        while True:
            pointer = c.c_void_p()
            status = self.lib.csv_next(self.pointer, c.byref(pointer))
            if status < 0:
                raise OSError(f"Cannot read UTF-8 CSV part: {self.path}")
            if status == 0:
                return None
            batch = Batch(self.lib, pointer)
            if self.lib.csv_batch_rows(pointer):
                return batch


def compiled_batches(lib, paths, schema, profile, table):
    workers = min(len(paths), max(1, int(os.environ.get("SHOPPING_SESSIONS_CSV_WORKERS", "4"))))
    if not workers:
        return
    # One outstanding batch per file. ctypes releases the GIL for native reads.
    # A FIFO of futures bounds queued data and never affects global SQL dedup.
    unread = iter(paths)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = deque()
        for _ in range(workers):
            reader = Reader(lib, next(unread), len(schema))
            pending.append((reader, pool.submit(reader.read)))
        while pending:
            reader, future = pending.popleft()
            with profile.measure(f"{table}_batch_source"):
                batch = future.result()
            if batch is None:
                path = next(unread, None)
                if path is not None:
                    reader = Reader(lib, path, len(schema))
                    pending.append((reader, pool.submit(reader.read)))
                continue
            pending.append((reader, pool.submit(reader.read)))
            with profile.measure(f"{table}_arrow_conversion"):
                record = batch.as_arrow(schema)
            yield record
