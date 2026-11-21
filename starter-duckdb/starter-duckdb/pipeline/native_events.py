"""Typed Arrow bridge for partitioned native event preprocessing."""

from contextlib import closing
import ctypes as c
import os

import duckdb
import pyarrow as pa

from compiled_csv import ColumnView, library


SCHEMA = pa.schema([
    ("person_id", pa.large_string()), ("event_ms", pa.int64()),
    ("purchase_cents", pa.int64()), ("category", pa.large_string()),
])


class Stats(c.Structure):
    _fields_ = [(name, c.c_uint64) for name in (
        "input_bytes", "indexed_rows", "index_capacity_bytes", "largest_partition",
    )] + [("index_seconds", c.c_double), ("reduction_seconds", c.c_double)]


class Batch:
    def __init__(self, lib, pointer):
        self.lib = lib
        self.pointer = pointer

    def __del__(self):
        self.lib.ev_free(self.pointer)

    def arrow(self):
        count = self.lib.ev_rows(self.pointer)
        arrays = []
        for index, field in enumerate(SCHEMA):
            column = self.lib.ev_column(self.pointer, index)
            buffers = [pa.foreign_buffer(column.validity, (count + 7) // 8, base=self)
                       if column.nulls else None]
            if column.offsets:
                buffers.append(pa.foreign_buffer(column.offsets, (count + 1) * 8, base=self))
            buffers.append(pa.foreign_buffer(column.data, column.data_bytes, base=self))
            arrays.append(pa.Array.from_buffers(field.type, count, buffers, column.nulls))
        return pa.RecordBatch.from_arrays(arrays, schema=SCHEMA)


def rows(lib, reader):
    while True:
        pointer = c.c_void_p()
        status = lib.ev_next(reader, c.byref(pointer))
        if status == 0:
            return
        if status < 0:
            raise ValueError("native event source could not complete")
        yield Batch(lib, pointer).arrow()


def load_native_events(con, profile, paths, start, end):
    if os.environ.get("SHOPPING_SESSIONS_REFERENCE_PREPROCESSING") == "1":
        return False
    lib = library()
    if lib is None:
        return False
    ptr, size = c.c_void_p, c.c_size_t
    for name, args, result in (
        ("ev_dims_new", [], ptr),
        ("ev_dims_add", [ptr, c.c_bool, size, ptr, ptr, ptr, ptr], None),
        ("ev_dims_free", [ptr], None),
        ("ev_open", [c.POINTER(c.c_char_p), size, ptr, c.c_int64, c.c_int64, size], ptr),
        ("ev_close", [ptr], None),
        ("ev_stats", [ptr], Stats),
        ("ev_next", [ptr, c.POINTER(ptr)], c.c_int),
        ("ev_rows", [ptr], size),
        ("ev_column", [ptr, size], ColumnView),
        ("ev_free", [ptr], None),
    ):
        f = getattr(lib, name)
        f.argtypes, f.restype = args, result
    with profile.measure("event_native_dimensions"):
        dims = lib.ev_dims_new()
        try:
            for product, sql in (
                (False, "SELECT user_id, person_id FROM aliases"),
                (True, "SELECT product_id, category FROM products WHERE product_id IS NOT NULL AND category IS NOT NULL"),
            ):
                with con.execute(sql).to_arrow_reader(32_768) as batches:
                    for batch in batches:
                        if not batch.num_rows:
                            continue
                        addresses = []
                        for array in batch.columns:
                            assert pa.types.is_string(array.type) and array.null_count == 0
                            _, offsets, values = array.buffers()
                            addresses.extend((offsets.address + array.offset * 4, values.address))
                        lib.ev_dims_add(dims, product, batch.num_rows, *addresses)
            files = (c.c_char_p * len(paths))(*(os.fsencode(path) for path in paths))
            threads = max(1, int(os.environ.get("NCPU", "8")))
            requested = int(os.environ.get("SHOPPING_SESSIONS_EVENT_WORKERS", str(threads)))
            workers = min(8, threads, max(1, requested), max(1, len(paths)))
            native = lib.ev_open(files, len(paths), dims, start, end, workers)
            dims = None  # Ownership transferred to the native worker.
        finally:
            if dims is not None:
                lib.ev_dims_free(dims)
    try:
        with closing(rows(lib, native)) as batches, pa.RecordBatchReader.from_batches(SCHEMA, batches) as stream:
            con.register("_native_events", stream)
            try:
                profile.execute(con, "event_native_preprocessing", """
                    CREATE TEMP TABLE events AS SELECT * FROM _native_events
                """)
                if profile.enabled:
                    stats = lib.ev_stats(native)
                    profile.stages["event_native_preprocessing"].update(
                        (name, getattr(stats, name)) for name, _ in Stats._fields_
                    )
            finally:
                con.unregister("_native_events")
    except duckdb.Error as error:
        if "native event source could not complete" not in str(error):
            raise
        return False
    finally:
        lib.ev_close(native)
    return True
