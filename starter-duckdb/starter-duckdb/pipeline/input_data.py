"""Read the challenge CSV dialect and stream typed batches into DuckDB."""

from itertools import islice

import pyarrow as pa


BATCH_ROWS = 32_768


def csv_rows(paths, width):
    for path in paths:
        with path.open("rb") as source:
            next(source, None)  # Exactly one header per part.
            for line in source:
                fields = line.removesuffix(b"\n").split(b",")
                if len(fields) != width:
                    continue
                row = []
                for field in fields:
                    if field.startswith(b'"') and field.endswith(b'"') and len(field) >= 2:
                        field = field[1:-1]
                    if b'"' in field:
                        break
                    row.append(field.decode("utf-8") if field else None)
                else:
                    yield row


class LinkedUsers:
    def __init__(self):
        self.parent = {}
        self.size = {}
        self.minimum = {}

    def find(self, user_id):
        while self.parent[user_id] != user_id:
            self.parent[user_id] = self.parent[self.parent[user_id]]
            user_id = self.parent[user_id]
        return user_id

    def union(self, a, b):
        for user_id in (a, b):
            if user_id not in self.parent:
                self.parent[user_id] = user_id
                self.size[user_id] = 1
                self.minimum[user_id] = user_id.encode("utf-8")
        a, b = self.find(a), self.find(b)
        if a == b:
            return
        if self.size[a] < self.size[b]:
            a, b = b, a
        self.parent[b] = a
        self.size[a] += self.size[b]
        self.minimum[a] = min(self.minimum[a], self.minimum[b])

    def aliases(self):
        for user_id in self.parent:
            yield user_id, self.minimum[self.find(user_id)].decode("utf-8")


def arrow_batches(rows, schema, profile, table):
    source = iter(rows)
    while True:
        with profile.measure(f"{table}_batch_source"):
            chunk = list(islice(source, BATCH_ROWS))
        if not chunk:
            return
        with profile.measure(f"{table}_arrow_conversion"):
            arrays = [pa.array(column, type=field.type)
                      for column, field in zip(zip(*chunk), schema, strict=True)]
            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        del chunk
        yield batch


def load_rows(con, profile, table, columns, rows):
    # Preserve nullable text, including raw numeric and timestamp lexemes.
    # DuckDB pulls batches into its own spill-managed temporary table.
    schema = pa.schema([(name, pa.string()) for name in columns])
    with pa.RecordBatchReader.from_batches(schema, arrow_batches(rows, schema, profile, table)) as reader:
        con.register("_input_batch_reader", reader)
        try:
            profile.execute(
                con, f"{table}_load",
                f"CREATE TEMP TABLE {table} AS SELECT * FROM _input_batch_reader",
            )
        finally:
            con.unregister("_input_batch_reader")
