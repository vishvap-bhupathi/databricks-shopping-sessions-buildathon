"""Read the challenge CSV dialect and stage parsed values for DuckDB."""

import json


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


def load_rows(con, profile, directory, table, columns, rows):
    # JSON escapes embedded CRs, tabs, and control characters without changing
    # their values. Explicit types also keep IDs and numeric lexemes as strings.
    path = directory / f"{table}.jsonl"
    column_types = ", ".join(f"'{name}': 'VARCHAR'" for name in columns)
    with profile.measure(f"{table}_staging") as stage:
        with path.open("w", encoding="utf-8") as staged:
            for row in rows:
                staged.write(json.dumps(dict(zip(columns, row)), ensure_ascii=False) + "\n")
        staged_bytes = path.stat().st_size
        if stage is not None:
            stage["bytes"] = staged_bytes
    if staged_bytes:
        profile.execute(
            con, f"{table}_load",
            f"""
            CREATE TEMP TABLE {table} AS
            SELECT * FROM read_json(?, format='newline_delimited', columns={{{column_types}}})
            """,
            [str(path)],
        )
    else:
        fields = ", ".join(f"{name} VARCHAR" for name in columns)
        con.execute(f"CREATE TEMP TABLE {table} ({fields})")
    path.unlink()
