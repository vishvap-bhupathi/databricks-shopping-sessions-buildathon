"""Run the partitioned Rust pipeline and encode its reduced outputs as Parquet."""
import os
from pathlib import Path
import resource
import subprocess


def run_engine(con, profile, data, scratch, output, start, end, threads):
    binary = Path("/app/native-engine")
    if os.environ.get("SHOPPING_SESSIONS_REFERENCE_ENGINE") == "1" or not binary.is_file():
        return False
    with profile.measure("native_execution") as stage:
        result = subprocess.run([binary, data, scratch, str(start), str(end), str(threads)])
    if stage is not None:
        stage["peak_child_rss_bytes"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024
    if result.returncode:
        return False

    for table, prefix, columns, projection in (
        ("user_daily", "user",
         "uid: 'VARCHAR', d: 'INTEGER', sessions: 'BIGINT', events: 'BIGINT', "
         "revenue_cents: 'BIGINT', category: 'VARCHAR'",
         "decode(unhex(uid)) AS user_id, DATE '1970-01-01'+d AS day, "
         "sessions, events, revenue_cents, decode(unhex(category)) AS top_category"),
        ("campaign_daily", "campaign",
         "id: 'VARCHAR', ch: 'VARCHAR', d: 'INTEGER', s: 'BIGINT', p: 'BIGINT', "
         "r: 'BIGINT', a: 'BIGINT'",
         "decode(unhex(id)) AS campaign_id, decode(unhex(ch)) AS channel, "
         "DATE '1970-01-01'+d AS day, sum(s)::BIGINT AS sessions, "
         "sum(p)::BIGINT AS purchasing_sessions, sum(r)::BIGINT AS revenue_cents, "
         "sum(a)::BIGINT AS assisted_sessions"),
    ):
        dest = output / table
        dest.mkdir(parents=True, exist_ok=True)
        grouping = "GROUP BY id, ch, d" if prefix == "campaign" else ""
        query = f"""COPY (SELECT {projection} FROM read_csv(?, columns={{{columns}}},
                    auto_detect=false, delim=',', header=false) {grouping})
                    TO ? (FORMAT PARQUET)"""
        # DuckDB binds the COPY destination before SELECT parameters.
        profile.execute(con, table + "_parquet", query,
                        [str(dest / "part-0.parquet"), str(scratch / (prefix + "-*.csv"))])
    return True
