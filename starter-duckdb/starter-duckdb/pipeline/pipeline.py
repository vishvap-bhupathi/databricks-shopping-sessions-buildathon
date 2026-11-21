#!/usr/bin/env python3
"""Build the Shopping Sessions daily Parquet tables with DuckDB."""

import json
import os
from pathlib import Path
import tempfile

import duckdb

from input_data import LinkedUsers, csv_rows, load_rows
from stage_profile import StageProfile


LOOKBACK_MS = 604_800_000
SESSION_GAP_MS = 1_800_000
LATE_MS = 86_400_000

EVENT_COLUMNS = (
    "event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"
)
CLICK_COLUMNS = ("click_id", "user_id", "campaign_id", "ts")


def load_dimensions(con, profile, data, staging):
    linked = LinkedUsers()
    with profile.measure("link_parsing_and_union_find"):
        for a, b in csv_rows(sorted((data / "links").glob("*.csv")), 2):
            if a is not None and b is not None:
                linked.union(a, b)
    load_rows(con, profile, staging, "aliases", ("user_id", "person_id"), linked.aliases())
    for table, columns in (
        ("products", ("product_id", "category")),
        ("campaigns", ("campaign_id", "channel")),
    ):
        load_rows(con, profile, staging, table, columns, csv_rows([data / f"{table}.csv"], 2))


def define_timestamp_parser(con):
    con.execute("""
        CREATE TEMP MACRO timestamp_ms(value) AS (
            CASE
                WHEN regexp_full_match(value, '[0-9]+')
                THEN try_cast(coalesce(nullif(ltrim(value, '0'), ''), '0') AS BIGINT)
                WHEN regexp_full_match(
                    value,
                    '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]'
                    '(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])'
                )
                THEN epoch_ms(try_strptime(substr(value, 1, 19), '%Y-%m-%dT%H:%M:%S'))
                    - CASE substr(value, 20, 1)
                        WHEN 'Z' THEN 0
                        ELSE (CASE substr(value, 20, 1) WHEN '+' THEN 1 ELSE -1 END)
                             * (cast(substr(value, 21, 2) AS INTEGER) * 60
                                + cast(substr(value, 24, 2) AS INTEGER)) * 60000
                      END
            END
        )
    """)


def clean_events(con, profile, data, staging, window_start, window_end):
    load_rows(con, profile, staging, "raw_events", EVENT_COLUMNS,
              csv_rows(sorted((data / "events").glob("*.csv")), len(EVENT_COLUMNS)))
    # Materialize the global winner before looking at any validation fields.
    # Fixed-width UTC ingest strings sort chronologically.
    profile.execute(con, "event_deduplication", """
        CREATE TEMP TABLE event_winners AS
        SELECT user_id, ts, ingest_ts, event_type, product_id, price_cents
        FROM raw_events
        WHERE event_id IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
    """)
    con.execute("DROP TABLE raw_events")
    profile.execute(con, "event_validation_and_lookup", f"""
        CREATE TEMP TABLE events AS
        WITH parsed AS (
            SELECT user_id, timestamp_ms(ts) AS event_ms,
                   epoch_ms(strptime(ingest_ts, '%Y-%m-%dT%H:%M:%SZ')) AS ingest_ms,
                   event_type, product_id,
                   CASE WHEN event_type = 'purchase' AND regexp_full_match(price_cents, '[0-9]+')
                        THEN try_cast(ltrim(price_cents, '0') AS BIGINT)
                   END AS purchase_cents
            FROM event_winners
            WHERE user_id IS NOT NULL AND product_id IS NOT NULL
              AND event_type IN ('view', 'cart', 'purchase')
        )
        SELECT coalesce(a.person_id, p.user_id) AS person_id, p.event_ms,
               p.purchase_cents,
               CASE WHEN p.event_type = 'purchase' THEN coalesce(products.category, 'unknown')
               END AS category
        FROM parsed p
        LEFT JOIN aliases a ON p.user_id = a.user_id
        LEFT JOIN products ON p.product_id = products.product_id AND p.event_type = 'purchase'
        WHERE p.event_ms >= ? AND p.event_ms < ?
          AND p.event_ms >= p.ingest_ms - {LATE_MS}
          AND (p.event_type <> 'purchase' OR p.purchase_cents BETWEEN 1 AND 10000000)
    """, [window_start, window_end])
    con.execute("DROP TABLE event_winners")


def clean_clicks(con, profile, data, staging, window_start, window_end):
    load_rows(con, profile, staging, "raw_clicks", CLICK_COLUMNS,
              csv_rows(sorted((data / "clicks").glob("*.csv")), len(CLICK_COLUMNS)))
    profile.execute(con, "click_deduplication_validation_and_lookup", """
        CREATE TEMP TABLE clicks AS
        WITH winners AS (
            SELECT click_id, user_id, campaign_id, ts
            FROM raw_clicks
            WHERE click_id IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY click_id) = 1
        ), parsed AS (
            SELECT click_id, user_id, campaign_id, timestamp_ms(ts) AS click_ms
            FROM winners
            WHERE user_id IS NOT NULL
        )
        SELECT coalesce(a.person_id, p.user_id) AS person_id,
               p.click_id, p.campaign_id, p.click_ms
        FROM parsed p
        JOIN campaigns c ON p.campaign_id = c.campaign_id
        LEFT JOIN aliases a ON p.user_id = a.user_id
        WHERE p.click_ms >= ? AND p.click_ms < ?
    """, [window_start - LOOKBACK_MS, window_end])
    con.execute("DROP TABLE raw_clicks")


def build_sessions(con, profile):
    # RANGE gives every event at a timestamp the same cumulative session
    # number, including peers of the first event after a gap.
    profile.execute(con, "sessionization", f"""
        CREATE TEMP TABLE session_events AS
        WITH gaps AS (
            SELECT *, event_ms - lag(event_ms) OVER (
                PARTITION BY person_id ORDER BY event_ms
            ) AS gap_ms
            FROM events
        )
        SELECT person_id, event_ms, purchase_cents, category,
               count(*) FILTER (WHERE gap_ms IS NULL OR gap_ms > {SESSION_GAP_MS})
               OVER (PARTITION BY person_id ORDER BY event_ms
                     RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_number
        FROM gaps
    """)
    con.execute("DROP TABLE events")
    profile.execute(con, "session_aggregation", """
        CREATE TEMP TABLE sessions AS
        SELECT person_id, session_number, min(event_ms) AS session_start,
               cast(make_timestamp_ms(min(event_ms)) AS DATE) AS day,
               count(*) AS events,
               cast(coalesce(sum(purchase_cents), 0) AS BIGINT) AS revenue_cents,
               count(purchase_cents) > 0 AS has_purchase
        FROM session_events
        GROUP BY person_id, session_number
    """)


def write_user_daily(con, profile, destination):
    profile.execute(con, "user_daily_aggregation_and_parquet", """
        COPY (
            WITH daily AS (
                SELECT person_id, day, count(*) AS sessions,
                       cast(sum(events) AS BIGINT) AS events,
                       cast(sum(revenue_cents) AS BIGINT) AS revenue_cents
                FROM sessions
                GROUP BY person_id, day
            ), category_revenue AS (
                SELECT e.person_id, s.day, e.category, sum(e.purchase_cents) AS cents
                FROM session_events e
                JOIN sessions s USING (person_id, session_number)
                WHERE e.purchase_cents IS NOT NULL
                GROUP BY e.person_id, s.day, e.category
            ), top_categories AS (
                SELECT person_id, day, category
                FROM category_revenue
                QUALIFY row_number() OVER (
                    PARTITION BY person_id, day ORDER BY cents DESC, encode(category)
                ) = 1
            )
            SELECT d.person_id AS user_id, d.day, d.sessions, d.events, d.revenue_cents,
                   t.category AS top_category
            FROM daily d LEFT JOIN top_categories t USING (person_id, day)
        ) TO ? (FORMAT PARQUET)
    """, [str(destination)])
    con.execute("DROP TABLE session_events")


def write_campaign_daily(con, profile, destination):
    # One qualifying row per session and campaign. Its best click is enough
    # to determine last-touch credit, while every row contributes one assist.
    profile.execute(con, "attribution_join_and_assist_deduplication", f"""
        CREATE TEMP TABLE session_campaigns AS
        SELECT s.person_id, s.session_number, s.day, c.campaign_id,
               min(struct_pack(negative_ms := -c.click_ms, id_bytes := encode(c.click_id))) AS best_click
        FROM sessions s
        JOIN clicks c ON s.person_id = c.person_id
            AND c.click_ms BETWEEN s.session_start - {LOOKBACK_MS} AND s.session_start
        GROUP BY s.person_id, s.session_number, s.day, c.campaign_id
    """)
    con.execute("DROP TABLE clicks")
    profile.execute(con, "campaign_daily_aggregation_and_parquet", """
        COPY (
            WITH winners AS (
                SELECT person_id, session_number, arg_min(campaign_id, best_click) AS campaign_id
                FROM session_campaigns
                GROUP BY person_id, session_number
            ), credited AS (
                SELECT coalesce(w.campaign_id, 'organic') AS campaign_id, s.day,
                       count(*) AS sessions,
                       count(*) FILTER (WHERE s.has_purchase) AS purchasing_sessions,
                       cast(sum(s.revenue_cents) AS BIGINT) AS revenue_cents
                FROM sessions s
                LEFT JOIN winners w USING (person_id, session_number)
                GROUP BY 1, 2
            ), assists AS (
                SELECT campaign_id, day, count(*) AS assisted_sessions
                FROM session_campaigns
                GROUP BY campaign_id, day
            )
            SELECT campaign_id, coalesce(c.channel, 'organic') AS channel, day,
                   coalesce(cr.sessions, 0) AS sessions,
                   coalesce(cr.purchasing_sessions, 0) AS purchasing_sessions,
                   coalesce(cr.revenue_cents, 0) AS revenue_cents,
                   coalesce(a.assisted_sessions, 0) AS assisted_sessions
            FROM credited cr FULL JOIN assists a USING (campaign_id, day)
            LEFT JOIN campaigns c USING (campaign_id)
        ) TO ? (FORMAT PARQUET)
    """, [str(destination)])


def main():
    profile = StageProfile()
    data = Path(os.environ.get("DATA_DIR", "/data"))
    output = Path(os.environ.get("OUTPUT_DIR", "/output"))
    scratch = Path(os.environ.get("SCRATCH_DIR", "/scratch"))
    threads = int(os.environ.get("NCPU", os.cpu_count() or 1))
    memory_gb = int(os.environ.get("MEM_GB", "8"))
    scratch.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))

    with tempfile.TemporaryDirectory(prefix="shopping-sessions-", dir=scratch) as work:
        staging = Path(work)
        with duckdb.connect(config={
            "threads": str(threads),
            "memory_limit": f"{max(1, int(memory_gb * 0.8))}GB",
            "temp_directory": str(staging / "spill"),
            "preserve_insertion_order": "false",
        }) as con:
            define_timestamp_parser(con)
            start, end = con.execute(
                "SELECT timestamp_ms(?), timestamp_ms(?)",
                [manifest["window_start"], manifest["window_end"]],
            ).fetchone()
            load_dimensions(con, profile, data, staging)
            clean_events(con, profile, data, staging, start, end)
            build_sessions(con, profile)
            (output / "user_daily").mkdir(parents=True, exist_ok=True)
            write_user_daily(con, profile, output / "user_daily" / "part-0.parquet")
            clean_clicks(con, profile, data, staging, start, end)
            (output / "campaign_daily").mkdir(parents=True, exist_ok=True)
            write_campaign_daily(con, profile, output / "campaign_daily" / "part-0.parquet")

    profile.report()


if __name__ == "__main__":
    main()
