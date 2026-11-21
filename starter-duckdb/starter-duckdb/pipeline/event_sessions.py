"""Aggregate labeled events into compact per-session category rows."""


SESSION_GAP_MS = 1_800_000

def build_session_categories(con, profile):
    # The running maximum propagates the most recent session start to every
    # event, including peers at an identical timestamp. Aggregate immediately
    # rather than retaining a separate table with one row per event.
    profile.execute(con, "sessionization_and_category_aggregation", f"""
        CREATE TEMP TABLE session_categories AS
        WITH gaps AS (
            SELECT *, event_ms - lag(event_ms) OVER timeline AS gap_ms
            FROM events
            WINDOW timeline AS (PARTITION BY person_id ORDER BY event_ms)
        ), labeled AS (
            SELECT person_id, category, purchase_cents,
                   max(CASE WHEN gap_ms IS NULL OR gap_ms > {SESSION_GAP_MS} THEN event_ms END)
                   OVER (PARTITION BY person_id ORDER BY event_ms
                         RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_start
            FROM gaps
        )
        SELECT person_id, session_start, category, count(*) AS events,
               cast(sum(purchase_cents) AS BIGINT) AS purchase_cents
        FROM labeled
        GROUP BY person_id, session_start, category
    """)
    con.execute("DROP TABLE events")
    profile.execute(con, "session_aggregation", """
        CREATE TEMP TABLE sessions AS
        SELECT person_id, row_number() OVER (PARTITION BY person_id ORDER BY session_start) AS session_number,
               session_start, cast(make_timestamp_ms(session_start) AS DATE) AS day,
               cast(sum(events) AS BIGINT) AS events,
               cast(coalesce(sum(purchase_cents), 0) AS BIGINT) AS revenue_cents,
               count(purchase_cents) > 0 AS has_purchase
        FROM session_categories
        GROUP BY person_id, session_start
    """)
