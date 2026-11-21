"""Last-click credit and inclusive campaign assist counts."""

import os


LOOKBACK_MS = 604_800_000


def write_campaign_daily_reference(con, profile, destination):
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


def credit_sessions(con, profile):
    # ASOF chooses a timestamp. Reduce ties to their byte-wise winner first.
    profile.execute(con, "last_click_credit", f"""
        CREATE TEMP TABLE credited AS
        WITH click_times AS (
            SELECT person_id, click_ms, arg_min(campaign_id, encode(click_id)) AS campaign_id
            FROM clicks
            GROUP BY person_id, click_ms
        )
        SELECT coalesce(
                   CASE WHEN c.click_ms >= s.session_start - {LOOKBACK_MS}
                        THEN c.campaign_id END, 'organic'
               ) AS campaign_id,
               s.day,
               count(*) AS sessions,
               count(*) FILTER (WHERE s.has_purchase) AS purchasing_sessions,
               cast(sum(s.revenue_cents) AS BIGINT) AS revenue_cents
        FROM sessions s
        ASOF LEFT JOIN click_times c
          ON s.person_id = c.person_id AND s.session_start >= c.click_ms
        GROUP BY 1, 2
    """)


def merge_assist_intervals(con, profile):
    # Equal-width inclusive intervals overlap exactly when the adjacent
    # click gap is at most the lookback. Keep only each run's endpoints.
    profile.execute(con, "merge_assist_intervals", f"""
        CREATE TEMP TABLE assist_intervals AS
        WITH adjacent AS (
            SELECT person_id, campaign_id, click_ms,
                   lag(click_ms) OVER timeline AS previous_ms,
                   lead(click_ms) OVER timeline AS next_ms
            FROM clicks
            WINDOW timeline AS (PARTITION BY person_id, campaign_id ORDER BY click_ms)
        ), endpoints AS (
            SELECT person_id, campaign_id, click_ms,
                   previous_ms IS NULL OR click_ms - previous_ms > {LOOKBACK_MS} AS opening,
                   next_ms IS NULL OR next_ms - click_ms > {LOOKBACK_MS} AS closing
            FROM adjacent
        ), boundaries AS (
            SELECT *, lead(click_ms) OVER (
                PARTITION BY person_id, campaign_id ORDER BY click_ms, opening DESC
            ) AS closing_ms
            FROM endpoints
            WHERE opening OR closing
        )
        SELECT person_id, campaign_id, click_ms AS first_ms,
               (CASE WHEN closing THEN click_ms ELSE closing_ms END) + {LOOKBACK_MS} AS last_ms
        FROM boundaries
        WHERE opening
    """)


def count_assists(con, profile):
    # Session numbers are consecutive per person, and UTC days are
    # contiguous in that order. Count intersections of rank ranges per day.
    profile.execute(con, "bound_assist_session_ranks", """
        CREATE TEMP TABLE assist_ranks AS
        WITH first_session AS (
            SELECT i.person_id, i.campaign_id, i.last_ms,
                   s.session_number AS first_number, s.session_start AS first_start
            FROM assist_intervals i
            ASOF JOIN sessions s
              ON i.person_id = s.person_id AND i.first_ms <= s.session_start
        )
        SELECT i.person_id, i.campaign_id, i.first_number, s.session_number AS last_number
        FROM (SELECT * FROM first_session WHERE first_start <= last_ms) i
        ASOF JOIN sessions s
          ON i.person_id = s.person_id AND i.last_ms >= s.session_start
    """)
    con.execute("DROP TABLE assist_intervals")
    profile.execute(con, "count_assist_days", """
        CREATE TEMP TABLE assists AS
        WITH session_days AS (
            SELECT person_id, day, min(session_number) AS first_number, max(session_number) AS last_number
            FROM sessions
            GROUP BY person_id, day
        )
        SELECT r.campaign_id, d.day,
               cast(sum(least(r.last_number, d.last_number) -
                        greatest(r.first_number, d.first_number) + 1) AS BIGINT) AS assisted_sessions
        FROM assist_ranks r
        JOIN session_days d ON r.person_id = d.person_id
          AND r.first_number <= d.last_number AND r.last_number >= d.first_number
        GROUP BY r.campaign_id, d.day
    """)
    con.execute("DROP TABLE assist_ranks")


def write_campaign_daily(con, profile, destination):
    if os.environ.get("SHOPPING_SESSIONS_REFERENCE_ATTRIBUTION") == "1":
        write_campaign_daily_reference(con, profile, destination)
        return

    credit_sessions(con, profile)
    merge_assist_intervals(con, profile)
    con.execute("DROP TABLE clicks")
    count_assists(con, profile)
    profile.execute(con, "campaign_daily_parquet", """
        COPY (
            SELECT campaign_id, coalesce(c.channel, 'organic') AS channel, day,
                   coalesce(cr.sessions, 0) AS sessions,
                   coalesce(cr.purchasing_sessions, 0) AS purchasing_sessions,
                   coalesce(cr.revenue_cents, 0) AS revenue_cents,
                   coalesce(a.assisted_sessions, 0) AS assisted_sessions
            FROM credited cr FULL JOIN assists a USING (campaign_id, day)
            LEFT JOIN campaigns c USING (campaign_id)
        ) TO ? (FORMAT PARQUET)
    """, [str(destination)])
