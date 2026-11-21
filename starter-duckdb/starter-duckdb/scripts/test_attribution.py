"""Differential tests for the campaign attribution implementations."""

from datetime import date, timedelta
from pathlib import Path
from random import Random
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
from attribution import LOOKBACK_MS, write_campaign_daily, write_campaign_daily_reference
from stage_profile import StageProfile


DAY_MS = 86_400_000
EPOCH = date(1970, 1, 1)
CATALOG = ("c_1", "c_2", "c_3")


def run_queries(session_starts, clicks):
    with tempfile.TemporaryDirectory() as tmp, duckdb.connect(config={"threads": "2"}) as con:
        con.execute("""
            CREATE TEMP TABLE sessions (
                person_id VARCHAR, session_number BIGINT, session_start BIGINT,
                day DATE, has_purchase BOOLEAN, revenue_cents BIGINT
            );
            CREATE TEMP TABLE click_input (
                person_id VARCHAR, click_id VARCHAR, campaign_id VARCHAR, click_ms BIGINT
            );
            CREATE TEMP TABLE campaigns (campaign_id VARCHAR, channel VARCHAR);
        """)
        sessions = [
            (person, index, ts, EPOCH + timedelta(days=ts // DAY_MS), cents > 0, cents)
            for person, starts in session_starts.items()
            for index, (ts, cents) in enumerate(sorted(starts), 1)
        ]
        if sessions:
            con.executemany("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?)", sessions)
        if clicks:
            con.executemany("INSERT INTO click_input VALUES (?, ?, ?, ?)", clicks)
        con.executemany("INSERT INTO campaigns VALUES (?, 'channel')", [(campaign,) for campaign in CATALOG])
        results = []
        for query in (write_campaign_daily_reference, write_campaign_daily):
            con.execute("CREATE TEMP TABLE clicks AS SELECT * FROM click_input")
            path = Path(tmp) / f"{query.__name__}.parquet"
            with patch.dict("os.environ", {"SHOPPING_SESSIONS_REFERENCE_ATTRIBUTION": "0"}):
                query(con, StageProfile(), path)
            results.append(con.execute("SELECT * FROM read_parquet(?) ORDER BY 1, 3", [str(path)]).fetchall())
        return results


class AttributionTest(unittest.TestCase):
    def check_parity(self, sessions, clicks):
        expected, actual = run_queries(sessions, clicks)
        self.assertEqual(actual, expected)
        return actual

    def test_inclusive_boundaries_and_organic(self):
        t = 10 * DAY_MS
        actual = self.check_parity(
            {"inclusive": [(t, 200)], "expired": [(t, 0)], "future": [(t, 0)], "none": [(t, 500)]},
            [
                ("inclusive", "a", "c_1", t - LOOKBACK_MS),
                ("expired", "b", "c_1", t - LOOKBACK_MS - 1),
                ("future", "c", "c_2", t + 1),
            ],
        )
        day = EPOCH + timedelta(days=10)
        self.assertEqual(actual, [
            ("c_1", "channel", day, 1, 1, 200, 1),
            ("organic", "organic", day, 3, 1, 500, 0),
        ])

    def test_equal_times_repeats_and_assist_only(self):
        t = 10 * DAY_MS
        actual = self.check_parity(
            {"u": [(t, 300), (t + DAY_MS, 0)]},
            [
                ("u", "Z", "c_1", t), ("u", "a", "c_2", t), ("u", "é", "c_3", t),
                ("u", "earlier", "c_1", t - 10), ("u", "other", "c_1", t - 10),
            ],
        )
        self.assertEqual(len(actual), 6)
        for campaign, _, _, sessions, _, revenue, assists in actual:
            self.assertEqual(assists, 1)
            if campaign != "c_1":
                self.assertEqual((sessions, revenue), (0, 0))

    def test_touching_intervals_and_disjoint_same_day(self):
        t = 15 * DAY_MS + DAY_MS // 2
        actual = self.check_parity(
            {"u": [(t - DAY_MS, 0), (t - 2_000_000, 0), (t, 0), (t + 2_000_000, 0),
                   (t + 7 * DAY_MS, 42), (t + 8 * DAY_MS, 0)]},
            [
                ("u", "1", "c_1", t - LOOKBACK_MS),
                ("u", "2", "c_1", t),
                ("u", "3", "c_2", t - LOOKBACK_MS - 2_000_000),
                ("u", "4", "c_2", t + 2_000_000),
            ],
        )
        same_day = EPOCH + timedelta(days=15)
        self.assertEqual([row[-1] for row in actual if row[0] == "c_2" and row[2] == same_day], [2])

    def test_empty_inputs(self):
        self.assertEqual(self.check_parity({}, []), [])
        self.assertEqual(self.check_parity({}, [("u", "1", "c_1", 0)]), [])

    def test_randomized_differential(self):
        for seed in range(32):
            with self.subTest(seed=seed):
                rng = Random(seed)
                sessions, clicks = {}, []
                for user in map(str, range(12)):
                    times = sorted(set(rng.choices(range(-3, 23 * 24), k=rng.randint(0, 35))))
                    starts = [(h * 3_600_000, rng.choice([0, 0, 40, 399])) for h in times]
                    sessions[user] = starts
                    anchors = [t for t, _ in starts] or [0]
                    for i in range(rng.randint(0, 45)):
                        start = rng.choice(anchors)
                        offset = rng.choice([0, 1, -1, -LOOKBACK_MS, -LOOKBACK_MS - 1,
                                             rng.randrange(-8 * DAY_MS, 8 * DAY_MS)])
                        clicks.append((user, f"{rng.choice(['A', 'b', 'é'])}{i:03}",
                                       rng.choice(CATALOG), start + offset))
                rng.shuffle(clicks)
                self.check_parity(sessions, clicks)


if __name__ == "__main__":
    unittest.main()
