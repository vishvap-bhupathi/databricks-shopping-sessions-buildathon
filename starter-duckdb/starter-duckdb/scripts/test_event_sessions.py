"""Differential tests for the compact session/category aggregation."""

from datetime import date
from pathlib import Path
from random import Random
import sys
import tempfile
import unittest

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
from event_sessions import SESSION_GAP_MS, build_session_categories
from pipeline import build_sessions, write_user_daily
from stage_profile import StageProfile


DAY_MS = 86_400_000


class EventSessionsTest(unittest.TestCase):
    def compare(self, events):
        with tempfile.TemporaryDirectory() as tmp, duckdb.connect(config={"threads": "2"}) as con:
            con.execute("""
                CREATE TEMP TABLE input_events (
                    person_id VARCHAR, event_ms BIGINT, purchase_cents BIGINT, category VARCHAR
                )
            """)
            if events:
                con.executemany("INSERT INTO input_events VALUES (?, ?, ?, ?)", events)
            outcomes = []
            for index, build in enumerate((build_sessions, build_session_categories)):
                con.execute("CREATE TEMP TABLE events AS SELECT * FROM input_events")
                profile = StageProfile()
                output = Path(tmp) / f"{index}.parquet"
                build(con, profile)
                sessions = con.execute("""
                    SELECT person_id,session_number,session_start,day,events,revenue_cents,has_purchase
                    FROM sessions ORDER BY person_id,session_number
                """).fetchall()
                write_user_daily(con, profile, output, compact=index == 1)
                daily = con.execute("SELECT * FROM read_parquet(?) ORDER BY user_id,day", [str(output)]).fetchall()
                con.execute("DROP TABLE sessions")
                outcomes.append((sessions, daily))
            self.assertEqual(outcomes[0], outcomes[1])
            return outcomes[1]

    def test_gap_boundary_midnight_and_timestamp_peers(self):
        base = DAY_MS - 1
        gap = SESSION_GAP_MS
        sessions, daily = self.compare([
            ("u", base + 2 * gap + 1, None, None),
            ("u", base, None, None),
            ("u", base + gap, 125, "shoes"),
            ("u", base + gap, 125, "boots"),
            ("u", base + 2 * gap + 1, 10, "unknown"),
        ])
        self.assertEqual([(s[1], s[4], s[5]) for s in sessions], [(1, 3, 250), (2, 2, 10)])
        self.assertEqual(daily, [
            ("u", date(1970, 1, 1), 1, 3, 250, "boots"),
            ("u", date(1970, 1, 2), 1, 2, 10, "unknown"),
        ])

    def test_empty_and_all_nonpurchase(self):
        self.assertEqual(self.compare([]), ([], []))
        _, daily = self.compare([("A", 1, None, None), ("A", 1, None, None)])
        self.assertEqual(daily, [("A", date(1970, 1, 1), 1, 2, 0, None)])

    def test_randomized_differential(self):
        for seed in range(24):
            with self.subTest(seed=seed):
                rng = Random(seed)
                events = []
                for person in ("é", "z ", " z", "Z", "z"):
                    timestamp = -10
                    for _ in range(rng.randrange(5, 80)):
                        timestamp += rng.choice([0, 1, SESSION_GAP_MS, SESSION_GAP_MS + 1, DAY_MS])
                        cents = rng.choice([None, None, 1, 25, 99])
                        events.append((person, timestamp, cents,
                                       rng.choice(["unknown", "A", "a", "é"]) if cents else None))
                rng.shuffle(events)
                self.compare(events)


if __name__ == "__main__":
    unittest.main()
