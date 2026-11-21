"""Differential tests for the compiled event preprocessor."""

from collections import Counter
from pathlib import Path
from random import Random
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
from compiled_csv import library
from pipeline import clean_events, define_timestamp_parser
from stage_profile import StageProfile


HEADER = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
EARLY = "2026-10-03T00:00:00Z"
LATE = "2026-10-03T00:01:00Z"
START = 1790985600000
END = START + 86_400_000


@unittest.skipUnless(library(), "requires the Docker-built native reader")
class NativeEventTest(unittest.TestCase):
    def compare(self, parts, expect_native=True):
        with tempfile.TemporaryDirectory() as directory, duckdb.connect(config={"threads": "2"}) as con:
            data = Path(directory)
            (data / "events").mkdir()
            for i, rows in enumerate(parts):
                (data / "events" / f"{i}.csv").write_bytes((HEADER + "\n".join(rows)).encode())
            define_timestamp_parser(con)
            con.execute("CREATE TEMP TABLE aliases AS SELECT 'é' user_id, ' A' person_id")
            con.execute("CREATE TEMP TABLE products AS SELECT 'p' product_id, '猫' category")
            results = []
            for reference in ("1", "0"):
                with patch.dict("os.environ", {
                    "SHOPPING_SESSIONS_REFERENCE_PREPROCESSING": reference,
                    "SHOPPING_SESSIONS_PROFILE": "1", "NCPU": "8",
                }):
                    profile = StageProfile()
                    clean_events(con, profile, data, START, END)
                if reference == "0":
                    self.assertEqual("event_deduplication" not in profile.stages, expect_native)
                results.append(Counter(con.execute("SELECT * FROM events").fetchall()))
                con.execute("DROP TABLE events")
            self.assertEqual(*results)
            return results[1]

    def test_cross_file_winner_quotes_long_ids_and_unicode(self):
        long_id = "x" * 8192
        actual = self.compare([
            [f'bad,é,{EARLY},{LATE},view,p,',
             f'"{long_id}",é,{EARLY},{LATE},purchase,p,010',
             f'x,é,{EARLY},{EARLY},view,p,',
             f'bad",é,{EARLY},{EARLY},view,p,'],
            [f'bad,é,invalid,{EARLY},view,p,',
             f'{long_id},é,{EARLY},{EARLY},purchase,p,010',
             f'{long_id},é,{EARLY},{EARLY},purchase,p,010',
             f'x,é,{EARLY},{EARLY},view,p,',
             f'x\r," z ",{EARLY},{EARLY},purchase,missing,99',
             f'empty,"",{EARLY},{EARLY},view,p,'],
        ])
        self.assertEqual(actual, Counter({
            (" A", START, 10, "猫"): 1, (" A", START, None, None): 1,
            (" z ", START, 99, "unknown"): 1,
        }))

    def test_empty_and_missing_ingest_fallback(self):
        self.assertEqual(self.compare([]), Counter())
        self.compare([[f'x,é,{EARLY},,view,p,']], expect_native=False)

    def test_randomized(self):
        for seed in range(20):
            with self.subTest(seed=seed):
                rng = Random(seed)
                parts = [[], [], [], []]
                for i in range(190):
                    at = rng.choice([EARLY, "2026-10-03T00:30:00+00:30", str(START - 1),
                                     str(END), "0" * 40 + str(START), str(2**63),
                                     "2026-02-29T00:00:00Z"])
                    row = ",".join((f'id-{i}', rng.choice(['é', '" a "', 'x\r']), at, EARLY,
                                    rng.choice(['view', 'cart', 'purchase', 'bad']), rng.choice(['p', '?', '']),
                                    rng.choice(['0010', '10000000', '10000001', '１２', '10\r'])))
                    rng.choice(parts).append(row)
                    if rng.random() < 0.5:
                        rng.choice(parts).append(f'id-{i},other,{EARLY},{LATE},purchase,p,1')
                    if rng.random() < 0.25:
                        rng.choice(parts).append(row)
                self.compare(parts)


if __name__ == "__main__":
    unittest.main()
