"""Exercise the packaged engine against the published pipeline on generated input."""
import os
from pathlib import Path
import random
import subprocess
import tempfile
import unittest

import duckdb


@unittest.skipUnless(Path("/app/native-engine").exists(), "requires the native engine image")
class EngineTest(unittest.TestCase):
    def test_differential(self):
        start = 1790985600000
        with tempfile.TemporaryDirectory() as directory, duckdb.connect() as con:
            root = Path(directory)
            for folder in ("events", "clicks", "links"):
                (root / folder).mkdir()
            (root / "manifest.json").write_text(
                '{"window_start":"2026-10-03T00:00:00Z","window_end":"2026-10-07T00:00:00Z"}'
            )
            (root / "links/a.csv").write_bytes('a,b\n猫,u\nu,v\nv,猫\n" z ",u\n'.encode())
            (root / "products.csv").write_bytes(b"id,cat\np1,C\rp\np2,c\np3,c\n")
            (root / "campaigns.csv").write_bytes(b"id,ch\nc1,Y\rc\nc2,web\n")
            for seed in range(6):
                rng = random.Random(seed)
                events = [[], []]
                clicks = [[], []]
                for i in range(500):
                    user = rng.choice(["u", "v", "猫", " a", "Z\r", "user" + str(i)])
                    t = start + rng.choice([0, 1, 1800000, 1800001, 86399999, 86400000,
                                            rng.randrange(200000000)])
                    row = f"e{i},{user},{t},2026-10-03T00:00:00Z,purchase,p{1+i%3},250"
                    events[i % 2].append(row)
                    if i % 4 == 0:
                        events[1-i%2].append(f"e{i},u,bad,2026-10-02T23:00:00Z,view,p1,")
                    click = f"k{i},{user},c{1+i%2},{t-rng.choice([0,1,604800000,604800001])}"
                    clicks[i%2].append(click)
                    if i % 3 == 0:
                        clicks[1-i%2].append(click)
                for name, shards in (("events",events),("clicks",clicks)):
                    for i, rows in enumerate(shards):
                        (root / name / f"{i}.csv").write_bytes(("header\n"+"\n".join(rows)).encode())
                for reference in ("0","1"):
                    env=dict(os.environ, DATA_DIR=str(root), SCRATCH_DIR=str(root/"scratch"),
                             OUTPUT_DIR=str(root/reference), SHOPPING_SESSIONS_REFERENCE_ENGINE=reference,
                             SHOPPING_SESSIONS_PROFILE="1")
                    result=subprocess.run(["/app/run.sh"],env=env,capture_output=True,check=True,text=True)
                    if reference=="0":
                        self.assertIn("session_attribution_and_aggregate_staging",result.stderr)
                    for table in ("user_daily","campaign_daily"):
                        con.read_parquet(str(root/reference/table/"*.parquet")).create_view(
                            table+reference,replace=True)
                for table in ("user_daily","campaign_daily"):
                    for a,b in ((0,1),(1,0)):
                        with self.subTest(seed=seed,table=table):
                            self.assertEqual(con.execute(
                                f"SELECT * FROM {table}{a} EXCEPT ALL SELECT * FROM {table}{b}"
                            ).fetchall(),[])


if __name__ == "__main__":
    unittest.main()
