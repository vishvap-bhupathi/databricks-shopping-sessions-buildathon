"""Opt-in stage timings and DuckDB operator diagnostics."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import resource
import sys
from time import perf_counter


class StageProfile:
    def __init__(self):
        self.enabled = os.environ.get("SHOPPING_SESSIONS_PROFILE") == "1"
        directory = os.environ.get("SHOPPING_SESSIONS_PROFILE_DIR")
        self.plan_directory = Path(directory) if self.enabled and directory else None
        if self.plan_directory is not None:
            self.plan_directory.mkdir(parents=True, exist_ok=True)
        self.stages = {}
        self.start = perf_counter() if self.enabled else None

    @contextmanager
    def measure(self, name):
        if not self.enabled:
            yield None
            return
        stage = self.stages.setdefault(name, {"seconds": 0.0})
        start = perf_counter()
        yield stage
        stage["seconds"] += perf_counter() - start

    def execute(self, con, name, query, parameters=None):
        if not self.enabled:
            con.execute(query, parameters)
            return

        # Operator profiling is separate from the ordinary wall-time runs.
        if self.plan_directory is not None:
            current_plan = self.plan_directory / "current.json"
            con.execute("SET enable_profiling = 'json'")
            con.execute("SET profiling_coverage = 'ALL'")
            con.execute("SET profiling_output = ?", [str(current_plan)])
        try:
            with self.measure(name) as stage:
                stage["rows"] = con.execute(query, parameters).fetchone()[0]
            if self.plan_directory is not None:
                # Later SET statements can overwrite DuckDB's current profile.
                current_plan.replace(self.plan_directory / f"{name}.json")
        finally:
            if self.plan_directory is not None:
                con.execute("PRAGMA disable_profiling")
                current_plan.unlink(missing_ok=True)

    def report(self):
        if self.enabled:
            peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Darwin reports bytes. Linux reports KiB.
            print(json.dumps({
                "pipeline_seconds": perf_counter() - self.start,
                "peak_rss_bytes": peak_rss if sys.platform == "darwin" else peak_rss * 1024,
                "stages": self.stages,
            }), file=sys.stderr)
