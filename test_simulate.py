from datetime import datetime, timedelta
from pathlib import Path
import random
from tempfile import TemporaryDirectory
import unittest

import duckdb

from simulate import Job, load_jobs, simulate, write_results


ORIGIN = datetime(2026, 1, 1)


def job(identifier, workspace="a", db=False, duration=10, arrival=0, deps=(), weight=1):
    return Job(identifier, ORIGIN + timedelta(seconds=arrival),
               timedelta(seconds=duration), db, workspace, deps, weight)


class SimulationTests(unittest.TestCase):
    def starts(self, jobs, **kwargs):
        return {identifier: (start - ORIGIN).total_seconds()
                for identifier, start, _ in simulate(jobs, **kwargs)}

    def test_workspace_counts_change_after_each_start(self):
        starts = self.starts([job("a1"), job("a2"), job("b1", "b")], max_workers=2)
        self.assertEqual(starts, {"a1": 0, "b1": 0, "a2": 10})

    def test_db_priority_separate_pools_and_cross_pool_fairness(self):
        jobs = [job("cpu-old", arrival=0), job("db-new", db=True, arrival=1),
                job("cpu-a", arrival=1), job("cpu-b", "b", arrival=1)]
        starts = self.starts(jobs, max_workers=1, max_db_workers=1)
        self.assertEqual(starts["db-new"], 1)
        self.assertEqual(starts["cpu-b"], 10)
        self.assertEqual(starts["cpu-a"], 20)
        self.assertEqual(self.starts([job("cpu"), job("db", db=True)]),
                         {"db": 0, "cpu": 0})

    def test_dependencies_arrivals_and_poll_rounding(self):
        starts = self.starts([
            job("first", duration=1.2),
            job("dependent", deps=("first", "absent")),
            job("future", arrival=4.1),
        ])
        self.assertEqual(starts, {"first": 0, "dependent": 2, "future": 5})

    def test_zero_runtime_finishes_on_next_poll(self):
        starts = self.starts([job("first", duration=0), job("second", deps=("first",))])
        self.assertEqual(starts, {"first": 0, "second": 1})

    def test_capacity_blocked_heavy_job_does_not_block_small_job(self):
        starts = self.starts([
            job("a", duration=5, weight=2), job("b", weight=2), job("c"),
        ], max_workers=3)
        self.assertEqual(starts, {"a": 0, "c": 0, "b": 5})

    def test_invalid_configuration_and_cycles(self):
        with self.assertRaisesRegex(ValueError, "cycle"):
            simulate([job("a", deps=("b",)), job("b", deps=("a",))])
        with self.assertRaisesRegex(ValueError, "fit"):
            simulate([job("a", weight=3)], max_workers=2)
        with self.assertRaisesRegex(ValueError, "positive"):
            simulate([], loop_interval=0)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            simulate([job("a"), job("a")])

    def test_matches_direct_controller_replay(self):
        # Independent, deliberately simple sort-after-every-job reference.
        rng = random.Random(42)
        for _ in range(100):
            jobs = [job(str(i), workspace=str(rng.randrange(4)),
                        db=bool(rng.randrange(2)), duration=rng.randrange(6),
                        arrival=rng.randrange(10), weight=rng.choice([0.5, 1, 2]),
                        deps=tuple(str(j) for j in range(i) if rng.random() < 0.07))
                    for i in range(20)]
            ordered = sorted(jobs, key=lambda j: (j.created_at, j.job_id))
            finished, running, started = set(), {}, set()
            expected = []
            now = ordered[0].created_at
            while len(finished) < len(jobs):
                for identifier, (_, end) in list(running.items()):
                    if end <= now:
                        finished.add(identifier)
                        del running[identifier]
                pending = [j for j in ordered if j.created_at <= now
                           and j.job_id not in started]
                counts = {j.workspace: sum(r.workspace == j.workspace
                          for r, _ in running.values()) for j in jobs}
                while pending:
                    pending.sort(key=lambda j: (counts[j.workspace],
                                 not j.requires_db, j.created_at, j.job_id))
                    candidate = pending.pop(0)
                    used = sum(r.weight for r, _ in running.values()
                               if r.requires_db == candidate.requires_db)
                    if (set(candidate.dependencies) <= finished
                            and used + candidate.weight <= 3):
                        end = now + candidate.runtime
                        running[candidate.job_id] = (candidate, end)
                        counts[candidate.workspace] += 1
                        started.add(candidate.job_id)
                        expected.append((candidate.job_id, now, end))
                now += timedelta(seconds=1)
            self.assertEqual(simulate(jobs, 3, 3), expected)

    def test_database_io_and_missing_runtime_exclusion(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "input.duckdb"
            target = Path(directory) / "output.duckdb"
            with duckdb.connect(str(source)) as connection:
                connection.execute("""CREATE TABLE jobs AS SELECT
                    'excluded' AS job_id, TIMESTAMP '2026-01-01' AS created_at,
                    NULL::BIGINT AS job_runtime, false AS requires_db,
                    'workspace' AS workspace, '[]' AS dependencies
                    UNION ALL SELECT 'included', TIMESTAMP '2026-01-01',
                    2, NULL, 'workspace', '["excluded"]'""")
            jobs, excluded = load_jobs(source)
            self.assertEqual(excluded, 1)
            self.assertFalse(jobs[0].requires_db)
            results = simulate(jobs)
            self.assertEqual(results, [("included", ORIGIN, ORIGIN + timedelta(seconds=2))])
            write_results(target, results)
            with duckdb.connect(str(target), read_only=True) as connection:
                self.assertEqual(connection.execute("SELECT * FROM jobs").fetchall(), results)
            with self.assertRaises(FileExistsError):
                write_results(target, [])
            empty = Path(directory) / "empty.duckdb"
            write_results(empty, [])
            with duckdb.connect(str(empty), read_only=True) as connection:
                self.assertEqual(connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
