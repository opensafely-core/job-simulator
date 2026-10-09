"""Replay historical jobs using the controller's scheduling rules.

Run with: uv run simulate.py output.duckdb
All simulated jobs succeed; maintenance, cancellation and retries are not replayed.
"""

import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
import heapq
import json
import math
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb


# Separate pools, matching the controller's default TPP configuration.
MAX_WORKERS = 10
MAX_DB_WORKERS = 10
JOB_LOOP_INTERVAL = 1.0  # seconds; first poll is at the earliest included arrival
# The controller treats NULL requires_db as false. Older historical rows may
# need a different classification; this default is explicit rather than inferred.
NULL_REQUIRES_DB = False
# Optional per-job resource weights. The historical export has no action field
# with which to reproduce controller workspace/action regex rules.
JOB_RESOURCE_WEIGHTS: dict[str, float] = {}


@dataclass(frozen=True)
class Job:
    job_id: str
    created_at: datetime
    runtime: timedelta
    requires_db: bool
    workspace: str
    dependencies: tuple[str, ...] = ()
    weight: float = 1.0


def load_jobs(filename: Path) -> tuple[list[Job], int]:
    """Ignore missing runtimes; omitted dependencies are already satisfied."""
    with duckdb.connect(str(filename), read_only=True) as connection:
        excluded = connection.execute(
            "SELECT count(*) FROM jobs WHERE job_runtime IS NULL"
        ).fetchone()[0]
        rows = connection.execute(
            """SELECT job_id, created_at, job_runtime, requires_db, workspace,
                      dependencies
               FROM jobs WHERE job_runtime IS NOT NULL
               ORDER BY created_at, job_id"""
        ).fetchall()
    jobs = []
    for job_id, created_at, runtime, requires_db, workspace, dependencies in rows:
        if not job_id or created_at is None or workspace is None:
            raise ValueError(f"Job {job_id!r} has missing scheduling metadata")
        if requires_db is None:
            requires_db = NULL_REQUIRES_DB
        if requires_db not in (0, 1):
            raise ValueError(f"Job {job_id!r} has invalid requires_db")
        if not math.isfinite(runtime) or runtime < 0:
            raise ValueError(f"Job {job_id!r} has invalid runtime {runtime!r}")
        dependencies = json.loads(dependencies) if isinstance(dependencies, str) else dependencies
        if dependencies is None:
            dependencies = []
        if not isinstance(dependencies, list) or not all(
            isinstance(dependency, str) for dependency in dependencies
        ):
            raise ValueError(f"Job {job_id!r} has invalid dependencies")
        jobs.append(Job(
            job_id, created_at, timedelta(seconds=runtime), bool(requires_db),
            workspace, tuple(dict.fromkeys(dependencies)),
            JOB_RESOURCE_WEIGHTS.get(job_id, 1.0),
        ))
    return jobs, excluded


def simulate(
    jobs: list[Job],
    max_workers: float = MAX_WORKERS,
    max_db_workers: float = MAX_DB_WORKERS,
    loop_interval: float = JOB_LOOP_INTERVAL,
) -> list[tuple[str, datetime, datetime]]:
    """Event-driven replay of handle_jobs(), with time rounded to poll ticks.

    Running jobs finish before pending jobs are considered. Pending priority is
    (workspace running count, DB first, creation time). Counts change after each
    start, so priority is recalculated within each controller pass.
    """
    for value in (max_workers, max_db_workers, loop_interval):
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Worker limits and polling interval must be positive")
    interval = timedelta(seconds=loop_interval)
    if interval <= timedelta(0):
        raise ValueError("Polling interval must be at least one microsecond")
    jobs = sorted(jobs, key=lambda job: (job.created_at, job.job_id))
    by_id = {job.job_id: index for index, job in enumerate(jobs)}
    if len(by_id) != len(jobs):
        raise ValueError("Duplicate job IDs")
    if not jobs:
        return []

    limits = {False: max_workers, True: max_db_workers}
    minimum_weight = {
        pool: min((job.weight for job in jobs if job.requires_db == pool), default=1)
        for pool in limits
    }
    for job in jobs:
        if job.runtime < timedelta(0):
            raise ValueError(f"Job {job.job_id} has a negative runtime")
        if not math.isfinite(job.weight) or not 0 < job.weight <= limits[job.requires_db]:
            raise ValueError(f"Job {job.job_id} cannot fit in its worker pool")

    dependents = defaultdict(list)
    remaining = [0] * len(jobs)
    for index, job in enumerate(jobs):
        for dependency in set(job.dependencies):
            # The controller's dependency lookup also ignores absent job IDs.
            if dependency in by_id:
                remaining[index] += 1
                dependents[by_id[dependency]].append(index)

    # One creation-ordered queue for each workspace/pool combination.
    ready = defaultdict(list)
    running_counts = Counter()
    usage = {False: 0.0, True: 0.0}
    running = []
    results = []
    next_arrival = 0
    now = origin = jobs[0].created_at

    def enqueue(index):
        job = jobs[index]
        heapq.heappush(ready[(job.workspace, job.requires_db)], index)

    def priority(index):
        job = jobs[index]
        return (running_counts[job.workspace], 0 if job.requires_db else 1, index)

    while len(results) < len(jobs) or running:
        # Completions free resources and satisfy dependencies at this poll.
        while running and running[0][0] <= now:
            _, index = heapq.heappop(running)
            job = jobs[index]
            running_counts[job.workspace] -= 1
            usage[job.requires_db] -= job.weight
            for dependent in dependents[index]:
                remaining[dependent] -= 1
                if remaining[dependent] == 0 and dependent < next_arrival:
                    enqueue(dependent)

        while next_arrival < len(jobs) and jobs[next_arrival].created_at <= now:
            if remaining[next_arrival] == 0:
                enqueue(next_arrival)
            next_arrival += 1

        def pool_has_room(pool):
            return usage[pool] + minimum_weight[pool] <= limits[pool]

        candidates = [priority(queue[0]) for (_, pool), queue in ready.items()
                      if queue and pool_has_room(pool)]
        heapq.heapify(candidates)
        deferred = defaultdict(list)
        while candidates:
            count, _, index = heapq.heappop(candidates)
            job = jobs[index]
            if not pool_has_room(job.requires_db):
                continue
            if count != running_counts[job.workspace]:
                # A preceding start changed this workspace's priority.
                heapq.heappush(candidates, priority(index))
                continue
            group = (job.workspace, job.requires_db)
            heapq.heappop(ready[group])
            if usage[job.requires_db] + job.weight > limits[job.requires_db]:
                deferred[group].append(index)
            else:
                completed_at = now + job.runtime
                results.append((job.job_id, now, completed_at))
                heapq.heappush(running, (completed_at, index))
                running_counts[job.workspace] += 1
                usage[job.requires_db] += job.weight
            if ready[group]:
                heapq.heappush(candidates, priority(ready[group][0]))
        for group, indices in deferred.items():
            for index in indices:
                heapq.heappush(ready[group], index)
        ready = defaultdict(list, {group: queue for group, queue in ready.items() if queue})

        events = []
        if next_arrival < len(jobs):
            events.append(jobs[next_arrival].created_at)
        if running:
            events.append(running[0][0])
        if not events:
            if len(results) < len(jobs):
                blocked = [jobs[i].job_id for i, count in enumerate(remaining) if count]
                raise ValueError(f"Dependency cycle leaves jobs blocked: {blocked[:10]}")
            break
        event = min(events)
        ticks, remainder = divmod(event - origin, interval)
        next_poll = origin + (ticks + bool(remainder)) * interval
        # A zero-duration job is still RUNNING until the next controller pass.
        now = max(now + interval, next_poll)
    return results


def write_results(filename: Path, results: list[tuple[str, datetime, datetime]]) -> None:
    """Create a new database without overwriting an existing file."""
    with TemporaryDirectory(prefix=".simulate-", dir=filename.parent) as directory:
        temporary = Path(directory) / "results.duckdb"
        csv_path = Path(directory) / "results.csv"
        with csv_path.open("w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(("job_id", "started_at", "completed_at"))
            writer.writerows(results)
        with duckdb.connect(str(temporary)) as connection:
            connection.execute(
                "CREATE TABLE jobs (job_id VARCHAR PRIMARY KEY, "
                "started_at TIMESTAMP NOT NULL, completed_at TIMESTAMP NOT NULL)"
            )
            # Bulk loading avoids slow Python parameter binding for each row.
            connection.execute(
                """INSERT INTO jobs SELECT * FROM read_csv(?, header=true,
                   columns={'job_id': 'VARCHAR', 'started_at': 'TIMESTAMP',
                            'completed_at': 'TIMESTAMP'})""",
                [str(csv_path)],
            )
        # Publish atomically and fail if another process created the output.
        os.link(temporary, filename)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new output DuckDB filename")
    parser.add_argument("--input", type=Path, default=Path("jobs.duckdb"))
    parser.add_argument("--workers", type=float, default=MAX_WORKERS)
    parser.add_argument("--db-workers", type=float, default=MAX_DB_WORKERS)
    parser.add_argument("--loop-interval", type=float, default=JOB_LOOP_INTERVAL)
    args = parser.parse_args()
    if args.output.exists() or args.output.resolve() == args.input.resolve():
        parser.error("Output must be a new file, different from the input database")
    try:
        jobs, excluded = load_jobs(args.input)
        results = simulate(jobs, args.workers, args.db_workers, args.loop_interval)
        write_results(args.output, results)
    except (ValueError, OSError, duckdb.Error) as error:
        parser.exit(1, f"Error: {error}\n")
    print(f"Wrote {len(results):,} jobs to {args.output}; excluded {excluded:,} missing runtimes")


if __name__ == "__main__":
    main()
