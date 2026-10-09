# Job Simulator

Replay historical job scheduling into a new DuckDB database:

```sh
uv run simulate.py simulation.duckdb
uv run simulate.py simulation-20-workers.duckdb --workers 20 --db-workers 10
```

The output `jobs` table contains `job_id`, `started_at`, and `completed_at`.
Existing output files are never overwritten. Use `--input` to choose another
historical database; the default is `jobs.duckdb` in the current directory.

Defaults at the top of `simulate.py` configure 10 non-database workers, 10 database
workers, and a one-second controller polling interval (`--loop-interval`). These
match the controller's default settings for TPP, rather than unknown production
environment overrides. All jobs share one backend. Workspace fairness counts
running jobs across both worker pools, then prioritizes database jobs and older
jobs. Equal-age ties use job ID for deterministic results.

NULL `requires_db` values use `NULL_REQUIRES_DB = False`, matching the supplied
controller's truthiness checks. This affects 91,331 included historical jobs and
may undercount database jobs from older exports; no command-based inference is
applied.

Jobs without `job_runtime` are excluded; dependencies on excluded or absent jobs
are treated as satisfied. Included jobs are assumed to succeed after their recorded
runtime, including historically failed or cancelled jobs. Maintenance, pauses,
retries, and agent overhead beyond that runtime are not modeled. Completions are
recorded at start plus runtime, but capacity and dependencies are released on the
next poll. Polling starts at the earliest included job's creation time.

Each job uses one worker by default. Optional per-job weights can be configured in
`JOB_RESOURCE_WEIGHTS`; the export lacks the action field needed to reproduce the
controller's workspace/action resource-weight patterns. Cyclic dependencies,
negative runtimes, and weights exceeding pool capacity produce an error.

Run focused checks with `uv run python -m unittest -v`.
