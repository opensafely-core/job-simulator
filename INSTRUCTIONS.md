I want to construct a simulation of job scheduling system that runs on actual historical
job data so that I can understand the impact that different approaches to scheduling
might have.

The historical data is in the `jobs` table in `jobs.duckdb` and the following fields are
of interest:
 * `job_id` identifies the job
 * `created_at` records when the job entered the queue
 * `job_runtime` records how long it took once it had started
 * `requires_db` records whether the job needed the database (which affects the scheduling)
 * `dependencies` is a list of job IDs that must be run before this job runs

 The existing scheduling logic is in `job-runner/controller/main.py` in the `handle_jobs()` function.

 Assume there is only one `job.backend` to which all historical jobs belong.

I want to start with a script called `simulate.py` which takes an output filename. It
reads `jobs.duckdb`, simulates the existing scheduling algorithm and writes an output
duckdb file with the following columns: `job_id` `started_at` `completed_at`

That is, given when jobs were actually created and how long each job took to run, and
given the various dependency and concurrency constraints determine when these jobs would
be scheduled to run.

There are various config values like the number of available database and non-database
workers. These should be easily configurable in the script. Code can be freely copied
from the `job-runner` directory where helpful, but don't directly import anything from
there.

You have `uv` available. Use it to manage any Python dependencies you might need.
