ATTACH DATABASE "file:/var/lib/dokku/data/storage/rap-controller/db.sqlite?mode=ro" AS prod;

CREATE TABLE task_info AS
  SELECT 
    tasks.id AS task_id,
    substr(tasks.id, 1, 16) AS job_id,
    CAST(json_extract(agent_results, '$.job_metrics.cpu_mean') AS INTEGER) AS cpu_mean,
    CAST(json_extract(agent_results, '$.job_metrics.cpu_peak') AS INTEGER) AS cpu_peak,
    CAST(json_extract(agent_results, '$.job_metrics.mem_mb_mean') AS INTEGER) AS mem_mean,
    CAST(json_extract(agent_results, '$.job_metrics.mem_mb_peak') AS INTEGER) AS mem_peak,
    CAST((
      julianday(
        replace(
          substr(json_extract(agent_results, '$.container_metadata.State.FinishedAt'), 1, 26),
          'T',
          ' '
        )
      )
      -
      julianday(
        replace(
          substr(json_extract(agent_results, '$.container_metadata.State.StartedAt'), 1, 26),
          'T',
          ' '
        )
      )
    ) * 86400.0 AS INTEGER) AS runtime
  FROM
    prod.tasks
  WHERE
    tasks.type = 'runjob';

CREATE TABLE job_tasks (
  job_id TEXT PRIMARY KEY,
  runtime INTEGER,
  cpu_mean INTEGER,
  cpu_peak INTEGER,
  mem_mean INTEGER,
  mem_peak INTEGER
);

INSERT INTO job_tasks
  SELECT job_id, runtime, cpu_mean, cpu_peak, mem_mean, mem_peak
  FROM (SELECT *, MAX(task_id) FROM task_info GROUP BY job_id);

CREATE TABLE job_runtimes AS 
SELECT
  j.id AS job_id,
  j.rap_id AS job_request_id,
  datetime('1970-01-01', j.created_at || ' seconds') AS created_at,
  datetime('1970-01-01', j.started_at || ' seconds') AS started_at,
  datetime('1970-01-01', j.completed_at || ' seconds') AS completed_at,
  j.project,
  j.workspace,
  -- j.user,
  "redacted" AS user,
  j.state != 'succeeded' AS failed,
  j.cancelled AS cancelled,
  j.wait_for_job_ids AS dependencies,
  j.run_command,
  substr(j.run_command, 1, instr(j.run_command, ' ') - 1) AS image,
  j.requires_db,
  (j.requires_db AND instr(j.run_command, ' generate-measures ') > 0) AS uses_measures,
  CAST(j.completed_at - j.started_at AS INTEGER) AS job_runtime,
  jt.runtime AS task_runtime,
  CAST(j.completed_at - j.created_at AS INTEGER) AS lifetime,
  jt.cpu_mean,
  jt.cpu_peak,
  jt.mem_mean,
  jt.mem_peak
FROM prod.job AS j
LEFT JOIN job_tasks AS jt
  ON jt.job_id = j.id
WHERE j.backend = 'tpp'
  AND j.state IN ('succeeded', 'failed')
  AND j.status_message NOT IN (
    'All actions have already completed succesfully',
    'All requested actions were already scheduled to run'
  )
ORDER BY j.completed_at DESC;

.mode json
SELECT * FROM job_runtimes;
