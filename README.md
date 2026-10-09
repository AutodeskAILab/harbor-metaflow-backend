# harbor-metaflow-backend

Run a [Harbor](https://github.com/harbor-framework/harbor) job's trials on AWS Batch,
with [Metaflow](https://metaflow.org) doing the fan-out.

```bash
harbor run -p tasks/ -a <agent> -e docker \
    --backend metaflow-batch \
    --backend-kwarg queue=<your-batch-queue> \
    --backend-kwarg image=<image> \
    --backend-kwarg store=s3://<bucket>/<prefix>
```

> **Status: alpha, and it depends on a Harbor change that is not merged yet.**
> `harbor run --backend` (pluggable trial backends) is proposed upstream. Until it is
> released, install Harbor from the hook branch
> `git+https://github.com/AutodeskAILab/harbor@feat/backend-plugins`. On a Harbor
> without the hook this package imports, but the backend refuses to start with a
> message saying so.

## Why

`harbor run` executes every trial of a job on the machine it was started on, with
local asyncio concurrency (`-n`). That machine's CPU, memory and Docker daemon cap
how many trials can run at once. Harbor's cloud environments move the *sandbox*
elsewhere, but the agent loop and the orchestration still run on one host.

This backend keeps the job a single, ordinary Harbor job on the submitting host
(the same `jobs/<job>/` directory, live progress, metrics, plugins and
`harbor job resume`) while the trials run on as many AWS Batch hosts as you allow.
Each Batch job runs a shard of trials with Harbor's own local runner, so agents,
environments, verifiers and retries behave exactly as they do locally.

## Install

Python 3.12 or newer.

```bash
# 1. Harbor with the trial backend hook (until it is released upstream).
pip install "harbor @ git+https://github.com/AutodeskAILab/harbor@feat/backend-plugins"

# 2. This package, with boto3 for s3:// stores.
pip install "harbor-metaflow-backend[s3] @ git+https://github.com/AutodeskAILab/harbor-metaflow-backend"
```

The package registers itself under Harbor's `harbor.backends` entry point group as
`metaflow-batch`, so `harbor run --backend metaflow-batch` finds it once installed.

Metaflow must be configured for AWS on the submitting host (S3 datastore and AWS
Batch; see Metaflow's
[AWS deployment docs](https://docs.metaflow.org/getting-started/infrastructure)).
The backend uses whatever Metaflow configuration is active.

## Quickstart

```bash
# 8 attempts per task, 8 trials per Batch job, 4 at a time on each Batch host.
harbor run -p tasks/ -a <agent> -m <model> -e docker -k 8 -n 4 \
    --backend metaflow-batch \
    --backend-kwarg queue=<your-batch-queue> \
    --backend-kwarg image=<registry>/<repository>:<tag> \
    --backend-kwarg store=s3://<bucket>/<prefix> \
    --backend-kwarg shard_size=8 \
    --backend-kwarg cpu=16 --backend-kwarg memory=65536

# Interrupted, or some trials did not come back? Resume reruns only those.
harbor job resume -p jobs/<job name> --backend metaflow-batch \
    --backend-kwarg queue=<your-batch-queue> \
    --backend-kwarg image=<registry>/<repository>:<tag> \
    --backend-kwarg store=s3://<bucket>/<prefix>
```

`-n` is the number of concurrent trials **per Batch job**. Size `cpu` and `memory`
for that many trial sandboxes plus the agents.

To try the whole path without AWS, add `--backend-kwarg local=true`: Metaflow runs
the shards as local processes on this machine (it then needs Docker locally, and a
local directory works as the store).

## Configuration

Every option can be passed as `--backend-kwarg key=value` or set as the environment
variable `HARBOR_METAFLOW_<KEY>` (for example `HARBOR_METAFLOW_QUEUE`). A kwarg wins
over the environment. There are no built-in account, queue or bucket defaults.

| Key | Default | Meaning |
|---|---|---|
| `store` | **required** | Where inputs and results are staged: `s3://<bucket>/<prefix>`, or a directory every host can see. One `<job id>-<token>/` per submission. |
| `image` | **required** (unless `local`) | Container image for the Batch jobs. See [requirements](#requirements-on-the-batch-host). |
| `queue` | Metaflow's `METAFLOW_BATCH_JOB_QUEUE` | AWS Batch job queue. |
| `iam_role` | Metaflow's `METAFLOW_ECS_S3_ACCESS_IAM_ROLE` | IAM role for the job containers. Needs read/write on `store`. |
| `cpu` | `4` | vCPUs per Batch job (one shard). |
| `memory` | `16384` | Memory per Batch job, in MiB. |
| `timeout_sec` | `86400` | Metaflow `@timeout` for one shard; `0` disables it. |
| `shard_size` | `8` | Trials per Batch job. |
| `max_workers` | `16` | Batch jobs in flight at once (Metaflow `--max-workers`). |
| `poll_interval_sec` | `30` | How often the store is polled for finished trials. |
| `work_dir` | `/var/lib/harbor-work` | Trial working directory on the Batch host, mounted at the same path. |
| `docker_socket` | `/var/run/docker.sock` | Host Docker socket mounted into the job. |
| `privileged` | `false` | Run the job container privileged. |
| `env_vars` | none | Comma-separated names of variables to copy from this shell into the Batch jobs (Metaflow `@environment`). **Not for secrets**: values end up in the generated flow and the Batch job definition. |
| `secrets` | none | Comma-separated Metaflow `@secrets` sources (e.g. AWS Secrets Manager secret ids) exposed to the jobs as environment variables. Use this for agent API keys. |
| `stage_agent_kwargs` | none | Comma-separated agent kwargs whose values are local files; they are uploaded and rebound to the Batch host's copy. |
| `keep_run_root` | `false` | Keep a submission's staged files even when all its trials came back. |
| `local` | `false` | Run the shards on this machine through Metaflow's local runtime instead of Batch. |
| `python` | this interpreter | Python used to launch the flow. |
| `metaflow_args` | none | Extra arguments appended to the flow's `run` command. |

## How it works

```
 submitting host                         store (S3)                       AWS Batch, one job per shard
 ---------------                         ----------                       ----------------------------
 harbor run --backend metaflow-batch
   Job plans trials (and, on resume,
   skips the finished ones)
   | submit_batch(configs)
   v
 stage ------------------------------->  manifest.json
                                         tasks/<task>/  (each once)
                                         files/<file>
 start Metaflow flow ---------------------------------------------------> run_shard(shard):
   start -> run_shard (foreach) -> join                                     download tasks, rebind paths
                                                                            environment preflight
                                                                            Harbor TrialQueue(-n, retries)
 poll <--------------------------------  events/<trial>/start.json <---- START hook
   replay START to the job's hooks
 poll <--------------------------------  results/<trial>/ + .done  <---- finished trial dir
   copy to jobs/<job>/<trial>/,
   restore the submitted config,
   emit END (progress, metrics, plugins)
   v
 Job writes result.json; aclose() deletes the staged files of complete submissions
```

- **Paths.** Trial configs point at the submitting host. The worker downloads each
  distinct task directory once and rebinds the task path, the trials directory and
  staged agent files to its own copies. On the way back, `config.json`,
  `result.json` and `lock.json` get the submitted values back, so `harbor job
  resume`, `harbor view` and uploads see an ordinary job directory.
- **Failures.** A trial that raised on the worker raises `RemoteTrialError` locally,
  as it would under Harbor's local runner. A trial with no result when the flow
  exits raises `TrialNotReturned`. Finished trials are already in the job
  directory, so `harbor job resume` reruns only the rest.
- **Cancellation.** Ctrl-C, or the job failing, stops the Metaflow process group
  (SIGINT, then SIGTERM, then SIGKILL) and terminates every Batch job the workers
  recorded.
- **Preflight.** The backend declares `runs_environment_remotely`, so a Harbor that
  supports it skips the environment check (e.g. "is Docker running?") on the
  submitting host. Each Batch job runs that check before its trials; if it fails,
  every trial of the shard fails with "environment preflight failed".
- **Cleanup.** On a Harbor that calls `aclose()`, the staged files of each
  submission whose trials all came back are deleted when the job ends. Submissions
  with a failed or missing trial are kept for debugging.

## Requirements on the Batch host

The image runs the trials with Harbor's local runner, so it needs what a local
`harbor run` needs:

- **Python 3.12+ with Harbor (hook branch), Metaflow, boto3 and this package
  installed**, at the same versions as the submitting host.
- **The Docker CLI** (with Compose) for `-e docker`. Trials start their sandboxes
  as sibling containers through the host's Docker daemon: the job mounts
  `docker_socket`, and the image's user must be allowed to use it (root, or a
  member of the socket's group). Metaflow's `@batch` has no `user` option, so set
  the `USER` in the image.
- **The same work directory path inside the container and on the host.** The
  Docker daemon resolves bind mounts on the host, so `work_dir` is mounted at the
  same path on both sides. Use a disk with room for the task images and trial
  outputs of one shard.
- **Credentials** for the agents (via `secrets`, an IAM role, or the image) and
  network access to whatever the agents call.
- An IAM role (`iam_role`, or Metaflow's default) that can read and write `store`.
  Terminating Batch jobs on cancellation needs `batch:TerminateJob` on the
  submitting host.

## Limitations

- No real AWS run is part of the test suite; Metaflow, Batch and S3 are replaced
  in-process (`tests/conftest.py`), and the generated flow is checked by Metaflow
  itself in local mode.
- Retries happen on the Batch host, so the job's `n_retries` statistic stays 0.
- `-n` and the Batch reservation (`cpu`, `memory`) are set independently; keep
  them consistent.
- Every submission's run root is kept on a Harbor without `aclose()`, and failed
  submissions are always kept. Expire the store prefix with an S3 lifecycle rule.
- Only `-e docker`-style environments that run on the Batch host have been
  considered. Cloud sandbox environments (e.g. Daytona, Modal) work in principle
  but gain little from this backend.
- Trial configs (including agent kwargs) are written to the store in plain JSON.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues and pull requests are welcome.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE).
