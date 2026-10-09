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

The image does not need this package (it is shipped with the flow), nor Harbor or
the Docker CLI if a bootstrap installs them. For an image with Python 3.12, Metaflow
and boto3 whose `USER` is not root:

```bash
harbor run -p tasks/ -a <agent> -e docker \
    --backend metaflow-batch \
    --backend-kwarg queue=<your-batch-queue> \
    --backend-kwarg image=<image> \
    --backend-kwarg store=s3://<bucket>/<prefix> \
    --backend-kwarg job_user=root \
    --backend-kwarg bootstrap=examples/bootstrap/docker-cli-and-harbor.sh \
    --backend-kwarg bootstrap_files=dist/harbor-<version>-py3-none-any.whl
```

See [Preparing the Batch host](#preparing-the-batch-host).

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
| `image` | **required** (unless `local`) | Container image for the Batch jobs. See [requirements](#preparing-the-batch-host). |
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
| `job_user` | the image's `USER` | Container user for the Batch jobs (e.g. `root`), set on the job definition. See [Preparing the Batch host](#preparing-the-batch-host). |
| `bootstrap` | none | Local shell script staged with the run and run with `bash` on every Batch job before its trials, before Harbor is imported. |
| `bootstrap_files` | none | Comma-separated local files staged next to the bootstrap script (e.g. a Harbor wheel). |
| `bootstrap_timeout_sec` | `0` (none) | Time limit for the bootstrap script. |
| `prepare` | none | `module:function` called on every Batch job after the bootstrap, with the shard's work dir, before Harbor is imported. |
| `code_paths` | none | Comma-separated local directories or files shipped with the flow (Metaflow code package) and importable on the Batch host by their top-level name, e.g. a package holding a custom agent or environment. |
| `package_suffixes` | Metaflow's default (`.py`) | File suffixes Metaflow puts in the code package (`--package-suffixes`), e.g. `.py,.json`. |
| `flow_exit_grace_sec` | `120` | After the last trial came back, how long the flow gets to finish before it is stopped. |
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
   start -> run_shard (foreach) -> join                                     bootstrap, prepare
                                                                            download tasks, rebind paths
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
  recorded. What it did, and any error, is appended to the run's
  `metaflow-<token>.log` in the job directory.
- **Preflight.** The backend declares `runs_environment_remotely`, so a Harbor that
  supports it skips the environment check (e.g. "is Docker running?") on the
  submitting host. Each Batch job runs that check before its trials; if it fails,
  every trial of the shard fails with "environment preflight failed".
- **Cleanup.** Once every trial is back, the flow gets `flow_exit_grace_sec` to
  finish its join step, then is stopped, and its temporary directory is removed. On
  a Harbor that calls `aclose()`, the staged files of each submission whose trials
  all came back are deleted when the job ends. Submissions with a failed or missing
  trial are kept for debugging.

## Preparing the Batch host

Each Batch job runs a shard with Harbor's local runner, so the host needs what a
local `harbor run` needs. What the backend provides, and what you provide:

- **This package**: shipped. The generated flow is written to a directory together
  with a copy of this package (and your `code_paths`); Metaflow uploads that
  directory as the flow's code package and puts it on `sys.path` in the job. The
  image needs Python 3.12+, Metaflow and boto3 (for an `s3://` store).
- **Harbor and the Docker CLI** (with Compose and Buildx for `-e docker`): in the
  image, or installed by a `bootstrap` script. Use the same Harbor on both sides;
  the simplest way is to build a wheel of the submitting host's Harbor and stage it
  with `bootstrap_files`.
- **The Docker socket and a same-path work directory**: mounted by the backend
  (`docker_socket`, `work_dir`). Trials start sibling containers through the host's
  daemon, which resolves bind mounts on the host, so `work_dir` has the same path on
  both sides.
- **A user that can open the socket**: root, or a member of the socket's group.
  Metaflow's `@batch` has no `user` option; set `USER` in the image or pass
  `job_user=root`, which registers the flow's job definitions with
  `containerProperties.user` (and a `_u<user>` name suffix, so a definition without
  the user is never reused). `job_user` wraps a private Metaflow method
  (`BatchJob._register_job_definition`); on a Metaflow where that changed it raises
  instead of running as the image's user.
- **Credentials and network** for the agents and the tasks (`secrets`, the IAM
  role, the image). Task Dockerfiles and verifiers that download things need egress
  from the Batch hosts to those sites.
- An IAM role (`iam_role`, or Metaflow's default) that can read and write `store`,
  and `batch:TerminateJob` on the submitting host for cancellation.

### Host preparation: `bootstrap` and `prepare`

Before a shard's trials, and before anything imports Harbor, the worker:

1. downloads the `bootstrap` script and `bootstrap_files` to
   `<work dir>/bootstrap/` and runs the script there with `bash`. It sees
   `HARBOR_METAFLOW_BOOTSTRAP_DIR`, `HARBOR_METAFLOW_WORK_DIR`,
   `HARBOR_METAFLOW_RUN_URI`, `HARBOR_METAFLOW_PYTHON` (the worker's interpreter:
   `pip install` into it) and `HARBOR_METAFLOW_ENV_FILE`. `KEY=VALUE` lines appended
   to that file are set in the worker, and so in every trial's subprocesses, e.g.
   `echo "PATH=/opt/docker/bin:$PATH" >> "$HARBOR_METAFLOW_ENV_FILE"`;
2. calls `prepare` (`module:function`, importable from `code_paths` or the image)
   with the shard's work dir: for anything that is easier in Python, such as
   building a task image on the host.

If either fails, no trial of the shard runs, and each raises `RemoteTrialError`
with "host preparation failed" and the last lines of the script's output. Both run
once per Batch job; jobs that share a host run them concurrently, so make them
idempotent and keep shared state (e.g. under `work_dir`) behind a lock.

[`examples/bootstrap/docker-cli-and-harbor.sh`](examples/bootstrap/docker-cli-and-harbor.sh)
installs the Docker CLI, Compose and Buildx (pinned, sha256-checked static
binaries) and Harbor (a staged `harbor-*.whl`, or `harbor==$HARBOR_BOOTSTRAP_VERSION`
from the pip index). Harbor's dependencies are installed under constraints frozen
from the image, so no package already in the image changes version;
`HARBOR_BOOTSTRAP_SKIP_DEPS` (pass it with `env_vars`) lists dependencies the
trials do not need, e.g. `litellm supabase` for the oracle agent. It needs egress
to download.docker.com, github.com and the pip index.

## Tested on AWS

Besides the unit tests, the backend was run end to end on AWS Batch (EC2 compute
environment, Docker 25 hosts, Metaflow 2.19.39 with an S3 datastore, Harbor from the
hook branch) from a submitting host with no Docker daemon. The image had Python
3.12, Metaflow and boto3 but neither Harbor nor the Docker CLI, and a non-root
`USER`; it ran with `job_user=root`, the example bootstrap and a staged wheel of the
submitting host's Harbor. With `-a oracle -e docker`:

| Check | Result |
|---|---|
| `examples/tasks/hello-workdir` and `hello-user` (Harbor repo), `shard_size=1` | 2 Batch jobs, reward 1.0 each, about 6 minutes including a cold host |
| Standard job dir | `result.json`, `config.json`, `lock.json` per trial with the submitted config; `harbor view` lists the job and trials |
| `harbor job resume` after deleting one finished trial | 1 trial submitted (1 Batch job), the job ends with 2/2 at reward 1.0 |
| Ctrl-C while the jobs bootstrap | `harbor` exits in about 2 seconds; both Batch jobs stop within about 25 seconds; the incomplete run root is kept |
| Complete runs | run root deleted by `aclose()`; no flow process or temporary flow directory left |
| A custom environment and verifier shipped with `code_paths`, task image built on the host by `prepare` | runs; a stock Harbor 0.24.0 worker under a hook-branch submitter works too |

A task whose verifier downloads tools (`examples/tasks/hello-world` installs `uv`
from the internet) scores 0 on hosts without egress to that site: check the
network path for your tasks first.

## Limitations
## Limitations

- No real AWS run is part of the test suite; Metaflow, Batch and S3 are replaced
  in-process (`tests/conftest.py`), and the generated flow is checked by Metaflow
  itself in local mode. The AWS checks above were run by hand.
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
