"""Run one shard of a staged job on this host (the flow's ``run_shard`` step).

Runs inside the generated Metaflow flow, normally in an AWS Batch container. It
needs only what stock Harbor has (``TrialQueue``): the trial backend hook is used
on the submitting side, not here.

    read manifest -> host preparation -> download task dirs -> environment preflight
                     (bootstrap script,   rebind paths to this  -> TrialQueue(n, retry)
                      prepare callable)   host                     START: events/
                                                                   result: results/

Host preparation runs before anything imports Harbor, so a bootstrap script can
install Harbor and the Docker CLI on an image that has neither.

Each trial's final directory is uploaded as soon as its coroutine returns (after
the queue's retries), so the submitting side can copy it back while the shard is
still running.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
import site
import subprocess
import sys
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

from harbor_metaflow_backend.store import RunStore

MANIFEST_KEY = "manifest.json"
MANIFEST_VERSION = 1


def task_key(name: str) -> str:
    return f"tasks/{name}"


def done_key(trial_name: str) -> str:
    return f"results/{trial_name}.done"


def error_key(trial_name: str) -> str:
    return f"results/{trial_name}.error"


def result_prefix(trial_name: str) -> str:
    return f"results/{trial_name}"


def start_event_key(trial_name: str) -> str:
    return f"events/{trial_name}/start.json"


def batch_job_key(shard: int) -> str:
    return f"batch/{int(shard)}.json"


#: Environment variables a bootstrap script sees (see :func:`run_bootstrap`).
BOOTSTRAP_ENV_FILE = "HARBOR_METAFLOW_ENV_FILE"
BOOTSTRAP_DIR = "HARBOR_METAFLOW_BOOTSTRAP_DIR"
BOOTSTRAP_WORK_DIR = "HARBOR_METAFLOW_WORK_DIR"
BOOTSTRAP_RUN_URI = "HARBOR_METAFLOW_RUN_URI"
BOOTSTRAP_PYTHON = "HARBOR_METAFLOW_PYTHON"
#: Lines of bootstrap output kept in the error reported for each trial.
BOOTSTRAP_TAIL_LINES = 60


class HostPreparationError(RuntimeError):
    """The bootstrap script or the prepare callable failed on the trial host."""


def load_manifest(store: RunStore) -> dict:
    text = store.get_text(MANIFEST_KEY)
    if text is None:
        raise FileNotFoundError(f"{store.uri}/{MANIFEST_KEY} does not exist")
    manifest = json.loads(text)
    if manifest.get("version") != MANIFEST_VERSION:
        raise ValueError(f"unsupported manifest version {manifest.get('version')!r}")
    return manifest


def localize_config(entry: dict, work_dir: Path, store: RunStore):
    """The trial's ``TrialConfig`` with task path, trials dir and files on this host."""
    from harbor.models.trial.config import TrialConfig

    config = TrialConfig.model_validate_json(entry["config"])
    if entry.get("task"):
        local_task = work_dir / entry["task"]
        if not local_task.exists():
            store.download_dir(entry["task"], local_task)
        config.task.path = local_task
    for kwarg, key in (entry.get("files") or {}).items():
        local_file = work_dir / key
        if not local_file.exists():
            store.download_file(key, local_file)
        config.agent.kwargs[kwarg] = str(local_file)
    config.trials_dir = work_dir / "trials"
    return config


def apply_env_file(path: Path, environ: dict | None = None) -> dict[str, str]:
    """Load ``KEY=VALUE`` lines a bootstrap script wrote into this process.

    Blank lines and ``#`` comments are skipped; values are taken literally, so the
    script expands them itself::

        echo "PATH=/opt/x/bin:$PATH" >> "$HARBOR_METAFLOW_ENV_FILE"

    ``PYTHONPATH`` entries are also put on ``sys.path``.
    """
    environ = os.environ if environ is None else environ
    applied: dict[str, str] = {}
    if not path.is_file():
        return applied
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not key:
            raise HostPreparationError(f"bad line in {path.name}: {line!r}")
        environ[key] = value
        applied[key] = value
    for entry in reversed(applied.get("PYTHONPATH", "").split(os.pathsep)):
        if entry and entry not in sys.path:
            sys.path.insert(0, entry)
    return applied


def _refresh_import_paths() -> None:
    """Make packages a bootstrap just installed importable in this process."""
    user_site = site.getusersitepackages()
    if os.path.isdir(user_site) and user_site not in sys.path:
        site.addsitedir(user_site)
    importlib.invalidate_caches()


def run_bootstrap(
    store: RunStore, spec: dict, work_dir: Path, *, shell: str = "bash"
) -> dict[str, str]:
    """Run the staged bootstrap script on this host; returns the env it exported.

    The script and ``bootstrap_files`` are downloaded to ``<work_dir>/bootstrap/``
    and the script runs there with ``bash``. It sees ``HARBOR_METAFLOW_BOOTSTRAP_DIR``
    (that dir), ``HARBOR_METAFLOW_WORK_DIR``, ``HARBOR_METAFLOW_RUN_URI``,
    ``HARBOR_METAFLOW_PYTHON`` (the worker's interpreter, the one to ``pip install``
    into) and ``HARBOR_METAFLOW_ENV_FILE``: ``KEY=VALUE`` lines appended to that
    file are set in the worker (and so in every trial's subprocesses) when the script
    exits 0.
    """
    bdir = work_dir / "bootstrap"
    bdir.mkdir(parents=True, exist_ok=True)
    keys = [spec["script"], *spec.get("files", [])]
    for key in keys:
        store.download_file(key, bdir / Path(key).name)
    script = bdir / Path(spec["script"]).name
    env_file = bdir / "env"
    env_file.write_text("")
    env = {
        **os.environ,
        BOOTSTRAP_ENV_FILE: str(env_file),
        BOOTSTRAP_DIR: str(bdir),
        BOOTSTRAP_WORK_DIR: str(work_dir),
        BOOTSTRAP_RUN_URI: store.uri,
        BOOTSTRAP_PYTHON: sys.executable,
    }
    timeout = spec.get("timeout_sec") or None
    print(f"harbor-metaflow: running bootstrap {script.name}", flush=True)
    try:
        proc = subprocess.run(
            [shell, str(script)],
            cwd=bdir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout or ""
        if isinstance(out, bytes):
            out = out.decode(errors="replace")
        sys.stdout.write(out)
        raise HostPreparationError(
            f"bootstrap {script.name} timed out after {timeout}s; last output:\n"
            + "\n".join(out.splitlines()[-BOOTSTRAP_TAIL_LINES:])
        ) from None
    sys.stdout.write(proc.stdout)
    sys.stdout.flush()
    if proc.returncode != 0:
        tail = "\n".join(proc.stdout.splitlines()[-BOOTSTRAP_TAIL_LINES:])
        raise HostPreparationError(
            f"bootstrap {script.name} exited {proc.returncode}; last output:\n{tail}"
        )
    applied = apply_env_file(env_file)
    _refresh_import_paths()
    return applied


def run_prepare(import_path: str, work_dir: Path) -> None:
    """Call ``module:function`` with this shard's work dir (in this process)."""
    module_name, sep, attr = import_path.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(f"prepare must be 'module:function', got {import_path!r}")
    func = importlib.import_module(module_name)
    for part in attr.split("."):
        func = getattr(func, part)
    func(work_dir)


def prepare_host(store: RunStore, manifest: dict, work_dir: Path) -> None:
    """The manifest's bootstrap script, then its prepare callable, if any."""
    if manifest.get("bootstrap"):
        run_bootstrap(store, manifest["bootstrap"], work_dir)
    if manifest.get("prepare"):
        run_prepare(manifest["prepare"], work_dir)


def environment_preflight(configs) -> None:
    """Harbor's environment preflight, once per distinct environment, on this host.

    The submitting host skips it when Harbor honours the backend's
    ``runs_environment_remotely`` (it may have no Docker daemon at all).
    """
    from harbor.environments.factory import EnvironmentFactory

    seen = set()
    for config in configs:
        env = config.environment
        if (env.type, env.import_path) in seen:
            continue
        seen.add((env.type, env.import_path))
        EnvironmentFactory.run_preflight(type=env.type, import_path=env.import_path)


async def run_shard_async(
    store: RunStore,
    shard: int,
    work_dir: str | Path,
    *,
    prepare: Callable[[RunStore, dict, Path], None] | None = None,
    preflight: Callable[[list], None] | None = None,
    queue_factory: Any = None,
    cleanup: bool = False,
) -> dict:
    """Run every trial of ``shard``; returns ``{"done": [...], "error": [...]}``.

    The host is prepared first (``prepare_host``: the manifest's bootstrap script and
    prepare callable), before Harbor is imported. If that or the environment
    preflight fails, no trial runs and every trial of the shard is reported as an
    error with the message. With ``cleanup``, the shard's work dir is removed once
    its results are uploaded.
    """
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(store)
    names = manifest["shards"][int(shard)]
    job_id = os.environ.get("AWS_BATCH_JOB_ID")
    if job_id:
        store.put_text(batch_job_key(shard), json.dumps({"job_id": job_id}))

    summary: dict[str, list[str]] = {"done": [], "error": []}

    def fail_all(what: str) -> dict:
        message = f"{what} failed on the trial host:\n{traceback.format_exc()}"
        print(message, file=sys.stderr, flush=True)
        for name in names:
            store.put_text(error_key(name), message)
        summary["error"] = list(names)
        return summary

    try:
        try:
            (prepare or prepare_host)(store, manifest, work_dir)
        except (Exception, SystemExit):
            return fail_all("host preparation")

        from harbor.models.job.config import RetryConfig
        from harbor.trial.hooks import TrialEvent

        configs = [
            localize_config(manifest["trials"][name], work_dir, store) for name in names
        ]
        try:
            (preflight or environment_preflight)(configs)
        except (Exception, SystemExit):  # Harbor preflights exit on failure
            return fail_all("environment preflight")

        if queue_factory is None:
            from harbor.trial.queue import TrialQueue as queue_factory
        queue = queue_factory(
            n_concurrent=max(1, min(int(manifest["n_concurrent"]), len(configs))),
            retry_config=RetryConfig.model_validate(manifest.get("retry") or {}),
        )

        async def on_start(event) -> None:
            await asyncio.to_thread(
                store.put_text,
                start_event_key(event.trial_name),
                event.model_dump_json(),
            )

        queue.add_hook(TrialEvent.START, on_start)

        async def finish(config, coro) -> None:
            name = config.trial_name
            try:
                await coro
            except Exception:  # reported per trial; siblings keep running
                await asyncio.to_thread(
                    store.put_text, error_key(name), traceback.format_exc()
                )
                summary["error"].append(name)
                return
            trial_dir = Path(config.trials_dir) / name
            await asyncio.to_thread(store.upload_dir, trial_dir, result_prefix(name))
            await asyncio.to_thread(store.put_text, done_key(name), "")
            summary["done"].append(name)

        coros = queue.submit_batch(configs)
        async with asyncio.TaskGroup() as tg:
            for config, coro in zip(configs, coros, strict=True):
                tg.create_task(finish(config, coro))
        return summary
    finally:
        if cleanup:
            shutil.rmtree(work_dir, ignore_errors=True)


def run_shard(store: RunStore, shard: int, work_dir: str | Path, **kwargs) -> dict:
    return asyncio.run(run_shard_async(store, shard, work_dir, **kwargs))
