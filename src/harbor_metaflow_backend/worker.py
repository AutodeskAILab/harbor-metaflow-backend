"""Run one shard of a staged job on this host (the flow's ``run_shard`` step).

Runs inside the generated Metaflow flow, normally in an AWS Batch container. It
needs only what stock Harbor has (``TrialQueue``): the trial backend hook is used
on the submitting side, not here.

    read manifest -> download task dirs  -> environment preflight
                     rebind paths to this     -> TrialQueue(n_concurrent, retry)
                     host                        START hook: events/<trial>/start.json
                                                 result:     results/<trial>/ + .done

Each trial's final directory is uploaded as soon as its coroutine returns (after
the queue's retries), so the submitting side can copy it back while the shard is
still running.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
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
    preflight: Callable[[list], None] | None = None,
    queue_factory: Any = None,
    cleanup: bool = False,
) -> dict:
    """Run every trial of ``shard``; returns ``{"done": [...], "error": [...]}``.

    If the environment preflight fails, no trial runs and every trial of the shard
    is reported as an error with the preflight's message. With ``cleanup``, the
    shard's work dir is removed once its results are uploaded.
    """
    from harbor.models.job.config import RetryConfig
    from harbor.trial.hooks import TrialEvent

    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(store)
    names = manifest["shards"][int(shard)]
    job_id = os.environ.get("AWS_BATCH_JOB_ID")
    if job_id:
        store.put_text(batch_job_key(shard), json.dumps({"job_id": job_id}))

    summary: dict[str, list[str]] = {"done": [], "error": []}
    try:
        configs = [
            localize_config(manifest["trials"][name], work_dir, store) for name in names
        ]
        try:
            (preflight or environment_preflight)(configs)
        except (Exception, SystemExit):  # Harbor preflights exit on failure
            message = (
                "environment preflight failed on the trial host:\n"
                f"{traceback.format_exc()}"
            )
            for name in names:
                store.put_text(error_key(name), message)
            summary["error"] = list(names)
            return summary

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
