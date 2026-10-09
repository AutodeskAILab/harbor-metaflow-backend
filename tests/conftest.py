"""Fakes that replace Metaflow, AWS Batch and the trial runtime.

``InProcessLauncher`` stands in for the generated flow on Batch: it runs the real
worker (``worker.run_shard``: Harbor's ``TrialQueue`` on the "Batch host") for each
shard in a thread, against a ``LocalDirStore`` run root. ``Trial.create`` is
replaced by ``FakeTrial``, which writes the standard trial dir a real ``Trial``
writes. No AWS, no Docker, no Metaflow scheduler.
"""

from __future__ import annotations

import asyncio
import functools
import importlib.util
import json
import threading
import uuid
from pathlib import Path

import pytest

from harbor_metaflow_backend import backend as B
from harbor_metaflow_backend import worker as W
from harbor_metaflow_backend.store import LocalDirStore

HAS_HOOK = importlib.util.find_spec("harbor.trial.backend") is not None
needs_hook = pytest.mark.skipif(not HAS_HOOK, reason="Harbor without trial backends")
needs_aclose = pytest.mark.skipif(
    not B.HARBOR_CLOSES_BACKENDS, reason="Harbor does not call TrialBackend.aclose"
)
needs_preflight_skip = pytest.mark.skipif(
    not B.HARBOR_SKIPS_REMOTE_ENV_PREFLIGHT,
    reason="Harbor without runs_environment_remotely",
)


def run_async(test):
    """Run an async test with asyncio.run (no pytest-asyncio needed)."""

    @functools.wraps(test)
    def wrapper(*args, **kwargs):
        return asyncio.run(test(*args, **kwargs))

    return wrapper


class FakeTrial:
    """``Trial`` stand-in: writes the standard trial dir and fires START and END."""

    seen: list = []
    fail_names: set[str] = set()

    def __init__(self, config):
        from harbor.models.trial.paths import TrialPaths
        from harbor.trial.hooks import TrialEvent

        self.config = config
        self.paths = TrialPaths(Path(config.trials_dir) / config.trial_name)
        self._hooks = {event: [] for event in TrialEvent}

    @classmethod
    async def create(cls, config):
        if any(config.trial_name.startswith(n) for n in cls.fail_names):
            raise RuntimeError(
                f"simulated Trial.create failure for {config.trial_name}"
            )
        cls.seen.append(config)
        return cls(config)

    def add_hook(self, event, hook) -> None:
        self._hooks[event].append(hook)

    async def run(self):
        from harbor.models.job.lock import build_trial_lock
        from harbor.models.trial.result import AgentInfo, TrialResult
        from harbor.models.verifier.result import VerifierResult
        from harbor.tasks.client import TaskDownloadResult
        from harbor.trial.backend import load_trial_hook_event
        from harbor.trial.hooks import TrialEvent

        config, paths = self.config, self.paths
        paths.trial_dir.mkdir(parents=True, exist_ok=True)
        paths.config_path.write_text(config.model_dump_json(indent=4))
        lock = build_trial_lock(
            trial_config=config,
            task_download_result=TaskDownloadResult(
                path=config.task.get_local_path(), download_time_sec=0.0, cached=True
            ),
        )
        paths.lock_path.write_text(lock.model_dump_json(indent=4, exclude_none=True))
        (paths.trial_dir / "agent").mkdir(exist_ok=True)
        (paths.trial_dir / "agent" / "log.txt").write_text(f"ran {config.trial_name}\n")
        result = TrialResult(
            task_name="test-task",
            trial_name=config.trial_name,
            trial_uri=paths.trial_dir.resolve().as_uri(),
            task_id=config.task.get_task_id(),
            source=config.task.source,
            task_checksum="abc123",
            config=config,
            agent_info=AgentInfo(name="fake-agent", version="1.0"),
            verifier_result=VerifierResult(rewards={"reward": 1}),
        )
        paths.result_path.write_text(result.model_dump_json(indent=4))
        for event in (TrialEvent.START, TrialEvent.END):
            hook_event = load_trial_hook_event(event, paths.trial_dir)
            for hook in self._hooks[event]:
                await hook(hook_event)
        return result


class FakeFlow:
    def __init__(self, thread: threading.Thread | None, log_path: Path):
        self.thread = thread
        self.log_path = log_path
        self.stopped = False
        self.cleaned = False

    def poll(self):
        if self.thread is None:
            return None  # "runs on Batch" until stopped
        return None if self.thread.is_alive() else 0

    def stop(self, grace: float = 0) -> None:
        self.stopped = True

    def cleanup(self) -> None:
        self.cleaned = True


class InProcessLauncher:
    """Runs the worker for each shard in a thread, like the flow's foreach on Batch."""

    def __init__(
        self,
        work_root: Path,
        shards: list[int] | None = None,
        hang_with_job_id: str | None = None,
        preflight=None,
    ):
        self.work_root = work_root
        self.only_shards = shards
        self.hang_with_job_id = hang_with_job_id
        # The real preflight checks Docker; the test host may have none.
        self.preflight = preflight or (lambda _configs: None)
        self.starts: list[str] = []
        self.flows: list[FakeFlow] = []

    def start(self, run_uri: str, n_shards: int, log_path: Path) -> FakeFlow:
        self.starts.append(run_uri)
        store = LocalDirStore(run_uri)
        assert n_shards == len(W.load_manifest(store)["shards"])
        if self.hang_with_job_id:
            store.put_text(
                W.batch_job_key(0), json.dumps({"job_id": self.hang_with_job_id})
            )
            flow = FakeFlow(None, log_path)
        else:

            def run_all():
                for shard in range(n_shards):
                    if self.only_shards is None or shard in self.only_shards:
                        W.run_shard(
                            store,
                            shard,
                            self.work_root / str(shard),
                            preflight=self.preflight,
                        )

            thread = threading.Thread(target=run_all, daemon=True)
            thread.start()
            flow = FakeFlow(thread, log_path)
        self.flows.append(flow)
        return flow


@pytest.fixture
def fake_trials(monkeypatch):
    FakeTrial.seen = []
    FakeTrial.fail_names = set()
    monkeypatch.setattr("harbor.trial.trial.Trial.create", FakeTrial.create)
    return FakeTrial


def task_dir(tmp_path: Path) -> Path:
    task = tmp_path / "submit-host" / "task"
    if task.is_dir():  # a resumed job plans against the same task dir
        return task
    (task / "environment").mkdir(parents=True)
    (task / "environment" / "Dockerfile").write_text("FROM alpine:3.19\n")
    (task / "tests").mkdir()
    (task / "tests" / "test.sh").write_text("#!/usr/bin/env sh\nexit 0\n")
    (task / "task.toml").write_text('[task]\nname = "test-org/test-task"\n')
    (task / "instruction.md").write_text("Do the thing.\n")
    return task


def job_config(tmp_path: Path, n_attempts: int = 3, agent_file: Path | None = None):
    from harbor.models.job.config import JobConfig
    from harbor.models.trial.config import AgentConfig, TaskConfig

    kwargs = {"config_path": str(agent_file)} if agent_file else {}
    return JobConfig(
        job_name="mf-job",
        jobs_dir=tmp_path / "submit-host" / "jobs",
        n_attempts=n_attempts,
        n_concurrent_trials=2,
        quiet=True,
        agents=[AgentConfig(name="oracle", kwargs=kwargs)],
        tasks=[TaskConfig(path=task_dir(tmp_path))],
    )


def make_backend(tmp_path: Path, launcher, **kw):
    terminated: list = []
    backend = B.MetaflowBatchBackend(
        n_concurrent=2,
        store=str(tmp_path / "object-store"),
        shard_size=2,
        poll_interval_sec=0.01,
        launcher=launcher,
        store_factory=LocalDirStore,
        terminate=lambda ids, reason: terminated.append((ids, reason)),
        **kw,
    )
    backend.terminated = terminated
    return backend


def trial_dirs(job_dir: Path) -> list[Path]:
    return sorted(p for p in job_dir.iterdir() if p.is_dir())


def planned_trial_configs(tmp_path: Path, n_attempts: int = 1):
    """Planned trial configs for a job, without creating the Job."""
    from harbor.job_plan import JobPlan
    from harbor.models.trial.config import TaskConfig

    config = job_config(tmp_path, n_attempts=n_attempts)
    task_configs = [TaskConfig(path=config.tasks[0].path)]
    return JobPlan.build_trial_configs(config, task_configs, job_id=uuid.uuid4())
