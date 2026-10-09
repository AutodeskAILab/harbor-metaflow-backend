"""A Harbor ``Job`` run through the backend, with Metaflow and Batch in-process."""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import sys
from importlib.metadata import entry_points
from pathlib import Path

import pytest

from harbor_metaflow_backend import backend as B
from harbor_metaflow_backend import worker as W
from harbor_metaflow_backend.store import LocalDirStore

from conftest import (
    InProcessLauncher,
    job_config,
    make_backend,
    needs_aclose,
    needs_hook,
    needs_preflight_skip,
    planned_trial_configs,
    run_async,
    trial_dirs,
)


@needs_hook
@run_async
async def test_trials_route_through_backend_and_land_in_job_dir(tmp_path, fake_trials):
    from harbor.job import Job
    from harbor.models.trial.config import TrialConfig

    agent_file = tmp_path / "submit-host" / "agent.json"
    agent_file.parent.mkdir(parents=True, exist_ok=True)
    agent_file.write_text('{"setting": 1}')
    launcher = InProcessLauncher(tmp_path / "batch-host")
    backend = make_backend(
        tmp_path, launcher, keep_run_root=True, stage_agent_kwargs="config_path"
    )
    job = await Job.create(job_config(tmp_path, agent_file=agent_file), backend=backend)

    result = await job.run()

    # One flow run, three trials in two shards of two.
    assert len(launcher.starts) == 1
    run_root = Path(launcher.starts[0])
    assert run_root.parent == tmp_path / "object-store"
    manifest = W.load_manifest(LocalDirStore(run_root))
    assert [len(s) for s in manifest["shards"]] == [2, 1]
    assert manifest["n_concurrent"] == 2
    # The task dir and the agent file were staged once, not per trial.
    assert len(list((run_root / "tasks").iterdir())) == 1
    assert len(list((run_root / "files").iterdir())) == 1

    # Every trial ran on the "Batch host", with its paths rebound there.
    assert len(fake_trials.seen) == 3
    for seen in fake_trials.seen:
        assert str(tmp_path / "batch-host") in str(seen.trials_dir)
        assert str(tmp_path / "batch-host") in str(seen.task.path)
        assert Path(seen.agent.kwargs["config_path"]).read_text() == '{"setting": 1}'

    # Standard layout in the job dir, with the submitted config restored.
    dirs = trial_dirs(job.job_dir)
    assert len(dirs) == 3
    planned = {c.trial_name: c for c in job._trial_configs}
    for trial_dir in dirs:
        for name in ("config.json", "lock.json", "result.json", "agent/log.txt"):
            assert (trial_dir / name).is_file(), name
        config = TrialConfig.model_validate_json(
            (trial_dir / "config.json").read_text()
        )
        assert config == planned[trial_dir.name]
        assert config.agent.kwargs["config_path"] == str(agent_file)
        saved = json.loads((trial_dir / "result.json").read_text())
        assert saved["trial_uri"] == trial_dir.resolve().as_uri()
    assert (job.job_dir / "result.json").is_file()
    assert result.stats.n_completed_trials == 3
    assert result.stats.n_errored_trials == 0
    assert not backend.terminated
    assert launcher.flows[0].cleaned


@needs_hook
@run_async
async def test_job_hooks_fire_for_every_trial(tmp_path, fake_trials):
    from harbor.job import Job
    from harbor.trial.hooks import TrialEvent

    events: list[tuple[str, str]] = []

    async def record(event):
        events.append((event.event.value, event.trial_name))
        # Hooks see the submitted config, not the Batch host's paths.
        assert str(tmp_path / "batch-host") not in str(event.config.trials_dir)

    backend = make_backend(tmp_path, InProcessLauncher(tmp_path / "batch-host"))
    job = await Job.create(job_config(tmp_path), backend=backend)
    job.add_hook(TrialEvent.START, record)
    job.add_hook(TrialEvent.END, record)
    await job.run()

    names = {c.trial_name for c in job._trial_configs}
    assert {n for e, n in events if e == "start"} == names
    assert sorted(n for e, n in events if e == "end") == sorted(names)


@needs_hook
@run_async
async def test_resume_submits_only_unfinished_trials(tmp_path, fake_trials):
    from harbor.job import Job

    # First run: the flow "dies" after shard 0, so the third trial never comes back.
    first = make_backend(tmp_path, InProcessLauncher(tmp_path / "host-1", shards=[0]))
    job = await Job.create(job_config(tmp_path), backend=first)
    with pytest.raises(Exception) as raised:
        await job.run()
    assert "did not come back" in repr(
        getattr(raised.value, "exceptions", raised.value)
    )
    finished = [d for d in trial_dirs(job.job_dir) if (d / "result.json").is_file()]
    assert len(finished) == 2

    # Resume on a fresh backend: only the missing trial is submitted.
    second_launcher = InProcessLauncher(tmp_path / "host-2")
    second = make_backend(tmp_path, second_launcher, keep_run_root=True)
    resumed = await Job.create(job_config(tmp_path), backend=second)
    assert len(resumed._remaining_trial_configs) == 1
    result = await resumed.run()

    assert len(second.runs) == 1 and len(second.runs[0].configs) == 1
    manifest = W.load_manifest(LocalDirStore(second_launcher.starts[0]))
    assert sum(len(s) for s in manifest["shards"]) == 1
    assert result.stats.n_completed_trials == 3
    assert len(trial_dirs(resumed.job_dir)) == 3


@needs_hook
@run_async
async def test_cancel_stops_flow_and_terminates_batch_jobs(tmp_path, fake_trials):
    launcher = InProcessLauncher(tmp_path / "batch-host", hang_with_job_id="job-123")
    backend = make_backend(tmp_path, launcher)

    tasks = [
        asyncio.create_task(c)
        for c in backend.submit_batch(planned_trial_configs(tmp_path))
    ]
    for _ in range(500):
        if launcher.flows:
            break
        await asyncio.sleep(0.01)
    assert launcher.flows, "flow never started"
    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)

    assert all(isinstance(r, asyncio.CancelledError) for r in results)
    assert launcher.flows[0].stopped
    assert backend.terminated == [(["job-123"], "cancelled by harbor")]


@needs_hook
@run_async
async def test_trial_that_raised_on_worker_raises_locally(tmp_path, fake_trials):
    configs = planned_trial_configs(tmp_path, n_attempts=2)
    fake_trials.fail_names = {configs[1].trial_name}
    backend = make_backend(tmp_path, InProcessLauncher(tmp_path / "batch-host"))

    results = await asyncio.gather(
        *backend.submit_batch(configs), return_exceptions=True
    )

    assert results[0].trial_name == configs[0].trial_name
    assert isinstance(results[1], B.RemoteTrialError)
    assert "simulated Trial.create failure" in str(results[1])


@needs_hook
@run_async
async def test_failed_preflight_on_worker_fails_every_shard_trial(
    tmp_path, fake_trials
):
    def no_docker(_configs):
        raise SystemExit(1)

    configs = planned_trial_configs(tmp_path, n_attempts=2)
    launcher = InProcessLauncher(tmp_path / "batch-host", preflight=no_docker)
    backend = make_backend(tmp_path, launcher)

    results = await asyncio.gather(
        *backend.submit_batch(configs), return_exceptions=True
    )

    assert all(isinstance(r, B.RemoteTrialError) for r in results)
    assert "environment preflight failed" in str(results[0])
    assert fake_trials.seen == []


@needs_aclose
@run_async
async def test_job_end_deletes_run_root_of_a_complete_run(tmp_path, fake_trials):
    from harbor.job import Job

    launcher = InProcessLauncher(tmp_path / "batch-host")
    backend = make_backend(tmp_path, launcher)
    job = await Job.create(job_config(tmp_path), backend=backend)
    result = await job.run()

    assert result.stats.n_completed_trials == 3
    assert not Path(launcher.starts[0]).exists()
    assert len(trial_dirs(job.job_dir)) == 3


@needs_aclose
@run_async
async def test_keep_run_root_and_failed_runs_keep_the_run_root(tmp_path, fake_trials):
    from harbor.job import Job

    kept = InProcessLauncher(tmp_path / "host-1")
    backend = make_backend(tmp_path, kept, keep_run_root="true")
    job = await Job.create(job_config(tmp_path), backend=backend)
    await job.run()
    assert Path(kept.starts[0]).is_dir()

    # A run whose flow lost a trial is kept for debugging, even without the option.
    other = tmp_path / "second-job"
    lost = InProcessLauncher(other / "batch-host", shards=[0])
    job = await Job.create(job_config(other), backend=make_backend(other, lost))
    with pytest.raises(Exception) as raised:  # noqa: B017 - Job may wrap it
        await job.run()
    assert "did not come back" in repr(
        getattr(raised.value, "exceptions", raised.value)
    )
    assert Path(lost.starts[0]).is_dir()


@needs_hook
@run_async
async def test_aclose_stops_a_run_that_is_still_going(tmp_path, fake_trials):
    launcher = InProcessLauncher(tmp_path / "batch-host", hang_with_job_id="job-9")
    backend = make_backend(tmp_path, launcher)
    coros = backend.submit_batch(planned_trial_configs(tmp_path))
    task = asyncio.create_task(coros[0])  # starts the driver
    for coro in coros[1:]:
        coro.close()
    for _ in range(500):
        if launcher.flows:
            break
        await asyncio.sleep(0.01)

    await backend.aclose()

    assert launcher.flows[0].stopped
    assert backend.terminated == [(["job-9"], "cancelled by harbor")]
    assert Path(launcher.starts[0]).is_dir()  # not complete: kept
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@needs_preflight_skip
def test_harbor_run_skips_local_preflight_for_this_backend(tmp_path, monkeypatch):
    pytest.importorskip("harbor.environments.docker.docker")
    from harbor.cli.jobs import _run_preflight

    monkeypatch.setattr("shutil.which", lambda _name: None)  # no Docker here
    config = job_config(tmp_path)
    with pytest.raises(SystemExit):
        _run_preflight(config)
    backend = make_backend(tmp_path, InProcessLauncher(tmp_path / "w"))
    assert backend.runs_environment_remotely is True
    _run_preflight(config, backend)

    # In local mode the trials run here, so Harbor's local check applies.
    local = make_backend(tmp_path, InProcessLauncher(tmp_path / "w"), local=True)
    assert local.runs_environment_remotely is False
    with pytest.raises(SystemExit):
        _run_preflight(config, local)


def test_registered_under_harbor_backends_entry_point():
    (ep,) = [
        e for e in entry_points(group="harbor.backends") if e.name == "metaflow-batch"
    ]
    assert ep.value == "harbor_metaflow_backend:MetaflowBatchBackend"
    assert ep.load() is B.MetaflowBatchBackend


@needs_hook
def test_harbor_cli_resolves_the_backend_by_name(tmp_path):
    from harbor.cli.backend_registry import create_trial_backend
    from harbor.models.job.config import RetryConfig

    backend = create_trial_backend(
        "metaflow-batch",
        n_concurrent=3,
        retry_config=RetryConfig(),
        kwargs={"store": str(tmp_path), "image": "example/harbor:1", "cpu": 8},
    )
    assert isinstance(backend, B.MetaflowBatchBackend)
    assert backend.settings.cpu == 8


def test_stock_harbor_imports_but_refuses_to_construct():
    """On a Harbor without ``harbor.trial.backend`` the backend refuses clearly."""
    code = """
import sys

class Block:
    def find_spec(self, name, path=None, target=None):
        if name == "harbor.trial.backend":
            raise ImportError("no hook in this Harbor")

sys.meta_path.insert(0, Block())
import harbor_metaflow_backend as m
assert m.HARBOR_HAS_TRIAL_BACKENDS is False
try:
    m.MetaflowBatchBackend(n_concurrent=1, store="/tmp/x")
except m.TrialBackendUnavailable as exc:
    print(exc)
else:
    raise SystemExit("constructed on a Harbor without the hook")
"""
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert "pluggable trial backends" in out.stdout
    assert "feat/backend-plugins" in out.stdout


@needs_aclose
@run_async
async def test_flow_that_outlives_its_trials_is_waited_for_then_stopped(
    tmp_path, fake_trials
):
    from harbor.job import Job

    launcher = InProcessLauncher(tmp_path / "batch-host", linger=True)
    backend = make_backend(tmp_path, launcher, flow_exit_grace_sec=0.05)
    job = await Job.create(job_config(tmp_path), backend=backend)
    result = await job.run()

    assert result.stats.n_completed_trials == 3
    flow = launcher.flows[0]
    assert flow.stopped and flow.cleaned  # flow dir removed, no process left
    assert backend.terminated == []  # nothing to terminate: every trial came back
    assert not Path(launcher.starts[0]).exists()


@needs_aclose
@run_async
async def test_flow_that_exits_on_its_own_is_cleaned_up_not_stopped(
    tmp_path, fake_trials
):
    from harbor.job import Job

    launcher = InProcessLauncher(tmp_path / "batch-host")
    backend = make_backend(tmp_path, launcher)
    job = await Job.create(job_config(tmp_path), backend=backend)
    await job.run()

    flow = launcher.flows[0]
    assert flow.cleaned and not flow.stopped


@needs_hook
@run_async
async def test_cancelling_the_job_terminates_batch_jobs_of_a_multi_trial_run(
    tmp_path, fake_trials, monkeypatch
):
    """Ctrl-C cancels every trial coroutine of a run at once; each cancels the shared
    driver while it is still stopping the flow. The Batch jobs must still be
    terminated, and the stop is recorded in the run's Metaflow log."""
    from harbor.job import Job

    from conftest import FakeFlow

    monkeypatch.setattr(FakeFlow, "stop_delay", 0.2)
    launcher = InProcessLauncher(tmp_path / "batch-host", hang_with_job_id="job-7")
    backend = make_backend(tmp_path, launcher)
    job = await Job.create(job_config(tmp_path, n_attempts=3), backend=backend)
    run = asyncio.create_task(job.run())
    for _ in range(500):
        if launcher.flows:
            break
        await asyncio.sleep(0.01)
    assert launcher.flows, "flow never started"
    await asyncio.sleep(0.05)

    run.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await run

    assert launcher.flows[0].stopped
    assert backend.terminated == [(["job-7"], "cancelled by harbor")]
    notes = launcher.flows[0].log_path.read_text()
    assert "stopping the flow (cancelled by harbor)" in notes
    assert "terminating Batch jobs job-7" in notes


@needs_hook
@run_async
async def test_a_failing_terminate_is_logged_not_swallowed(
    tmp_path, fake_trials, caplog
):
    launcher = InProcessLauncher(tmp_path / "batch-host", hang_with_job_id="job-8")
    backend = make_backend(tmp_path, launcher)

    def denied(ids, reason):
        raise PermissionError("batch:TerminateJob denied")

    backend.terminate = denied
    task = asyncio.create_task(backend.submit_batch(planned_trial_configs(tmp_path))[0])
    for _ in range(500):
        if launcher.flows:
            break
        await asyncio.sleep(0.01)
    launcher.flows[0].log_path.parent.mkdir(parents=True, exist_ok=True)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert "terminating the Batch jobs" in caplog.text
    assert "TerminateJob denied" in launcher.flows[0].log_path.read_text()
