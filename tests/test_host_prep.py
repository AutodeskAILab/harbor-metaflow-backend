"""Host preparation on the worker (bootstrap script, prepare callable), job_user,
and what the flow dir ships to the Batch jobs."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

from harbor_metaflow_backend import backend as B
from harbor_metaflow_backend import jobdef
from harbor_metaflow_backend import worker as W
from harbor_metaflow_backend.launcher import (
    FLOW_FILE_NAME,
    FlowSettings,
    MetaflowLauncher,
    render_flow,
    write_flow_dir,
)

import conftest
from conftest import (
    InProcessLauncher,
    make_backend,
    needs_hook,
    planned_trial_configs,
    run_async,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in list(os.environ):
        if name.startswith("HARBOR_METAFLOW_") or name.startswith("HMB_TEST_"):
            monkeypatch.delenv(name)


def _script(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / "submit-host" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("set -euo pipefail\n" + body)
    return path


# -- bootstrap script ------------------------------------------------------------


@needs_hook
@run_async
async def test_bootstrap_runs_before_trials_and_exports_env(
    tmp_path, fake_trials, monkeypatch
):
    payload = _script(tmp_path, "payload.txt", "")
    payload.write_text("from-bootstrap-file\n")
    script = _script(
        tmp_path,
        "boot.sh",
        'test -f "$HARBOR_METAFLOW_BOOTSTRAP_DIR/payload.txt"\n'
        'test "$(pwd)" = "$HARBOR_METAFLOW_BOOTSTRAP_DIR"\n'
        'test -x "$HARBOR_METAFLOW_PYTHON"\n'
        'echo "HMB_TEST_MARK=$(cat payload.txt)" >> "$HARBOR_METAFLOW_ENV_FILE"\n'
        'echo "HMB_TEST_RUN=$HARBOR_METAFLOW_RUN_URI" >> "$HARBOR_METAFLOW_ENV_FILE"\n'
        "echo bootstrap-ran\n",
    )
    monkeypatch.setenv("HMB_TEST_MARK", "unset")  # restored after the test
    monkeypatch.setenv("HMB_TEST_RUN", "unset")
    seen = []

    async def record(event):
        seen.append(os.environ["HMB_TEST_MARK"])

    launcher = InProcessLauncher(tmp_path / "batch-host")
    backend = make_backend(
        tmp_path, launcher, bootstrap=str(script), bootstrap_files=str(payload)
    )
    from harbor.trial.hooks import TrialEvent

    backend.add_hook(TrialEvent.END, record)
    results = await asyncio.gather(
        *backend.submit_batch(planned_trial_configs(tmp_path))
    )

    assert len(results) == 1
    assert seen == ["from-bootstrap-file"]
    assert os.environ["HMB_TEST_RUN"] == launcher.starts[0]
    manifest = W.load_manifest(backend.runs[0].store)
    assert manifest["bootstrap"] == {
        "script": "bootstrap/boot.sh",
        "files": ["bootstrap/payload.txt"],
        "timeout_sec": 0,
    }


@needs_hook
@run_async
async def test_failed_bootstrap_fails_every_shard_trial_with_its_output(
    tmp_path, fake_trials
):
    script = _script(tmp_path, "boot.sh", "echo cannot-install-things\nexit 3\n")
    launcher = InProcessLauncher(tmp_path / "batch-host")
    backend = make_backend(tmp_path, launcher, bootstrap=str(script))

    configs = planned_trial_configs(tmp_path, n_attempts=2)
    results = await asyncio.gather(
        *backend.submit_batch(configs), return_exceptions=True
    )

    assert all(isinstance(r, B.RemoteTrialError) for r in results)
    message = str(results[0])
    assert "host preparation failed" in message
    assert "exited 3" in message and "cannot-install-things" in message
    assert fake_trials.seen == []


def test_bootstrap_timeout_is_a_host_preparation_error(tmp_path):
    from harbor_metaflow_backend.store import LocalDirStore

    store = LocalDirStore(tmp_path / "run")
    store.put_text("bootstrap/slow.sh", "echo started\nsleep 30\n")
    with pytest.raises(W.HostPreparationError, match="timed out"):
        W.run_bootstrap(
            store,
            {"script": "bootstrap/slow.sh", "timeout_sec": 1},
            tmp_path / "work",
        )


def test_env_file_parsing(tmp_path, monkeypatch):
    extra = tmp_path / "site"
    env_file = tmp_path / "env"
    env_file.write_text(
        f"# comment\n\nHMB_TEST_A=1=2\nPYTHONPATH={extra}{os.pathsep}\n"
    )
    monkeypatch.setattr(sys, "path", list(sys.path))
    environ: dict = {}
    applied = W.apply_env_file(env_file, environ)
    assert environ["HMB_TEST_A"] == "1=2"
    assert applied.keys() == {"HMB_TEST_A", "PYTHONPATH"}
    assert sys.path[0] == str(extra)

    env_file.write_text("not a pair\n")
    with pytest.raises(W.HostPreparationError, match="bad line"):
        W.apply_env_file(env_file, {})


# -- prepare callable ------------------------------------------------------------


@needs_hook
@run_async
async def test_prepare_callable_runs_on_the_worker(tmp_path, fake_trials):
    conftest.PREPARED.clear()
    launcher = InProcessLauncher(tmp_path / "batch-host")
    backend = make_backend(tmp_path, launcher, prepare="conftest:record_prepare")

    await asyncio.gather(*backend.submit_batch(planned_trial_configs(tmp_path)))

    prepared = conftest.PREPARED
    assert prepared == [tmp_path / "batch-host" / "0"]


@needs_hook
@run_async
async def test_prepare_callable_that_raises_fails_the_shard(tmp_path, fake_trials):
    launcher = InProcessLauncher(tmp_path / "batch-host")
    backend = make_backend(tmp_path, launcher, prepare="conftest:no_such_function")

    results = await asyncio.gather(
        *backend.submit_batch(planned_trial_configs(tmp_path)), return_exceptions=True
    )

    assert "host preparation failed" in str(results[0])
    assert "no_such_function" in str(results[0])


# -- options ---------------------------------------------------------------------


@needs_hook
def test_host_preparation_options_are_validated(tmp_path):
    script = _script(tmp_path, "boot.sh", "true\n")
    common = {"n_concurrent": 1, "store": str(tmp_path / "s"), "local": True}
    with pytest.raises(ValueError, match="does not exist"):
        B.MetaflowBatchBackend(**common, bootstrap=str(tmp_path / "missing.sh"))
    with pytest.raises(ValueError, match="needs a bootstrap script"):
        B.MetaflowBatchBackend(**common, bootstrap_files=str(script))
    with pytest.raises(ValueError, match="distinct file names"):
        B.MetaflowBatchBackend(
            **common, bootstrap=str(script), bootstrap_files=str(script)
        )
    with pytest.raises(ValueError, match="module:function"):
        B.MetaflowBatchBackend(**common, prepare="just_a_module")
    with pytest.raises(ValueError, match="code_paths"):
        B.MetaflowBatchBackend(**common, code_paths=str(tmp_path / "nope"))


@needs_hook
def test_harbor_telemetry_setting_is_forwarded(tmp_path, monkeypatch):
    monkeypatch.setenv("HARBOR_TELEMETRY", "0")
    backend = B.MetaflowBatchBackend(n_concurrent=1, store=str(tmp_path), local=True)
    assert backend.settings.env == {"HARBOR_TELEMETRY": "0"}


@needs_hook
def test_job_user_and_code_options_reach_the_launcher(tmp_path, monkeypatch):
    code = tmp_path / "mycode"
    code.mkdir()
    monkeypatch.setenv("HARBOR_METAFLOW_JOB_USER", "root")
    backend = B.MetaflowBatchBackend(
        n_concurrent=1,
        store="s3://example-bucket/runs",
        image="example/harbor:1",
        code_paths=str(code),
        package_suffixes=".py,.tmpl",
    )
    assert backend.settings.job_user == "root"
    assert backend.launcher.code_paths == [str(code)]
    cmd = backend.launcher.command(Path("/tmp/f/flow.py"), "s3://b/p", 1)
    assert cmd[2:5] == ["--no-pylint", "--package-suffixes", ".py,.tmpl"]
    assert cmd.index("--package-suffixes") < cmd.index("run")


# -- flow dir --------------------------------------------------------------------


def test_flow_dir_ships_this_package_and_code_paths(tmp_path):
    pkg = tmp_path / "src" / "mypkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("X = 1\n")
    (pkg / "__pycache__").mkdir()
    (pkg / "__pycache__" / "junk.pyc").write_text("")
    single = tmp_path / "helper.py"
    single.write_text("Y = 2\n")

    flow_dir = tmp_path / "flow"
    flow_file = write_flow_dir(
        flow_dir, FlowSettings(local=True), [str(tmp_path / "src"), str(single)]
    )

    assert flow_file == flow_dir / FLOW_FILE_NAME
    assert (flow_dir / "harbor_metaflow_backend" / "worker.py").is_file()
    assert not (flow_dir / "harbor_metaflow_backend" / "__pycache__").exists()
    assert (flow_dir / "src" / "mypkg" / "__init__.py").is_file()
    assert not (flow_dir / "src" / "mypkg" / "__pycache__").exists()
    assert (flow_dir / "helper.py").is_file()

    with pytest.raises(ValueError, match="already in the flow dir"):
        write_flow_dir(tmp_path / "flow2", FlowSettings(local=True), [str(single)] * 2)


def test_launcher_start_removes_the_flow_dir_when_writing_fails(tmp_path, monkeypatch):
    made = []
    real = __import__("tempfile").mkdtemp

    def mkdtemp(**kw):
        made.append(Path(real(dir=tmp_path, **kw)))
        return str(made[-1])

    monkeypatch.setattr("tempfile.mkdtemp", mkdtemp)
    launcher = MetaflowLauncher(FlowSettings(local=True), code_paths=["/no/such/dir"])
    with pytest.raises(FileNotFoundError):
        launcher.start("s3://b/p", 1, tmp_path / "log")
    assert made and not made[0].exists()


def test_generated_flow_installs_job_user_only_for_batch(tmp_path, monkeypatch):
    import importlib.util

    from metaflow.plugins.aws.batch.batch_client import BatchJob

    monkeypatch.setattr(
        BatchJob, "_register_job_definition", BatchJob._register_job_definition
    )

    def load(settings, name):
        flow = tmp_path / f"{name}.py"
        flow.write_text(render_flow(settings))
        spec = importlib.util.spec_from_file_location(name, flow)
        spec.loader.exec_module(importlib.util.module_from_spec(spec))

    load(FlowSettings(local=True, job_user="root"), "local_flow")
    assert not hasattr(BatchJob._register_job_definition, "_harbor_metaflow_job_user")
    load(FlowSettings(image="example/harbor:1", job_user="root"), "batch_flow")
    assert BatchJob._register_job_definition._harbor_metaflow_job_user == "root"


# -- job_user wrapper ------------------------------------------------------------


class _FakeBatchApi:
    def __init__(self, existing=()):
        self.existing = set(existing)
        self.registered: list[dict] = []
        self.looked_up: list[str] = []

    def describe_job_definitions(self, jobDefinitionName, status):
        self.looked_up.append(jobDefinitionName)
        found = jobDefinitionName in self.existing
        return {"jobDefinitions": [{"jobDefinitionArn": "arn:old"}] if found else []}

    def register_job_definition(self, **definition):
        self.registered.append(definition)
        return {"jobDefinitionArn": "arn:new"}

    def describe_job_queues(self, **kwargs):  # passed through by the proxy
        return {"queues": kwargs}


def _fake_batch_job_cls():
    class FakeBatchJob:
        def __init__(self, client):
            self._client = client

        def _register_job_definition(self, image, host_volumes=None):
            definition = {
                "type": "container",
                "containerProperties": {"image": image},
            }
            name = "metaflow_abc"
            found = self._client.describe_job_definitions(
                jobDefinitionName=name, status="ACTIVE"
            )["jobDefinitions"]
            if found:
                return found[0]["jobDefinitionArn"]
            definition["jobDefinitionName"] = name
            return self._client.register_job_definition(**definition)[
                "jobDefinitionArn"
            ]

    return FakeBatchJob


def test_job_user_registers_definitions_with_the_user():
    cls = _fake_batch_job_cls()
    assert jobdef.install_job_user("root", batch_job_cls=cls)
    api = _FakeBatchApi(existing={"metaflow_abc"})  # registered without a user
    job = cls(api)

    assert job._register_job_definition("img:1") == "arn:new"
    assert api.looked_up == ["metaflow_abc_uroot"]
    assert api.registered[0]["jobDefinitionName"] == "metaflow_abc_uroot"
    assert api.registered[0]["containerProperties"] == {
        "image": "img:1",
        "user": "root",
    }
    assert job._client is api  # restored
    assert job._client.describe_job_queues(x=1) == {"queues": {"x": 1}}


def test_job_user_reuses_its_own_definition_and_is_idempotent():
    cls = _fake_batch_job_cls()
    assert jobdef.install_job_user("root", batch_job_cls=cls)
    assert jobdef.install_job_user("root", batch_job_cls=cls)  # no double wrap
    api = _FakeBatchApi(existing={"metaflow_abc_uroot"})
    assert cls(api)._register_job_definition("img:1") == "arn:old"
    assert api.registered == []
    with pytest.raises(ValueError, match="already set"):
        jobdef.install_job_user("1000", batch_job_cls=cls)
    assert jobdef.install_job_user("", batch_job_cls=cls) is False


def test_job_user_refuses_an_unexpected_metaflow():
    class NoRegister:
        pass

    with pytest.raises(jobdef.JobUserUnsupported):
        jobdef.install_job_user("root", batch_job_cls=NoRegister)


def test_job_user_wraps_the_real_metaflow_signature(monkeypatch):
    """Metaflow's own BatchJob._register_job_definition goes through the proxy."""
    import inspect

    from metaflow.plugins.aws.batch.batch_client import BatchJob

    monkeypatch.setattr(
        BatchJob, "_register_job_definition", BatchJob._register_job_definition
    )
    original = BatchJob._register_job_definition
    params = list(inspect.signature(original).parameters)
    assert params[:2] == ["self", "image"]
    jobdef.install_job_user("root")
    assert BatchJob._register_job_definition.__wrapped__ is original
