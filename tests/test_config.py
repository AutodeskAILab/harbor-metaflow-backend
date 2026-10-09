"""Backend options, the generated flow, and the stores."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

from harbor_metaflow_backend import worker as W
from harbor_metaflow_backend.backend import MetaflowBatchBackend
from harbor_metaflow_backend.launcher import (
    FlowSettings,
    MetaflowLauncher,
    render_flow,
)
from harbor_metaflow_backend.store import LocalDirStore, S3Store, open_store

from conftest import needs_hook


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in list(os.environ):
        if name.startswith("HARBOR_METAFLOW_"):
            monkeypatch.delenv(name)


@needs_hook
def test_options_from_kwargs():
    backend = MetaflowBatchBackend(
        n_concurrent=4,
        store="s3://example-bucket/harbor/runs/",
        queue="my-queue",
        image="example/harbor:1",
        iam_role="arn:aws:iam::123456789012:role/example",
        cpu=16,
        memory="65536",
        timeout_sec=600,
        shard_size=3,
        privileged="true",
    )
    assert backend.store_uri == "s3://example-bucket/harbor/runs"
    assert backend.shard_size == 3
    assert backend.runs_environment_remotely is True
    kwargs = backend.settings.batch_kwargs()
    assert kwargs == {
        "image": "example/harbor:1",
        "queue": "my-queue",
        "iam_role": "arn:aws:iam::123456789012:role/example",
        "cpu": 16,
        "memory": 65536,
        "host_volumes": ["/var/run/docker.sock", "/var/lib/harbor-work"],
        "privileged": True,
    }


@needs_hook
def test_options_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("HARBOR_METAFLOW_STORE", str(tmp_path))
    monkeypatch.setenv("HARBOR_METAFLOW_IMAGE", "example/harbor:2")
    monkeypatch.setenv("HARBOR_METAFLOW_SHARD_SIZE", "5")
    monkeypatch.setenv("HARBOR_METAFLOW_WORK_DIR", "/scratch/harbor")
    backend = MetaflowBatchBackend(n_concurrent=1, shard_size=None)
    assert backend.store_uri == str(tmp_path)
    assert backend.shard_size == 5
    assert backend.settings.image == "example/harbor:2"
    assert backend.settings.batch_kwargs()["host_volumes"][1] == "/scratch/harbor"
    # Queue and IAM role fall back to the Metaflow configuration.
    assert "queue" not in backend.settings.batch_kwargs()
    # A kwarg beats the environment.
    assert MetaflowBatchBackend(n_concurrent=1, shard_size=2).shard_size == 2


@needs_hook
def test_missing_store_or_image_is_refused(tmp_path):
    with pytest.raises(ValueError, match="store=s3://"):
        MetaflowBatchBackend(n_concurrent=1, image="example/harbor:1")
    with pytest.raises(ValueError, match="image="):
        MetaflowBatchBackend(n_concurrent=1, store=str(tmp_path))
    with pytest.raises(ValueError, match="shard_size"):
        MetaflowBatchBackend(
            n_concurrent=1, store=str(tmp_path), local=True, shard_size=0
        )


@needs_hook
def test_local_mode_runs_steps_on_this_host(tmp_path):
    backend = MetaflowBatchBackend(n_concurrent=1, store=str(tmp_path), local="true")
    assert backend.settings.batch_kwargs() is None
    assert backend.runs_environment_remotely is False
    assert "harbor-metaflow-work" in backend.settings.work_dir


@needs_hook
def test_env_vars_must_be_set(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="UNSET_FOR_TEST"):
        MetaflowBatchBackend(
            n_concurrent=1, store=str(tmp_path), local=True, env_vars="UNSET_FOR_TEST"
        )
    monkeypatch.setenv("SOME_SETTING", "x")
    backend = MetaflowBatchBackend(
        n_concurrent=1, store=str(tmp_path), local=True, env_vars="SOME_SETTING"
    )
    assert backend.settings.env == {"SOME_SETTING": "x"}


def test_launcher_command():
    launcher = MetaflowLauncher(FlowSettings(local=True), python="/usr/bin/python3")
    cmd = launcher.command(Path("/tmp/f/flow.py"), "s3://bucket/prefix/run-1", 250)
    assert cmd[:4] == ["/usr/bin/python3", "/tmp/f/flow.py", "--no-pylint", "run"]
    assert cmd[cmd.index("--run_uri") + 1] == "s3://bucket/prefix/run-1"
    assert cmd[cmd.index("--max-workers") + 1] == "16"
    assert cmd[cmd.index("--max-num-splits") + 1] == "250"


def _graph(flow_text: str, tmp_path: Path) -> str:
    flow = tmp_path / "harbor_trials_flow.py"
    flow.write_text(flow_text)
    env = {**os.environ, "METAFLOW_HOME": str(tmp_path / "mf-home")}
    out = subprocess.run(
        [sys.executable, str(flow), "--no-pylint", "show"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stderr
    return out.stdout + out.stderr


def test_generated_flow_applies_batch_and_friends(tmp_path):
    settings = FlowSettings(
        image="example/harbor:1",
        queue="my-queue",
        env={"SOME_SETTING": "x"},
        timeout_sec=60,
    )
    flow = tmp_path / "harbor_trials_flow.py"
    flow.write_text(render_flow(settings))
    spec = importlib.util.spec_from_file_location("generated_flow", flow)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    decos = {d.name: d for d in module.HarborTrialsFlow.run_shard.decorators}
    assert {"batch", "timeout", "environment"} <= set(decos)
    batch = decos["batch"].attributes
    assert batch["image"] == "example/harbor:1"
    assert batch["queue"] == "my-queue"
    assert batch["host_volumes"] == ["/var/run/docker.sock", "/var/lib/harbor-work"]
    assert decos["environment"].attributes["vars"] == {"SOME_SETTING": "x"}
    assert not module.HarborTrialsFlow.start.decorators


def test_generated_local_flow_is_a_valid_metaflow_flow(tmp_path):
    text = render_flow(FlowSettings(local=True))
    assert '"batch": null' in text
    graph = _graph(text, tmp_path)
    for step in ("start", "run_shard", "join", "end"):
        assert step in graph


def test_worker_preflight_runs_each_distinct_environment_once(monkeypatch):
    from harbor.models.trial.config import EnvironmentConfig

    calls = []
    monkeypatch.setattr(
        "harbor.environments.factory.EnvironmentFactory.run_preflight",
        lambda **kw: calls.append(kw),
    )

    class Config:
        def __init__(self, env):
            self.environment = env

    docker = EnvironmentConfig(type="docker")
    custom = EnvironmentConfig(import_path="pkg.mod:Env")
    W.environment_preflight([Config(docker), Config(docker), Config(custom)])

    assert calls == [
        {"type": docker.type, "import_path": None},
        {"type": None, "import_path": "pkg.mod:Env"},
    ]


def test_local_store_round_trip(tmp_path):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "a.txt").write_text("a")
    (src / "sub" / "b.txt").write_text("b")
    store = open_store(str(tmp_path / "root"))
    assert isinstance(store, LocalDirStore)
    store.upload_dir(src, "tasks/t1")
    store.put_text("manifest.json", "{}")
    assert sorted(store.list("tasks")) == ["tasks/t1/a.txt", "tasks/t1/sub/b.txt"]
    assert store.get_text("missing") is None
    store.download_dir("tasks/t1", tmp_path / "out")
    assert (tmp_path / "out" / "sub" / "b.txt").read_text() == "b"
    store.delete_all()
    assert not (tmp_path / "root").exists()


def test_s3_delete_all_stays_under_the_run_root():
    class FakeS3:
        def __init__(self, keys):
            self.keys, self.deleted, self.prefixes = keys, [], []

        def get_paginator(self, _name):
            fake = self

            class Paginator:
                def paginate(self, Bucket, Prefix):
                    fake.prefixes.append(Prefix)
                    contents = [{"Key": k} for k in fake.keys if k.startswith(Prefix)]
                    return [{"Contents": contents}]

            return Paginator()

        def delete_objects(self, Bucket, Delete):
            self.deleted.append([o["Key"] for o in Delete["Objects"]])

    keys = [f"runs/r1/results/t{i}" for i in range(1500)] + ["runs/r10/manifest.json"]
    client = FakeS3(keys)
    S3Store("s3://bucket/runs/r1", client=client).delete_all()
    assert client.prefixes == ["runs/r1/"]
    assert [len(batch) for batch in client.deleted] == [1000, 500]
    assert "runs/r10/manifest.json" not in sum(client.deleted, [])

    with pytest.raises(ValueError, match="whole bucket"):
        S3Store("s3://bucket", client=FakeS3([])).delete_all()
