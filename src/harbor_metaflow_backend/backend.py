"""``harbor run --backend metaflow-batch``: run a Harbor job's trials on AWS Batch.

Needs a Harbor with the pluggable trial backend hook (``harbor.trial.backend``).
On a Harbor without it this module still imports, but constructing the backend
raises :class:`TrialBackendUnavailable`.

Harbor's ``Job`` plans the trials, reconciles a resumed job directory and hands the
remaining ``TrialConfig`` s to :meth:`MetaflowBatchBackend.submit_batch`. The backend:

1. stages them under ``<store>/<job id>-<token>/`` (layout in ``store.py``): a
   manifest with one shard per ``shard_size`` trials, each distinct task dir once,
   and the agent kwarg files named in ``stage_agent_kwargs``;
2. starts a generated Metaflow flow with one foreach task (one Batch job) per
   shard; each runs the shard with Harbor's own ``TrialQueue`` (``worker.py``);
3. polls the run root: a worker's START event is replayed to the job's hooks, and
   each finished trial dir is copied to ``config.trials_dir / trial_name`` with the
   submitted ``TrialConfig`` restored (so resume matches it); END is then emitted
   with Harbor's ``load_trial_hook_event`` and the trial's coroutine returns;
4. on cancellation (Ctrl-C, or the job failing) stops the flow and terminates
   every Batch job the workers recorded;
5. in ``aclose`` (on a Harbor that calls it) stops any run still going and deletes
   the run roots whose trials all came back.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import uuid
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

from harbor_metaflow_backend import worker as W
from harbor_metaflow_backend.launcher import (
    FlowSettings,
    MetaflowLauncher,
    terminate_batch_jobs,
)
from harbor_metaflow_backend.store import RunStore, open_store

logger = logging.getLogger(__name__)

try:  # Harbor with the trial backend hook
    from harbor.trial.backend import BaseTrialBackend, load_trial_hook_event

    HARBOR_HAS_TRIAL_BACKENDS = True
except ImportError:  # stock Harbor, or no Harbor at all
    HARBOR_HAS_TRIAL_BACKENDS = False

    class BaseTrialBackend:  # type: ignore[no-redef]
        """Placeholder so this module imports on a Harbor without the hook."""

        def __init__(self, *args, **kwargs):
            raise TrialBackendUnavailable()

    load_trial_hook_event = None  # type: ignore[assignment]

#: Optional parts of the hook, detected by their presence on BaseTrialBackend.
#: Without the first, ``harbor run`` runs the environment preflight (e.g. the Docker
#: daemon check) on the submitting host too. Without the second, nothing calls
#: ``aclose`` and run roots are left in the store.
HARBOR_SKIPS_REMOTE_ENV_PREFLIGHT = hasattr(
    BaseTrialBackend, "runs_environment_remotely"
)
HARBOR_CLOSES_BACKENDS = hasattr(BaseTrialBackend, "aclose")

ENV_PREFIX = "HARBOR_METAFLOW_"
HOOK_BRANCH = "git+https://github.com/AutodeskAILab/harbor@feat/backend-plugins"
DEFAULT_SHARD_SIZE = 8
DEFAULT_POLL_INTERVAL_SEC = 30.0


class TrialBackendUnavailable(RuntimeError):
    def __init__(self):
        super().__init__(
            "harbor-metaflow-backend needs a Harbor with pluggable trial backends "
            "(harbor.trial.backend, `harbor run --backend`), which this Harbor does "
            "not have. The hook is proposed upstream; until it is released, install "
            f"Harbor from the hook branch: pip install 'harbor @ {HOOK_BRANCH}'"
        )


class RemoteTrialError(RuntimeError):
    """The trial raised on the worker (its traceback is the message)."""


class TrialNotReturned(RuntimeError):
    """The flow exited without a result for this trial."""


def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)[:80] or "x"


def _digest(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:10]


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return [str(v).strip() for v in value if str(v).strip()]


def _opt(name: str, value: Any, default: Any = None) -> Any:
    """``value`` if given, else ``$HARBOR_METAFLOW_<NAME>``, else ``default``."""
    if value is not None and value != "":
        return value
    env = os.environ.get(ENV_PREFIX + name.upper(), "").strip()
    return env if env else default


class MetaflowBatchBackend(BaseTrialBackend):
    """Harbor trial backend: shards of ``shard_size`` trials, one Batch job each.

    ``n_concurrent`` (``harbor run -n``) is the number of concurrent trials per Batch
    job; ``retry_config`` is applied on the Batch host by Harbor's ``TrialQueue``.
    Every option can be given with ``--backend-kwarg key=value`` or the environment
    variable ``HARBOR_METAFLOW_<KEY>``; see the README for the full table.

    ``launcher``, ``store_factory`` and ``terminate`` replace Metaflow, the store and
    AWS Batch (tests).
    """

    #: Trial environments start on the Batch hosts; the worker runs their preflight.
    runs_environment_remotely = True

    def __init__(
        self,
        *,
        n_concurrent: int,
        retry_config=None,
        store: str | None = None,
        queue: str | None = None,
        image: str | None = None,
        iam_role: str | None = None,
        cpu: int | str | None = None,
        memory: int | str | None = None,
        timeout_sec: int | str | None = None,
        shard_size: int | str | None = None,
        poll_interval_sec: float | str | None = None,
        max_workers: int | str | None = None,
        work_dir: str | None = None,
        docker_socket: str | None = None,
        privileged: bool | str | None = None,
        local: bool | str | None = None,
        env_vars: str | list[str] | None = None,
        secrets: str | list[str] | None = None,
        stage_agent_kwargs: str | list[str] | None = None,
        keep_run_root: bool | str | None = None,
        python: str | None = None,
        metaflow_args: str | list[str] | None = None,
        launcher: Any = None,
        store_factory: Callable[[str], RunStore] | None = None,
        terminate: Callable[[list[str], str], None] | None = None,
    ):
        super().__init__(n_concurrent=n_concurrent, retry_config=retry_config)

        store_uri = _opt("store", store)
        if not store_uri:
            raise ValueError(
                "metaflow-batch needs a store for staged inputs and results: "
                "--backend-kwarg store=s3://<bucket>/<prefix> "
                f"(or {ENV_PREFIX}STORE)"
            )
        self.store_uri = str(store_uri).rstrip("/")
        self.local = _as_bool(_opt("local", local, False))
        self.shard_size = int(_opt("shard_size", shard_size, DEFAULT_SHARD_SIZE))
        if self.shard_size < 1:
            raise ValueError(f"shard_size must be >= 1, got {self.shard_size!r}")
        self.poll_interval_sec = float(
            _opt("poll_interval_sec", poll_interval_sec, DEFAULT_POLL_INTERVAL_SEC)
        )
        self.stage_agent_kwargs = _as_list(
            _opt("stage_agent_kwargs", stage_agent_kwargs)
        )
        self.keep_run_root = _as_bool(_opt("keep_run_root", keep_run_root, False))
        # In local mode trials run on this host, so Harbor's local preflight applies.
        self.runs_environment_remotely = not self.local

        env_names = _as_list(_opt("env_vars", env_vars))
        missing = [n for n in env_names if n not in os.environ]
        if missing:
            raise ValueError(f"env_vars names unset variables: {', '.join(missing)}")
        default_work_dir = (
            str(Path(tempfile.gettempdir()) / "harbor-metaflow-work")
            if self.local
            else FlowSettings.work_dir
        )
        self.settings = FlowSettings(
            local=self.local,
            image=_opt("image", image),
            queue=_opt("queue", queue),
            iam_role=_opt("iam_role", iam_role),
            cpu=int(_opt("cpu", cpu, FlowSettings.cpu)),
            memory=int(_opt("memory", memory, FlowSettings.memory)),
            timeout_sec=int(_opt("timeout_sec", timeout_sec, FlowSettings.timeout_sec)),
            work_dir=str(_opt("work_dir", work_dir, default_work_dir)),
            docker_socket=str(
                _opt("docker_socket", docker_socket, FlowSettings.docker_socket)
            ),
            privileged=_as_bool(_opt("privileged", privileged, False)),
            env={name: os.environ[name] for name in env_names},
            secrets=_as_list(_opt("secrets", secrets)),
        )
        if launcher is None:
            if not self.local and not self.settings.image:
                raise ValueError(
                    "metaflow-batch needs a container image with Python 3.12, Harbor "
                    "and this package: --backend-kwarg image=<image> "
                    f"(or {ENV_PREFIX}IMAGE), or local=true to run on this host"
                )
            extra = _opt("metaflow_args", metaflow_args)
            launcher = MetaflowLauncher(
                self.settings,
                python=_opt("python", python),
                max_workers=int(_opt("max_workers", max_workers, 16)),
                extra_args=extra.split() if isinstance(extra, str) else extra,
            )
        self.launcher = launcher
        self.store_factory = store_factory or open_store
        self.terminate = terminate or terminate_batch_jobs
        self.runs: list[_BatchRun] = []
        if not HARBOR_SKIPS_REMOTE_ENV_PREFLIGHT and not self.local:
            logger.warning(
                "This Harbor does not support runs_environment_remotely: "
                "`harbor run` checks the trial environment (e.g. a Docker daemon) "
                "on this host as well as on the Batch hosts."
            )

    def submit_batch(self, configs) -> list[Coroutine[Any, Any, Any]]:
        if not configs:
            return []
        run = _BatchRun(self, list(configs))
        self.runs.append(run)
        return [run.result(config.trial_name) for config in configs]

    async def aclose(self) -> None:
        """Stop any run still going, then delete the run roots whose trials all
        came back (unless ``keep_run_root``). Run roots with a failed or missing
        trial are kept for debugging."""
        for run in self.runs:
            await run.close(delete=not self.keep_run_root)

    # -- staging (blocking; runs in a thread) --------------------------------

    def stage(self, configs, store: RunStore) -> dict:
        tasks: dict[str, str] = {}
        files: dict[str, str] = {}
        trials: dict[str, dict] = {}
        for config in configs:
            entry: dict[str, Any] = {"config": config.model_dump_json()}
            if config.task.path is not None:
                local = Path(config.task.path).expanduser().resolve()
                if str(local) not in tasks:
                    key = W.task_key(f"{_safe(local.name)}-{_digest(str(local))}")
                    store.upload_dir(local, key)
                    tasks[str(local)] = key
                entry["task"] = tasks[str(local)]
            staged = {}
            for kwarg in self.stage_agent_kwargs:
                value = config.agent.kwargs.get(kwarg)
                if not isinstance(value, (str, Path)) or not Path(value).is_file():
                    continue
                local = Path(value).expanduser().resolve()
                if str(local) not in files:
                    key = f"files/{_digest(str(local))}-{_safe(local.name)}"
                    store.upload_file(local, key)
                    files[str(local)] = key
                staged[kwarg] = files[str(local)]
            entry["files"] = staged
            trials[config.trial_name] = entry
        names = [c.trial_name for c in configs]
        manifest = {
            "version": W.MANIFEST_VERSION,
            "n_concurrent": self._n_concurrent,
            "retry": self._retry_config.model_dump(mode="json"),
            "shards": [
                names[i : i + self.shard_size]
                for i in range(0, len(names), self.shard_size)
            ],
            "trials": trials,
        }
        store.put_text(W.MANIFEST_KEY, json.dumps(manifest))
        return manifest

    # -- copy-back (blocking; runs in a thread) ------------------------------

    def copy_back(self, config, store: RunStore) -> Path:
        """Put the worker's trial dir at ``config.trials_dir / trial_name``.

        ``config.json``, ``result.json`` and ``lock.json`` get the submitted config
        back (task path, trials dir, agent kwargs) so a resumed ``Job`` matches the
        trial, and ``trial_uri`` points at the local copy.
        """
        from harbor.models.job.lock import TrialLock
        from harbor.models.trial.paths import TrialPaths
        from harbor.models.trial.result import TrialResult

        dest = Path(config.trials_dir) / config.trial_name
        incoming = Path(tempfile.mkdtemp(prefix=f"{_safe(config.trial_name)}-"))
        try:
            store.download_dir(W.result_prefix(config.trial_name), incoming / "t")
            paths = TrialPaths(incoming / "t")
            result = TrialResult.model_validate_json(paths.result_path.read_text())
            result.config = config
            result.trial_uri = dest.resolve().as_uri()
            paths.result_path.write_text(result.model_dump_json(indent=4))
            paths.config_path.write_text(config.model_dump_json(indent=4))
            if paths.lock_path.exists():
                lock = TrialLock.model_validate_json(paths.lock_path.read_text())
                lock.agent = config.agent
                if lock.task.path is not None:
                    lock.task.path = config.task.path
                paths.lock_path.write_text(
                    lock.model_dump_json(indent=4, exclude_none=True)
                )
            if dest.exists():
                shutil.rmtree(dest)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(incoming / "t"), dest)
        finally:
            shutil.rmtree(incoming, ignore_errors=True)
        return dest


class _BatchRun:
    """One ``submit_batch`` call: one staged run root and one flow run."""

    def __init__(self, backend: MetaflowBatchBackend, configs):
        self.backend = backend
        self.configs = {c.trial_name: c for c in configs}
        self.token = f"{_safe(str(configs[0].job_id))[:36]}-{uuid.uuid4().hex[:8]}"
        self.run_uri = f"{backend.store_uri}/{self.token}"
        self.store = backend.store_factory(self.run_uri)
        self.futures: dict[str, asyncio.Future] = {}
        self.driver: asyncio.Task | None = None
        self.flow: Any = None
        self.started: set[str] = set()
        self.finished: set[str] = set()
        self.failed: set[str] = set()

    def _ensure_driver(self) -> None:
        if self.driver is None:
            loop = asyncio.get_running_loop()
            self.futures = {name: loop.create_future() for name in self.configs}
            self.driver = loop.create_task(self._drive())

    async def result(self, trial_name: str):
        self._ensure_driver()
        try:
            return await asyncio.shield(self.futures[trial_name])
        except asyncio.CancelledError:
            # Job cancels every trial coroutine together (interrupt or failure).
            await self._cancel_driver()
            raise

    async def _cancel_driver(self) -> None:
        if self.driver is not None and not self.driver.done():
            self.driver.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.driver

    async def _drive(self) -> None:
        backend = self.backend
        try:
            manifest = await asyncio.to_thread(
                backend.stage, list(self.configs.values()), self.store
            )
            log_dir = Path(next(iter(self.configs.values())).trials_dir)
            self.flow = backend.launcher.start(
                self.run_uri,
                len(manifest["shards"]),
                log_dir / f"metaflow-{self.token}.log",
            )
            while True:
                exit_code = self.flow.poll()
                await self._collect()
                if len(self.finished) == len(self.configs):
                    break
                if exit_code is not None:
                    await self._collect()  # a last look after the flow exited
                    for name in set(self.configs) - self.finished:
                        self._fail(
                            name,
                            TrialNotReturned(
                                f"trial {name} did not come back from the Metaflow "
                                f"run (exit code {exit_code}, run root "
                                f"{self.run_uri}, log {self.flow.log_path})"
                            ),
                        )
                    break
                await asyncio.sleep(backend.poll_interval_sec)
        except asyncio.CancelledError:
            await asyncio.shield(asyncio.to_thread(self._stop, "cancelled by harbor"))
            raise
        except Exception as exc:  # surfaced on every pending trial
            await asyncio.to_thread(self._stop, f"backend error: {exc}")
            for name in set(self.configs) - self.finished:
                self._fail(name, exc)
        finally:
            self._cleanup_flow()

    async def _collect(self) -> None:
        from harbor.trial.hooks import TrialEvent

        backend = self.backend
        keys = set(await asyncio.to_thread(self.store.list, "results"))
        for key in await asyncio.to_thread(self.store.list, "events"):
            name = key.split("/")[1]
            if name in self.configs and name not in self.started | self.finished:
                text = await asyncio.to_thread(self.store.get_text, key)
                if text:
                    self.started.add(name)
                    await backend.emit(self._replayed_start(name, text))
        for name, config in self.configs.items():
            if name in self.finished:
                continue
            if W.error_key(name) in keys:
                text = await asyncio.to_thread(self.store.get_text, W.error_key(name))
                self._fail(name, RemoteTrialError(text or f"trial {name} failed"))
            elif W.done_key(name) in keys:
                trial_dir = await asyncio.to_thread(
                    backend.copy_back, config, self.store
                )
                event = load_trial_hook_event(TrialEvent.END, trial_dir)
                await backend.emit(event)
                self.finished.add(name)
                if not self.futures[name].done():
                    self.futures[name].set_result(event.result)

    def _replayed_start(self, name: str, text: str):
        from harbor.trial.hooks import TrialHookEvent

        event = TrialHookEvent.model_validate_json(text)
        config = self.configs[name]
        event.config = config
        if event.result is not None:
            event.result.config = config
        if event.lock is not None:
            event.lock.agent = config.agent
        return event

    async def close(self, delete: bool) -> None:
        await self._cancel_driver()
        complete = (
            self.driver is not None
            and not self.failed
            and self.finished == set(self.configs)
        )
        if delete and complete:
            await asyncio.to_thread(self.store.delete_all)

    def _fail(self, name: str, exc: BaseException) -> None:
        self.finished.add(name)
        self.failed.add(name)
        future = self.futures.get(name)
        if future is not None and not future.done():
            future.set_exception(exc)

    def _stop(self, reason: str) -> None:
        if self.flow is not None:
            self.flow.stop()
        job_ids = []
        for key in self.store.list("batch"):
            text = self.store.get_text(key)
            if text:
                job_ids.append(json.loads(text)["job_id"])
        if job_ids:
            self.backend.terminate(job_ids, reason)

    def _cleanup_flow(self) -> None:
        cleanup = getattr(self.flow, "cleanup", None)
        if cleanup is not None:
            cleanup()
