"""Register the flow's AWS Batch job definitions with a container ``user``.

Trials start sibling containers through the host's Docker socket, which is usually
owned by ``root:docker``. If the image's ``USER`` is neither root nor in that group,
nothing in the job can use the socket. Metaflow's ``@batch`` has no ``user`` option,
so ``job_user`` adds ``containerProperties.user`` to the job definitions Metaflow
registers for this flow.

Metaflow builds and registers the job definition inside
``BatchJob._register_job_definition``. :func:`install_job_user` wraps that method so
it talks to the Batch API through :class:`_JobUserClient`, which:

- adds ``containerProperties.user`` (and the same ``user`` on every
  ``nodeRangeProperties`` container) when registering;
- appends ``_u<user>`` to the job definition name in both the lookup and the
  registration, so a definition registered without the user is never reused.

This depends on a private Metaflow method; if Metaflow changes it,
:func:`install_job_user` raises instead of silently running as the image's user.
The generated flow calls it at import, in the process that submits the Batch jobs.
"""

from __future__ import annotations

import copy
import re
from typing import Any

#: Marker attribute on the installed wrapper (its value is the user).
_MARKER = "_harbor_metaflow_job_user"


class JobUserUnsupported(RuntimeError):
    """This Metaflow does not register job definitions the way the wrapper expects."""


def suffixed_name(name: str, user: str) -> str:
    return f"{name}_u{re.sub(r'[^A-Za-z0-9_-]', '_', user)}"[:128]


class _JobUserClient:
    """boto3 Batch client proxy that registers job definitions with ``user``."""

    def __init__(self, inner: Any, user: str):
        self._inner = inner
        self._user = user

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def describe_job_definitions(self, **kwargs):
        if kwargs.get("jobDefinitionName"):
            kwargs = {
                **kwargs,
                "jobDefinitionName": suffixed_name(
                    kwargs["jobDefinitionName"], self._user
                ),
            }
        return self._inner.describe_job_definitions(**kwargs)

    def register_job_definition(self, **job_definition):
        job_definition = copy.deepcopy(job_definition)
        job_definition["jobDefinitionName"] = suffixed_name(
            job_definition["jobDefinitionName"], self._user
        )
        if "containerProperties" in job_definition:
            job_definition["containerProperties"]["user"] = self._user
        for node_range in (job_definition.get("nodeProperties") or {}).get(
            "nodeRangeProperties", []
        ):
            if "container" in node_range:
                node_range["container"]["user"] = self._user
        return self._inner.register_job_definition(**job_definition)


def install_job_user(user: str, batch_job_cls: Any = None) -> bool:
    """Make Metaflow register this process's Batch job definitions with ``user``.

    Idempotent for the same user; returns whether the wrapper is installed (``False``
    for an empty user). ``batch_job_cls`` replaces Metaflow's ``BatchJob`` (tests).
    """
    user = (user or "").strip()
    if not user:
        return False
    if batch_job_cls is None:
        try:
            from metaflow.plugins.aws.batch.batch_client import (
                BatchJob as batch_job_cls,
            )
        except ImportError as exc:  # pragma: no cover - depends on Metaflow
            raise JobUserUnsupported(
                f"job_user needs Metaflow's AWS Batch client: {exc}"
            ) from exc
    original = getattr(batch_job_cls, "_register_job_definition", None)
    if original is None:
        raise JobUserUnsupported(
            "job_user: this Metaflow has no BatchJob._register_job_definition; set "
            "the user in the image (USER) instead"
        )
    installed = getattr(original, _MARKER, None)
    if installed is not None:
        if installed != user:
            raise ValueError(f"job_user already set to {installed!r} in this process")
        return True

    def _register_job_definition(self, *args, **kwargs):
        inner = getattr(self, "_client", None)
        if inner is None:
            raise JobUserUnsupported(
                "job_user: Metaflow's BatchJob has no _client; set the user in the "
                "image (USER) instead"
            )
        self._client = _JobUserClient(inner, user)
        try:
            return original(self, *args, **kwargs)
        finally:
            self._client = inner

    setattr(_register_job_definition, _MARKER, user)
    _register_job_definition.__wrapped__ = original
    batch_job_cls._register_job_definition = _register_job_definition
    return True
