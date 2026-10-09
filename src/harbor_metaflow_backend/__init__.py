"""Run Harbor trials on AWS Batch with Metaflow.

A trial backend for ``harbor run --backend metaflow-batch``. See the README.
"""

from harbor_metaflow_backend.backend import (
    HARBOR_CLOSES_BACKENDS,
    HARBOR_HAS_TRIAL_BACKENDS,
    HARBOR_SKIPS_REMOTE_ENV_PREFLIGHT,
    MetaflowBatchBackend,
    RemoteTrialError,
    TrialBackendUnavailable,
    TrialNotReturned,
)
from harbor_metaflow_backend.store import LocalDirStore, S3Store, open_store

__version__ = "0.1.0"

__all__ = [
    "HARBOR_CLOSES_BACKENDS",
    "HARBOR_HAS_TRIAL_BACKENDS",
    "HARBOR_SKIPS_REMOTE_ENV_PREFLIGHT",
    "LocalDirStore",
    "MetaflowBatchBackend",
    "RemoteTrialError",
    "S3Store",
    "TrialBackendUnavailable",
    "TrialNotReturned",
    "__version__",
    "open_store",
]
