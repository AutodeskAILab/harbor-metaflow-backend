"""Where a run is staged: an ``s3://`` prefix, or a directory both sides can see.

Layout under a run root (``<store>/<job id>-<token>/``)::

    manifest.json                 trial configs, shards, n_concurrent, retry config
    tasks/<task key>/...          task dirs, one copy per distinct local task path
    files/<file key>              staged agent kwarg files (``stage_agent_kwargs``)
    batch/<shard>.json            {"job_id": AWS_BATCH_JOB_ID}, written by each worker
    events/<trial>/start.json     the worker's START hook event, replayed locally
    results/<trial>/...           the finished trial dir, uploaded by the worker
    results/<trial>.done          written after the trial dir upload completes
    results/<trial>.error         the trial raised on the worker (traceback)

Keys are always ``/``-separated and relative to the run root.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol


class RunStore(Protocol):
    uri: str

    def put_text(self, key: str, text: str) -> None: ...
    def get_text(self, key: str) -> str | None: ...
    def list(self, prefix: str) -> list[str]: ...
    def upload_dir(self, local_dir: Path, prefix: str) -> None: ...
    def upload_file(self, local_path: Path, key: str) -> None: ...
    def download_dir(self, prefix: str, dest: Path) -> None: ...
    def download_file(self, key: str, dest: Path) -> None: ...
    def delete_all(self) -> None: ...


def _files(local_dir: Path) -> Iterator[tuple[Path, str]]:
    for path in sorted(Path(local_dir).rglob("*")):
        if path.is_file() and not path.is_symlink():
            yield path, path.relative_to(local_dir).as_posix()


class LocalDirStore:
    """A run root on a filesystem both sides can see (tests, a shared mount)."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.uri = str(self.root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_text(self, key: str, text: str) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(text)
        tmp.replace(path)

    def get_text(self, key: str) -> str | None:
        path = self._path(key)
        return path.read_text() if path.is_file() else None

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.is_dir():
            return []
        return [
            f"{prefix.rstrip('/')}/{rel}"
            for _, rel in _files(base)
            if not rel.endswith(".tmp")
        ]

    def upload_dir(self, local_dir: Path, prefix: str) -> None:
        for path, rel in _files(Path(local_dir)):
            self.upload_file(path, f"{prefix.rstrip('/')}/{rel}")

    def upload_file(self, local_path: Path, key: str) -> None:
        dest = self._path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)

    def download_dir(self, prefix: str, dest: Path) -> None:
        for _, rel in _files(self._path(prefix)):
            self.download_file(f"{prefix.rstrip('/')}/{rel}", Path(dest) / rel)

    def download_file(self, key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self._path(key), dest)

    def delete_all(self) -> None:
        """Remove the whole run root."""
        shutil.rmtree(self.root, ignore_errors=True)


class S3Store:
    """A run root at ``s3://bucket/prefix``. boto3 is imported on first use."""

    def __init__(self, uri: str, client=None):
        if not uri.startswith("s3://"):
            raise ValueError(f"not an s3 uri: {uri!r}")
        bucket, _, prefix = uri[len("s3://") :].partition("/")
        if not bucket:
            raise ValueError(f"no bucket in {uri!r}")
        self.uri = uri.rstrip("/")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = client

    @property
    def client(self):
        if self._client is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - depends on extras
                raise ImportError(
                    "An s3:// store needs boto3: "
                    "pip install 'harbor-metaflow-backend[s3]'"
                ) from exc
            self._client = boto3.client("s3")
        return self._client

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put_text(self, key: str, text: str) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=self._key(key), Body=text.encode()
        )

    def get_text(self, key: str) -> str | None:
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        except self.client.exceptions.NoSuchKey:
            return None
        return body["Body"].read().decode()

    def list(self, prefix: str) -> list[str]:
        full = self._key(prefix.rstrip("/") + "/")
        strip = len(self._key(""))
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full):
            keys.extend(obj["Key"][strip:] for obj in page.get("Contents", []))
        return keys

    def upload_dir(self, local_dir: Path, prefix: str) -> None:
        for path, rel in _files(Path(local_dir)):
            self.upload_file(path, f"{prefix.rstrip('/')}/{rel}")

    def upload_file(self, local_path: Path, key: str) -> None:
        self.client.upload_file(str(local_path), self.bucket, self._key(key))

    def download_dir(self, prefix: str, dest: Path) -> None:
        base = prefix.rstrip("/") + "/"
        for key in self.list(prefix):
            self.download_file(key, Path(dest) / key[len(base) :])

    def download_file(self, key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, self._key(key), str(dest))

    def delete_all(self) -> None:
        """Remove every object under the run root (never a whole bucket)."""
        if not self.prefix:
            raise ValueError(f"refusing to delete a whole bucket: {self.uri!r}")
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix + "/"):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        for i in range(0, len(keys), 1000):
            self.client.delete_objects(
                Bucket=self.bucket,
                Delete={
                    "Objects": [{"Key": k} for k in keys[i : i + 1000]],
                    "Quiet": True,
                },
            )


def open_store(uri: str) -> RunStore:
    """An :class:`S3Store` for ``s3://`` URIs, else a :class:`LocalDirStore`."""
    return S3Store(uri) if uri.startswith("s3://") else LocalDirStore(uri)
