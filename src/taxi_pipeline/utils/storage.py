"""Accès minimal au stockage objet, hors Spark (fichiers bruts, marqueurs, rapports).

Deux implémentations derrière la même interface : S3/MinIO (URI `s3a://bucket/...`)
et système de fichiers local (chemin simple), pour les tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from taxi_pipeline.config import S3Config


class ObjectStore(Protocol):
    def exists(self, uri: str) -> bool: ...

    def read_bytes(self, uri: str) -> bytes: ...

    def write_bytes(self, uri: str, data: bytes) -> None: ...

    def delete_prefix(self, uri: str) -> None: ...


class S3Store:
    def __init__(self, s3: S3Config):
        import boto3

        self._client = boto3.client(
            "s3",
            endpoint_url=s3.endpoint,
            aws_access_key_id=s3.access_key,
            aws_secret_access_key=s3.secret_key,
        )

    @staticmethod
    def _split(uri: str) -> tuple[str, str]:
        parsed = urlparse(uri)
        return parsed.netloc, parsed.path.lstrip("/")

    def exists(self, uri: str) -> bool:
        from botocore.exceptions import ClientError

        bucket, key = self._split(uri)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = self._split(uri)
        return self._client.get_object(Bucket=bucket, Key=key)["Body"].read()

    def write_bytes(self, uri: str, data: bytes) -> None:
        bucket, key = self._split(uri)
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def delete_prefix(self, uri: str) -> None:
        bucket, prefix = self._split(uri)
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                self._client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


class LocalStore:
    """Datalake sur disque local : `DATALAKE_ROOT` est alors un simple chemin."""

    @staticmethod
    def _path(uri: str) -> Path:
        return Path(uri)

    def exists(self, uri: str) -> bool:
        return self._path(uri).exists()

    def read_bytes(self, uri: str) -> bytes:
        return self._path(uri).read_bytes()

    def write_bytes(self, uri: str, data: bytes) -> None:
        path = self._path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def delete_prefix(self, uri: str) -> None:
        path = self._path(uri)
        if path.is_dir():
            for child in sorted(path.rglob("*"), reverse=True):
                child.unlink() if child.is_file() else child.rmdir()
            path.rmdir()


def get_store(datalake_root: str, s3: S3Config) -> ObjectStore:
    return S3Store(s3) if datalake_root.startswith("s3a://") else LocalStore()
