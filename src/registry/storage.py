from pathlib import Path
from typing import Optional

import yaml

from src.shared.db_credentials import DBCredentials


DEFAULT_REGISTRY_BUCKET = "ifx-registry"


def load_minio_credentials(path: Path) -> DBCredentials:
    with path.open("r", encoding="utf-8") as handle:
        return DBCredentials.from_yaml(yaml.safe_load(handle))


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


class MinioStorage:
    def __init__(
        self,
        credentials: DBCredentials,
        bucket: Optional[str] = DEFAULT_REGISTRY_BUCKET,
        use_internal_url: bool = False,
    ):
        self.credentials = credentials
        self._bucket = bucket or DEFAULT_REGISTRY_BUCKET or credentials.schema
        self.use_internal_url = use_internal_url
        if not self._bucket:
            raise ValueError("MinIO credentials must include schema as bucket name, or bucket must be provided")

    @property
    def bucket(self) -> str:
        return self._bucket

    def client(self):
        import boto3
        from botocore.client import Config

        endpoint = self.credentials.internal_url if self.use_internal_url else self.credentials.url
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=self.credentials.user,
            aws_secret_access_key=self.credentials.password,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            verify=False,
        )

    def ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        client = self.client()
        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchBucket", "403"):
                client.create_bucket(Bucket=self.bucket)
            else:
                raise

    def upload_file(self, local_path: Path, key: str, content_type: Optional[str] = None) -> str:
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        self.ensure_bucket()
        self.client().upload_file(str(local_path), self.bucket, key, ExtraArgs=extra_args or None)
        return s3_uri(self.bucket, key)

    def list_keys(self, prefix: str = "") -> list[str]:
        client = self.client()
        paginator = client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def read_text(self, key: str) -> str:
        response = self.client().get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read().decode("utf-8")
