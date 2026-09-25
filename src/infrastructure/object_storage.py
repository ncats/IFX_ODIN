"""Generic object storage for ODIN graph, workbook, and curation assets."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import yaml

from src.shared.db_credentials import DBCredentials


DEFAULT_OBJECT_STORAGE_BUCKET = "ifx-registry"


@dataclass
class AwsAssumeRoleCredentials:
    access_key_id: str
    secret_access_key: str
    role_arn: str
    bucket: str
    region: Optional[str] = None
    session_name: str = "ifx-odin"
    external_id: Optional[str] = None
    endpoint_url: Optional[str] = None

    @staticmethod
    def from_yaml(yaml_dict: dict) -> "AwsAssumeRoleCredentials":
        return AwsAssumeRoleCredentials(
            access_key_id=yaml_dict["access_key_id"],
            secret_access_key=yaml_dict["secret_access_key"],
            role_arn=yaml_dict["role_arn"],
            bucket=yaml_dict["bucket"],
            region=yaml_dict.get("region"),
            session_name=yaml_dict.get("session_name", "ifx-odin"),
            external_id=yaml_dict.get("external_id"),
            endpoint_url=yaml_dict.get("endpoint_url"),
        )


ObjectStorageCredentials = Union[DBCredentials, AwsAssumeRoleCredentials]


def load_object_storage_credentials(path: Path) -> ObjectStorageCredentials:
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if "role_arn" in config or config.get("type") == "aws_assume_role":
        return AwsAssumeRoleCredentials.from_yaml(config)
    return DBCredentials.from_yaml(config)


def object_storage_from_credentials(
    credentials: ObjectStorageCredentials,
    *,
    use_internal_url: bool = False,
):
    if isinstance(credentials, AwsAssumeRoleCredentials):
        return AwsAssumeRoleStorage(credentials)
    return S3CompatibleStorage(credentials, use_internal_url=use_internal_url)


def s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


class S3CompatibleStorage:
    def __init__(
        self,
        credentials: DBCredentials,
        bucket: Optional[str] = DEFAULT_OBJECT_STORAGE_BUCKET,
        use_internal_url: bool = False,
        connect_timeout: int = 5,
        read_timeout: int = 30,
    ):
        self.credentials = credentials
        self._bucket = bucket or credentials.schema or DEFAULT_OBJECT_STORAGE_BUCKET
        self.use_internal_url = use_internal_url
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        if not self._bucket:
            raise ValueError("S3-compatible credentials must include a bucket name")

    @property
    def bucket(self) -> str:
        return self._bucket

    def client(self):
        import boto3
        from botocore.client import Config

        endpoint = (
            self.credentials.internal_url if self.use_internal_url else self.credentials.url
        )
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=self.credentials.user,
            aws_secret_access_key=self.credentials.password,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
            ),
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

    def upload_file(
        self,
        local_path: Path,
        key: str,
        content_type: Optional[str] = None,
    ) -> str:
        extra_args = {"ContentType": content_type} if content_type else None
        self.ensure_bucket()
        self.client().upload_file(
            str(local_path),
            self.bucket,
            key,
            ExtraArgs=extra_args,
        )
        return s3_uri(self.bucket, key)

    def download_file(self, key: str, local_path: Path) -> Path:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client().download_file(self.bucket, key, str(local_path))
        return local_path

    def delete_file(self, key: str, *, if_match: Optional[str] = None) -> None:
        request = {"Bucket": self.bucket, "Key": key}
        if if_match:
            request["IfMatch"] = if_match
        self.client().delete_object(**request)

    def list_keys(self, prefix: str = "") -> list[str]:
        paginator = self.client().get_paginator("list_objects_v2")
        return [
            obj["Key"]
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]

    def read_text(self, key: str) -> str:
        response = self.client().get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    def read_text_with_etag(self, key: str) -> tuple[str, str]:
        response = self.client().get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read().decode("utf-8"), str(response.get("ETag") or "")

    def write_text(
        self,
        key: str,
        text: str,
        content_type: str = "text/plain; charset=utf-8",
        *,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> str:
        self.ensure_bucket()
        request = dict(
            Bucket=self.bucket,
            Key=key,
            Body=text.encode("utf-8"),
            ContentType=content_type,
        )
        if if_match:
            request["IfMatch"] = if_match
        if if_none_match:
            request["IfNoneMatch"] = if_none_match
        self.client().put_object(**request)
        return s3_uri(self.bucket, key)


class AwsAssumeRoleStorage:
    def __init__(
        self,
        credentials: AwsAssumeRoleCredentials,
        bucket: Optional[str] = None,
        connect_timeout: int = 5,
        read_timeout: int = 30,
    ):
        self.credentials = credentials
        self._bucket = bucket or credentials.bucket
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        if not self._bucket:
            raise ValueError("AWS S3 credentials must include a bucket name")

    @property
    def bucket(self) -> str:
        return self._bucket

    def client(self):
        import boto3
        from botocore.client import Config

        session_kwargs = {
            "aws_access_key_id": self.credentials.access_key_id,
            "aws_secret_access_key": self.credentials.secret_access_key,
        }
        if self.credentials.region:
            session_kwargs["region_name"] = self.credentials.region
        base_session = boto3.Session(**session_kwargs)
        assume_role_kwargs = {
            "RoleArn": self.credentials.role_arn,
            "RoleSessionName": self.credentials.session_name,
        }
        if self.credentials.external_id:
            assume_role_kwargs["ExternalId"] = self.credentials.external_id
        role = base_session.client("sts").assume_role(**assume_role_kwargs)["Credentials"]
        return boto3.client(
            "s3",
            region_name=self.credentials.region,
            endpoint_url=self.credentials.endpoint_url,
            aws_access_key_id=role["AccessKeyId"],
            aws_secret_access_key=role["SecretAccessKey"],
            aws_session_token=role["SessionToken"],
            config=Config(
                signature_version="s3v4",
                connect_timeout=self.connect_timeout,
                read_timeout=self.read_timeout,
            ),
        )

    def ensure_bucket(self) -> None:
        self.client().head_bucket(Bucket=self.bucket)

    def upload_file(
        self,
        local_path: Path,
        key: str,
        content_type: Optional[str] = None,
    ) -> str:
        extra_args = {"ContentType": content_type} if content_type else None
        self.client().upload_file(
            str(local_path),
            self.bucket,
            key,
            ExtraArgs=extra_args,
        )
        return s3_uri(self.bucket, key)

    def download_file(self, key: str, local_path: Path) -> Path:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client().download_file(self.bucket, key, str(local_path))
        return local_path

    def delete_file(self, key: str, *, if_match: Optional[str] = None) -> None:
        request = {"Bucket": self.bucket, "Key": key}
        if if_match:
            request["IfMatch"] = if_match
        self.client().delete_object(**request)

    def list_keys(self, prefix: str = "") -> list[str]:
        paginator = self.client().get_paginator("list_objects_v2")
        return [
            obj["Key"]
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]

    def read_text(self, key: str) -> str:
        response = self.client().get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    def read_text_with_etag(self, key: str) -> tuple[str, str]:
        response = self.client().get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read().decode("utf-8"), str(response.get("ETag") or "")

    def write_text(
        self,
        key: str,
        text: str,
        content_type: str = "text/plain; charset=utf-8",
        *,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> str:
        self.ensure_bucket()
        request = dict(
            Bucket=self.bucket,
            Key=key,
            Body=text.encode("utf-8"),
            ContentType=content_type,
        )
        if if_match:
            request["IfMatch"] = if_match
        if if_none_match:
            request["IfNoneMatch"] = if_none_match
        self.client().put_object(**request)
        return s3_uri(self.bucket, key)
