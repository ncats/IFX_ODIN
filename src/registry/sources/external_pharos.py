from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from sqlalchemy import text

from src.input_adapters.sql_adapter import MySqlAdapter, PostgreSqlAdapter
from src.registry.manifest import MANIFEST_FILENAME, iso_timestamp, manifest_checksum, today_utc, write_manifest
from src.registry.storage import DEFAULT_REGISTRY_BUCKET, MinioStorage, load_minio_credentials, s3_uri
from src.shared.db_credentials import DBCredentials


def external_storage_prefix(source: str, dataset: str, version: str) -> str:
    return f"external/{source}/{dataset}/{version}"


def _safe_credentials_summary(credentials_path: Path, database_type: str) -> Dict[str, Any]:
    with credentials_path.open("r", encoding="utf-8") as handle:
        credentials = yaml.safe_load(handle) or {}
    return {
        "type": database_type,
        "host": credentials.get("url"),
        "port": credentials.get("port"),
        "schema": credentials.get("schema"),
        "credential_ref": str(credentials_path),
    }


def _load_db_credentials(credentials_path: Path) -> DBCredentials:
    with credentials_path.open("r", encoding="utf-8") as handle:
        credentials = yaml.safe_load(handle) or {}
    return DBCredentials(
        url=credentials["url"],
        user=credentials["user"],
        password=credentials.get("password"),
        port=credentials.get("port"),
        schema=credentials.get("schema"),
        internal_url=credentials.get("internal_url"),
    )


def _yaml_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _yaml_safe_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: _yaml_safe(value) for key, value in row.items()}


def _version_token(value: Any) -> str:
    token = str(_yaml_safe(value)).strip()
    return token.replace(" ", "T").replace(":", "-").replace("/", "-")


def _drugcentral_version_info(credentials_path: Path) -> Dict[str, Any]:
    fallback = {
        "version": "drugcentral",
        "version_date": None,
        "version_method": {
            "type": "configured_database",
            "description": "No file snapshot is downloaded; adapters query the configured DrugCentral database.",
        },
    }
    try:
        adapter = PostgreSqlAdapter(_load_db_credentials(credentials_path))
        with adapter.get_engine().connect() as conn:
            row = conn.execute(text("SELECT * FROM public.dbversion LIMIT 1")).mappings().first()
    except Exception as exc:
        fallback["version_method"]["probe_error"] = str(exc)
        return fallback

    if not row:
        return fallback

    evidence = _yaml_safe_row(dict(row))
    preferred_keys = ("version", "dbversion", "db_version", "release", "releasedate", "release_date")
    version_value = next((evidence.get(key) for key in preferred_keys if evidence.get(key)), None)
    if version_value is None:
        version_value = next((value for value in evidence.values() if value not in (None, "")), None)
    if version_value is None:
        return fallback

    return {
        "version": _version_token(version_value),
        "version_date": _yaml_safe(evidence.get("release_date") or evidence.get("releasedate")),
        "version_method": {
            "type": "drugcentral_dbversion_table",
            "description": "Read DrugCentral release metadata from public.dbversion.",
            "evidence": evidence,
        },
    }


def _ifx_pubmed_version_info(credentials_path: Path) -> Dict[str, Any]:
    fallback = {
        "version": "ifx_pubmed",
        "version_date": None,
        "version_method": {
            "type": "configured_database",
            "description": "No file snapshot is downloaded; adapter queries the configured IFX PubMed mirror.",
        },
    }
    try:
        adapter = MySqlAdapter(_load_db_credentials(credentials_path))
        with adapter.get_engine().connect() as conn:
            latest_article = conn.execute(text(
                "SELECT id, fetch_date, source_file FROM pubmed ORDER BY id DESC LIMIT 1"
            )).mappings().first()
            file_state = conn.execute(text(
                """
                SELECT
                    COUNT(*) AS tracked_archives,
                    SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END) AS processed_archives,
                    SUM(CASE WHEN status IN ('failed', 'checksum_failed') THEN 1 ELSE 0 END) AS failed_archives,
                    MAX(remote_last_modified) AS latest_remote_last_modified,
                    MAX(downloaded_at) AS latest_downloaded_at,
                    MAX(processed_at) AS latest_processed_at
                FROM pubmed_mirror_file_state
                """
            )).mappings().first()
    except Exception as exc:
        fallback["version_method"]["probe_error"] = str(exc)
        return fallback

    latest_article_evidence = _yaml_safe_row(dict(latest_article or {}))
    file_state_evidence = _yaml_safe_row(dict(file_state or {}))
    latest_processed_at = file_state_evidence.get("latest_processed_at")
    latest_fetch_date = latest_article_evidence.get("fetch_date")
    version_basis = latest_processed_at or latest_fetch_date or latest_article_evidence.get("id")
    if version_basis is None:
        return fallback

    return {
        "version": _version_token(version_basis),
        "version_date": str(version_basis)[:10] if not isinstance(version_basis, int) else None,
        "version_method": {
            "type": "pubmed_mirror_latest_processed_state",
            "description": "Use latest PubMed mirror file-state processed_at when available; include highest PMID row as secondary evidence.",
            "evidence": {
                "latest_article": latest_article_evidence,
                "file_state": file_state_evidence,
            },
        },
    }


def _build_external_manifest(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    connection: Dict[str, Any],
    access: Dict[str, Any],
    extra: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "kind": "external_source_registration",
        "schema_version": 1,
        "source": source,
        "dataset": dataset,
        "registration_id": f"{source}:{dataset}:{version}",
        "version": version,
        "version_date": version_date,
        "registered_date": today_utc(),
        "created_at": iso_timestamp(),
        "connection": connection,
        "access": access,
        "extra": extra,
    }


def _register_external_manifest(
    *,
    manifest: Dict[str, Any],
    dest: Path,
    minio_credentials: Optional[Path],
    upload: bool,
    bucket: str = DEFAULT_REGISTRY_BUCKET,
) -> Path:
    source = manifest["source"]
    dataset = manifest["dataset"]
    version = manifest["version"]
    final_dir = dest / source / dataset / version
    manifest_path = final_dir / MANIFEST_FILENAME
    write_manifest(manifest, manifest_path)

    if upload:
        if not minio_credentials:
            raise ValueError("minio_credentials is required when upload=True")
        storage = MinioStorage(load_minio_credentials(minio_credentials), bucket=bucket)
        object_prefix = external_storage_prefix(source, dataset, version)
        storage.upload_file(manifest_path, f"{object_prefix}/{MANIFEST_FILENAME}", "application/x-yaml")
        print(f"Uploaded external registration to s3://{storage.bucket}/{object_prefix}/")

    print(f"Wrote {manifest_path}")
    print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
    return manifest_path


def register_pharos_external_sources(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    chembl_credentials: Path = Path("src/use_cases/secrets/chembl_credentials.yaml"),
    drugcentral_credentials: Path = Path("src/use_cases/secrets/drugcentral_credentials.yaml"),
    pubmed_credentials: Path = Path("src/use_cases/secrets/ifx_pubmed_credentials.yaml"),
    legacy_pharos_credentials: Path = Path("src/use_cases/secrets/pharos_credentials.yaml"),
) -> List[Path]:
    drugcentral_info = _drugcentral_version_info(drugcentral_credentials)
    pubmed_info = _ifx_pubmed_version_info(pubmed_credentials)

    registrations = [
        _build_external_manifest(
            source="chembl",
            dataset="activity_database",
            version="chembl36",
            version_date=None,
            connection=_safe_credentials_summary(chembl_credentials, "mysql"),
            access={
                "mode": "query",
                "interface": "sql",
                "database_type": "mysql",
            },
            extra={
                "version_method": {
                    "type": "database_schema_and_chembl_version_table",
                    "description": "Configured schema is chembl36; adapters also read the ChEMBL Version table at runtime.",
                },
            },
        ),
        _build_external_manifest(
            source="drugcentral",
            dataset="drug_database",
            version=drugcentral_info["version"],
            version_date=drugcentral_info["version_date"],
            connection=_safe_credentials_summary(drugcentral_credentials, "postgresql"),
            access={
                "mode": "query",
                "interface": "sql",
                "database_type": "postgresql",
            },
            extra={"version_method": drugcentral_info["version_method"]},
        ),
        _build_external_manifest(
            source="ifx_pubmed",
            dataset="pubmed_mirror",
            version=pubmed_info["version"],
            version_date=pubmed_info["version_date"],
            connection=_safe_credentials_summary(pubmed_credentials, "mysql"),
            access={
                "mode": "query",
                "interface": "sql",
                "database_type": "mysql",
            },
            extra={"version_method": pubmed_info["version_method"]},
        ),
        _build_external_manifest(
            source="legacy_pharos",
            dataset="pharos319_mysql",
            version="pharos319",
            version_date=None,
            connection=_safe_credentials_summary(legacy_pharos_credentials, "mysql"),
            access={
                "mode": "query",
                "interface": "sql",
                "database_type": "mysql",
            },
            extra={
                "version_method": {
                    "type": "configured_legacy_database",
                    "description": "No file snapshot is downloaded; adapters query the legacy Pharos/TCRD database.",
                },
            },
        ),
    ]

    paths = []
    for manifest in registrations:
        if upload:
            source = manifest["source"]
            dataset = manifest["dataset"]
            version = manifest["version"]
            manifest["manifest_uri"] = s3_uri(DEFAULT_REGISTRY_BUCKET, f"{external_storage_prefix(source, dataset, version)}/{MANIFEST_FILENAME}")
        paths.append(_register_external_manifest(
            manifest=manifest,
            dest=dest,
        ))
    return paths
