from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from sqlalchemy import text

from src.input_adapters.sql_adapter import MySqlAdapter, PostgreSqlAdapter
from src.registry.fetchers import ExternalSourceProvider, ExternalSourceRegistration
from src.shared.db_credentials import DBCredentials


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
    try:
        adapter = PostgreSqlAdapter(_load_db_credentials(credentials_path))
        with adapter.get_engine().connect() as conn:
            row = conn.execute(text("SELECT * FROM public.dbversion LIMIT 1")).mappings().first()
    except Exception as exc:
        raise RuntimeError(f"Could not read DrugCentral version from public.dbversion: {exc}") from exc

    if not row:
        raise RuntimeError("Could not read DrugCentral version from public.dbversion: table returned no rows")

    evidence = _yaml_safe_row(dict(row))
    preferred_keys = ("version", "dbversion", "db_version", "release", "releasedate", "release_date")
    version_value = next((evidence.get(key) for key in preferred_keys if evidence.get(key)), None)
    if version_value is None:
        version_value = next((value for value in evidence.values() if value not in (None, "")), None)
    if version_value is None:
        raise RuntimeError("Could not read DrugCentral version from public.dbversion: no non-empty version value")

    return {
        "version": _version_token(version_value),
        "version_date": str(_yaml_safe(evidence.get("release_date") or evidence.get("releasedate") or evidence.get("dtime")))[:10],
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


def _credentials_path(config: Dict[str, Any]) -> Path:
    credentials = config.get("credentials")
    if not credentials:
        raise ValueError("External source config requires credentials")
    return Path(credentials)


def _sql_access(database_type: str) -> Dict[str, str]:
    return {
        "mode": "query",
        "interface": "sql",
        "database_type": database_type,
    }


class RdasDiseasesGraphqlExternalSource(ExternalSourceProvider):
    source = "rdas"
    dataset = "diseases_graphql"

    def build_registration(self, *, config: Dict[str, Any]) -> ExternalSourceRegistration:
        endpoint = config.get("endpoint", "https://rdas.ncats.nih.gov/api/diseases/graphql")
        return ExternalSourceRegistration(
            source=self.source,
            dataset=self.dataset,
            version=config.get("version", "rdas-diseases-graphql"),
            version_date=config.get("version_date"),
            connection={
                "type": "graphql",
                "endpoint": endpoint,
            },
            access={
                "mode": "query",
                "interface": "graphql",
            },
            extra={
                "version_method": {
                    "type": "configured_graphql_endpoint",
                    "description": "No file snapshot is downloaded; adapter queries the configured RDAS GraphQL endpoint.",
                },
            },
        )


class ChemblActivityDatabaseExternalSource(ExternalSourceProvider):
    source = "chembl"
    dataset = "activity_database"

    def build_registration(self, *, config: Dict[str, Any]) -> ExternalSourceRegistration:
        credentials_path = _credentials_path(config)
        return ExternalSourceRegistration(
            source=self.source,
            dataset=self.dataset,
            version="chembl36",
            version_date=None,
            connection=_safe_credentials_summary(credentials_path, "mysql"),
            access=_sql_access("mysql"),
            extra={
                "version_method": {
                    "type": "database_schema_and_chembl_version_table",
                    "description": "Configured schema is chembl36; adapters also read the ChEMBL Version table at runtime.",
                },
            },
        )


class DrugcentralDrugDatabaseExternalSource(ExternalSourceProvider):
    source = "drugcentral"
    dataset = "drug_database"

    def build_registration(self, *, config: Dict[str, Any]) -> ExternalSourceRegistration:
        credentials_path = _credentials_path(config)
        version_info = _drugcentral_version_info(credentials_path)
        return ExternalSourceRegistration(
            source=self.source,
            dataset=self.dataset,
            version=version_info["version"],
            version_date=version_info["version_date"],
            connection=_safe_credentials_summary(credentials_path, "postgresql"),
            access=_sql_access("postgresql"),
            extra={"version_method": version_info["version_method"]},
        )


class IfxPubmedMirrorExternalSource(ExternalSourceProvider):
    source = "ifx_pubmed"
    dataset = "pubmed_mirror"

    def build_registration(self, *, config: Dict[str, Any]) -> ExternalSourceRegistration:
        credentials_path = _credentials_path(config)
        version_info = _ifx_pubmed_version_info(credentials_path)
        return ExternalSourceRegistration(
            source=self.source,
            dataset=self.dataset,
            version=version_info["version"],
            version_date=version_info["version_date"],
            connection=_safe_credentials_summary(credentials_path, "mysql"),
            access=_sql_access("mysql"),
            extra={"version_method": version_info["version_method"]},
        )


class LegacyPharosMysqlExternalSource(ExternalSourceProvider):
    source = "legacy_pharos"
    dataset = "pharos319_mysql"

    def build_registration(self, *, config: Dict[str, Any]) -> ExternalSourceRegistration:
        credentials_path = _credentials_path(config)
        return ExternalSourceRegistration(
            source=self.source,
            dataset=self.dataset,
            version="pharos319",
            version_date=None,
            connection=_safe_credentials_summary(credentials_path, "mysql"),
            access=_sql_access("mysql"),
            extra={
                "version_method": {
                    "type": "configured_legacy_database",
                    "description": "No file snapshot is downloaded; adapters query the legacy Pharos/TCRD database.",
                },
            },
        )
