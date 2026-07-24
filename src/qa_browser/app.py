import argparse
import csv
import gzip
import hashlib
import importlib.util
import io
import json
import math
import os
import re
import shutil
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List
from urllib.parse import quote as url_quote, urlencode

import urllib3
import yaml
from arango import ArangoClient
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text, inspect as sa_inspect
from sqlalchemy.engine import Engine
from starlette.concurrency import run_in_threadpool

import uvicorn

from src.core.data_registry import DataRegistry
from src.registry.storage import DEFAULT_REGISTRY_CACHE_DIR
from src.models.node import Node
from src.qa_browser.ramp_id_graph import set_ramp_diagnosis_file
from src.qa_browser.registry_usage import (
    extract_registry_datasets,
    graph_usage_filters,
    graph_usage_styles,
    group_by_source_dataset,
    load_registry_graphs_cached,
    load_graph_registry_usage_cached,
    with_graph_usages,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def _app_lifespan(_app: FastAPI):
    _start_resolver_warmup_thread()
    yield


app = FastAPI(title="QA Browser", lifespan=_app_lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["root_path"] = ""

# -- Global state set at startup --
_client: Optional[ArangoClient] = None
_credentials: dict = {}
_mysql_credentials: dict = {}
_mysql_sources: dict = {}
_mysql_db_engines: dict = {}
_mysql_inspector_cache: dict = {}   # db_name -> CachableInspector data
_minio_credentials: dict = {}
_parquet_storage_credentials: dict = {}
_registry_usage_cache: dict = {
    "loaded_at": 0.0,
    "usage_by_registry_id": None,
    "error": None,
}
_registry_graph_cache: dict = {
    "loaded_at": 0.0,
    "graphs": None,
    "error": None,
}
_registry_catalog_cache: dict = {
    "loaded_at": {},
    "source_snapshots": None,
    "derived_artifacts": None,
    "external_registrations": None,
    "resolver_snapshots": None,
}
_registry_update_status_cache: dict = {
    "checked_at": None,
    "elapsed_seconds": None,
    "sections": [],
    "error": None,
}
REGISTRY_UPDATE_RETURN_PATHS = {
    "sources": "/registry",
    "resolvers": "/registry/resolvers",
    "graphs": "/registry/graphs",
}
_resolver_instance_cache: dict = {}
_resolver_instance_cache_locks: dict = {}
_resolver_instance_cache_locks_guard = threading.Lock()
_resolver_warmup_thread: Optional[threading.Thread] = None
_resolver_warmup_started = False
_resolver_warmup_status: dict = {
    "started_at": None,
    "completed_at": None,
    "total": 0,
    "warmed": 0,
    "errors": [],
}
_REGISTRY_USAGE_TTL_SECONDS = 60
_REGISTRY_CATALOG_TTL_SECONDS = int(os.getenv("QA_BROWSER_REGISTRY_CATALOG_TTL_SECONDS", "300"))
_RESOLVER_API_MAX_IDS = 1000
_RESOLVER_WARMUP_ENABLED = os.getenv("QA_BROWSER_WARM_RESOLVERS", "1").lower() in {
    "1", "true", "yes", "on"
}
_RESOLVER_WARMUP_ALL_SNAPSHOTS = os.getenv("QA_BROWSER_WARM_ALL_RESOLVER_SNAPSHOTS", "").lower() in {
    "1", "true", "yes", "on"
}
_HARMONIZATION_PIPELINE_COLLECTION = "HarmonizationPipeline"
_HARMONIZATION_PIPELINE_RUN_COLLECTION = "HarmonizationPipelineRun"
_HARMONIZATION_STAGE_COLLECTION = "HarmonizationStage"
_HARMONIZED_METABOLITE_COLLECTION = "HarmonizedMetabolite"
_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION = "HarmonizedMetaboliteMemberEdge"
_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION = "HarmonizationStageEvidenceEdge"
_HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION = "HarmonizationStageActiveIdentifierChunk"
_HARMONIZATION_ENGINE_VERSION = "staged-pipeline-v1"
_RAMP_MAPPING_DENYLIST_PATH = BASE_DIR / "data" / "ramp_mapping_denylist.tsv"
_HMDB_IGNORED_PREFIX_DEFAULTS = [
    "BiGG",
    "BioCyc",
    "DRUGBANK",
    "DRUGBANK.METABOLITE",
    "FoodDB",
    "KNApSAcK",
    "METLIN",
    "MetaGene",
    "NugoWiki",
    "PDB.HET",
    "PhenolExplorer.COMPOUND",
    "PhenolExplorer.METABOLITE",
    "Wikipedia",
]
_METABOLITE_HARMONIZATION_RULES = [
    {
        "id": "ignore_generic_structure_mismatch",
        "label": "Ignore generic/non-generic structure equivalence",
        "description": "Drop equivalence edges where exactly one endpoint has an R-group or wildcard structure signal.",
    },
    {
        "id": "ignore_ramp_mapping_denylist",
        "label": "Ignore RaMP mapping deny list",
        "description": "Drop source equivalence edges listed in the RaMP curated mapping deny list.",
    },
    {
        "id": "ignore_hmdb_prefixes",
        "label": "Ignore HMDB prefixes",
        "description": "Drop HMDB-reported equivalence edges to configured identifier prefixes.",
        "parameters": [
            {
                "id": "prefixes",
                "label": "Prefixes",
                "type": "textarea",
                "default": "\n".join(_HMDB_IGNORED_PREFIX_DEFAULTS),
                "placeholder": "FoodDB\nKNApSAcK\nDRUGBANK",
            },
        ],
    },
    {
        "id": "merge_shared_inchikey_prefix",
        "label": "Merge shared InChIKey prefix",
        "description": "Merge identifiers when any stored structure has the same InChIKey first block.",
    },
    {
        "id": "merge_shared_inchikey_duplex",
        "label": "Merge shared InChIKey duplex",
        "description": "Merge identifiers when any stored structure has the same first two InChIKey blocks.",
    },
    {
        "id": "merge_inchikey_by_mw_cutoff",
        "label": "Merge InChIKey by MW cutoff",
        "description": "Use InChIKey duplex below the cutoff and InChIKey prefix at or above it.",
        "parameters": [
            {
                "id": "mw_cutoff",
                "label": "MW cutoff",
                "type": "number",
                "default": 500,
                "min": 0,
                "step": 1,
                "unit": "Da",
            },
        ],
    },
]
_metabolite_snapshot_jobs: dict = {}
_metabolite_snapshot_jobs_lock = threading.Lock()
_demo_queries_enabled = os.getenv("QA_BROWSER_ENABLE_POUNCE_DEMOS", "").lower() in {
    "1", "true", "yes", "on"
}


def _root_path(request: Optional[Request] = None) -> str:
    if request is not None:
        root_path = request.scope.get("root_path")
        if root_path:
            return str(root_path).rstrip("/")
    return str(templates.env.globals.get("root_path") or "").rstrip("/")


def _app_path(path: str, request: Optional[Request] = None) -> str:
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"{_root_path(request)}{normalized_path}"


def _redirect_to(path: str, *, request: Optional[Request] = None, status_code: int = 303) -> RedirectResponse:
    return RedirectResponse(url=_app_path(path, request), status_code=status_code)


def _is_hidden_qa_browser_collection(collection_name: str) -> bool:
    return collection_name.startswith("_")


def _is_qa_visible_edge_collection(collection_name: str) -> bool:
    return collection_name in {
        _HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION,
        _HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION,
    }


_GENERIC_STRUCTURE_TOKEN_RE = re.compile(r"(?<![A-Za-z])R\d*(?![a-z])")


def _structure_values_for_generic_check(structure: dict) -> List[tuple[str, str]]:
    values = []
    for field in ("smiles", "formula", "inchi"):
        value = structure.get(field)
        if value:
            values.append((field, str(value)))
    return values


def _value_has_generic_structure_token(field: str, value: str) -> bool:
    if "*" in value:
        return True
    if "[R" in value or "(R)" in value:
        return True
    if field == "formula" and _GENERIC_STRUCTURE_TOKEN_RE.search(value):
        return True
    if field in {"smiles", "inchi"} and _GENERIC_STRUCTURE_TOKEN_RE.search(value):
        return True
    return False


def _generic_structure_evidence(structures: List[dict]) -> List[dict]:
    evidence = []
    for structure in structures or []:
        for field, value in _structure_values_for_generic_check(structure):
            if _value_has_generic_structure_token(field, value):
                evidence.append({
                    "source": structure.get("source"),
                    "source_id": structure.get("source_id"),
                    "field": field,
                    "value": value,
                })
    return evidence


_INCHI_KEY_PREFIX_RE = re.compile(r"^[A-Z]{14}$")
_INCHI_KEY_DUPLEX_RE = re.compile(r"^[A-Z]{14}-[A-Z]{10}$")
_INCHI_KEY_FULL_RE = re.compile(r"^([A-Z]{14})-([A-Z]{10})-[A-Z]$")


def _normalize_inchi_key_prefix(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    prefix = str(value).strip().upper()
    if not prefix:
        return None
    prefix = prefix.split("-", 1)[0]
    if not _INCHI_KEY_PREFIX_RE.match(prefix):
        return None
    return prefix


def _normalize_inchi_key_duplex(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    match = _INCHI_KEY_FULL_RE.match(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    parts = text.split("-")
    if len(parts) >= 2:
        duplex = "-".join(parts[:2])
        if _INCHI_KEY_DUPLEX_RE.match(duplex):
            return duplex
    return None


def _parse_optional_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _identifier_prefix(identifier: Optional[str]) -> Optional[str]:
    if not identifier or ":" not in str(identifier):
        return None
    prefix = str(identifier).split(":", 1)[0].strip()
    return prefix or None


def _normalize_prefix_list(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(item) for item in value]
    else:
        raw_items = re.split(r"[\s,;]+", str(value))
    seen = set()
    prefixes = []
    for item in raw_items:
        prefix = item.strip()
        if not prefix:
            continue
        prefix_key = prefix.lower()
        if prefix_key in seen:
            continue
        seen.add(prefix_key)
        prefixes.append(prefix)
    return prefixes


def _normalize_ramp_denylist_identifier(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or ":" not in text:
        return None
    prefix, local_id = text.split(":", 1)
    prefix_key = prefix.strip().lower()
    local_id = local_id.strip()
    if not local_id:
        return None
    prefix_map = {
        "hmdb": "HMDB",
        "chebi": "CHEBI",
        "kegg": "KEGG.COMPOUND",
        "kegg.compound": "KEGG.COMPOUND",
        "pubchem": "PUBCHEM.COMPOUND",
        "pubchem.compound": "PUBCHEM.COMPOUND",
        "chemspider": "ChemSpider",
        "refmet": "REFMET",
        "lipidmaps": "LIPIDMAPS",
        "cas": "CAS",
        "wikidata": "Wikidata",
    }
    normalized_prefix = prefix_map.get(prefix_key, prefix.strip())
    return f"{normalized_prefix}:{local_id}"


def _load_ramp_mapping_denylist_pairs() -> set[tuple[str, str]]:
    denylist_pairs = set()
    if not _RAMP_MAPPING_DENYLIST_PATH.exists():
        return denylist_pairs
    with _RAMP_MAPPING_DENYLIST_PATH.open(newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        next(reader, None)
        for row in reader:
            if len(row) < 2:
                continue
            left = _normalize_ramp_denylist_identifier(row[0])
            right = _normalize_ramp_denylist_identifier(row[1])
            if not left or not right or left == right:
                continue
            denylist_pairs.add(tuple(sorted((left, right))))
    return denylist_pairs


def _normalize_metabolite_rule_ids(rule_ids: Optional[List[str]]) -> List[str]:
    allowed = {rule["id"] for rule in _METABOLITE_HARMONIZATION_RULES}
    normalized = []
    for rule_id in rule_ids or []:
        if rule_id in allowed and rule_id not in normalized:
            normalized.append(rule_id)
    return normalized


def _default_metabolite_rule_parameters(rule_id: str) -> dict:
    rule = next((item for item in _METABOLITE_HARMONIZATION_RULES if item["id"] == rule_id), None)
    if not rule:
        return {}
    return {
        parameter["id"]: parameter.get("default")
        for parameter in rule.get("parameters", [])
    }


def _normalize_metabolite_rule_parameters(rule_ids: List[str], rule_parameters: Optional[dict]) -> dict:
    normalized = {}
    submitted = rule_parameters or {}
    for rule_id in rule_ids:
        defaults = _default_metabolite_rule_parameters(rule_id)
        if not defaults:
            continue
        normalized[rule_id] = {}
        for parameter_id, default_value in defaults.items():
            raw_value = submitted.get(rule_id, {}).get(parameter_id, default_value)
            if parameter_id == "mw_cutoff":
                parsed_value = _parse_optional_float(raw_value)
                normalized[rule_id][parameter_id] = parsed_value if parsed_value is not None else default_value
            elif parameter_id == "prefixes":
                normalized[rule_id][parameter_id] = _normalize_prefix_list(raw_value)
            else:
                normalized[rule_id][parameter_id] = raw_value
    return normalized


def _metabolite_rule_metadata(rule_ids: List[str], rule_parameters: Optional[dict] = None) -> List[dict]:
    rule_by_id = {rule["id"]: rule for rule in _METABOLITE_HARMONIZATION_RULES}
    parameter_by_rule_id = rule_parameters or {}
    metadata = []
    for rule_id in rule_ids:
        if rule_id not in rule_by_id:
            continue
        item = {
            "id": rule_id,
            "label": rule_by_id[rule_id]["label"],
            "description": rule_by_id[rule_id]["description"],
        }
        if parameter_by_rule_id.get(rule_id):
            item["parameters"] = parameter_by_rule_id[rule_id]
        metadata.append(item)
    return metadata


def _chunked_records(records: List[dict], chunk_size: int = 10000):
    for index in range(0, len(records), chunk_size):
        yield records[index:index + chunk_size]


def _load_generic_structure_ids(db) -> set:
    generic_structure_ids = set()
    metabolite_structure_cursor = db.aql.execute(
        """
        FOR d IN MetaboliteIdentifier
          FILTER LENGTH(d.chem_props || []) > 0
          RETURN {
            id: d.id,
            structures: (
              FOR prop IN d.chem_props || []
                RETURN {
                  source: prop.source,
                  source_id: prop.source_id,
                  smiles: prop.iso_smiles || prop.isomeric_smiles || prop.canonical_smiles,
                  formula: prop.molecular_formula,
                  inchi: prop.inchi
                }
            )
          }
        """,
        batch_size=10000,
        max_runtime=600,
    )
    for row in metabolite_structure_cursor:
        if _generic_structure_evidence(row.get("structures", [])):
            generic_structure_ids.add(row["id"])
    chebi_structure_cursor = db.aql.execute(
        """
        FOR d IN ChemicalEntity
          FILTER d.smiles != null OR d.formula != null OR d.inchi != null
          RETURN {
            id: d.id,
            structures: [
              {
                source: "ChEBI",
                source_id: d.id,
                smiles: d.smiles,
                formula: d.formula,
                inchi: d.inchi
              }
            ]
          }
        """,
        batch_size=10000,
        max_runtime=600,
    )
    for row in chebi_structure_cursor:
        if _generic_structure_evidence(row.get("structures", [])):
            generic_structure_ids.add(row["id"])
    return generic_structure_ids


def _iter_metabolite_identifier_inchi_key_matches(db, mode: str, mw_cutoff: Optional[float] = None):
    cursor = db.aql.execute(
        """
        FOR d IN MetaboliteIdentifier
          LET inchi_keys = UNIQUE(
            FLATTEN(
              FOR prop IN d.chem_props || []
                RETURN [prop.inchi_key]
            )
          )
          LET prefixes = UNIQUE(
            FLATTEN(
              FOR prop IN d.chem_props || []
                RETURN [prop.inchi_key_prefix]
            )
          )
          LET masses = UNIQUE(
            FLATTEN(
              FOR prop IN d.chem_props || []
                RETURN [prop.mw, prop.monoisotopic_mass]
            )
          )
          FILTER LENGTH(inchi_keys) > 0 OR LENGTH(prefixes) > 0
          RETURN {id: d.id, inchi_keys: inchi_keys, prefixes: prefixes, masses: masses}
        """,
        batch_size=10000,
        max_runtime=600,
    )
    for row in cursor:
        numeric_masses = [
            parsed
            for parsed in (_parse_optional_float(value) for value in row.get("masses", []))
            if parsed is not None
        ]
        mass = min(numeric_masses) if numeric_masses else None
        effective_mode = mode
        if mode == "mw_cutoff":
            effective_mode = "prefix" if mass is not None and mass >= (mw_cutoff or 500) else "duplex"
        matches = set()
        if effective_mode == "prefix":
            matches.update(
                prefix
                for prefix in (_normalize_inchi_key_prefix(value) for value in row.get("inchi_keys", []))
                if prefix
            )
            matches.update(
                prefix
                for prefix in (_normalize_inchi_key_prefix(value) for value in row.get("prefixes", []))
                if prefix
            )
        elif effective_mode == "duplex":
            matches.update(
                duplex
                for duplex in (_normalize_inchi_key_duplex(value) for value in row.get("inchi_keys", []))
                if duplex
            )
        if matches:
            yield row["id"], sorted(matches), effective_mode, mass


def _ensure_harmonization_pipeline_collections(db) -> None:
    for collection_name in (
        _HARMONIZATION_PIPELINE_COLLECTION,
        _HARMONIZATION_PIPELINE_RUN_COLLECTION,
        _HARMONIZATION_STAGE_COLLECTION,
        _HARMONIZED_METABOLITE_COLLECTION,
        _HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION,
    ):
        if not db.has_collection(collection_name):
            db.create_collection(collection_name)
    for collection_name in (
        _HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION,
        _HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION,
    ):
        if not db.has_collection(collection_name):
            db.create_collection(collection_name, edge=True)
    graph = db.graph("graph") if db.has_graph("graph") else db.create_graph("graph")
    definitions = {
        definition["edge_collection"]: definition
        for definition in graph.edge_definitions()
    }

    def upsert_edge_definition(collection_name: str, from_collections: List[str], to_collections: List[str]) -> None:
        existing = definitions.get(collection_name)
        if existing is None:
            graph.create_edge_definition(collection_name, from_collections, to_collections)
            return
        updated_from = sorted(set(existing.get("from_vertex_collections", []) + from_collections))
        updated_to = sorted(set(existing.get("to_vertex_collections", []) + to_collections))
        if (
            updated_from != sorted(existing.get("from_vertex_collections", []))
            or updated_to != sorted(existing.get("to_vertex_collections", []))
        ):
            graph.replace_edge_definition(collection_name, updated_from, updated_to)

    upsert_edge_definition(
        _HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION,
        [_HARMONIZED_METABOLITE_COLLECTION],
        ["MetaboliteIdentifier"],
    )
    upsert_edge_definition(
        _HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION,
        ["MetaboliteIdentifier"],
        ["MetaboliteIdentifier"],
    )


def _canonical_json_digest(payload: dict, length: int = 16) -> str:
    return hashlib.sha1(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:length]


def _harmonization_graph_fingerprint(db) -> dict:
    row = list(db.aql.execute(
        """
        RETURN {
          metabolite_identifier_count: LENGTH(MetaboliteIdentifier),
          equivalence_edge_count: LENGTH(MetaboliteIdentifierMappingEdge)
        }
        """,
        max_runtime=120,
    ))[0]
    return {
        "database": "metabolite_harmonization",
        "engine_version": _HARMONIZATION_ENGINE_VERSION,
        **row,
    }


def _harmonization_stage_key(rule_ids: List[str], rule_parameters: dict, graph_fingerprint: dict) -> str:
    digest = _canonical_json_digest({
        "kind": "harmonization_stage",
        "graph_fingerprint": graph_fingerprint,
        "rule_ids": rule_ids,
        "rule_parameters": rule_parameters,
    })
    if not rule_ids:
        return f"baseline-{digest}"
    return f"stage-{len(rule_ids):02d}-{digest}"


def _cumulative_rule_parameters(rule_ids: List[str], rule_parameters: dict) -> dict:
    return {
        rule_id: rule_parameters.get(rule_id, {})
        for rule_id in rule_ids
        if rule_parameters.get(rule_id)
    }


def _delete_harmonization_stage_artifacts(db, stage_key: str) -> None:
    for collection_name in (
        _HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION,
        _HARMONIZED_METABOLITE_COLLECTION,
        _HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION,
        _HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION,
    ):
        if not db.has_collection(collection_name):
            continue
        while True:
            deleted = list(db.aql.execute(
                f"""
                LET keys = (
                  FOR d IN {collection_name}
                    FILTER d.stage_key == @stage_key
                    LIMIT 10000
                    RETURN d._key
                )
                FOR key IN keys
                  REMOVE key IN {collection_name}
                RETURN OLD._key
                """,
                bind_vars={"stage_key": stage_key},
                max_runtime=120,
            ))
            if not deleted:
                break


def _stage_is_referenced_by_any_run(db, stage_key: str) -> bool:
    if not db.has_collection(_HARMONIZATION_PIPELINE_RUN_COLLECTION):
        return False
    rows = list(db.aql.execute(
        f"""
        FOR r IN {_HARMONIZATION_PIPELINE_RUN_COLLECTION}
          FILTER @stage_key IN (r.stage_keys || [])
          LIMIT 1
          RETURN 1
        """,
        bind_vars={"stage_key": stage_key},
        max_runtime=120,
    ))
    return bool(rows)


def _delete_harmonization_pipeline(db, pipeline_key: str) -> dict:
    _ensure_harmonization_pipeline_collections(db)
    pipeline = _get_harmonization_pipeline(db, pipeline_key)
    stage_keys = list(db.aql.execute(
        f"""
        FOR r IN {_HARMONIZATION_PIPELINE_RUN_COLLECTION}
          FILTER r.pipeline_key == @pipeline_key
          FOR stage_key IN r.stage_keys || []
            COLLECT unique_stage_key = stage_key
            RETURN unique_stage_key
        """,
        bind_vars={"pipeline_key": pipeline_key},
        max_runtime=120,
    ))
    db.aql.execute(
        f"""
        FOR r IN {_HARMONIZATION_PIPELINE_RUN_COLLECTION}
          FILTER r.pipeline_key == @pipeline_key
          REMOVE r IN {_HARMONIZATION_PIPELINE_RUN_COLLECTION}
        """,
        bind_vars={"pipeline_key": pipeline_key},
        max_runtime=120,
    )
    pipeline_collection = db.collection(_HARMONIZATION_PIPELINE_COLLECTION)
    if pipeline_collection.has(pipeline_key):
        pipeline_collection.delete(pipeline_key)
    deleted_stage_keys = []
    for stage_key in stage_keys:
        if not _stage_is_referenced_by_any_run(db, stage_key):
            _delete_harmonization_stage_artifacts(db, stage_key)
            stage_collection = db.collection(_HARMONIZATION_STAGE_COLLECTION)
            if stage_collection.has(stage_key):
                stage_collection.delete(stage_key)
            deleted_stage_keys.append(stage_key)
    return {
        "pipeline": pipeline,
        "deleted_stage_keys": deleted_stage_keys,
    }


def _load_metabolite_identifier_source_support(db) -> Dict[str, set]:
    support_by_id: Dict[str, set] = {}
    cursor = db.aql.execute(
        """
        FOR d IN MetaboliteIdentifier
          LET support = UNIQUE(FLATTEN([
            (FOR item IN d.sources || [] RETURN IS_OBJECT(item) ? (item.name != null ? item.name : (item.source != null ? item.source : item.id)) : item),
            (FOR item IN d.names || [] RETURN item.source),
            (FOR item IN d.synonyms || [] RETURN item.source),
            (FOR item IN d.chem_props || [] RETURN item.source)
          ]))
          RETURN {id: d.id, support: support}
        """,
        batch_size=10000,
        max_runtime=600,
    )
    for row in cursor:
        sources = {
            str(source).strip()
            for source in row.get("support") or []
            if source is not None and str(source).strip()
        }
        support_by_id[row["id"]] = sources or {"__unsourced_node__"}
    return support_by_id


def _filter_identifier_support_for_rules(
    support_by_id: Dict[str, set],
    rule_ids: List[str],
    rule_parameters: dict,
) -> Dict[str, set]:
    filtered = {identifier: set(sources) for identifier, sources in support_by_id.items()}
    if "ignore_hmdb_prefixes" in rule_ids:
        prefixes = {
            prefix.lower()
            for prefix in rule_parameters.get("ignore_hmdb_prefixes", {}).get("prefixes", [])
        }
        for identifier, sources in filtered.items():
            if (_identifier_prefix(identifier) or "").lower() in prefixes:
                filtered[identifier] = {
                    source for source in sources
                    if str(source).lower() != "hmdb"
                }
    return filtered


def _active_metabolite_identifier_mapping_edges_for_rules(
    db,
    active_ids: set,
    rule_ids: List[str],
    rule_parameters: dict,
) -> tuple[List[dict], dict]:
    generic_structure_ids = _load_generic_structure_ids(db) if "ignore_generic_structure_mismatch" in rule_ids else set()
    ramp_mapping_denylist_pairs = (
        _load_ramp_mapping_denylist_pairs() if "ignore_ramp_mapping_denylist" in rule_ids else set()
    )
    hmdb_ignored_prefixes = set()
    if "ignore_hmdb_prefixes" in rule_ids:
        hmdb_ignored_prefixes = {
            prefix.lower()
            for prefix in rule_parameters.get("ignore_hmdb_prefixes", {}).get("prefixes", [])
        }

    active_edges = []
    summary = {
        "generic_structure_identifier_count": len(generic_structure_ids),
        "ramp_denylist_pair_count": len(ramp_mapping_denylist_pairs),
        "hmdb_ignored_prefixes": sorted(hmdb_ignored_prefixes),
        "hmdb_ignored_prefix_count": len(hmdb_ignored_prefixes),
        "ignored_edge_count": 0,
        "generic_structure_ignored_edge_count": 0,
        "ramp_denylist_ignored_edge_count": 0,
        "hmdb_prefix_ignored_detail_count": 0,
        "hmdb_prefix_ignored_edge_count": 0,
    }
    cursor = db.aql.execute(
        """
        FOR e IN MetaboliteIdentifierMappingEdge
          RETURN {
            key: e._key,
            id: e.id || e._key,
            start_id: e.start_id,
            end_id: e.end_id,
            details: e.details || []
          }
        """,
        batch_size=10000,
        max_runtime=600,
    )
    for edge in cursor:
        start_id = edge["start_id"]
        end_id = edge["end_id"]
        if start_id not in active_ids or end_id not in active_ids:
            summary["ignored_edge_count"] += 1
            continue
        if "ignore_generic_structure_mismatch" in rule_ids and ((start_id in generic_structure_ids) != (end_id in generic_structure_ids)):
            summary["ignored_edge_count"] += 1
            summary["generic_structure_ignored_edge_count"] += 1
            continue
        if "ignore_ramp_mapping_denylist" in rule_ids and tuple(sorted((start_id, end_id))) in ramp_mapping_denylist_pairs:
            summary["ignored_edge_count"] += 1
            summary["ramp_denylist_ignored_edge_count"] += 1
            continue
        details = edge.get("details") or []
        active_details = details
        if hmdb_ignored_prefixes:
            endpoint_has_ignored_prefix = (
                (_identifier_prefix(start_id) or "").lower() in hmdb_ignored_prefixes
                or (_identifier_prefix(end_id) or "").lower() in hmdb_ignored_prefixes
            )
            if endpoint_has_ignored_prefix:
                active_details = [
                    detail for detail in details
                    if str(detail.get("source") or "").lower() != "hmdb"
                ]
                removed_count = len(details) - len(active_details)
                summary["hmdb_prefix_ignored_detail_count"] += removed_count
                if removed_count and not active_details:
                    summary["hmdb_prefix_ignored_edge_count"] += 1
        if details and not active_details:
            summary["ignored_edge_count"] += 1
            continue
        sources = sorted({
            str(detail.get("source")).strip()
            for detail in active_details
            if detail.get("source") is not None and str(detail.get("source")).strip()
        })
        active_edges.append({
            **edge,
            "details": active_details,
            "sources": sources,
        })
    summary["active_edge_count"] = len(active_edges)
    return active_edges, summary


def _build_harmonized_groups(
    db,
    active_ids: set,
    active_edges: List[dict],
    rule_ids: List[str],
    rule_parameters: dict,
) -> tuple[List[List[str]], dict]:
    parent: Dict[str, str] = {}
    size: Dict[str, int] = {}

    def find(identifier: str) -> str:
        while parent[identifier] != identifier:
            parent[identifier] = parent[parent[identifier]]
            identifier = parent[identifier]
        return identifier

    def add(identifier: str) -> None:
        if identifier not in parent:
            parent[identifier] = identifier
            size[identifier] = 1

    def union(left: str, right: str) -> bool:
        add(left)
        add(right)
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return False
        if size[left_root] < size[right_root]:
            left_root, right_root = right_root, left_root
        parent[right_root] = left_root
        size[left_root] += size[right_root]
        return True

    for identifier in active_ids:
        add(identifier)
    for edge in active_edges:
        union(edge["start_id"], edge["end_id"])

    merge_summary = {
        "inchi_key_prefix_count": 0,
        "inchi_key_prefix_identifier_count": 0,
        "inchi_key_prefix_merge_count": 0,
        "inchi_key_duplex_count": 0,
        "inchi_key_duplex_identifier_count": 0,
        "inchi_key_duplex_merge_count": 0,
        "inchi_key_mw_cutoff_prefix_identifier_count": 0,
        "inchi_key_mw_cutoff_duplex_identifier_count": 0,
        "inchi_key_mw_cutoff_identifier_count": 0,
        "inchi_key_mw_cutoff_merge_count": 0,
    }

    def merge_by_inchi_key_matches(mode: str, mw_cutoff: Optional[float] = None) -> dict:
        first_identifier_by_match: Dict[str, str] = {}
        identifier_count = 0
        merge_count = 0
        prefix_identifier_count = 0
        duplex_identifier_count = 0
        for identifier, matches, effective_mode, _mass in _iter_metabolite_identifier_inchi_key_matches(db, mode, mw_cutoff):
            if identifier not in active_ids:
                continue
            identifier_count += 1
            if effective_mode == "prefix":
                prefix_identifier_count += 1
            if effective_mode == "duplex":
                duplex_identifier_count += 1
            for match in matches:
                match_key = f"{effective_mode}:{match}"
                if match_key not in first_identifier_by_match:
                    first_identifier_by_match[match_key] = identifier
                    continue
                if union(first_identifier_by_match[match_key], identifier):
                    merge_count += 1
        return {
            "match_count": len(first_identifier_by_match),
            "identifier_count": identifier_count,
            "merge_count": merge_count,
            "prefix_identifier_count": prefix_identifier_count,
            "duplex_identifier_count": duplex_identifier_count,
        }

    if "merge_shared_inchikey_prefix" in rule_ids:
        stats = merge_by_inchi_key_matches("prefix")
        merge_summary["inchi_key_prefix_count"] = stats["match_count"]
        merge_summary["inchi_key_prefix_identifier_count"] = stats["identifier_count"]
        merge_summary["inchi_key_prefix_merge_count"] = stats["merge_count"]
    if "merge_shared_inchikey_duplex" in rule_ids:
        stats = merge_by_inchi_key_matches("duplex")
        merge_summary["inchi_key_duplex_count"] = stats["match_count"]
        merge_summary["inchi_key_duplex_identifier_count"] = stats["identifier_count"]
        merge_summary["inchi_key_duplex_merge_count"] = stats["merge_count"]
    if "merge_inchikey_by_mw_cutoff" in rule_ids:
        mw_cutoff = rule_parameters.get("merge_inchikey_by_mw_cutoff", {}).get("mw_cutoff", 500)
        stats = merge_by_inchi_key_matches("mw_cutoff", mw_cutoff)
        merge_summary["inchi_key_mw_cutoff_identifier_count"] = stats["identifier_count"]
        merge_summary["inchi_key_mw_cutoff_merge_count"] = stats["merge_count"]
        merge_summary["inchi_key_mw_cutoff_prefix_identifier_count"] = stats["prefix_identifier_count"]
        merge_summary["inchi_key_mw_cutoff_duplex_identifier_count"] = stats["duplex_identifier_count"]

    members_by_root: Dict[str, List[str]] = {}
    for identifier in active_ids:
        members_by_root.setdefault(find(identifier), []).append(identifier)
    non_singleton_groups = [
        sorted(members)
        for members in members_by_root.values()
        if len(members) > 1
    ]
    non_singleton_groups.sort(key=lambda members: (-len(members), members[0]))
    return non_singleton_groups, merge_summary


def _materialize_harmonization_stage(
    db,
    stage_key: str,
    stage_doc: dict,
    active_ids: set,
    active_edges: List[dict],
    groups: List[List[str]],
) -> None:
    _delete_harmonization_stage_artifacts(db, stage_key)
    db.collection(_HARMONIZATION_STAGE_COLLECTION).insert(stage_doc, overwrite=True)

    active_chunk_collection = db.collection(_HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION)
    active_ids_sorted = sorted(active_ids)
    for chunk_index, identifiers in enumerate(_chunked_records(active_ids_sorted, 20000)):
        active_chunk_collection.insert({
            "_key": f"{stage_key}-{chunk_index:05d}",
            "id": f"HarmonizationStageActiveIdentifierChunk:{stage_key}-{chunk_index:05d}",
            "stage_key": stage_key,
            "chunk_index": chunk_index,
            "identifier_ids": identifiers,
        }, overwrite=True)

    evidence_collection = db.collection(_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION)
    evidence_batch = []
    for edge in active_edges:
        edge_key = f"{stage_key}-{_canonical_json_digest({'edge_key': edge.get('key'), 'start_id': edge.get('start_id'), 'end_id': edge.get('end_id')})}"
        evidence_batch.append({
            "_key": edge_key,
            "_from": f"MetaboliteIdentifier/{edge['start_id']}",
            "_to": f"MetaboliteIdentifier/{edge['end_id']}",
            "id": f"HarmonizationStageEvidenceEdge:{edge_key}",
            "stage_key": stage_key,
            "raw_edge_key": edge.get("key"),
            "raw_edge_id": edge.get("id"),
            "start_id": edge["start_id"],
            "end_id": edge["end_id"],
            "sources": edge.get("sources") or [],
            "source_count": len(edge.get("sources") or []),
            "detail_count": len(edge.get("details") or []),
            "details": edge.get("details") or [],
        })
        if len(evidence_batch) >= 10000:
            evidence_collection.insert_many(evidence_batch, overwrite=True)
            evidence_batch = []
    if evidence_batch:
        evidence_collection.insert_many(evidence_batch, overwrite=True)

    metabolite_collection = db.collection(_HARMONIZED_METABOLITE_COLLECTION)
    member_collection = db.collection(_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION)
    metabolite_batch = []
    member_batch = []
    for rank, members in enumerate(groups, start=1):
        member_digest = _canonical_json_digest({"members": members}, 16)
        harmonized_key = f"{stage_key}-{rank:06d}-{member_digest}"
        harmonized_id = f"HarmonizedMetabolite:{harmonized_key}"
        metabolite_batch.append({
            "_key": harmonized_key,
            "id": harmonized_id,
            "stage_key": stage_key,
            "stage_id": stage_doc["id"],
            "name": f"{stage_doc['name']} group {rank}",
            "size": len(members),
            "rank_by_size": rank,
            "representative_id": members[0],
            "sample_member_ids": members[:25],
            "member_hash": member_digest,
        })
        for member_index, member_id in enumerate(members):
            member_edge_key = f"{harmonized_key}-{_canonical_json_digest({'member': member_id})}"
            member_batch.append({
                "_key": member_edge_key,
                "_from": f"{_HARMONIZED_METABOLITE_COLLECTION}/{harmonized_key}",
                "_to": f"MetaboliteIdentifier/{member_id}",
                "id": f"HarmonizedMetaboliteMemberEdge:{member_edge_key}",
                "stage_key": stage_key,
                "stage_id": stage_doc["id"],
                "harmonized_metabolite_id": harmonized_id,
                "member_id": member_id,
                "member_index": member_index,
            })
        if len(metabolite_batch) >= 5000:
            metabolite_collection.insert_many(metabolite_batch, overwrite=True)
            metabolite_batch = []
        if len(member_batch) >= 10000:
            member_collection.insert_many(member_batch, overwrite=True)
            member_batch = []
    if metabolite_batch:
        metabolite_collection.insert_many(metabolite_batch, overwrite=True)
    for batch in _chunked_records(member_batch):
        member_collection.insert_many(batch, overwrite=True)


def _ensure_harmonization_stage(
    db,
    rule_ids: List[str],
    rule_parameters: dict,
    graph_fingerprint: dict,
    display_name: str,
    stage_index: int,
) -> dict:
    stage_key = _harmonization_stage_key(rule_ids, rule_parameters, graph_fingerprint)
    existing = db.collection(_HARMONIZATION_STAGE_COLLECTION).get(stage_key)
    if existing and existing.get("status") == "complete":
        return existing

    created_at = datetime.now(timezone.utc).isoformat()
    support_by_id = _load_metabolite_identifier_source_support(db)
    filtered_support_by_id = _filter_identifier_support_for_rules(support_by_id, rule_ids, rule_parameters)
    active_ids = {
        identifier
        for identifier, sources in filtered_support_by_id.items()
        if sources
    }
    active_edges, edge_summary = _active_metabolite_identifier_mapping_edges_for_rules(
        db,
        active_ids,
        rule_ids,
        rule_parameters,
    )
    for edge in active_edges:
        active_ids.add(edge["start_id"])
        active_ids.add(edge["end_id"])
    groups, merge_summary = _build_harmonized_groups(db, active_ids, active_edges, rule_ids, rule_parameters)
    non_singleton_member_count = sum(len(members) for members in groups)
    singleton_count = max(len(active_ids) - non_singleton_member_count, 0)
    summary = {
        **graph_fingerprint,
        **edge_summary,
        **merge_summary,
        "active_identifier_count": len(active_ids),
        "harmonized_metabolite_count": len(groups),
        "non_singleton_clique_count": len(groups),
        "singleton_identifier_count": singleton_count,
        "clique_count": len(groups) + singleton_count,
        "largest_clique_sizes": [len(members) for members in groups[:10]],
    }
    stage_doc = {
        "_key": stage_key,
        "id": f"HarmonizationStage:{stage_key}",
        "name": display_name,
        "created_at": created_at,
        "updated_at": created_at,
        "status": "complete",
        "graph_database": "metabolite_harmonization",
        "engine_version": _HARMONIZATION_ENGINE_VERSION,
        "stage_index": stage_index,
        "rule_ids": rule_ids,
        "rule_parameters": rule_parameters,
        "rules": _metabolite_rule_metadata(rule_ids, rule_parameters),
        "summary": summary,
        "materialization": {
            "active_identifier_chunks": _HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION,
            "evidence_edges": _HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION,
            "harmonized_metabolites": _HARMONIZED_METABOLITE_COLLECTION,
            "member_edges": _HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION,
            "singleton_note": "Singleton active identifiers are stored in active identifier chunks, not as HarmonizedMetabolite nodes.",
        },
    }
    _materialize_harmonization_stage(db, stage_key, stage_doc, active_ids, active_edges, groups)
    return stage_doc


def _stage_display_label(stage: dict, rule_parameters: Optional[dict] = None) -> str:
    rule_ids = stage.get("rule_ids") or []
    if not rule_ids:
        return "Baseline"
    rule_metadata = _metabolite_rule_metadata([rule_ids[-1]], rule_parameters or {})
    if rule_metadata:
        return rule_metadata[0]["label"]
    return rule_ids[-1]


def _list_harmonization_pipelines(limit: int = 25) -> List[dict]:
    db = get_db("metabolite_harmonization")
    if not db.has_collection(_HARMONIZATION_PIPELINE_COLLECTION):
        return []
    pipelines = list(db.aql.execute(
        f"""
        FOR p IN {_HARMONIZATION_PIPELINE_COLLECTION}
          SORT p.updated_at DESC
          LIMIT @limit
          LET runs = (
            FOR r IN {_HARMONIZATION_PIPELINE_RUN_COLLECTION}
              FILTER r.pipeline_key == p._key
              SORT r.created_at DESC
              LIMIT 5
              RETURN r
          )
          RETURN MERGE(p, {{runs: runs}})
        """,
        bind_vars={"limit": limit},
        max_runtime=120,
    ))
    for pipeline in pipelines:
        rule_parameters = pipeline.get("rule_parameters") or {}
        for run in pipeline.get("runs") or []:
            stages = run.get("stages") or []
            stages.sort(key=_stage_sort_key)
            for stage in stages:
                stage["display_label"] = _stage_display_label(stage, rule_parameters)
    return pipelines


def _list_harmonization_stages(limit: int = 50) -> List[dict]:
    db = get_db("metabolite_harmonization")
    if not db.has_collection(_HARMONIZATION_STAGE_COLLECTION):
        return []
    return list(db.aql.execute(
        f"""
        FOR s IN {_HARMONIZATION_STAGE_COLLECTION}
          FILTER s.status == "complete"
          SORT s.created_at ASC
          LIMIT @limit
          RETURN s
        """,
        bind_vars={"limit": limit},
        max_runtime=120,
    ))


def _harmonization_jobs_by_pipeline_key(jobs: List[dict]) -> Dict[str, List[dict]]:
    jobs_by_pipeline_key: Dict[str, List[dict]] = {}
    for job in jobs:
        pipeline_key = job.get("pipeline_key")
        if not pipeline_key:
            continue
        jobs_by_pipeline_key.setdefault(pipeline_key, []).append(job)
    return jobs_by_pipeline_key


def _list_harmonization_stage_overview_stats(limit: int = 100, stage_keys: Optional[List[str]] = None) -> List[dict]:
    db = get_db("metabolite_harmonization")
    if not db.has_collection(_HARMONIZATION_STAGE_COLLECTION):
        return []
    unique_stage_keys = list(dict.fromkeys(stage_keys or []))
    if unique_stage_keys:
        stage_filter = "FILTER s._key IN @stage_keys"
        bind_vars = {"limit": max(limit, len(unique_stage_keys)), "stage_keys": unique_stage_keys}
    else:
        stage_filter = "FILTER s.status == \"complete\""
        bind_vars = {"limit": limit, "stage_keys": []}
    stages = list(db.aql.execute(
        f"""
        FOR s IN {_HARMONIZATION_STAGE_COLLECTION}
          {stage_filter}
          SORT s.created_at DESC
          LIMIT @limit
          RETURN KEEP(s, "_key", "name", "created_at", "stage_index", "rule_ids", "summary")
        """,
        bind_vars=bind_vars,
        max_runtime=120,
    ))
    if not stages:
        return []
    stage_keys = [stage["_key"] for stage in stages]
    distribution_by_stage = {}
    if db.has_collection(_HARMONIZED_METABOLITE_COLLECTION):
        distribution_rows = list(db.aql.execute(
            f"""
            FOR c IN {_HARMONIZED_METABOLITE_COLLECTION}
              FILTER c.stage_key IN @stage_keys
              COLLECT stage_key = c.stage_key
              AGGREGATE
                non_singleton_groups = COUNT(),
                non_singleton_members = SUM(c.size),
                max_size = MAX(c.size),
                average_size = AVG(c.size),
                size_2 = SUM(c.size == 2 ? 1 : 0),
                size_3_5 = SUM(c.size >= 3 AND c.size <= 5 ? 1 : 0),
                size_6_10 = SUM(c.size >= 6 AND c.size <= 10 ? 1 : 0),
                size_11_50 = SUM(c.size >= 11 AND c.size <= 50 ? 1 : 0),
                size_51_100 = SUM(c.size >= 51 AND c.size <= 100 ? 1 : 0),
                size_101_plus = SUM(c.size >= 101 ? 1 : 0)
              RETURN {{
                stage_key: stage_key,
                non_singleton_groups: non_singleton_groups,
                non_singleton_members: non_singleton_members,
                max_size: max_size,
                average_size: average_size,
                bins: {{
                  size_2: size_2,
                  size_3_5: size_3_5,
                  size_6_10: size_6_10,
                  size_11_50: size_11_50,
                  size_51_100: size_51_100,
                  size_101_plus: size_101_plus
                }}
              }}
            """,
            bind_vars={"stage_keys": stage_keys},
            max_runtime=120,
        ))
        distribution_by_stage = {
            row["stage_key"]: row
            for row in distribution_rows
        }
    for stage in stages:
        summary = stage.get("summary") or {}
        distribution = distribution_by_stage.get(stage["_key"], {})
        stage["overview_stats"] = {
            "active_identifier_count": summary.get("active_identifier_count", 0),
            "clique_count": summary.get("clique_count", 0),
            "non_singleton_clique_count": summary.get(
                "non_singleton_clique_count",
                distribution.get("non_singleton_groups", 0),
            ),
            "singleton_identifier_count": summary.get("singleton_identifier_count", 0),
            "active_edge_count": summary.get("active_edge_count", 0),
            "ignored_edge_count": summary.get("ignored_edge_count", 0),
            "max_size": distribution.get("max_size") or (
                (summary.get("largest_clique_sizes") or [0])[0]
            ),
            "average_non_singleton_size": distribution.get("average_size") or 0,
            "largest_clique_sizes": summary.get("largest_clique_sizes") or [],
            "bins": distribution.get("bins") or {
                "size_2": 0,
                "size_3_5": 0,
                "size_6_10": 0,
                "size_11_50": 0,
                "size_51_100": 0,
                "size_101_plus": 0,
            },
        }
    return stages


def _stage_sort_key(stage: dict) -> tuple:
    stage_index = stage.get("stage_index")
    if stage_index is None:
        stage_index = len(stage.get("rule_ids") or [])
    return (
        stage_index,
        len(stage.get("rule_ids") or []),
        stage.get("_key") or "",
    )


def _list_harmonization_pipeline_stage_overview_stats(pipelines: List[dict]) -> List[dict]:
    if not pipelines:
        return []
    stage_keys = []
    for pipeline in pipelines:
        latest_run = (pipeline.get("runs") or [None])[0]
        for stage in (latest_run or {}).get("stages") or []:
            stage_key = stage.get("_key")
            if stage_key and stage_key not in stage_keys:
                stage_keys.append(stage_key)
    if not stage_keys:
        return []
    stats_by_key = {
        stage["_key"]: stage
        for stage in _list_harmonization_stage_overview_stats(limit=max(len(stage_keys), 100), stage_keys=stage_keys)
    }
    pipeline_stats = []
    for pipeline in pipelines:
        latest_run = (pipeline.get("runs") or [None])[0]
        stages = [
            stats_by_key[stage["_key"]]
            for stage in (latest_run or {}).get("stages") or []
            if stage.get("_key") in stats_by_key
        ]
        for stage in stages:
            stage["display_label"] = _stage_display_label(stage, pipeline.get("rule_parameters") or {})
        stages.sort(key=_stage_sort_key)
        if not stages:
            continue
        pipeline_stats.append({
            "pipeline_key": pipeline.get("_key"),
            "pipeline_name": pipeline.get("name"),
            "run": latest_run,
            "stages": stages,
        })
    return pipeline_stats


def _get_harmonization_pipeline(db, pipeline_key: str) -> dict:
    if not db.has_collection(_HARMONIZATION_PIPELINE_COLLECTION):
        raise ValueError("Harmonization pipeline collection does not exist.")
    pipeline = db.collection(_HARMONIZATION_PIPELINE_COLLECTION).get(pipeline_key)
    if not pipeline:
        raise ValueError(f"Harmonization pipeline {pipeline_key} does not exist.")
    return pipeline


def _rename_harmonization_pipeline(pipeline_key: str, pipeline_name: str) -> dict:
    db = get_db("metabolite_harmonization")
    _ensure_harmonization_pipeline_collections(db)
    pipeline = _get_harmonization_pipeline(db, pipeline_key)
    display_name = pipeline_name.strip()
    if not display_name:
        raise ValueError("Pipeline name cannot be empty.")
    updated_at = datetime.now(timezone.utc).isoformat()
    updated = {
        "_key": pipeline_key,
        "name": display_name,
        "updated_at": updated_at,
    }
    db.collection(_HARMONIZATION_PIPELINE_COLLECTION).update(updated)
    if db.has_collection(_HARMONIZATION_PIPELINE_RUN_COLLECTION):
        db.aql.execute(
            f"""
            FOR r IN {_HARMONIZATION_PIPELINE_RUN_COLLECTION}
              FILTER r.pipeline_key == @pipeline_key
              UPDATE r WITH {{pipeline_name: @pipeline_name}} IN {_HARMONIZATION_PIPELINE_RUN_COLLECTION}
            """,
            bind_vars={"pipeline_key": pipeline_key, "pipeline_name": display_name},
            max_runtime=120,
        )
    return {**pipeline, **updated}


def _upsert_harmonization_pipeline(
    pipeline_key: str,
    pipeline_name: str,
    rule_ids: List[str],
    rule_parameters: dict,
) -> dict:
    db = get_db("metabolite_harmonization")
    _ensure_harmonization_pipeline_collections(db)
    normalized_rule_ids = _normalize_metabolite_rule_ids(rule_ids)
    normalized_rule_parameters = _normalize_metabolite_rule_parameters(normalized_rule_ids, rule_parameters)
    now = datetime.now(timezone.utc).isoformat()
    display_name = pipeline_name.strip() or "Untitled harmonization pipeline"
    if pipeline_key:
        existing = db.collection(_HARMONIZATION_PIPELINE_COLLECTION).get(pipeline_key)
        if not existing:
            raise ValueError(f"Harmonization pipeline {pipeline_key} does not exist.")
        created_at = existing.get("created_at") or now
    else:
        pipeline_key = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
        created_at = now
    steps = []
    for index, rule_id in enumerate(normalized_rule_ids, start=1):
        steps.append({
            "index": index,
            "rule_id": rule_id,
            "parameters": normalized_rule_parameters.get(rule_id, {}),
            "rule": _metabolite_rule_metadata([rule_id], normalized_rule_parameters)[0],
        })
    pipeline_doc = {
        "_key": pipeline_key,
        "id": f"HarmonizationPipeline:{pipeline_key}",
        "name": display_name,
        "created_at": created_at,
        "updated_at": now,
        "engine_version": _HARMONIZATION_ENGINE_VERSION,
        "rule_ids": normalized_rule_ids,
        "rule_parameters": normalized_rule_parameters,
        "rules": _metabolite_rule_metadata(normalized_rule_ids, normalized_rule_parameters),
        "steps": steps,
        "status": "ready",
    }
    db.collection(_HARMONIZATION_PIPELINE_COLLECTION).insert(pipeline_doc, overwrite=True)
    return pipeline_doc


def _run_harmonization_pipeline(pipeline_key: str) -> dict:
    db = get_db("metabolite_harmonization")
    _ensure_harmonization_pipeline_collections(db)
    pipeline = _get_harmonization_pipeline(db, pipeline_key)
    started = time.time()
    created_at = datetime.now(timezone.utc).isoformat()
    run_key = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    run_doc = {
        "_key": run_key,
        "id": f"HarmonizationPipelineRun:{run_key}",
        "pipeline_key": pipeline_key,
        "pipeline_id": pipeline["id"],
        "pipeline_name": pipeline.get("name"),
        "created_at": created_at,
        "started_at": created_at,
        "completed_at": None,
        "status": "running",
        "stage_keys": [],
        "stages": [],
        "error": None,
    }
    run_collection = db.collection(_HARMONIZATION_PIPELINE_RUN_COLLECTION)
    run_collection.insert(run_doc, overwrite=True)
    try:
        graph_fingerprint = _harmonization_graph_fingerprint(db)
        stage_docs = []
        normalized_rule_ids = pipeline.get("rule_ids") or []
        normalized_rule_parameters = pipeline.get("rule_parameters") or {}
        cumulative_rule_ids: List[str] = []
        cumulative_rule_parameters = _cumulative_rule_parameters(cumulative_rule_ids, normalized_rule_parameters)
        baseline_stage = _ensure_harmonization_stage(
            db,
            cumulative_rule_ids,
            cumulative_rule_parameters,
            graph_fingerprint,
            f"{pipeline.get('name') or 'Pipeline'}: baseline",
            0,
        )
        stage_docs.append(baseline_stage)
        run_collection.update({
            "_key": run_key,
            "stage_keys": [stage["_key"] for stage in stage_docs],
            "stages": [
                {
                    "_key": stage["_key"],
                    "name": stage.get("name"),
                    "stage_index": stage.get("stage_index"),
                    "rule_ids": stage.get("rule_ids") or [],
                    "summary": stage.get("summary") or {},
                }
                for stage in stage_docs
            ],
        })
        for stage_index, rule_id in enumerate(normalized_rule_ids, start=1):
            cumulative_rule_ids = normalized_rule_ids[:stage_index]
            cumulative_rule_parameters = _cumulative_rule_parameters(cumulative_rule_ids, normalized_rule_parameters)
            rule_metadata = _metabolite_rule_metadata([rule_id], normalized_rule_parameters)
            rule_label = rule_metadata[0]["label"] if rule_metadata else rule_id
            stage_docs.append(_ensure_harmonization_stage(
                db,
                cumulative_rule_ids,
                cumulative_rule_parameters,
                graph_fingerprint,
                f"{pipeline.get('name') or 'Pipeline'}: {stage_index}. {rule_label}",
                stage_index,
            ))
            run_collection.update({
                "_key": run_key,
                "stage_keys": [stage["_key"] for stage in stage_docs],
                "stages": [
                    {
                        "_key": stage["_key"],
                        "name": stage.get("name"),
                        "stage_index": stage.get("stage_index"),
                        "rule_ids": stage.get("rule_ids") or [],
                        "summary": stage.get("summary") or {},
                    }
                    for stage in stage_docs
                ],
            })
        completed_at = datetime.now(timezone.utc).isoformat()
        elapsed_seconds = round(time.time() - started, 1)
        completed_update = {
            "_key": run_key,
            "status": "complete",
            "completed_at": completed_at,
            "elapsed_seconds": elapsed_seconds,
            "stage_keys": [stage["_key"] for stage in stage_docs],
            "stages": [
                {
                    "_key": stage["_key"],
                    "name": stage.get("name"),
                    "stage_index": stage.get("stage_index"),
                    "rule_ids": stage.get("rule_ids") or [],
                    "summary": stage.get("summary") or {},
                }
                for stage in stage_docs
            ],
        }
        run_collection.update(completed_update)
        db.collection(_HARMONIZATION_PIPELINE_COLLECTION).update({
            "_key": pipeline_key,
            "latest_run_key": run_key,
            "latest_stage_key": stage_docs[-1]["_key"] if stage_docs else None,
            "updated_at": completed_at,
            "status": "complete",
        })
        return {**run_doc, **completed_update}
    except Exception as exc:
        failed_at = datetime.now(timezone.utc).isoformat()
        run_collection.update({
            "_key": run_key,
            "status": "failed",
            "completed_at": failed_at,
            "error": str(exc),
        })
        db.collection(_HARMONIZATION_PIPELINE_COLLECTION).update({
            "_key": pipeline_key,
            "updated_at": failed_at,
            "status": "failed",
        })
        raise


def _load_harmonization_pipeline_workbench() -> dict:
    jobs = _list_metabolite_snapshot_jobs()
    active_pipeline_keys = _active_harmonization_pipeline_job_keys(jobs)
    deleting_pipeline_keys = _active_harmonization_delete_pipeline_keys(jobs)
    active_jobs = [
        job for job in jobs
        if job.get("status") in {"queued", "running"}
    ]
    try:
        db = get_db("metabolite_harmonization")
        _ensure_harmonization_pipeline_collections(db)
        pipelines = _list_harmonization_pipelines()
        return {
            "available_rules": _METABOLITE_HARMONIZATION_RULES,
            "pipelines": pipelines,
            "pipeline_stage_stats": _list_harmonization_pipeline_stage_overview_stats(pipelines),
            "jobs": jobs,
            "jobs_by_pipeline_key": _harmonization_jobs_by_pipeline_key(jobs),
            "active_job_count": len(active_jobs),
            "active_pipeline_keys": sorted(active_pipeline_keys),
            "deleting_pipeline_keys": sorted(deleting_pipeline_keys),
            "error": None,
        }
    except Exception as exc:
        return {
            "available_rules": _METABOLITE_HARMONIZATION_RULES,
            "pipelines": [],
            "pipeline_stage_stats": [],
            "jobs": jobs,
            "jobs_by_pipeline_key": _harmonization_jobs_by_pipeline_key(jobs),
            "active_job_count": len(active_jobs),
            "active_pipeline_keys": sorted(active_pipeline_keys),
            "deleting_pipeline_keys": sorted(deleting_pipeline_keys),
            "error": str(exc),
        }


def _get_harmonization_stage(stage_key: str) -> dict:
    db = get_db("metabolite_harmonization")
    if not db.has_collection(_HARMONIZATION_STAGE_COLLECTION):
        raise ValueError("Harmonization stage collection does not exist.")
    stage = db.collection(_HARMONIZATION_STAGE_COLLECTION).get(stage_key)
    if not stage:
        raise ValueError(f"Harmonization stage {stage_key} does not exist.")
    return stage


def _load_harmonization_stage_stats(stage_key: str) -> dict:
    db = get_db("metabolite_harmonization")
    stage = _get_harmonization_stage(stage_key)
    counts = {
        "harmonized_metabolite_count": 0,
        "member_edge_count": 0,
        "evidence_edge_count": 0,
        "active_identifier_count_from_chunks": 0,
    }
    if db.has_collection(_HARMONIZED_METABOLITE_COLLECTION):
        counts["harmonized_metabolite_count"] = list(db.aql.execute(
            f"""
            RETURN LENGTH(
              FOR d IN {_HARMONIZED_METABOLITE_COLLECTION}
                FILTER d.stage_key == @stage_key
                RETURN 1
            )
            """,
            bind_vars={"stage_key": stage_key},
            max_runtime=120,
        ))[0]
    if db.has_collection(_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION):
        counts["member_edge_count"] = list(db.aql.execute(
            f"""
            RETURN LENGTH(
              FOR d IN {_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION}
                FILTER d.stage_key == @stage_key
                RETURN 1
            )
            """,
            bind_vars={"stage_key": stage_key},
            max_runtime=120,
        ))[0]
    if db.has_collection(_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION):
        counts["evidence_edge_count"] = list(db.aql.execute(
            f"""
            RETURN LENGTH(
              FOR d IN {_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION}
                FILTER d.stage_key == @stage_key
                RETURN 1
            )
            """,
            bind_vars={"stage_key": stage_key},
            max_runtime=120,
        ))[0]
    if db.has_collection(_HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION):
        counts["active_identifier_count_from_chunks"] = list(db.aql.execute(
            f"""
            RETURN SUM(
              FOR d IN {_HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION}
                FILTER d.stage_key == @stage_key
                RETURN LENGTH(d.identifier_ids || [])
            )
            """,
            bind_vars={"stage_key": stage_key},
            max_runtime=120,
        ))[0] or 0
    largest_groups = []
    if db.has_collection(_HARMONIZED_METABOLITE_COLLECTION):
        largest_groups = list(db.aql.execute(
            f"""
            FOR d IN {_HARMONIZED_METABOLITE_COLLECTION}
              FILTER d.stage_key == @stage_key
              SORT d.size DESC, d._key ASC
              LIMIT 20
              RETURN KEEP(d, "_key", "id", "name", "size", "rank_by_size", "representative_id", "sample_member_ids")
            """,
            bind_vars={"stage_key": stage_key},
            max_runtime=120,
        ))
    source_counts = []
    if db.has_collection(_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION):
        source_counts = list(db.aql.execute(
            f"""
            FOR e IN {_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION}
              FILTER e.stage_key == @stage_key
              FOR source IN e.sources || []
                COLLECT source_name = source WITH COUNT INTO edge_count
                SORT edge_count DESC, source_name ASC
                LIMIT 20
                RETURN {{source: source_name, edge_count: edge_count}}
            """,
            bind_vars={"stage_key": stage_key},
            max_runtime=120,
        ))
    return {
        "stage": stage,
        "summary": stage.get("summary") or {},
        "counts": counts,
        "largest_groups": largest_groups,
        "source_counts": source_counts,
    }


def _load_stage_harmonized_member_sets(stage_key: str) -> List[dict]:
    db = get_db("metabolite_harmonization")
    if (
        not db.has_collection(_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION)
        or not db.has_collection(_HARMONIZED_METABOLITE_COLLECTION)
    ):
        return []
    return list(db.aql.execute(
        f"""
        FOR c IN {_HARMONIZED_METABOLITE_COLLECTION}
          FILTER c.stage_key == @stage_key
          LET member_ids = (
            FOR e IN {_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION}
              FILTER e._from == c._id
              SORT e.member_id
              RETURN e.member_id
          )
          FILTER LENGTH(member_ids) > 1
          SORT c.rank_by_size ASC
          RETURN {{
            clique_key: c._key,
            clique_id: c.id,
            size: LENGTH(member_ids),
            rank_by_size: c.rank_by_size,
            representative_id: c.representative_id,
            member_ids: member_ids,
            signature: CONCAT_SEPARATOR("\\n", member_ids)
          }}
        """,
        bind_vars={"stage_key": stage_key},
        batch_size=1000,
        max_runtime=600,
    ))


def _load_metabolite_identifier_display_map(member_ids: List[str]) -> Dict[str, dict]:
    if not member_ids:
        return {}
    db = get_db("metabolite_harmonization")
    rows = list(db.aql.execute(
        """
        FOR node IN MetaboliteIdentifier
          FILTER node.id IN @member_ids
          LET chemical_entity = STARTS_WITH(node.id, "CHEBI:") ? DOCUMENT("ChemicalEntity", node.id) : null
          LET node_names = UNIQUE(
            FOR name IN node.names || []
              FILTER name.value != null AND name.value != ""
              RETURN name.value
          )
          LET chemical_names = UNIQUE(APPEND(
            chemical_entity != null && chemical_entity.name != null && chemical_entity.name != "" ? [chemical_entity.name] : [],
            (
              FOR synonym IN chemical_entity != null ? (chemical_entity.synonyms || []) : []
                FILTER synonym.value != null AND synonym.value != ""
                RETURN synonym.value
            )
          ))
          RETURN {
            id: node.id,
            label: LENGTH(node_names) > 0 ? node_names[0] : (
              LENGTH(chemical_names) > 0 ? chemical_names[0] : node.id
            ),
            names: SLICE(UNIQUE(APPEND(node_names, chemical_names)), 0, 6),
            prefix: node.prefix
          }
        """,
        bind_vars={"member_ids": member_ids},
        max_runtime=120,
    ))
    return {row["id"]: row for row in rows}


def _sample_member_ids(member_ids: set, limit: int = 20) -> List[str]:
    return sorted(member_ids)[:limit]


def _format_compare_member_samples(member_ids: set, display_by_id: Dict[str, dict], limit: int = 20) -> List[dict]:
    samples = []
    for member_id in _sample_member_ids(member_ids, limit):
        display = display_by_id.get(member_id) or {}
        samples.append({
            "id": member_id,
            "label": display.get("label") or member_id,
            "names": display.get("names") or [],
            "prefix": display.get("prefix") or _identifier_prefix(member_id),
        })
    return samples


def _build_snapshot_comparison_review_fields(
    component: dict,
    left_key: str,
    right_key: str,
) -> None:
    review_ids = []
    for bucket in (
        component.get("_added_members", set()),
        component.get("_removed_members", set()),
        component.get("_retained_members", set()),
    ):
        for member_id in _sample_member_ids(bucket, 12):
            if member_id not in review_ids:
                review_ids.append(member_id)
            if len(review_ids) >= 12:
                break
        if len(review_ids) >= 12:
            break
    component["review_ids"] = review_ids
    query_params = {
        "id": " ".join(review_ids),
        "stages": ",".join([left_key, right_key]),
    }
    component["review_query_string"] = urlencode(query_params)


def _snapshot_compare_clique_summary(clique: dict, sample_members: Optional[set] = None) -> dict:
    return {
        "clique_key": clique.get("clique_key"),
        "clique_id": clique.get("clique_id"),
        "size": clique.get("size"),
        "rank_by_size": clique.get("rank_by_size"),
        "representative_id": clique.get("representative_id"),
        "member_ids": _sample_member_ids(sample_members if sample_members is not None else clique.get("member_set", set()), 12),
    }


def _snapshot_compare_nontrivial_transitions(
    source_items: List[dict],
    target_items: List[dict],
    min_size: int,
    target_label: str,
) -> List[dict]:
    transitions = []
    for source_item in source_items:
        fragments = []
        source_members = source_item["member_set"]
        for target_item in target_items:
            overlap_members = source_members & target_item["member_set"]
            if len(overlap_members) < min_size:
                continue
            fragments.append({
                "target_clique": _snapshot_compare_clique_summary(target_item, overlap_members),
                "overlap_count": len(overlap_members),
            })
        if len(fragments) < 2:
            continue
        fragments.sort(key=lambda item: (-item["overlap_count"], item["target_clique"]["clique_key"]))
        transitions.append({
            "source_clique": _snapshot_compare_clique_summary(source_item),
            "target_label": target_label,
            "fragment_count": len(fragments),
            "member_count": sum(fragment["overlap_count"] for fragment in fragments),
            "fragments": fragments,
        })
    transitions.sort(key=lambda item: (-item["fragment_count"], -item["member_count"], item["source_clique"]["clique_key"]))
    return transitions


def _load_metabolite_snapshot_comparison(
    left_key: str,
    right_key: str,
    limit: int = 100,
) -> dict:
    if not left_key or not right_key:
        raise ValueError("Choose two stages to compare.")
    if left_key == right_key:
        raise ValueError("Choose two different stages.")

    left_snapshot = _get_harmonization_stage(left_key)
    right_snapshot = _get_harmonization_stage(right_key)
    left_cliques = _load_stage_harmonized_member_sets(left_key)
    right_cliques = _load_stage_harmonized_member_sets(right_key)
    right_signatures = {clique["signature"] for clique in right_cliques}
    left_signatures = {clique["signature"] for clique in left_cliques}
    changed_left = [
        {**clique, "side": "left", "member_set": set(clique["member_ids"])}
        for clique in left_cliques
        if clique["signature"] not in right_signatures
    ]
    changed_right = [
        {**clique, "side": "right", "member_set": set(clique["member_ids"])}
        for clique in right_cliques
        if clique["signature"] not in left_signatures
    ]
    node_parent = {}
    node_payload = {}
    members_by_change_node = {}

    def node_id(side: str, clique_key: str) -> str:
        return f"{side}:{clique_key}"

    def add_node(identifier: str, payload: dict) -> None:
        node_parent.setdefault(identifier, identifier)
        node_payload[identifier] = payload

    def find(identifier: str) -> str:
        while node_parent[identifier] != identifier:
            node_parent[identifier] = node_parent[node_parent[identifier]]
            identifier = node_parent[identifier]
        return identifier

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            node_parent[right_root] = left_root

    member_to_change_nodes: Dict[str, List[str]] = {}
    for clique in [*changed_left, *changed_right]:
        identifier = node_id(clique["side"], clique["clique_key"])
        add_node(identifier, clique)
        members_by_change_node[identifier] = clique["member_set"]
        for member_id in clique["member_set"]:
            member_to_change_nodes.setdefault(member_id, []).append(identifier)

    for change_nodes in member_to_change_nodes.values():
        if len(change_nodes) < 2:
            continue
        first = change_nodes[0]
        for other in change_nodes[1:]:
            union(first, other)

    nodes_by_component: Dict[str, List[str]] = {}
    for identifier in node_parent:
        nodes_by_component.setdefault(find(identifier), []).append(identifier)

    components = []
    nontrivial_split_candidates = []
    recombination_candidates = []
    nontrivial_split_min_size = 2
    for component_nodes in nodes_by_component.values():
        left_items = [node_payload[node] for node in component_nodes if node_payload[node]["side"] == "left"]
        right_items = [node_payload[node] for node in component_nodes if node_payload[node]["side"] == "right"]
        left_members = set().union(*(item["member_set"] for item in left_items)) if left_items else set()
        right_members = set().union(*(item["member_set"] for item in right_items)) if right_items else set()
        retained_members = left_members & right_members
        added_members = right_members - left_members
        removed_members = left_members - right_members
        if not left_items and not right_items:
            continue
        change_score = len(added_members) + len(removed_members) + max(len(left_items), len(right_items))
        change_type = "reconfigured"
        if not left_items:
            change_type = "new clique"
        elif not right_items:
            change_type = "removed clique"
        elif len(left_items) == 1 and len(right_items) > 1:
            change_type = "split"
        elif len(left_items) > 1 and len(right_items) == 1:
            change_type = "merged"
        elif len(added_members) or len(removed_members):
            change_type = "membership changed"
        split_transitions = _snapshot_compare_nontrivial_transitions(
            left_items,
            right_items,
            nontrivial_split_min_size,
            "To",
        )
        merge_transitions = _snapshot_compare_nontrivial_transitions(
            right_items,
            left_items,
            nontrivial_split_min_size,
            "From",
        )
        split_fragment_count = sum(transition["fragment_count"] for transition in split_transitions)
        split_member_count = sum(transition["member_count"] for transition in split_transitions)
        merge_fragment_count = sum(transition["fragment_count"] for transition in merge_transitions)
        merge_member_count = sum(transition["member_count"] for transition in merge_transitions)
        is_recombination = bool(split_transitions and merge_transitions)
        components.append({
            "change_type": change_type,
            "change_score": change_score,
            "nontrivial_split_count": len(split_transitions),
            "nontrivial_split_fragment_count": split_fragment_count,
            "nontrivial_split_member_count": split_member_count,
            "nontrivial_merge_count": len(merge_transitions),
            "nontrivial_merge_fragment_count": merge_fragment_count,
            "nontrivial_merge_member_count": merge_member_count,
            "is_recombination": is_recombination,
            "nontrivial_split_min_size": nontrivial_split_min_size,
            "split_transitions": split_transitions,
            "merge_transitions": merge_transitions,
            "left_clique_count": len(left_items),
            "right_clique_count": len(right_items),
            "left_member_count": len(left_members),
            "right_member_count": len(right_members),
            "retained_count": len(retained_members),
            "added_count": len(added_members),
            "removed_count": len(removed_members),
            "left_cliques": sorted(left_items, key=lambda item: (item.get("rank_by_size") or 10**12, item["clique_key"])),
            "right_cliques": sorted(right_items, key=lambda item: (item.get("rank_by_size") or 10**12, item["clique_key"])),
            "_retained_members": retained_members,
            "_added_members": added_members,
            "_removed_members": removed_members,
        })
        if split_transitions:
            nontrivial_split_candidates.append(components[-1])
        if is_recombination:
            recombination_candidates.append(components[-1])

    components.sort(
        key=lambda item: (
            -item["change_score"],
            -max(item["left_member_count"], item["right_member_count"]),
            item["change_type"],
        )
    )
    nontrivial_split_candidates.sort(
        key=lambda item: (
            -item["nontrivial_split_fragment_count"],
            -item["nontrivial_split_member_count"],
            -item["left_member_count"],
            item["left_cliques"][0]["clique_key"] if item["left_cliques"] else "",
        )
    )
    recombination_candidates.sort(
        key=lambda item: (
            -item["nontrivial_split_fragment_count"] - item["nontrivial_merge_fragment_count"],
            -item["nontrivial_split_member_count"] - item["nontrivial_merge_member_count"],
            -max(item["left_member_count"], item["right_member_count"]),
            item["left_cliques"][0]["clique_key"] if item["left_cliques"] else "",
        )
    )
    limited_components = components[:limit]
    limited_nontrivial_splits = nontrivial_split_candidates[:limit]
    limited_recombinations = recombination_candidates[:limit]
    display_components = []
    seen_component_ids = set()
    for component in [*limited_recombinations, *limited_nontrivial_splits, *limited_components]:
        component_id = id(component)
        if component_id in seen_component_ids:
            continue
        seen_component_ids.add(component_id)
        display_components.append(component)
    sample_ids = sorted({
        member_id
        for component in display_components
        for bucket in ("_added_members", "_removed_members", "_retained_members")
        for member_id in _sample_member_ids(component[bucket], 20)
    })
    display_by_id = _load_metabolite_identifier_display_map(sample_ids)
    for component in display_components:
        _build_snapshot_comparison_review_fields(component, left_key, right_key)
        added_members = component.pop("_added_members")
        removed_members = component.pop("_removed_members")
        retained_members = component.pop("_retained_members")
        component["added_samples"] = _format_compare_member_samples(added_members, display_by_id)
        component["removed_samples"] = _format_compare_member_samples(removed_members, display_by_id)
        component["retained_samples"] = _format_compare_member_samples(retained_members, display_by_id, 12)
        for clique in [*component["left_cliques"], *component["right_cliques"]]:
            clique["member_ids"] = clique["member_ids"][:12]
            clique.pop("member_set", None)
            clique.pop("signature", None)

    return {
        "left_snapshot": left_snapshot,
        "right_snapshot": right_snapshot,
        "left_materialized_clique_count": len(left_cliques),
        "right_materialized_clique_count": len(right_cliques),
        "unchanged_clique_count": len(left_cliques) - len(changed_left),
        "changed_left_clique_count": len(changed_left),
        "changed_right_clique_count": len(changed_right),
        "changed_component_count": len(components),
        "nontrivial_split_count": len(nontrivial_split_candidates),
        "recombination_count": len(recombination_candidates),
        "nontrivial_split_min_size": nontrivial_split_min_size,
        "display_limit": limit,
        "components": limited_components,
        "nontrivial_splits": limited_nontrivial_splits,
        "recombinations": limited_recombinations,
        "snapshots": _list_harmonization_stages(),
        "comparison_kind": "stage",
    }


def _list_metabolite_snapshot_jobs() -> List[dict]:
    with _metabolite_snapshot_jobs_lock:
        jobs = [dict(job) for job in _metabolite_snapshot_jobs.values()]
    return sorted(jobs, key=lambda job: job.get("created_at") or "", reverse=True)[:10]


def _active_harmonization_pipeline_job_keys(jobs: Optional[List[dict]] = None) -> set:
    job_rows = jobs if jobs is not None else _list_metabolite_snapshot_jobs()
    return {
        job.get("pipeline_key")
        for job in job_rows
        if job.get("pipeline_key") and job.get("status") in {"queued", "running"}
        and job.get("action") == "run_pipeline"
    }


def _active_harmonization_delete_pipeline_keys(jobs: Optional[List[dict]] = None) -> set:
    job_rows = jobs if jobs is not None else _list_metabolite_snapshot_jobs()
    return {
        job.get("pipeline_key")
        for job in job_rows
        if job.get("pipeline_key")
        and job.get("action") == "delete_pipeline"
        and job.get("status") in {"queued", "running"}
    }


def _update_metabolite_snapshot_job(job_id: str, **updates) -> None:
    with _metabolite_snapshot_jobs_lock:
        job = _metabolite_snapshot_jobs.get(job_id)
        if job is None:
            return
        job.update(updates)


def _run_metabolite_snapshot_job(job_id: str, action: str, payload: dict) -> None:
    _update_metabolite_snapshot_job(job_id, status="running", started_at=datetime.now(timezone.utc).isoformat())
    try:
        if action == "run_pipeline":
            run = _run_harmonization_pipeline(payload["pipeline_key"])
            _update_metabolite_snapshot_job(
                job_id,
                status="complete",
                completed_at=datetime.now(timezone.utc).isoformat(),
                pipeline_key=payload["pipeline_key"],
                run_key=run.get("_key"),
                stage_keys=run.get("stage_keys") or [],
                message=f"Completed pipeline {run.get('pipeline_name') or payload['pipeline_key']}",
            )
        elif action == "delete_pipeline":
            result = _delete_harmonization_pipeline(get_db("metabolite_harmonization"), payload["pipeline_key"])
            _update_metabolite_snapshot_job(
                job_id,
                status="complete",
                completed_at=datetime.now(timezone.utc).isoformat(),
                pipeline_key=payload["pipeline_key"],
                stage_keys=result.get("deleted_stage_keys") or [],
                message=(
                    f"Deleted pipeline {result.get('pipeline', {}).get('name') or payload['pipeline_key']} "
                    f"and {len(result.get('deleted_stage_keys') or [])} unreferenced stages"
                ),
            )
        else:
            raise ValueError(f"Unknown harmonization job action: {action}")
    except Exception as exc:
        _update_metabolite_snapshot_job(
            job_id,
            status="failed",
            completed_at=datetime.now(timezone.utc).isoformat(),
            error=str(exc),
        )


def _enqueue_metabolite_snapshot_job(action: str, label: str, payload: dict) -> str:
    pipeline_key = payload.get("pipeline_key")
    active_pipeline_keys = _active_harmonization_pipeline_job_keys()
    deleting_pipeline_keys = _active_harmonization_delete_pipeline_keys()
    if action == "run_pipeline" and pipeline_key in (active_pipeline_keys | deleting_pipeline_keys):
        return ""
    if action == "delete_pipeline" and pipeline_key in (active_pipeline_keys | deleting_pipeline_keys):
        return ""
    job_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    job = {
        "id": job_id,
        "action": action,
        "label": label,
        "snapshot_key": payload.get("snapshot_key"),
        "pipeline_key": payload.get("pipeline_key"),
        "status": "queued",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "completed_at": None,
        "message": None,
        "error": None,
    }
    with _metabolite_snapshot_jobs_lock:
        _metabolite_snapshot_jobs[job_id] = job
    thread = threading.Thread(
        target=_run_metabolite_snapshot_job,
        args=(job_id, action, payload),
        daemon=True,
    )
    thread.start()
    return job_id


def _load_metabolite_snapshot_memberships(identifier_ids: List[str]) -> List[dict]:
    if not identifier_ids:
        return []
    db = get_db("metabolite_harmonization")
    if (
        not db.has_collection(_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION)
        or not db.has_collection(_HARMONIZED_METABOLITE_COLLECTION)
        or not db.has_collection(_HARMONIZATION_STAGE_COLLECTION)
    ):
        return []
    vertex_ids = [f"MetaboliteIdentifier/{identifier_id}" for identifier_id in identifier_ids]
    return list(db.aql.execute(
        f"""
        FOR e IN {_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION}
          FILTER e._to IN @vertex_ids
          LET clique = DOCUMENT("{_HARMONIZED_METABOLITE_COLLECTION}", PARSE_IDENTIFIER(e._from).key)
          LET snapshot = DOCUMENT("{_HARMONIZATION_STAGE_COLLECTION}", e.stage_key)
          FILTER clique != null AND snapshot != null
          SORT snapshot.created_at DESC, clique.rank_by_size ASC, e.member_id
          RETURN {{
            member_id: e.member_id,
            snapshot_key: snapshot._key,
            snapshot_name: snapshot.name,
            snapshot_created_at: snapshot.created_at,
            rule_ids: snapshot.rule_ids,
            rules: snapshot.rules,
            clique_id: clique.id,
            clique_key: clique._key,
            clique_size: clique.size,
            clique_rank_by_size: clique.rank_by_size,
            representative_id: clique.representative_id,
            sample_member_ids: clique.sample_member_ids
          }}
        """,
        bind_vars={"vertex_ids": vertex_ids},
        max_runtime=120,
    ))


def _parse_metabolite_mass(value) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed


def _metabolite_mass_summary(values: List[float]) -> Optional[dict]:
    sorted_values = sorted(values)
    if not sorted_values:
        return None
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        median = sorted_values[midpoint]
    else:
        median = (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2
    return {
        "count": len(sorted_values),
        "min": sorted_values[0],
        "median": median,
        "max": sorted_values[-1],
    }


def _metabolite_member_mass_summary(member_ids: List[str], node_by_member_id: Dict[str, dict]) -> Optional[dict]:
    masses = []
    for member_id in member_ids:
        node = node_by_member_id.get(member_id) or {}
        masses.extend(node.get("masses") or [])
    return _metabolite_mass_summary(masses)


def _metabolite_mass_summary_label(summary: Optional[dict]) -> Optional[str]:
    if not summary:
        return None
    min_mass = summary.get("min")
    median_mass = summary.get("median")
    max_mass = summary.get("max")
    count = summary.get("count")
    if min_mass is None or median_mass is None or max_mass is None:
        return None
    if min_mass == max_mass:
        return f"MW {min_mass:.4g} (n={count})"
    return f"MW {min_mass:.4g}-{max_mass:.4g}; med {median_mass:.4g} (n={count})"


def _metabolite_compact_mass_label(summary: Optional[dict]) -> Optional[str]:
    if not summary:
        return None
    median_mass = summary.get("median")
    if median_mass is None:
        return None
    return f"MW {median_mass:.4g}"


def _metabolite_query_id_labels(query_ids: List[str], max_ids: int = 4) -> List[str]:
    if not query_ids:
        return []
    labels = [f"ID {query_id}" for query_id in query_ids[:max_ids]]
    if len(query_ids) > max_ids:
        labels.append(f"+{len(query_ids) - max_ids} more query IDs")
    return labels


def _parse_metabolite_snapshot_key_filter(value: str) -> List[str]:
    snapshot_keys = []
    seen = set()
    for token in re.split(r"[\s,;|]+", value or ""):
        snapshot_key = token.strip()
        if not snapshot_key or snapshot_key in seen:
            continue
        seen.add(snapshot_key)
        snapshot_keys.append(snapshot_key)
    return snapshot_keys


def _load_metabolite_snapshot_union(
    identifier_ids: List[str],
    snapshot_keys: Optional[List[str]] = None,
) -> dict:
    selected_snapshot_keys = snapshot_keys or []
    memberships = _load_metabolite_snapshot_memberships(identifier_ids)
    if selected_snapshot_keys:
        selected_snapshot_key_set = set(selected_snapshot_keys)
        memberships = [
            membership
            for membership in memberships
            if membership.get("snapshot_key") in selected_snapshot_key_set
        ]
    db = get_db("metabolite_harmonization")
    existing_query_ids = sorted(list(db.aql.execute(
        """
        FOR id IN @ids
          FILTER DOCUMENT("MetaboliteIdentifier", id) != null
          COLLECT existing_id = id
          SORT existing_id
          RETURN existing_id
        """,
        bind_vars={"ids": identifier_ids},
        max_runtime=120,
    )))
    if not memberships and not existing_query_ids:
        return {
            "memberships": [],
            "member_ids": [],
            "snapshot_graphs": [],
            "sankey": {"nodes": [], "links": []},
        }
    clique_keys = []
    for membership in memberships:
        clique_key = membership.get("clique_key")
        if clique_key and clique_key not in clique_keys:
            clique_keys.append(clique_key)
    clique_member_ids = []
    if clique_keys:
        clique_vertex_ids = [
            f"{_HARMONIZED_METABOLITE_COLLECTION}/{clique_key}"
            for clique_key in clique_keys
        ]
        clique_member_ids = list(db.aql.execute(
            f"""
            FOR e IN {_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION}
              FILTER e._from IN @clique_vertex_ids
              FILTER DOCUMENT("MetaboliteIdentifier", e.member_id) != null
              COLLECT member_id = e.member_id
              SORT member_id
              RETURN member_id
            """,
            bind_vars={"clique_vertex_ids": clique_vertex_ids},
            max_runtime=120,
        ))
    union_member_ids = sorted(set(clique_member_ids) | set(existing_query_ids))
    if not union_member_ids:
        return {
            "memberships": memberships,
            "member_ids": [],
            "snapshot_graphs": [],
            "sankey": {"nodes": [], "links": []},
        }
    rows = list(db.aql.execute(
        f"""
        FOR e IN {_HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION}
          FILTER e._to IN @member_vertex_ids
          FILTER LENGTH(@snapshot_keys) == 0 OR e.stage_key IN @snapshot_keys
          LET clique = DOCUMENT("{_HARMONIZED_METABOLITE_COLLECTION}", PARSE_IDENTIFIER(e._from).key)
          LET snapshot = DOCUMENT("{_HARMONIZATION_STAGE_COLLECTION}", e.stage_key)
          LET node = DOCUMENT("MetaboliteIdentifier", e.member_id)
          FILTER clique != null AND snapshot != null AND node != null
          LET names = node.names || []
          SORT snapshot.created_at, clique.rank_by_size, e.member_index
          RETURN {{
            member_id: e.member_id,
            member_label: LENGTH(names) > 0 ? names[0].value : e.member_id,
            member_prefix: node.prefix,
            name_count: LENGTH(node.names || []),
            synonym_count: LENGTH(node.synonyms || []),
            chem_prop_count: LENGTH(node.chem_props || []),
            snapshot_key: snapshot._key,
            snapshot_name: snapshot.name,
            snapshot_created_at: snapshot.created_at,
            rules: snapshot.rules,
            clique_id: clique.id,
            clique_key: clique._key,
            clique_size: clique.size,
            clique_rank_by_size: clique.rank_by_size
          }}
        """,
        bind_vars={
            "member_vertex_ids": [f"MetaboliteIdentifier/{member_id}" for member_id in union_member_ids],
            "snapshot_keys": selected_snapshot_keys,
        },
        max_runtime=120,
    ))
    node_rows = list(db.aql.execute(
        """
        FOR node IN MetaboliteIdentifier
          FILTER node.id IN @member_ids
          LET chemical_entity = STARTS_WITH(node.id, "CHEBI:") ? DOCUMENT("ChemicalEntity", node.id) : null
          LET names = node.names || []
          LET name_values = UNIQUE(
            FOR name IN names
              FILTER name.value != null AND name.value != ""
              RETURN name.value
          )
          LET chemical_entity_names = UNIQUE(APPEND(
            chemical_entity != null && chemical_entity.name != null && chemical_entity.name != "" ? [chemical_entity.name] : [],
            (
              FOR synonym IN chemical_entity != null ? (chemical_entity.synonyms || []) : []
                FILTER synonym.value != null AND synonym.value != ""
                RETURN synonym.value
            )
          ))
          LET synonym_values = UNIQUE(
            FOR synonym IN node.synonyms || []
              FILTER synonym.value != null AND synonym.value != ""
              RETURN synonym.value
          )
          LET display_names = UNIQUE(APPEND(
            LENGTH(name_values) > 0 ? name_values : chemical_entity_names,
            LENGTH(name_values) > 0 ? chemical_entity_names : synonym_values
          ))
          LET raw_masses = UNIQUE(FLATTEN(
            APPEND(
              (FOR prop IN node.chem_props || [] RETURN [prop.mw, prop.monoisotopic_mass]),
              chemical_entity == null ? [] : [[chemical_entity.mass, chemical_entity.monoisotopic_mass]]
            )
          ))
          LET chem_prop_summaries = (
            FOR prop IN node.chem_props || []
              RETURN {
                source: prop.source,
                source_id: prop.source_id,
                name: prop.common_name || prop.iupac_name,
                formula: prop.molecular_formula,
                mw: prop.mw,
                monoisotopic_mass: prop.monoisotopic_mass,
                inchi_key: prop.inchi_key,
                smiles: prop.iso_smiles || prop.isomeric_smiles || prop.canonical_smiles,
                inchi: prop.inchi
              }
          )
          RETURN {
            id: node.id,
            label: LENGTH(display_names) > 0 ? display_names[0] : node.id,
            names: SLICE(display_names, 0, 12),
            prefix: node.prefix,
            name_count: LENGTH(node.names || []),
            synonym_count: LENGTH(node.synonyms || []),
            chem_prop_count: LENGTH(node.chem_props || []),
            raw_masses: raw_masses,
            chem_props: SLICE(chem_prop_summaries, 0, 8),
            formulas: UNIQUE(
              FOR prop IN chem_prop_summaries
                FILTER prop.formula != null AND prop.formula != ""
                RETURN prop.formula
            ),
            smiles: UNIQUE(
              FOR prop IN chem_prop_summaries
                FILTER prop.smiles != null AND prop.smiles != ""
                RETURN prop.smiles
            ),
            inchi_keys: UNIQUE(
              FOR prop IN chem_prop_summaries
                FILTER prop.inchi_key != null AND prop.inchi_key != ""
                RETURN prop.inchi_key
            ),
            chemical_entity: chemical_entity == null ? null : {
              id: chemical_entity.id,
              name: chemical_entity.name,
              formula: chemical_entity.formula,
              mass: chemical_entity.mass,
              monoisotopic_mass: chemical_entity.monoisotopic_mass,
              inchi_key: chemical_entity.inchi_key,
              smiles: chemical_entity.smiles,
              inchi: chemical_entity.inchi
            }
          }
        """,
        bind_vars={"member_ids": union_member_ids},
        max_runtime=120,
    ))
    union_member_vertex_ids = [f"MetaboliteIdentifier/{member_id}" for member_id in union_member_ids]
    mapping_edge_rows = list(db.aql.execute(
        f"""
        FOR e IN {_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION}
          FILTER e._from IN @vertex_ids
          FILTER e._to IN @vertex_ids
          FILTER LENGTH(@snapshot_keys) == 0 OR e.stage_key IN @snapshot_keys
          RETURN {{
            id: e.id || e._key,
            key: e._key,
            stage_key: e.stage_key,
            start_id: e.start_id,
            end_id: e.end_id,
            sources: e.sources || [],
            details: e.details || []
          }}
        """,
        bind_vars={"vertex_ids": union_member_vertex_ids, "snapshot_keys": selected_snapshot_keys},
        max_runtime=120,
    )) if db.has_collection(_HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION) else []
    highlighted_id_set = set(identifier_ids)
    sections_by_snapshot: Dict[str, dict] = {}
    node_by_member_id: Dict[str, dict] = {
        row["id"]: row
        for row in node_rows
    }
    for node in node_by_member_id.values():
        node["masses"] = [
            mass
            for mass in (_parse_metabolite_mass(value) for value in node.get("raw_masses") or [])
            if mass is not None
        ]
        mass_summary = _metabolite_mass_summary(node["masses"])
        mass_label = _metabolite_mass_summary_label(mass_summary)
        compact_mass_label = _metabolite_compact_mass_label(mass_summary)
        node["mass_summary"] = mass_summary
        node["node_label"] = compact_mass_label or (node.get("label") or node.get("id"))
        node["node_detail_label"] = "\n".join([part for part in [node.get("label") or node.get("id"), mass_label] if part])
    for snapshot in _list_harmonization_stages():
        if selected_snapshot_keys and snapshot["_key"] not in selected_snapshot_keys:
            continue
        sections_by_snapshot.setdefault(snapshot["_key"], {
            "snapshot_key": snapshot["_key"],
            "snapshot_name": snapshot.get("name"),
            "snapshot_created_at": snapshot.get("created_at"),
            "rules": snapshot.get("rules") or [],
            "cliques_by_key": {},
        })
    for row in rows:
        node_by_member_id.setdefault(row["member_id"], {
            "id": row["member_id"],
            "label": row["member_label"],
            "node_label": row["member_label"],
            "names": [row["member_label"]] if row.get("member_label") else [],
            "prefix": row["member_prefix"],
            "name_count": row["name_count"],
            "synonym_count": row["synonym_count"],
            "chem_prop_count": row["chem_prop_count"],
        })
        snapshot_key = row["snapshot_key"]
        section = sections_by_snapshot.setdefault(snapshot_key, {
            "snapshot_key": snapshot_key,
            "snapshot_name": row["snapshot_name"],
            "snapshot_created_at": row["snapshot_created_at"],
            "rules": row.get("rules") or [],
            "cliques_by_key": {},
        })
        clique = section["cliques_by_key"].setdefault(row["clique_key"], {
            "snapshot_key": snapshot_key,
            "snapshot_name": row["snapshot_name"],
            "snapshot_created_at": row["snapshot_created_at"],
            "rules": row.get("rules") or [],
            "clique_id": row["clique_id"],
            "clique_key": row["clique_key"],
            "clique_size": row["clique_size"],
            "clique_rank_by_size": row["clique_rank_by_size"],
            "member_ids": [],
        })
        if row["member_id"] not in clique["member_ids"]:
            clique["member_ids"].append(row["member_id"])
    union_member_id_set = set(union_member_ids)
    for section in sections_by_snapshot.values():
        represented_member_ids = {
            member_id
            for clique in section["cliques_by_key"].values()
            for member_id in clique["member_ids"]
        }
        missing_member_ids = sorted(union_member_id_set - represented_member_ids)
        if missing_member_ids:
            singleton_bucket_key = f"{section['snapshot_key']}__singleton_bucket"
            section["cliques_by_key"][singleton_bucket_key] = {
                "snapshot_key": section["snapshot_key"],
                "snapshot_name": section["snapshot_name"],
                "snapshot_created_at": section["snapshot_created_at"],
                "rules": section["rules"],
                "clique_id": f"MetaboliteHarmonizationClique:{singleton_bucket_key}",
                "clique_key": singleton_bucket_key,
                "clique_size": len(missing_member_ids),
                "clique_rank_by_size": None,
                "member_ids": missing_member_ids,
                "is_singleton_bucket": True,
            }

    snapshot_sections = []
    for section in sections_by_snapshot.values():
        cliques = []
        for clique in sorted(
            section["cliques_by_key"].values(),
            key=lambda item: (
                item.get("is_singleton_bucket", False),
                item["clique_rank_by_size"] if item["clique_rank_by_size"] is not None else 10**12,
                item["clique_key"],
            ),
        ):
            member_name_samples = []
            member_id_samples = []
            member_name_by_id = {}
            member_mass_by_id = {}
            seen_member_names = set()
            for member_id in clique["member_ids"]:
                node = node_by_member_id.get(member_id, {"id": member_id, "label": member_id, "node_label": member_id, "names": []})
                member_mass_by_id[member_id] = node.get("masses") or []
                display_names = node.get("names") or []
                if display_names:
                    member_name_by_id[member_id] = display_names[0]
                for display_name in display_names:
                    if display_name and display_name not in seen_member_names:
                        seen_member_names.add(display_name)
                        if len(member_name_samples) < 25:
                            member_name_samples.append(display_name)
                if len(member_id_samples) < 25:
                    member_id_samples.append(member_id)
            selected_query_member_ids = [
                query_id
                for query_id in identifier_ids
                if query_id in clique["member_ids"]
            ]
            elements = []
            displayed_member_ids = clique["member_ids"][:1500]
            displayed_member_id_set = set(displayed_member_ids)
            for member_id in displayed_member_ids:
                node = node_by_member_id.get(member_id, {"id": member_id, "label": member_id, "node_label": member_id, "prefix": "unknown"})
                classes = f"metabolite-id-node prefix-{str(node.get('prefix') or 'unknown').lower().replace('.', '-')}"
                if member_id in highlighted_id_set:
                    classes += " selected-query"
                elements.append({
                    "data": {**node, "selected": member_id in highlighted_id_set},
                    "classes": classes,
                })
            for edge in mapping_edge_rows:
                if edge.get("stage_key") and edge.get("stage_key") != section["snapshot_key"]:
                    continue
                start_id = edge.get("start_id")
                end_id = edge.get("end_id")
                if start_id not in displayed_member_id_set or end_id not in displayed_member_id_set:
                    continue
                sources = edge.get("sources") or []
                elements.append({
                    "data": {
                        "id": f"metabolite-equivalence::{clique['clique_key']}::{edge.get('key') or edge.get('id')}",
                        "source": start_id,
                        "target": end_id,
                        "label": "equiv" if not sources else ", ".join(sources[:3]),
                        "kind": "MetaboliteIdentifierMappingEdge",
                        "start_id": start_id,
                        "end_id": end_id,
                        "sources": ", ".join(sources),
                        "source_count": len(sources),
                        "detail_count": len(edge.get("details") or []),
                        "snapshot": section["snapshot_name"],
                        "snapshot_key": section["snapshot_key"],
                    },
                    "classes": "metabolite-equivalence-edge",
                })
            cliques.append({
                **clique,
                "member_name_samples": member_name_samples,
                "member_id_samples": member_id_samples,
                "member_name_by_id": member_name_by_id,
                "member_mass_by_id": member_mass_by_id,
                "selected_query_member_ids": selected_query_member_ids,
                "mass_summary": _metabolite_member_mass_summary(clique["member_ids"], node_by_member_id),
                "display_member_count": min(len(clique["member_ids"]), 1500),
                "display_truncated": len(clique["member_ids"]) > 1500,
                "elements": elements,
            })
        snapshot_sections.append({
            **{key: value for key, value in section.items() if key != "cliques_by_key"},
            "cliques": cliques,
            "materialized_clique_count": sum(1 for clique in cliques if not clique.get("is_singleton_bucket")),
            "singleton_bucket_count": sum(1 for clique in cliques if clique.get("is_singleton_bucket")),
            "singleton_bucket_member_count": sum(
                len(clique.get("member_ids", []))
                for clique in cliques
                if clique.get("is_singleton_bucket")
            ),
        })

    sankey = _build_metabolite_snapshot_sankey(snapshot_sections)
    return {
        "memberships": memberships,
        "member_ids": union_member_ids,
        "snapshot_graphs": snapshot_sections,
        "sankey": sankey,
    }


def _build_metabolite_snapshot_sankey(snapshot_sections: List[dict]) -> dict:
    nodes = []
    links_by_pair: Dict[tuple[str, str], dict] = {}
    node_ids = set()
    ordered_sections = sorted(snapshot_sections, key=lambda section: section.get("snapshot_created_at") or "")
    for stage_index, section in enumerate(ordered_sections):
        for clique in section.get("cliques", []):
            node_id = f"{section['snapshot_key']}::{clique['clique_key']}"
            if node_id not in node_ids:
                node_ids.add(node_id)
                mass_label = _metabolite_mass_summary_label(clique.get("mass_summary"))
                detail_label = mass_label or ("Single ID" if clique.get("is_singleton_bucket") else None)
                node_name_parts = [section.get("snapshot_name") or section["snapshot_key"]]
                if detail_label:
                    node_name_parts.append(detail_label)
                node_name_parts.extend(_metabolite_query_id_labels(clique.get("selected_query_member_ids") or []))
                nodes.append({
                    "id": node_id,
                    "name": "\\n".join(node_name_parts),
                    "snapshot": section.get("snapshot_name"),
                    "snapshot_key": section.get("snapshot_key"),
                    "clique_key": clique.get("clique_key"),
                    "clique_size": clique.get("clique_size"),
                    "is_singleton_bucket": clique.get("is_singleton_bucket", False),
                    "name_samples": clique.get("member_name_samples", [])[:25],
                    "member_id_samples": clique.get("member_id_samples", [])[:25],
                    "selected_query_member_ids": clique.get("selected_query_member_ids", []),
                    "mass_summary": clique.get("mass_summary"),
                    "stageIndex": stage_index,
                })
    clique_lookup_by_section = {
        section["snapshot_key"]: {
            f"{section['snapshot_key']}::{clique['clique_key']}": clique
            for clique in section.get("cliques", [])
        }
        for section in ordered_sections
    }
    for left, right in zip(ordered_sections, ordered_sections[1:]):
        left_by_member = {}
        right_by_member = {}
        for clique in left.get("cliques", []):
            for member_id in clique.get("member_ids", []):
                left_by_member[member_id] = f"{left['snapshot_key']}::{clique['clique_key']}"
        for clique in right.get("cliques", []):
            for member_id in clique.get("member_ids", []):
                right_by_member[member_id] = f"{right['snapshot_key']}::{clique['clique_key']}"
        for member_id in sorted(set(left_by_member) & set(right_by_member)):
            pair = (left_by_member[member_id], right_by_member[member_id])
            link = links_by_pair.setdefault(pair, {
                "source": pair[0],
                "target": pair[1],
                "value": 0,
                "member_ids": [],
                "member_names": [],
                "masses": [],
            })
            link["value"] += 1
            source_clique = clique_lookup_by_section.get(left["snapshot_key"], {}).get(pair[0], {})
            link["masses"].extend(source_clique.get("member_mass_by_id", {}).get(member_id, []))
            if len(link["member_ids"]) < 25:
                link["member_ids"].append(member_id)
                member_name = source_clique.get("member_name_by_id", {}).get(member_id, member_id)
                if member_name != member_id:
                    link["member_names"].append(member_name)
    links = []
    for link in links_by_pair.values():
        link["mass_summary"] = _metabolite_mass_summary(link.pop("masses", []))
        links.append(link)
    return {"nodes": nodes, "links": links}


def _load_metabolite_identifier_qa(
    identifier: str,
    snapshot_keys: Optional[List[str]] = None,
    include_snapshot_union: bool = True,
) -> dict:
    if not identifier:
        raise HTTPException(status_code=400, detail="Provide a metabolite identifier.")
    db = get_db("metabolite_harmonization")
    query = """
    LET metabolite_doc = FIRST(
      FOR d IN MetaboliteIdentifier
        FILTER d.id == @identifier
        LIMIT 1
        RETURN d
    )
    LET metabolite = metabolite_doc == null ? null : UNSET(metabolite_doc, "_id", "_key", "_rev")
    LET chemical_entity_doc = FIRST(
      FOR d IN ChemicalEntity
        FILTER d.id == @identifier
        LIMIT 1
        RETURN d
    )
    LET chemical_entity = chemical_entity_doc == null ? null : UNSET(chemical_entity_doc, "_id", "_key", "_rev")
    LET mapping_edges = (
      FOR e IN MetaboliteIdentifierMappingEdge
        FILTER e.start_id == @identifier OR e.end_id == @identifier
        SORT e.start_id, e.end_id
        LIMIT 500
        LET neighbor_id = e.start_id == @identifier ? e.end_id : e.start_id
        LET neighbor = FIRST(
          FOR n IN MetaboliteIdentifier
            FILTER n.id == neighbor_id
            LIMIT 1
            RETURN KEEP(n, "id", "prefix", "names", "synonyms", "sources")
        )
        RETURN {
          start_id: e.start_id,
          end_id: e.end_id,
          neighbor_id: neighbor_id,
          neighbor: neighbor,
          details: e.details,
          sources: e.sources
        }
    )
    LET mapping_edge_count = LENGTH(
      FOR e IN MetaboliteIdentifierMappingEdge
        FILTER e.start_id == @identifier OR e.end_id == @identifier
        RETURN 1
    )
    LET chebi_bridge_edges = (
      FOR e IN ChebiChemicalEntityMetaboliteIdentifierEdge
        FILTER e.start_id == @identifier OR e.end_id == @identifier
        LIMIT 50
        RETURN UNSET(e, "_id", "_key", "_rev", "_from", "_to")
    )
    RETURN {
      query_id: @identifier,
      found: metabolite != null,
      metabolite: metabolite,
      chemical_entity: chemical_entity,
      mapping_edges: mapping_edges,
      mapping_edge_count: mapping_edge_count,
      mapping_edges_truncated: mapping_edge_count > LENGTH(mapping_edges),
      chebi_bridge_edges: chebi_bridge_edges
    }
    """
    cursor = db.aql.execute(query, bind_vars={"identifier": identifier}, max_runtime=120)
    rows = list(cursor)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No result for {identifier}")
    result = rows[0]
    if include_snapshot_union:
        snapshot_union = _load_metabolite_snapshot_union([identifier], snapshot_keys)
        result["snapshot_memberships"] = snapshot_union["memberships"]
        result["snapshot_union_ids"] = snapshot_union["member_ids"]
        result["snapshot_graphs"] = snapshot_union["snapshot_graphs"]
        result["snapshot_sankey"] = snapshot_union["sankey"]
    return result


def _parse_metabolite_identifier_query(value: str) -> List[str]:
    tokens = (
        value.replace(",", " ")
        .replace("|", " ")
        .replace(";", " ")
        .replace("\n", " ")
        .split()
    )
    ids = []
    seen = set()
    for token in tokens:
        identifier = token.strip()
        if identifier and identifier not in seen:
            ids.append(identifier)
            seen.add(identifier)
    return ids


def _load_metabolite_identifier_qa_many(
    value: str,
    snapshot_keys: Optional[List[str]] = None,
) -> dict:
    query_ids = _parse_metabolite_identifier_query(value)
    if not query_ids:
        raise HTTPException(status_code=400, detail="Provide at least one metabolite identifier.")
    if len(query_ids) > 12:
        raise HTTPException(status_code=400, detail="Compare 12 or fewer identifiers at a time.")

    results = [
        _load_metabolite_identifier_qa(identifier, snapshot_keys, include_snapshot_union=False)
        for identifier in query_ids
    ]
    found_results = [result for result in results if result.get("found")]
    missing_results = [result for result in results if not result.get("found")]
    combined_result = {
        "query_id": " ".join(query_ids),
        "query_ids": query_ids,
        "found": bool(found_results),
        "results": results,
        "found_results": found_results,
        "missing_results": missing_results,
    }
    snapshot_union = _load_metabolite_snapshot_union(query_ids, snapshot_keys)
    combined_result["snapshot_memberships"] = snapshot_union["memberships"]
    combined_result["snapshot_union_ids"] = snapshot_union["member_ids"]
    combined_result["snapshot_graphs"] = snapshot_union["snapshot_graphs"]
    combined_result["snapshot_sankey"] = snapshot_union["sankey"]
    combined_result["selected_snapshot_keys"] = snapshot_keys or []
    return combined_result


def _slugify_mysql_source(value: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "mysql"


def _register_mysql_source(source_id: str, creds: dict, label: Optional[str] = None):
    _mysql_sources[source_id] = {
        "credentials": creds or {},
        "label": label or source_id,
    }


def _get_mysql_source(source_id: str) -> dict:
    source = _mysql_sources.get(source_id)
    if source:
        return source
    if source_id == "default" and _mysql_credentials:
        return {"credentials": _mysql_credentials, "label": "default"}
    raise KeyError(f"Unknown MySQL source: {source_id}")


def _get_parquet_buffer(file_ref: str):
    """Fetch a parquet file from registry object storage and return (BytesIO, size_bytes).

    file_ref must be an s3:// URI produced by the ETL pipeline.
    Returns (None, None) if the file cannot be fetched.
    """
    if not file_ref.startswith("s3://"):
        return None, None
    try:
        import io

        without_prefix = file_ref[len("s3://"):]
        bucket, key = without_prefix.split("/", 1)
        credentials_options = []
        if _parquet_storage_credentials:
            credentials_options.append(("parquet storage", _parquet_storage_credentials))
        if _minio_credentials:
            credentials_options.append(("registry storage", _minio_credentials))
        if not credentials_options:
            return None, None

        errors = []
        for label, credentials in credentials_options:
            try:
                storage = _storage_from_credentials(credentials, use_internal_url=True)
                response = storage.client().get_object(Bucket=bucket, Key=key)
                data = response["Body"].read()
                return io.BytesIO(data), response["ContentLength"]
            except Exception as exc:
                errors.append(f"{label}: {exc}")
        raise RuntimeError("; ".join(errors))
    except Exception as e:
        raise RuntimeError(f"Failed to fetch {file_ref} from registry object storage: {e}") from e


def get_client() -> ArangoClient:
    global _client
    if _client is None:
        url = _credentials.get("internal_url") or _credentials.get("url", "http://localhost:8529")
        _client = ArangoClient(hosts=url, request_timeout=120, verify_override=False)
    return _client


def get_db(name: str):
    client = get_client()
    return client.db(name, username=_credentials.get("user", "root"),
                     password=_credentials.get("password", "password"))


def get_sys_db():
    client = get_client()
    return client.db("_system", username=_credentials.get("user", "root"),
                     password=_credentials.get("password", "password"))


def get_mysql_engine(source_id: str = "default") -> Optional[Engine]:
    """Get a MySQL engine (no specific database) for listing databases."""
    try:
        source = _get_mysql_source(source_id)
    except KeyError:
        return None

    credentials = source["credentials"]
    if not credentials:
        return None

    cache_key = f"{source_id}::_root"
    if cache_key not in _mysql_db_engines:
        host = credentials.get("url", "localhost")
        port = credentials.get("port", 3306)
        user = credentials.get("user", "root")
        password = url_quote(credentials.get("password", ""), safe="")
        _mysql_db_engines[cache_key] = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}",
            pool_pre_ping=True, pool_size=2,
        )
    return _mysql_db_engines[cache_key]


def get_mysql_db_engine(db_name: str, source_id: str = "default") -> Engine:
    """Get a MySQL engine scoped to a specific database."""
    source = _get_mysql_source(source_id)
    credentials = source["credentials"]
    cache_key = f"{source_id}::{db_name}"
    if cache_key not in _mysql_db_engines:
        host = credentials.get("url", "localhost")
        port = credentials.get("port", 3306)
        user = credentials.get("user", "root")
        password = url_quote(credentials.get("password", ""), safe="")
        _mysql_db_engines[cache_key] = create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}",
            pool_pre_ping=True, pool_size=2,
        )
    return _mysql_db_engines[cache_key]


def get_mysql_inspector(db_name: str, source_id: str = "default"):
    """Return cached schema metadata for a database.

    Cached as a plain dict so we don't hold live Inspector objects.
    Call invalidate_mysql_inspector(db_name) to force a refresh.
    """
    cache_key = f"{source_id}::{db_name}"
    if cache_key not in _mysql_inspector_cache:
        engine = get_mysql_db_engine(db_name, source_id=source_id)
        insp = sa_inspect(engine)
        table_names = insp.get_table_names()
        meta = {}
        for tbl in table_names:
            meta[tbl] = {
                "columns": insp.get_columns(tbl),
                "pk": insp.get_pk_constraint(tbl).get("constrained_columns", []),
                "fks": insp.get_foreign_keys(tbl),
            }
        _mysql_inspector_cache[cache_key] = meta
    return _mysql_inspector_cache[cache_key]


def invalidate_mysql_inspector(db_name: str, source_id: str = "default"):
    """Drop the cached schema for a database so the next request re-fetches it."""
    _mysql_inspector_cache.pop(f"{source_id}::{db_name}", None)


# ── Jinja2 filters ──────────────────────────────────────────────────────────

def json_pretty(value):
    try:
        return json.dumps(value, indent=2, default=str)
    except Exception:
        return str(value)


templates.env.filters["json_pretty"] = json_pretty


def _get_graph_views(db) -> dict:
    if not db.has_collection("metadata_store"):
        return {}
    try:
        store = db.collection("metadata_store")
        doc = store.get("graph_views")
        if not doc:
            return {}
        return doc.get("value", {}).get("views", {}) or {}
    except Exception:
        return {}


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _format_duration_seconds(total_seconds: float | int | None) -> str:
    if total_seconds is None:
        return ""
    total_seconds = int(total_seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _format_elapsed(started_at: Optional[str], finished_at: Optional[str] = None) -> str:
    start_dt = _parse_iso_datetime(started_at)
    if start_dt is None:
        return ""
    end_dt = _parse_iso_datetime(finished_at) or datetime.utcnow()
    return _format_duration_seconds(max((end_dt - start_dt).total_seconds(), 0))


def _summarize_checkpoint(checkpoint: dict) -> dict:
    adapters = checkpoint.get("adapters") or {}
    rows = []
    completed = 0
    running = 0
    failed = 0
    total = 0
    total_records = 0

    for adapter_name, metadata in sorted(
        adapters.items(),
        key=lambda item: (item[1].get("adapter_position") or 10**9, item[0])
    ):
        status = metadata.get("status", "unknown")
        if status == "completed":
            completed += 1
        elif status == "running":
            running += 1
        elif status == "failed":
            failed += 1
        total = max(total, metadata.get("adapter_total") or 0)
        total_records += metadata.get("records_written") or 0
        rows.append({
            "name": adapter_name,
            "status": status,
            "adapter_position": metadata.get("adapter_position"),
            "adapter_total": metadata.get("adapter_total"),
            "records_written": metadata.get("records_written"),
            "started_at": metadata.get("started_at"),
            "completed_at": metadata.get("completed_at"),
            "failed_at": metadata.get("failed_at"),
            "error_message": metadata.get("error_message"),
            "elapsed": _format_elapsed(
                metadata.get("started_at"),
                metadata.get("completed_at") or metadata.get("failed_at"),
            ),
        })

    if not total:
        total = len(rows)

    run_id = checkpoint.get("run_id")
    run_name = (run_id or checkpoint.get("_key") or "Unknown run").split("/")[-1]
    run_name = run_name.replace(".yaml", "").replace("_", " ")

    return {
        "run_id": run_id,
        "run_label": run_name,
        "last_updated": checkpoint.get("last_updated"),
        "adapters": rows,
        "completed": completed,
        "running": running,
        "failed": failed,
        "known_total": total,
        "seen_total": len(rows),
        "total_records": total_records,
        "is_active": running > 0,
        "status": (
            "failed" if failed > 0 else
            "running" if running > 0 else
            "completed" if completed > 0 and completed >= total else
            "partial" if completed > 0 else
            "unknown"
        ),
    }


def _get_build_status(db) -> Optional[dict]:
    if not db.has_collection("metadata_store"):
        return None
    try:
        cursor = db.aql.execute("""
            FOR d IN metadata_store
                FILTER d.type == "etl_checkpoint"
                SORT d.last_updated DESC
                RETURN d
        """)
        checkpoints = list(cursor)
    except Exception:
        return None

    if not checkpoints:
        return None

    run_summaries = [_summarize_checkpoint(checkpoint) for checkpoint in checkpoints]

    etl_meta = None
    try:
        cursor = db.aql.execute('RETURN DOCUMENT("metadata_store", "etl_metadata").value')
        results = list(cursor)
        if results and results[0]:
            etl_meta = results[0]
    except Exception:
        etl_meta = None

    latest_checkpoint = checkpoints[0]
    checkpoint_updated_dt = _parse_iso_datetime(latest_checkpoint.get("last_updated"))
    etl_run_dt = _parse_iso_datetime((etl_meta or {}).get("run_date"))
    any_failed = any(summary["failed"] > 0 for summary in run_summaries)
    any_running = any(summary["running"] > 0 for summary in run_summaries)
    any_partial = any(
        summary["completed"] > 0 and summary["completed"] < summary["known_total"]
        for summary in run_summaries
    )
    all_complete = bool(run_summaries) and all(
        summary["completed"] >= summary["known_total"] and summary["known_total"] > 0
        for summary in run_summaries
    )

    if any_failed:
        overall_status = "failed"
        overall_message = "One or more ETL runs have failed adapters."
    elif any_running:
        overall_status = "running"
        overall_message = "One or more ETL runs are still running."
    elif all_complete:
        if etl_run_dt and (checkpoint_updated_dt is None or etl_run_dt >= checkpoint_updated_dt):
            overall_status = "completed"
            overall_message = "ETL and post-processing completed."
        else:
            overall_status = "post_processing"
            overall_message = "Adapters completed. Post-processing or cleanup is still running."
    elif any_partial:
        overall_status = "partial"
        overall_message = "Some ETL runs are only partially complete."
    else:
        overall_status = "unknown"
        overall_message = "Build state is not yet clear from metadata."

    return {
        "run_id": latest_checkpoint.get("run_id"),
        "last_updated": latest_checkpoint.get("last_updated"),
        "etl_run_date": (etl_meta or {}).get("run_date"),
        "runs": run_summaries,
        "completed": sum(summary["completed"] for summary in run_summaries),
        "running": sum(summary["running"] for summary in run_summaries),
        "failed": sum(summary["failed"] for summary in run_summaries),
        "known_total": sum(summary["known_total"] for summary in run_summaries),
        "seen_total": sum(summary["seen_total"] for summary in run_summaries),
        "total_records": sum(summary["total_records"] for summary in run_summaries),
        "is_active": any_running,
        "overall_status": overall_status,
        "overall_message": overall_message,
    }


def _get_collection_schema_entry(db, coll_name: str) -> dict:
    if not db.has_collection("metadata_store"):
        return {}
    try:
        store = db.collection("metadata_store")
        doc = store.get("collection_schemas")
        if not doc:
            return {}
        return (doc.get("collections") or {}).get(coll_name, {}) or {}
    except Exception:
        return {}


def _edge_endpoint_pairs(db, edge_collection: str) -> List[dict]:
    if not db.has_collection(edge_collection):
        return []
    query = """
    FOR e IN @@edge_collection
      COLLECT from_collection = SPLIT(e._from, "/")[0],
              to_collection = SPLIT(e._to, "/")[0]
      WITH COUNT INTO count
      SORT from_collection, to_collection
      RETURN {
        from_collection: from_collection,
        to_collection: to_collection,
        count: count
      }
    """
    try:
        return list(db.aql.execute(query, bind_vars={"@edge_collection": edge_collection}))
    except Exception:
        return []


def _edge_definitions_with_endpoint_pairs(db) -> List[dict]:
    if not db.has_graph("graph"):
        return []
    graph = db.graph("graph")
    edge_defs = graph.edge_definitions()
    enriched_defs = []
    for edge_def in edge_defs:
        enriched_def = dict(edge_def)
        enriched_def["endpoint_pairs"] = _edge_endpoint_pairs(db, edge_def["edge_collection"])
        enriched_defs.append(enriched_def)
    return enriched_defs


def _get_collection_facet_metadata(db, coll_name: str) -> dict:
    schema_entry = _get_collection_schema_entry(db, coll_name)
    facet_metadata = schema_entry.get("facet_metadata") or {}
    category_fields = list(facet_metadata.get("category_fields") or [])
    if not category_fields:
        category_fields = _infer_collection_category_fields(db, coll_name)
    return {
        "category_fields": category_fields,
        "numeric_fields": list(facet_metadata.get("numeric_fields") or []),
    }


def _get_collection_search_metadata(db, coll_name: str) -> dict:
    schema_entry = _get_collection_schema_entry(db, coll_name)
    search_metadata = schema_entry.get("search_metadata") or {}
    text_fields = list(search_metadata.get("text_fields") or [])
    if not text_fields:
        text_fields = _infer_collection_search_fields(db, coll_name, schema_entry=schema_entry)
    return {
        "text_fields": text_fields,
    }


def _infer_collection_category_fields(db, coll_name: str) -> List[str]:
    try:
        coll = db.collection(coll_name)
        indexes = coll.indexes()
    except Exception:
        return []

    # Older graphs only expose index intent indirectly. Restrict the fallback to
    # simple single-field hash indexes and suppress obviously noisy/internal fields.
    suppressed_fields = {
        "_key",
        "_id",
        "_rev",
        "_from",
        "_to",
        "id",
        "xref",
        "resolved_ids",
        "provenance",
        "creation",
        "updates",
    }
    inferred_fields = []
    for index in indexes:
        if index.get("type") != "hash":
            continue
        fields = index.get("fields") or []
        if len(fields) != 1:
            continue
        field = fields[0]
        if not field or field.startswith("_") or field in suppressed_fields:
            continue
        inferred_fields.append(field)
    return sorted(set(inferred_fields))


def _infer_collection_search_fields(db, coll_name: str, schema_entry: dict = None) -> List[str]:
    schema_entry = schema_entry or _get_collection_schema_entry(db, coll_name)
    schema_fields = set((schema_entry.get("fields") or {}).keys())
    preferred_fields = [
        "id",
        "name",
        "symbol",
        "preferred_symbol",
        "description",
        "label",
        "gene_name",
        "type",
        "xref",
        "uniprot_id",
        "ensembl_id",
        "refseq_id",
        "ncbi_id",
    ]
    return [field for field in preferred_fields if field in schema_fields]


def _parse_collection_facet_filters(request: Request, category_fields: List[str]) -> Dict[str, List[str]]:
    allowed_fields = set(category_fields)
    active_filters = {}
    for field in category_fields:
        key = f"facet_{field}"
        values = [value for value in request.query_params.getlist(key) if value != ""]
        if values:
            active_filters[field] = values
    for key in request.query_params.keys():
        if not key.startswith("facet_"):
            continue
        field = key[len("facet_"):]
        if field not in allowed_fields:
            continue
        values = [value for value in request.query_params.getlist(key) if value != ""]
        if values:
            active_filters[field] = values
    return active_filters


def _parse_collection_search_term(request: Request) -> str:
    return request.query_params.get("q", "").strip()


def _build_collection_query_params(page: int, page_size: int, facet_filters: Dict[str, List[str]],
                                   search_term: str = "", overrides: dict = None) -> List[tuple]:
    params = [("page", page), ("page_size", page_size)]
    if search_term:
        params.append(("q", search_term))
    merged_filters = {field: list(values) for field, values in facet_filters.items()}
    overrides = overrides or {}
    for field, values in overrides.items():
        merged_filters[field] = list(values)
    for field in sorted(merged_filters):
        for value in merged_filters[field]:
            params.append((f"facet_{field}", value))
    return params


def _build_collection_url(db_name: str, coll_name: str, page: int, page_size: int,
                          facet_filters: Dict[str, List[str]], search_term: str = "",
                          overrides: dict = None) -> str:
    params = _build_collection_query_params(page, page_size, facet_filters, search_term=search_term, overrides=overrides)
    query_string = urlencode(params, doseq=True)
    return f"{_app_path(f'/db/{db_name}/collection/{coll_name}')}?{query_string}"


def _build_collection_stats_url(db_name: str, coll_name: str, facet_filters: Dict[str, List[str]],
                                search_term: str = "") -> str:
    params = []
    if search_term:
        params.append(("q", search_term))
    for field in sorted(facet_filters):
        for value in facet_filters[field]:
            params.append((f"facet_{field}", value))
    query_string = urlencode(params, doseq=True)
    base_url = _app_path(f"/db/{db_name}/collection/{coll_name}/stats")
    return f"{base_url}?{query_string}" if query_string else base_url


def _build_collection_facets_url(db_name: str, coll_name: str, facet_filters: Dict[str, List[str]],
                                 search_term: str = "", page_size: int = 25) -> str:
    params = [("page_size", page_size)]
    if search_term:
        params.append(("q", search_term))
    for field in sorted(facet_filters):
        for value in facet_filters[field]:
            params.append((f"facet_{field}", value))
    query_string = urlencode(params, doseq=True)
    base_url = _app_path(f"/db/{db_name}/collection/{coll_name}/facets")
    return f"{base_url}?{query_string}" if query_string else base_url


def _build_collection_facet_url(db_name: str, coll_name: str, field: str,
                                facet_filters: Dict[str, List[str]],
                                search_term: str = "", page_size: int = 25) -> str:
    params = [("page_size", page_size)]
    if search_term:
        params.append(("q", search_term))
    for filter_field in sorted(facet_filters):
        for value in facet_filters[filter_field]:
            params.append((f"facet_{filter_field}", value))
    query_string = urlencode(params, doseq=True)
    base_url = _app_path(f"/db/{db_name}/collection/{coll_name}/facet/{url_quote(field, safe='')}")
    return f"{base_url}?{query_string}" if query_string else base_url


def _build_collection_download_url(db_name: str, coll_name: str, page: int, page_size: int,
                                   facet_filters: Dict[str, List[str]], search_term: str = "") -> str:
    params = _build_collection_query_params(page, page_size, facet_filters, search_term=search_term)
    query_string = urlencode(params, doseq=True)
    base_url = _app_path(f"/db/{db_name}/collection/{coll_name}/download.csv")
    return f"{base_url}?{query_string}" if query_string else base_url


def _get_search_constraint_clause(search_fields: List[str], search_term: str, bind_vars: dict,
                                  variable: str = "doc") -> str:
    if not search_term or not search_fields:
        return ""

    bind_vars["search_term"] = search_term.lower()
    field_bind_names = []
    for idx, field in enumerate(search_fields):
        bind_name = f"search_field_{idx}"
        bind_vars[bind_name] = field
        field_bind_names.append(bind_name)

    field_clauses = []
    for bind_name in field_bind_names:
        field_clauses.append(
            "("
            f"HAS({variable}, @{bind_name}) && {variable}[@{bind_name}] != null && "
            "("
            f"IS_ARRAY({variable}[@{bind_name}]) "
            f"? LENGTH(FOR item IN {variable}[@{bind_name}] FILTER CONTAINS(LOWER(TO_STRING(item)), @search_term) RETURN 1) > 0 "
            f": CONTAINS(LOWER(TO_STRING({variable}[@{bind_name}])), @search_term)"
            ")"
            ")"
        )
    return " OR ".join(field_clauses)


def _get_filter_constraint_clause(filter_settings: Dict[str, List[str]], bind_vars: dict, variable: str = "doc") -> str:
    clauses = []
    for idx, (field, values) in enumerate(filter_settings.items()):
        field_bind = f"facet_field_{idx}"
        values_bind = f"facet_values_{idx}"
        bind_vars[field_bind] = field
        bind_vars[values_bind] = [_coerce_facet_filter_value(value) for value in values]
        clauses.append(
            "("
            f"IS_ARRAY({variable}[@{field_bind}]) "
            f"? LENGTH(INTERSECTION({variable}[@{field_bind}], @{values_bind})) > 0 "
            f": {variable}[@{field_bind}] IN @{values_bind}"
            ")"
        )
    return " AND ".join(clauses)


def _get_facet_clause(field_bind_name: str, top_bind_name: str, variable: str = "doc") -> str:
    return f"""
        LET values = (
            !HAS({variable}, @{field_bind_name}) || {variable}[@{field_bind_name}] == null
                ? [null]
                : (IS_ARRAY({variable}[@{field_bind_name}]) ? UNIQUE({variable}[@{field_bind_name}]) : [{variable}[@{field_bind_name}]])
        )
            FOR item IN values
                COLLECT value = item WITH COUNT INTO count
                SORT count DESC, value
                LIMIT @{top_bind_name}
                RETURN {{ value, count }}"""


def _normalize_facet_value(value):
    return "null" if value is None else str(value)


def _format_facet_value(value):
    return "missing" if value is None else str(value)


def _coerce_facet_filter_value(value: str):
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None
    return value


def _build_collection_constraints(active_filters: Dict[str, List[str]], search_fields: List[str],
                                  search_term: str, bind_vars: dict, variable: str = "doc") -> str:
    clauses = []
    filter_clause = _get_filter_constraint_clause(active_filters, bind_vars, variable=variable)
    if filter_clause:
        clauses.append(f"({filter_clause})")
    search_clause = _get_search_constraint_clause(search_fields, search_term, bind_vars, variable=variable)
    if search_clause:
        clauses.append(f"({search_clause})")
    return " AND ".join(clauses)


def _build_collection_facet_panels(db, db_name: str, coll_name: str, page_size: int,
                                   category_fields: List[str], active_filters: Dict[str, List[str]],
                                   search_fields: List[str], search_term: str,
                                   top: int = 20) -> List[dict]:
    ordered_fields = sorted(
        category_fields,
        key=lambda field: (0 if active_filters.get(field) else 1, field),
    )
    return [
        _build_collection_facet_panel(
            db=db,
            db_name=db_name,
            coll_name=coll_name,
            page_size=page_size,
            field=field,
            active_filters=active_filters,
            search_fields=search_fields,
            search_term=search_term,
            top=top,
        )
        for field in ordered_fields
    ]


def _build_collection_facet_panel(db, db_name: str, coll_name: str, page_size: int,
                                  field: str, active_filters: Dict[str, List[str]],
                                  search_fields: List[str], search_term: str,
                                  top: int = 20) -> dict:
    other_filters = {k: v for k, v in active_filters.items() if k != field}
    bind_vars = {
        "facet_field": field,
        "facet_top": top,
    }
    filter_clause = _build_collection_constraints(
        active_filters=other_filters,
        search_fields=search_fields,
        search_term=search_term,
        bind_vars=bind_vars,
    )
    query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            {_get_facet_clause('facet_field', 'facet_top')}
    """
    rows = list(db.aql.execute(query, bind_vars=bind_vars))
    selected_values = set(active_filters.get(field, []))
    facet_values = []
    for row in rows:
        normalized_value = _normalize_facet_value(row.get("value"))
        updated_values = sorted(selected_values ^ {normalized_value})
        overrides = dict(active_filters)
        if updated_values:
            overrides[field] = updated_values
        else:
            overrides[field] = []
        facet_values.append({
            "value": normalized_value,
            "label": _format_facet_value(row.get("value")),
            "count": row.get("count", 0),
            "selected": normalized_value in selected_values,
            "href": _build_collection_url(
                db_name=db_name,
                coll_name=coll_name,
                page=1,
                page_size=page_size,
                facet_filters=active_filters,
                search_term=search_term,
                overrides=overrides,
            ),
        })
    return {
        "field": field,
        "title": field.replace("_", " "),
        "values": facet_values,
        "selected_values": sorted(selected_values),
        "clear_href": _build_collection_url(
            db_name=db_name,
            coll_name=coll_name,
            page=1,
            page_size=page_size,
            facet_filters=active_filters,
            search_term=search_term,
            overrides={field: []},
        ),
    }


def _build_active_filter_summary(db_name: str, coll_name: str, page_size: int,
                                 active_filters: Dict[str, List[str]], search_term: str = "") -> List[dict]:
    summaries = []
    for field in sorted(active_filters):
        selected_values = list(active_filters[field])
        values = []
        for value in selected_values:
            remaining_values = [item for item in selected_values if item != value]
            overrides = dict(active_filters)
            if remaining_values:
                overrides[field] = remaining_values
            else:
                overrides[field] = []
            values.append({
                "label": _format_facet_value(None if value == "null" else value),
                "remove_href": _build_collection_url(
                    db_name=db_name,
                    coll_name=coll_name,
                    page=1,
                    page_size=page_size,
                    facet_filters=active_filters,
                    search_term=search_term,
                    overrides=overrides,
                ),
            })
        summaries.append({
            "field": field,
            "values": values,
            "clear_href": _build_collection_url(
                db_name=db_name,
                coll_name=coll_name,
                page=1,
                page_size=page_size,
                facet_filters=active_filters,
                search_term=search_term,
                overrides={field: []},
            ),
        })
    return summaries


def _get_collection_preview_fields(is_edge: bool, facet_metadata: dict, search_metadata: dict) -> List[str]:
    """Fields to show in collection tables without loading every large document field."""
    fields = ["_key"]
    if is_edge:
        fields.extend(["_from", "_to"])
    priority = [
        "id",
        "name",
        "symbol",
        "preferred_symbol",
        "type",
        "description",
        "label",
        "gene_name",
        "uniprot_id",
        "ensembl_id",
        "refseq_id",
        "ncbi_id",
    ]
    fields.extend(priority)
    fields.extend(sorted(facet_metadata.get("category_fields") or []))
    fields.extend(sorted(facet_metadata.get("numeric_fields") or []))
    fields.extend(search_metadata.get("text_fields") or [])
    return list(dict.fromkeys(fields))


def _build_collection_facet_loaders(db_name: str, coll_name: str, page_size: int,
                                    category_fields: List[str],
                                    active_filters: Dict[str, List[str]],
                                    search_term: str = "") -> List[dict]:
    ordered_fields = sorted(
        category_fields,
        key=lambda field: (0 if active_filters.get(field) else 1, field),
    )
    return [
        {
            "field": field,
            "title": field.replace("_", " "),
            "url": _build_collection_facet_url(
                db_name=db_name,
                coll_name=coll_name,
                field=field,
                facet_filters=active_filters,
                search_term=search_term,
                page_size=page_size,
            ),
        }
        for field in ordered_fields
    ]


def _get_dashboard_collection_summaries(db) -> List[dict]:
    collections = []
    for coll in db.collections():
        if _is_hidden_qa_browser_collection(coll["name"]):
            continue
        coll_type = coll.get("type")
        is_edge = coll_type in ("edge", 3)
        count_cursor = db.aql.execute(
            f"FOR doc IN `{coll['name']}` COLLECT WITH COUNT INTO c RETURN c"
        )
        count = list(count_cursor)[0]
        collections.append({
            "name": coll["name"],
            "type": "edge" if is_edge else "document",
            "count": count,
            "qa_visible": _is_qa_visible_edge_collection(coll["name"]),
        })
    collections.sort(key=lambda c: c["name"])
    return collections


def _get_dashboard_collection_shell(db) -> List[dict]:
    collections = []
    for coll in db.collections():
        if _is_hidden_qa_browser_collection(coll["name"]):
            continue
        coll_type = coll.get("type")
        is_edge = coll_type in ("edge", 3)
        collections.append({
            "name": coll["name"],
            "type": "edge" if is_edge else "document",
            "count": None,
            "qa_visible": _is_qa_visible_edge_collection(coll["name"]),
        })
    collections.sort(key=lambda c: c["name"])
    return collections


def _build_browser_home_context(request: Request) -> dict:
    # Arango databases
    arango_databases = []
    arango_url = _credentials.get("url", "")
    if _credentials:
        try:
            sys_db = get_sys_db()
            arango_databases = [db for db in sys_db.databases() if not db.startswith("_")]
        except Exception:
            pass

    # MySQL databases
    mysql_sources = []
    mysql_databases = []
    system_dbs = {"information_schema", "mysql", "performance_schema", "sys"}
    for source_id, source in _mysql_sources.items():
        credentials = source["credentials"]
        engine = get_mysql_engine(source_id)
        if not engine:
            continue
        host = credentials.get("url", "localhost")
        port = credentials.get("port", 3306)
        mysql_url = f"{host}:{port}"
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SHOW DATABASES"))
                dbs = [row[0] for row in result if row[0] not in system_dbs]
                print(f"MySQL databases for {source_id}: {dbs}")
                mysql_databases.extend(dbs)
                mysql_sources.append({
                    "id": source_id,
                    "label": source["label"],
                    "url": mysql_url,
                    "databases": dbs,
                })
        except Exception:
            print(f"Failed to connect to MySQL source {source_id}:", sys.exc_info()[1])
            pass

    return {
        "request": request,
        "databases": arango_databases,
        "arango_url": arango_url,
        "mysql_databases": mysql_databases,
        "mysql_sources": mysql_sources,
        "demo_queries_enabled": _demo_queries_enabled,
    }


def _registry_catalog_cache_fresh(category: str, now: float) -> bool:
    loaded_at = (_registry_catalog_cache.get("loaded_at") or {}).get(category, 0.0)
    return (
        _registry_catalog_cache.get(category) is not None
        and now - loaded_at < _REGISTRY_CATALOG_TTL_SECONDS
    )


def _load_registry_catalog_categories(categories: List[str]) -> tuple[dict, Optional[str]]:
    if not _minio_credentials:
        return {}, "Registry storage credentials are not configured for this QA Browser instance."

    now = time.time()
    requested = list(dict.fromkeys(categories))
    missing = [
        category
        for category in requested
        if not _registry_catalog_cache_fresh(category, now)
    ]

    if not missing:
        return {
            category: _registry_catalog_cache.get(category) or []
            for category in requested
        }, None

    try:
        def load_catalog(registry: DataRegistry):
            loaded = {}
            if "source_snapshots" in missing:
                loaded["source_snapshots"] = registry.list_source_snapshots()
            if "derived_artifacts" in missing:
                loaded["derived_artifacts"] = registry.list_derived_artifacts()
            if "external_registrations" in missing:
                loaded["external_registrations"] = registry.list_external_sources()
            if "resolver_snapshots" in missing:
                loaded["resolver_snapshots"] = registry.list_resolver_snapshots()
            return loaded

        loaded_categories = _with_registry_endpoint_fallback(
            load_catalog,
            error_prefix="Loading registry catalog",
        )
        loaded_at = _registry_catalog_cache.setdefault("loaded_at", {})
        cache_time = time.time()
        for category, value in loaded_categories.items():
            _registry_catalog_cache[category] = value
            loaded_at[category] = cache_time
        return {
            category: _registry_catalog_cache.get(category) or []
            for category in requested
        }, None
    except Exception as exc:
        return {}, str(exc)


def _load_registry_catalog() -> tuple[List[dict], List[dict], List[dict], List[dict], Optional[str]]:
    catalog, error = _load_registry_catalog_categories([
        "source_snapshots",
        "derived_artifacts",
        "external_registrations",
        "resolver_snapshots",
    ])
    return (
        catalog.get("source_snapshots", []),
        catalog.get("derived_artifacts", []),
        catalog.get("external_registrations", []),
        catalog.get("resolver_snapshots", []),
        error,
    )


def _storage_from_credentials(credentials_config: dict, *, use_internal_url: bool):
    from src.registry.storage import AwsAssumeRoleCredentials
    from src.registry.storage import AwsAssumeRoleStorage
    from src.registry.storage import MinioStorage
    from src.shared.db_credentials import DBCredentials

    if "role_arn" in credentials_config or credentials_config.get("type") == "aws_assume_role":
        return AwsAssumeRoleStorage(AwsAssumeRoleCredentials.from_yaml(credentials_config))
    return MinioStorage(DBCredentials.from_yaml(credentials_config), use_internal_url=use_internal_url)


def _registry_from_credentials(*, use_internal_url: bool):
    if not _minio_credentials:
        raise HTTPException(status_code=503, detail="Registry storage credentials are not configured for this QA Browser instance.")
    from src.registry.storage import AwsAssumeRoleCredentials
    from src.shared.db_credentials import DBCredentials

    if "role_arn" in _minio_credentials or _minio_credentials.get("type") == "aws_assume_role":
        credentials = AwsAssumeRoleCredentials.from_yaml(_minio_credentials)
    else:
        credentials = DBCredentials.from_yaml(_minio_credentials)

    return DataRegistry.from_credentials(
        credentials,
        use_internal_url=use_internal_url,
        connect_timeout=2,
        read_timeout=10,
    )


def _registry_endpoint_order() -> List[bool]:
    if "role_arn" in _minio_credentials or _minio_credentials.get("type") == "aws_assume_role":
        return [False]
    configured = os.getenv("QA_BROWSER_MINIO_URL_ORDER", "external,internal")
    order = []
    for part in configured.split(","):
        name = part.strip().lower()
        if name in {"internal", "internal_url", "docker"}:
            order.append(True)
        elif name in {"external", "url", "public"}:
            order.append(False)
    return order or [False, True]


def _registry_endpoint_label(use_internal_url: bool) -> str:
    if "role_arn" in _minio_credentials or _minio_credentials.get("type") == "aws_assume_role":
        return "aws_assume_role"
    return "internal_url" if use_internal_url else "url"


def _with_registry_endpoint_fallback(operation, *, error_prefix: str):
    errors = []
    tried = []
    for use_internal_url in _registry_endpoint_order():
        endpoint_label = _registry_endpoint_label(use_internal_url)
        tried.append(endpoint_label)
        try:
            registry = _registry_from_credentials(use_internal_url=use_internal_url)
            return operation(registry)
        except HTTPException:
            raise
        except Exception as exc:
            errors.append((endpoint_label, exc))
            print(f"{error_prefix} failed using registry storage {endpoint_label}: {exc}", flush=True)
    details = "; ".join(f"{label}: {error}" for label, error in errors)
    raise RuntimeError(f"{error_prefix} failed using registry storage endpoints {', '.join(tried)}: {details}")


def _load_registry_update_inputs(registry: DataRegistry, timeout: int):
    return {
        "source_statuses": registry.check_all_latest_registered(timeout=timeout),
        "external_statuses": registry.check_external_registrations(),
        "derived_statuses": registry.check_derived_artifacts(),
        "resolver_statuses": registry.check_resolvers(),
    }



def _registry_status_category(status: dict) -> str:
    if status.get("check_status") == "manual_unavailable":
        return "manual"
    if status.get("error"):
        return "error"
    if not status.get("registered_versions"):
        return "missing"
    if status.get("is_latest_registered") is True:
        return "current"
    if status.get("is_latest_registered") is False:
        return "update_available"
    return "unknown"


def _registry_status_item_label(status: dict) -> str:
    name = status.get("resolver") or status.get("dataset") or ""
    source = status.get("source") or ""
    return f"{source}:{name}" if source and name else source or name


def _summarize_registry_status_section(label: str, statuses: List[dict]) -> dict:
    counts = {
        "current": 0,
        "update_available": 0,
        "missing": 0,
        "manual": 0,
        "unknown": 0,
        "error": 0,
    }
    items = []
    for status in statuses:
        category = _registry_status_category(status)
        counts[category] += 1
        if category != "current":
            detail = status.get("sync_reason") or status.get("error")
            if not detail and category == "manual":
                detail = status.get("manual_check_message") or "manual source check unavailable in this environment"
            if not detail and category == "unknown":
                detail = "latest version not available from checker"
            items.append({
                "label": _registry_status_item_label(status),
                "category": category,
                "latest_version": status.get("latest_version"),
                "latest_version_date": status.get("latest_version_date"),
                "latest_registered_version": status.get("latest_registered_version"),
                "days_since_last_update": status.get("days_since_last_update"),
                "latest_build_key": status.get("latest_build_key"),
                "sync_reason": detail,
                "error": status.get("error"),
            })
    items.sort(key=lambda item: (
        {"update_available": 0, "missing": 1, "manual": 2, "error": 3, "unknown": 4}.get(item["category"], 5),
        item["label"],
    ))
    return {
        "label": label,
        "total": len(statuses),
        "counts": counts,
        "items": items,
    }


def _registry_status_key(status: dict) -> Optional[tuple]:
    source = status.get("source")
    resolver = status.get("resolver")
    dataset = status.get("dataset")
    if source and resolver:
        return "resolver", source, resolver
    if source and dataset:
        return "dataset", source, dataset
    return None


def _graph_dependency_keys(graph: dict) -> List[tuple]:
    keys = []

    def visit_dependency(dependency: dict):
        if not isinstance(dependency, dict):
            return
        source = dependency.get("source")
        dataset = dependency.get("dataset")
        if source and dataset:
            keys.append(("dataset", source, dataset))
        for upstream in dependency.get("derived_from") or []:
            visit_dependency(upstream)

    for adapter in graph.get("adapters") or []:
        for dependency in adapter.get("datasets") or []:
            visit_dependency(dependency)
    for resolver in graph.get("resolvers") or []:
        snapshot = resolver.get("snapshot") or {}
        source = snapshot.get("source")
        resolver_name = resolver.get("name")
        if source and resolver_name:
            keys.append(("resolver", source, resolver_name))
        for dependency in resolver.get("inputs") or []:
            visit_dependency(dependency)
    return keys


def _graph_update_statuses(graphs: List[dict], stale_keys: set, unknown_keys: set) -> List[dict]:
    statuses = []
    for graph in graphs:
        dependency_keys = set(_graph_dependency_keys(graph))
        stale_dependency_count = len(dependency_keys & stale_keys)
        unknown_dependency_count = len(dependency_keys & unknown_keys)
        if stale_dependency_count:
            is_latest_registered = False
            sync_reason = f"{stale_dependency_count} dependency updates available"
        elif unknown_dependency_count:
            is_latest_registered = None
            sync_reason = f"{unknown_dependency_count} dependencies unknown"
        else:
            is_latest_registered = True
            sync_reason = None
        statuses.append({
            "source": "graph",
            "dataset": graph.get("name"),
            "registered_versions": [graph.get("run_date") or "built"],
            "latest_registered_version": graph.get("run_date"),
            "latest_version": None,
            "is_latest_registered": is_latest_registered,
            "sync_reason": sync_reason,
            "error": None,
        })
    return statuses


def _run_registry_update_checks() -> dict:
    timeout = int(os.getenv("QA_BROWSER_REGISTRY_UPDATE_CHECK_TIMEOUT", "20"))
    started = time.time()
    status_inputs = _with_registry_endpoint_fallback(
        lambda registry: _load_registry_update_inputs(registry, timeout),
        error_prefix="Checking registry update status",
    )
    source_statuses = status_inputs["source_statuses"]
    external_statuses = status_inputs["external_statuses"]
    derived_statuses = status_inputs["derived_statuses"]
    resolver_statuses = status_inputs["resolver_statuses"]
    stale_keys = {
        key
        for status in [*source_statuses, *external_statuses, *derived_statuses, *resolver_statuses]
        for key in [_registry_status_key(status)]
        if key and _registry_status_category(status) in {"update_available", "missing"}
    }
    unknown_keys = {
        key
        for status in [*source_statuses, *external_statuses, *derived_statuses, *resolver_statuses]
        for key in [_registry_status_key(status)]
        if key and _registry_status_category(status) in {"unknown", "error"}
    }
    graphs, graph_error = load_registry_graphs_cached(
        credentials=_credentials,
        cache=_registry_graph_cache,
        ttl_seconds=0,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    sections = [
        _summarize_registry_status_section(
            "Source Snapshots",
            source_statuses,
        ),
        _summarize_registry_status_section(
            "External Sources",
            external_statuses,
        ),
        _summarize_registry_status_section(
            "Derived Artifacts",
            derived_statuses,
        ),
        _summarize_registry_status_section(
            "Resolvers",
            resolver_statuses,
        ),
    ]
    if graph_error:
        sections.append(_summarize_registry_status_section("Graphs", [{
            "source": "graph",
            "dataset": "registry graphs",
            "registered_versions": ["unknown"],
            "is_latest_registered": None,
            "error": graph_error,
        }]))
    else:
        sections.append(_summarize_registry_status_section(
            "Graphs",
            _graph_update_statuses(graphs, stale_keys, unknown_keys),
        ))
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "elapsed_seconds": round(time.time() - started, 1),
        "sections": sections,
        "error": None,
    }


def _registry_update_status_context() -> dict:
    return _registry_update_status_cache


def _gunzip_if_needed(path: Path) -> Path:
    if path.suffix != ".gz":
        return path
    output_path = path.with_suffix("")
    if output_path.exists() and output_path.stat().st_mtime >= path.stat().st_mtime:
        return output_path
    tmp_path = output_path.with_name(f"{output_path.name}.tmp")
    with gzip.open(path, "rb") as source, tmp_path.open("wb") as dest:
        shutil.copyfileobj(source, dest)
    tmp_path.replace(output_path)
    return output_path


def _materialize_ramp_sqlite_database() -> tuple[Path, dict]:
    cache_dir = Path(os.getenv("QA_BROWSER_REGISTRY_CACHE_DIR", str(DEFAULT_REGISTRY_CACHE_DIR)))
    source = "ramp"
    dataset_name = "sqlite_database"
    version = "3.0.7"
    manifest_key = f"sources/{source}/{dataset_name}/{version}/manifest.yaml"

    def materialize(registry: DataRegistry):
        manifest = yaml.safe_load(registry.storage.read_text(manifest_key))
        local_dir = cache_dir / source / dataset_name / version
        local_dir.mkdir(parents=True, exist_ok=True)
        for entry in manifest.get("files") or []:
            local_path = local_dir / entry["path"]
            if registry._local_file_matches_entry(local_path, entry):
                continue
            storage_uri = entry.get("storage_uri") or ""
            if storage_uri.startswith(f"s3://{registry.storage.bucket}/"):
                key = storage_uri.removeprefix(f"s3://{registry.storage.bucket}/")
            else:
                key = f"sources/{source}/{dataset_name}/{version}/{entry['path']}"
            registry.storage.download_file(key, local_path)
        files = manifest.get("files") or []
        if len(files) != 1:
            raise ValueError(f"Expected one RaMP SQLite file in {manifest_key}; found {len(files)}")
        sqlite_path = _gunzip_if_needed(local_dir / files[0]["path"])
        return sqlite_path, {
            "kind": manifest.get("kind"),
            "source": source,
            "dataset": dataset_name,
            "version": version,
            "version_date": manifest.get("version_date"),
            "download_date": manifest.get("download_date"),
            "snapshot_id": manifest.get("snapshot_id"),
            "manifest_uri": f"s3://{registry.storage.bucket}/{manifest_key}",
            "local_dir": str(local_dir),
            "files": files,
        }

    return _with_registry_endpoint_fallback(
        materialize,
        error_prefix="Materializing RaMP SQLite registry snapshot",
    )


def _list_resolver_snapshots_for_warmup() -> List[dict]:
    return _with_registry_endpoint_fallback(
        lambda registry: registry.list_resolver_snapshots(),
        error_prefix="Listing resolver snapshots",
    )


def _latest_resolver_snapshots(resolver_snapshots: List[dict]) -> List[dict]:
    latest_by_resolver = {}
    for snapshot in resolver_snapshots:
        source = snapshot.get("source")
        resolver = snapshot.get("resolver")
        version = snapshot.get("version")
        if not source or not resolver or not version:
            continue
        key = (source, resolver)
        current = latest_by_resolver.get(key)
        if current is None or (
            snapshot.get("created_at") or "",
            snapshot.get("version") or "",
        ) > (
            current.get("created_at") or "",
            current.get("version") or "",
        ):
            latest_by_resolver[key] = snapshot
    return sorted(
        latest_by_resolver.values(),
        key=lambda snapshot: (
            snapshot.get("source") or "",
            snapshot.get("resolver") or "",
            snapshot.get("created_at") or "",
            snapshot.get("version") or "",
        ),
    )


def _warm_resolver_instances_in_background():
    _resolver_warmup_status.update({
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None,
        "total": 0,
        "warmed": 0,
        "errors": [],
    })

    try:
        resolver_snapshots = _list_resolver_snapshots_for_warmup()
        if not _RESOLVER_WARMUP_ALL_SNAPSHOTS:
            resolver_snapshots = _latest_resolver_snapshots(resolver_snapshots)
        else:
            resolver_snapshots = sorted(
                resolver_snapshots,
                key=lambda snapshot: (
                    snapshot.get("source") or "",
                    snapshot.get("resolver") or "",
                    snapshot.get("created_at") or "",
                    snapshot.get("version") or "",
                ),
            )
        _resolver_warmup_status["total"] = len(resolver_snapshots)
        scope = "all resolver snapshots" if _RESOLVER_WARMUP_ALL_SNAPSHOTS else "latest resolver snapshots"
        print(f"Starting resolver warmup for {len(resolver_snapshots)} {scope}.")

        for snapshot in resolver_snapshots:
            source = snapshot.get("source")
            resolver = snapshot.get("resolver")
            version = snapshot.get("version")
            if not source or not resolver or not version:
                continue
            try:
                _get_resolver_instance_for_api(source, resolver, version)
                _resolver_warmup_status["warmed"] += 1
                print(f"Warmed resolver {source}:{resolver}:{version}")
            except Exception as exc:
                message = f"{source}:{resolver}:{version}: {exc}"
                _resolver_warmup_status["errors"].append(message)
                print(f"Failed to warm resolver {message}")
    except Exception as exc:
        message = str(exc)
        _resolver_warmup_status["errors"].append(message)
        print(f"Resolver warmup failed: {message}")
    finally:
        _resolver_warmup_status["completed_at"] = datetime.now(timezone.utc).isoformat()
        print(
            "Resolver warmup complete: "
            f"{_resolver_warmup_status['warmed']}/{_resolver_warmup_status['total']} warmed, "
            f"{len(_resolver_warmup_status['errors'])} errors."
        )


def _start_resolver_warmup_thread():
    global _resolver_warmup_thread, _resolver_warmup_started
    if not _RESOLVER_WARMUP_ENABLED:
        print("Resolver warmup disabled by QA_BROWSER_WARM_RESOLVERS.")
        return
    if _resolver_warmup_started:
        return
    if not _minio_credentials:
        print("Resolver warmup skipped because registry storage credentials are not configured.")
        return
    _resolver_warmup_started = True
    _resolver_warmup_thread = threading.Thread(
        target=_warm_resolver_instances_in_background,
        name="qa-browser-resolver-warmup",
        daemon=True,
    )
    _resolver_warmup_thread.start()


def _materialize_resolver_snapshot_for_api(source: str, resolver: str, version: str):
    cache_dir = Path(os.getenv("QA_BROWSER_REGISTRY_CACHE_DIR", str(DEFAULT_REGISTRY_CACHE_DIR)))
    try:
        return _with_registry_endpoint_fallback(
            lambda registry: registry.materialize_resolver_snapshot(source, resolver, version, dest=cache_dir),
            error_prefix=f"Materializing resolver snapshot {source}/{resolver}/{version}",
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Could not materialize resolver snapshot: {exc}") from exc


def _load_class_from_definition(definition: dict):
    module_path = definition.get("import")
    class_name = definition.get("class")
    if not module_path or not class_name:
        raise HTTPException(status_code=500, detail="Resolver snapshot definition is missing import/class metadata.")
    abs_module_path = os.path.abspath(module_path)
    normalized_module_path = os.path.normpath(abs_module_path)
    module_name = (
        "qa_resolver_import__"
        + normalized_module_path.replace(":", "").replace(os.sep, "_").replace(".", "_")
    )
    module = sys.modules.get(module_name)
    if module is None:
        spec = importlib.util.spec_from_file_location(module_name, abs_module_path)
        if spec is None or spec.loader is None:
            raise HTTPException(status_code=500, detail=f"Could not load resolver module {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
    return getattr(module, class_name)


def _get_resolver_instance_for_api(source: str, resolver: str, version: str):
    cache_key = (source, resolver, version)
    if cache_key in _resolver_instance_cache:
        return _resolver_instance_cache[cache_key]

    with _resolver_instance_cache_locks_guard:
        lock = _resolver_instance_cache_locks.setdefault(cache_key, threading.Lock())

    with lock:
        if cache_key in _resolver_instance_cache:
            return _resolver_instance_cache[cache_key]

        resolver_snapshot = _materialize_resolver_snapshot_for_api(source, resolver, version)
        definition = resolver_snapshot.manifest.get("definition") or {}
        accepted_types = list(definition.get("accepted_types") or [])
        if not accepted_types:
            raise HTTPException(status_code=400, detail=f"Resolver snapshot {resolver_snapshot.snapshot_id} has no accepted_types.")
        type_sensitive = bool(definition.get("type_sensitive"))
        resolver_class = _load_class_from_definition(definition)
        resolver_instance = resolver_class(
            resolver_snapshot=resolver_snapshot,
            types=accepted_types,
        )
        _resolver_instance_cache[cache_key] = resolver_snapshot, resolver_instance, accepted_types, type_sensitive
        return _resolver_instance_cache[cache_key]


def _node_class_for_api_type(input_type: str):
    return type(input_type, (Node,), {})


def _nodes_for_resolver_api(input_type: str, ids: List[str]) -> List[Node]:
    node_class = _node_class_for_api_type(input_type)
    nodes = []
    for value in ids:
        node = node_class(id=value)
        setattr(node, "name", value)
        setattr(node, "text", value)
        nodes.append(node)
    return nodes


def _serialize_id_match(match) -> dict:
    equivalent_ids = []
    for equivalent_id in match.equivalent_ids or []:
        if hasattr(equivalent_id, "id_str"):
            equivalent_ids.append(equivalent_id.id_str())
        else:
            equivalent_ids.append(str(equivalent_id))
    return {
        "input": match.input,
        "match": match.match,
        "equivalent_ids": equivalent_ids,
        "context": list(match.context or []),
    }


def _resolve_ids_for_type(resolver_instance, input_type: str, ids: List[str]) -> List[dict]:
    nodes = _nodes_for_resolver_api(input_type, ids)
    raw_matches = resolver_instance.resolve_internal(nodes)
    return [
        {
            "input": input_id,
            "matches": [
                _serialize_id_match(match)
                for match in raw_matches.get(input_id, []) or []
            ],
        }
        for input_id in ids
    ]


def _resolver_prefix_counts_for_api(resolver_instance) -> List[dict]:
    counts = []
    for row in resolver_instance.get_prefix_counts() or []:
        if not isinstance(row, dict):
            continue
        prefix = str(row.get("prefix") or "").strip()
        if not prefix:
            continue
        raw_count = row.get("count")
        count = raw_count if isinstance(raw_count, int) else None
        counts.append({"prefix": prefix, "count": count})
    return sorted(
        counts,
        key=lambda row: (
            -(row["count"] if isinstance(row.get("count"), int) else -1),
            row["prefix"].lower(),
        ),
    )


def _resolver_example_ids_for_api(resolver_instance) -> List[str]:
    return [
        str(example_id)
        for example_id in resolver_instance.get_example_ids(limit=5) or []
        if str(example_id).strip()
    ]


def _normalize_resolver_api_ids(payload: dict) -> List[str]:
    raw_ids = payload.get("ids")
    if raw_ids is None and "id" in payload:
        raw_ids = [payload.get("id")]
    if isinstance(raw_ids, str):
        raw_ids = [raw_ids]
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="Request body must include ids as a string or list of strings.")
    ids = [str(value).strip() for value in raw_ids if str(value).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="At least one non-empty id is required.")
    if len(ids) > _RESOLVER_API_MAX_IDS:
        raise HTTPException(status_code=400, detail=f"At most {_RESOLVER_API_MAX_IDS} ids can be resolved in one request.")
    return ids


def _resolver_snapshots_for_page(
    resolver_snapshots: List[dict],
    source: str,
    resolver: str,
) -> List[dict]:
    snapshots = [
        snapshot
        for snapshot in resolver_snapshots
        if snapshot.get("source") == source and snapshot.get("resolver") == resolver
    ]
    return sorted(
        snapshots,
        key=lambda snapshot: (snapshot.get("created_at") or "", snapshot.get("version") or ""),
        reverse=True,
    )


def _resolver_resolve_payload_for_api(source: str, resolver: str, version: str, payload: dict) -> dict:
    ids = _normalize_resolver_api_ids(payload)
    resolver_snapshot, resolver_instance, accepted_types, type_sensitive = _get_resolver_instance_for_api(source, resolver, version)
    requested_type = payload.get("input_type") or payload.get("type")
    if requested_type is not None:
        requested_type = str(requested_type).strip()
        if requested_type not in accepted_types:
            raise HTTPException(
                status_code=400,
                detail=f"input_type must be one of {accepted_types}; got {requested_type!r}",
            )
        input_types = [requested_type]
        result_specs = [(requested_type, requested_type)]
    elif type_sensitive:
        input_types = accepted_types
        result_specs = [(input_type, input_type) for input_type in accepted_types]
    else:
        input_types = ["Any accepted type"]
        result_specs = [(input_types[0], accepted_types[0])]

    return {
        "resolver_snapshot": resolver_snapshot.snapshot_id,
        "source": source,
        "resolver": resolver,
        "version": version,
        "accepted_types": accepted_types,
        "type_sensitive": type_sensitive,
        "input_types": input_types,
        "results_by_type": {
            result_label: _resolve_ids_for_type(resolver_instance, node_type, ids)
            for result_label, node_type in result_specs
        },
    }


def _resolver_prefix_counts_payload_for_api(source: str, resolver: str, version: str) -> dict:
    resolver_snapshot, resolver_instance, accepted_types, type_sensitive = _get_resolver_instance_for_api(source, resolver, version)
    try:
        prefix_counts = _resolver_prefix_counts_for_api(resolver_instance)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not load resolver prefix counts: {exc}") from exc
    return {
        "resolver_snapshot": resolver_snapshot.snapshot_id,
        "source": source,
        "resolver": resolver,
        "version": version,
        "accepted_types": accepted_types,
        "type_sensitive": type_sensitive,
        "prefix_counts": prefix_counts,
    }


def _resolver_examples_payload_for_api(source: str, resolver: str, version: str) -> dict:
    resolver_snapshot, resolver_instance, accepted_types, type_sensitive = _get_resolver_instance_for_api(source, resolver, version)
    try:
        example_ids = _resolver_example_ids_for_api(resolver_instance)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not load resolver examples: {exc}") from exc
    return {
        "resolver_snapshot": resolver_snapshot.snapshot_id,
        "source": source,
        "resolver": resolver,
        "version": version,
        "accepted_types": accepted_types,
        "type_sensitive": type_sensitive,
        "example_ids": example_ids,
    }


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request, "home.html", {
        "request": request,
    })


@app.get("/qa-browser", response_class=HTMLResponse)
async def qa_browser_home(request: Request):
    return templates.TemplateResponse(
        request,
        "qa_browser_home.html",
        _build_browser_home_context(request),
    )


@app.get("/ramp-id-qa", response_class=HTMLResponse)
def ramp_id_qa(request: Request, id: str = "", ids: str = "", stages: str = ""):
    query_id = (id or ids or "").strip()
    selected_stage_keys = _parse_metabolite_snapshot_key_filter(stages)
    result = None
    error = None
    overview = None
    if query_id:
        try:
            result = _load_metabolite_identifier_qa_many(query_id, selected_stage_keys)
        except Exception as exc:
            error = str(exc)
    else:
        overview = _load_harmonization_pipeline_workbench()
    return templates.TemplateResponse(request, "ramp_id_qa.html", {
        "request": request,
        "query_id": query_id,
        "selected_snapshot_keys": selected_stage_keys,
        "selected_stage_keys": selected_stage_keys,
        "result": result,
        "error": error,
        "overview": overview,
    })


@app.post("/ramp-id-qa/pipelines")
async def ramp_id_qa_save_pipeline(
    request: Request,
    pipeline_key: str = Form(""),
    pipeline_name: str = Form(""),
    enabled_rules: List[str] = Form([]),
    run_now: str = Form(""),
):
    form = await request.form()
    rule_parameters = {}
    for key, value in form.multi_items():
        if not key.startswith("rule_param__"):
            continue
        _prefix, rule_id, parameter_id = key.split("__", 2)
        rule_parameters.setdefault(rule_id, {})[parameter_id] = value
    pipeline = _upsert_harmonization_pipeline(pipeline_key, pipeline_name, enabled_rules, rule_parameters)
    if run_now:
        _enqueue_metabolite_snapshot_job(
            "run_pipeline",
            f"Run {pipeline.get('name')}",
            {"pipeline_key": pipeline["_key"]},
        )
    return _redirect_to("/ramp-id-qa")


@app.post("/ramp-id-qa/pipelines/{pipeline_key}/run")
def ramp_id_qa_run_pipeline(pipeline_key: str):
    _enqueue_metabolite_snapshot_job(
        "run_pipeline",
        f"Run {pipeline_key}",
        {"pipeline_key": pipeline_key},
    )
    return _redirect_to("/ramp-id-qa")


@app.post("/ramp-id-qa/pipelines/{pipeline_key}/rename")
def ramp_id_qa_rename_pipeline(pipeline_key: str, pipeline_name: str = Form("")):
    _rename_harmonization_pipeline(pipeline_key, pipeline_name)
    return _redirect_to("/ramp-id-qa")


@app.post("/ramp-id-qa/pipelines/{pipeline_key}/delete")
def ramp_id_qa_delete_pipeline(pipeline_key: str):
    _enqueue_metabolite_snapshot_job(
        "delete_pipeline",
        f"Delete {pipeline_key}",
        {"pipeline_key": pipeline_key},
    )
    return _redirect_to("/ramp-id-qa")


@app.get("/ramp-id-qa/stage-comparison", response_class=HTMLResponse)
def ramp_id_qa_stage_comparison(
    request: Request,
    left_stage: str = "",
    right_stage: str = "",
    limit: int = 100,
):
    comparison = None
    error = None
    normalized_limit = max(10, min(limit, 250))
    try:
        comparison = _load_metabolite_snapshot_comparison(left_stage, right_stage, normalized_limit)
    except Exception as exc:
        error = str(exc)
    if comparison is None:
        comparison = {
            "left_snapshot": None,
            "right_snapshot": None,
            "snapshots": _list_harmonization_stages(),
            "components": [],
            "display_limit": normalized_limit,
            "comparison_kind": "stage",
        }
    return templates.TemplateResponse(request, "ramp_id_snapshot_compare.html", {
        "request": request,
        "left_snapshot_key": left_stage,
        "right_snapshot_key": right_stage,
        "limit": normalized_limit,
        "comparison": comparison,
        "error": error,
    })


@app.get("/ramp-id-qa/stages/{stage_key}", response_class=HTMLResponse)
def ramp_id_qa_stage_stats(request: Request, stage_key: str):
    stats = None
    error = None
    try:
        stats = _load_harmonization_stage_stats(stage_key)
    except Exception as exc:
        error = str(exc)
    return templates.TemplateResponse(request, "ramp_id_stage_stats.html", {
        "request": request,
        "stage_key": stage_key,
        "stats": stats,
        "error": error,
    })


@app.get("/ramp-id-qa/harmonization-jobs", response_class=HTMLResponse)
def ramp_id_qa_harmonization_jobs(request: Request):
    overview = _load_harmonization_pipeline_workbench()
    return templates.TemplateResponse(request, "ramp_id_pipeline_table.html", {
        "request": request,
        "overview": overview,
    })


@app.get("/ramp-id-qa/api/metabolite")
def ramp_id_qa_metabolite(id: str = "", ids: str = "", stages: str = ""):
    query_id = (id or ids or "").strip()
    query_ids = _parse_metabolite_identifier_query(query_id)
    selected_snapshot_keys = _parse_metabolite_snapshot_key_filter(stages)
    if len(query_ids) == 1:
        return _load_metabolite_identifier_qa(query_ids[0], selected_snapshot_keys)
    return _load_metabolite_identifier_qa_many(query_id, selected_snapshot_keys)


@app.get("/registry", response_class=HTMLResponse)
def registry_home(request: Request):
    catalog, registry_error = _load_registry_catalog_categories([
        "source_snapshots",
        "derived_artifacts",
        "external_registrations",
    ])
    snapshots = catalog.get("source_snapshots", [])
    derived_artifacts = catalog.get("derived_artifacts", [])
    external_registrations = catalog.get("external_registrations", [])
    graph_usage_by_registry_id, graph_usage_error = load_graph_registry_usage_cached(
        credentials=_credentials,
        cache=_registry_usage_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    graph_filters = graph_usage_filters(graph_usage_by_registry_id)
    graph_styles = graph_usage_styles(graph_filters)

    snapshot_list = [
        with_graph_usages(snapshot, graph_usage_by_registry_id)
        for snapshot in snapshots
    ]
    derived_artifact_list = [
        with_graph_usages(artifact, graph_usage_by_registry_id)
        for artifact in derived_artifacts
    ]
    external_registration_list = [
        with_graph_usages(registration, graph_usage_by_registry_id)
        for registration in external_registrations
    ]

    grouped_source_list = []
    grouped_derived_source_list = []
    registry_stats = {
        "source_count": 0,
        "dataset_count": 0,
        "derived_count": 0,
        "external_count": 0,
        "total_size": "",
    }
    if not registry_error:
        grouped_source_list = group_by_source_dataset(snapshot_list, "snapshots")
        grouped_derived_source_list = group_by_source_dataset(derived_artifact_list, "artifacts")
        total_size_bytes = (
            sum(snapshot.get("total_size_bytes", 0) or 0 for snapshot in snapshots)
            + sum(artifact.get("total_size_bytes", 0) or 0 for artifact in derived_artifacts)
        )
        registry_stats = {
            "source_count": len(grouped_source_list),
            "dataset_count": sum(len(group["datasets"]) for group in grouped_source_list),
            "derived_count": len(derived_artifacts),
            "external_count": len(external_registrations),
            "total_size": DataRegistry.format_size(total_size_bytes),
        }

    return templates.TemplateResponse(request, "registry_home.html", {
        "request": request,
        "snapshots": snapshot_list,
        "derived_artifacts": derived_artifact_list,
        "external_registrations": external_registration_list,
        "grouped_sources": grouped_source_list,
        "grouped_derived_sources": grouped_derived_source_list,
        "registry_stats": registry_stats,
        "registry_error": registry_error,
        "graph_usage_error": graph_usage_error,
        "graph_usage_filters": graph_filters,
        "graph_usage_styles": graph_styles,
        "registry_update_status": _registry_update_status_context(),
    })


@app.post("/registry/update-status", response_class=HTMLResponse)
def registry_update_status(request: Request, return_page: str = Form("sources")):
    try:
        status = _run_registry_update_checks()
    except Exception as exc:
        status = {
            "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "elapsed_seconds": None,
            "sections": [],
            "error": str(exc),
        }
    _registry_update_status_cache.clear()
    _registry_update_status_cache.update(status)
    return_path = REGISTRY_UPDATE_RETURN_PATHS.get(return_page, "/registry")
    return _redirect_to(return_path, request=request, status_code=303)


@app.get("/registry/resolvers", response_class=HTMLResponse)
def registry_resolvers(request: Request):
    catalog, registry_error = _load_registry_catalog_categories(["resolver_snapshots"])
    resolver_snapshots = catalog.get("resolver_snapshots", [])
    graph_usage_by_registry_id, graph_usage_error = load_graph_registry_usage_cached(
        credentials=_credentials,
        cache=_registry_usage_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    graph_filters = graph_usage_filters(graph_usage_by_registry_id)
    graph_styles = graph_usage_styles(graph_filters)

    resolver_snapshot_list = [
        with_graph_usages(snapshot, graph_usage_by_registry_id)
        for snapshot in resolver_snapshots
    ]
    grouped_resolver_list = []
    registry_stats = {
        "source_count": 0,
        "resolver_count": 0,
        "snapshot_count": 0,
        "total_size": "",
    }
    if not registry_error:
        grouped_resolver_list = group_by_source_dataset(resolver_snapshot_list, "snapshots")
        total_size_bytes = sum(snapshot.get("total_size_bytes", 0) or 0 for snapshot in resolver_snapshots)
        registry_stats = {
            "source_count": len(grouped_resolver_list),
            "resolver_count": sum(len(group["datasets"]) for group in grouped_resolver_list),
            "snapshot_count": len(resolver_snapshots),
            "total_size": DataRegistry.format_size(total_size_bytes),
        }

    return templates.TemplateResponse(request, "registry_resolvers.html", {
        "request": request,
        "resolver_snapshots": resolver_snapshot_list,
        "grouped_resolvers": grouped_resolver_list,
        "registry_stats": registry_stats,
        "registry_error": registry_error,
        "graph_usage_error": graph_usage_error,
        "graph_usage_filters": graph_filters,
        "graph_usage_styles": graph_styles,
        "registry_update_status": _registry_update_status_context(),
    })


@app.get("/registry/graphs", response_class=HTMLResponse)
def registry_graphs(request: Request):
    graphs, graph_error = load_registry_graphs_cached(
        credentials=_credentials,
        cache=_registry_graph_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    registry_stats = {
        "graph_count": len(graphs),
        "adapter_count": sum(len(graph.get("adapters") or []) for graph in graphs),
        "resolver_count": sum(len(graph.get("resolvers") or []) for graph in graphs),
        "dependency_count": sum(
            sum(len(adapter.get("datasets") or []) for adapter in graph.get("adapters") or [])
            + sum(
                (1 if resolver.get("snapshot") else 0) + len(resolver.get("inputs") or [])
                for resolver in graph.get("resolvers") or []
            )
            for graph in graphs
        ),
    }

    return templates.TemplateResponse(request, "registry_graphs.html", {
        "request": request,
        "graphs": graphs,
        "registry_stats": registry_stats,
        "graph_error": graph_error,
        "registry_update_status": _registry_update_status_context(),
    })


@app.get("/registry/resolvers/{source}/{resolver}", response_class=HTMLResponse)
def registry_resolver_detail(request: Request, source: str, resolver: str, version: Optional[str] = None):
    catalog, registry_error = _load_registry_catalog_categories(["resolver_snapshots"])
    resolver_snapshots = catalog.get("resolver_snapshots", [])
    graph_usage_by_registry_id, graph_usage_error = load_graph_registry_usage_cached(
        credentials=_credentials,
        cache=_registry_usage_cache,
        ttl_seconds=_REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=get_sys_db,
        get_db=get_db,
    )
    graph_styles = graph_usage_styles(graph_usage_filters(graph_usage_by_registry_id))
    resolver_snapshot_list = [
        with_graph_usages(snapshot, graph_usage_by_registry_id)
        for snapshot in _resolver_snapshots_for_page(resolver_snapshots, source, resolver)
    ]
    for snapshot in resolver_snapshot_list:
        definition = snapshot.get("definition") or {}
        definition["type_sensitive"] = bool(definition.get("type_sensitive"))
    selected_snapshot = None
    if resolver_snapshot_list:
        selected_snapshot = next(
            (
                snapshot
                for snapshot in resolver_snapshot_list
                if snapshot.get("version") == version
            ),
            resolver_snapshot_list[0],
        )

    return templates.TemplateResponse(request, "registry_resolver_detail.html", {
        "request": request,
        "source": source,
        "resolver": resolver,
        "resolver_snapshots": resolver_snapshot_list,
        "selected_snapshot": selected_snapshot,
        "registry_error": registry_error,
        "graph_usage_error": graph_usage_error,
        "graph_usage_styles": graph_styles,
    })


@app.post("/registry/resolvers/{source}/{resolver}/{version}/resolve")
async def registry_resolver_resolve(source: str, resolver: str, version: str, request: Request):
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object.")

    return await run_in_threadpool(_resolver_resolve_payload_for_api, source, resolver, version, payload)


@app.get("/registry/resolvers/{source}/{resolver}/{version}/prefix-counts")
async def registry_resolver_prefix_counts(source: str, resolver: str, version: str):
    return await run_in_threadpool(_resolver_prefix_counts_payload_for_api, source, resolver, version)


@app.get("/registry/resolvers/{source}/{resolver}/{version}/examples")
async def registry_resolver_examples(source: str, resolver: str, version: str):
    return await run_in_threadpool(_resolver_examples_payload_for_api, source, resolver, version)


@app.get("/db/{db_name}", response_class=HTMLResponse)
def dashboard(request: Request, db_name: str):
    db = get_db(db_name)
    collections = _get_dashboard_collection_shell(db)

    # Edge definitions for schema summary
    edge_defs = _edge_definitions_with_endpoint_pairs(db)

    # ETL metadata
    etl_meta = None
    if db.has_collection("metadata_store"):
        try:
            cursor = db.aql.execute('RETURN DOCUMENT("metadata_store", "etl_metadata").value')
            results = list(cursor)
            if results and results[0]:
                etl_meta = results[0]
        except Exception:
            pass

    graph_views = _get_graph_views(db)
    registry_datasets = extract_registry_datasets(etl_meta)
    return templates.TemplateResponse(request, "dashboard.html", {
        "request": request,
        "db_name": db_name,
        "collections": collections,
        "edge_defs": edge_defs,
        "etl_meta": etl_meta,
        "registry_datasets": registry_datasets,
        "graph_views": graph_views,
        "doc_count": None,
        "edge_count": None,
        "counts_url": _app_path(f"/db/{db_name}/collections", request),
    })


@app.get("/db/{db_name}/collections", response_class=HTMLResponse)
def dashboard_collections(request: Request, db_name: str):
    db = get_db(db_name)
    collections = _get_dashboard_collection_summaries(db)
    return templates.TemplateResponse(request, "dashboard_collections.html", {
        "request": request,
        "db_name": db_name,
        "collections": collections,
        "doc_count": sum(c["count"] for c in collections if c["type"] == "document"),
        "edge_count": sum(c["count"] for c in collections if c["type"] == "edge"),
    })


@app.get("/db/{db_name}/build-status", response_class=HTMLResponse)
async def build_status_page(request: Request, db_name: str):
    db = get_db(db_name)
    build_status = _get_build_status(db)
    return templates.TemplateResponse(request, "build_status.html", {
        "request": request,
        "db_name": db_name,
        "build_status": build_status,
    })


@app.get("/db/{db_name}/view/{view_id}/preview", response_class=HTMLResponse)
async def preview_graph_view(request: Request, db_name: str, view_id: str, limit: int = 50):
    db = get_db(db_name)
    graph_views = _get_graph_views(db)
    graph_view = graph_views.get(view_id)

    if not graph_view:
        return HTMLResponse(f"Graph view '{view_id}' not found.", status_code=404)

    if graph_view.get("query_language") != "aql":
        return HTMLResponse("Only AQL graph views are supported.", status_code=400)

    if graph_view.get("output_format") != "jsonl":
        return HTMLResponse("Only JSONL graph views can be previewed.", status_code=400)

    query = graph_view.get("query")
    if not query:
        return HTMLResponse("Graph view is missing query metadata.", status_code=400)

    preview_limit = max(1, min(limit, 200))
    preview_query = f"""
    LET graph_view_rows = (
      {query}
    )
    RETURN {{
      total_count: LENGTH(graph_view_rows),
      rows: (
        FOR row IN graph_view_rows
          LIMIT @limit
          RETURN row
      )
    }}
    """

    try:
        result = next(iter(db.aql.execute(preview_query, bind_vars={"limit": preview_limit}, max_runtime=60)), None)
    except Exception as exc:
        return templates.TemplateResponse(request, "graph_view_preview.html", {
            "request": request,
            "db_name": db_name,
            "view_id": view_id,
            "graph_view": graph_view,
            "rows": [],
            "total_count": None,
            "preview_limit": preview_limit,
            "error": str(exc),
        })

    return templates.TemplateResponse(request, "graph_view_preview.html", {
        "request": request,
        "db_name": db_name,
        "view_id": view_id,
        "graph_view": graph_view,
        "rows": (result or {}).get("rows") or [],
        "total_count": (result or {}).get("total_count"),
        "preview_limit": preview_limit,
        "error": None,
    })


@app.get("/db/{db_name}/view/{view_id}")
async def execute_graph_view(db_name: str, view_id: str):
    db = get_db(db_name)
    graph_views = _get_graph_views(db)
    graph_view = graph_views.get(view_id)

    if not graph_view:
        return HTMLResponse(f"Graph view '{view_id}' not found.", status_code=404)

    if graph_view.get("query_language") != "aql":
        return HTMLResponse("Only AQL graph views are supported.", status_code=400)

    output_format = graph_view.get("output_format")
    if output_format not in {"csv", "jsonl"}:
        return HTMLResponse("Only CSV and JSONL graph views are supported.", status_code=400)

    query = graph_view.get("query")
    columns = graph_view.get("columns") or []
    if not query:
        return HTMLResponse("Graph view is missing query metadata.", status_code=400)
    if output_format == "csv" and not columns:
        return HTMLResponse("CSV graph view is missing columns metadata.", status_code=400)

    try:
        rows = list(db.aql.execute(query, max_runtime=60))
    except Exception as exc:
        return HTMLResponse(f"Failed to execute graph view '{view_id}': {exc}", status_code=500)

    if output_format == "jsonl":
        def generate_jsonl():
            for row in rows:
                yield json.dumps(row, default=str) + "\n"

        return StreamingResponse(
            generate_jsonl(),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f"attachment; filename={view_id}.jsonl"},
        )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        normalized_row = {}
        for column in columns:
            value = row.get(column) if isinstance(row, dict) else None
            if isinstance(value, (dict, list)):
                normalized_row[column] = json.dumps(value, default=str)
            elif value is None:
                normalized_row[column] = ""
            else:
                normalized_row[column] = value
        writer.writerow(normalized_row)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={view_id}.csv"},
    )


@app.get("/db/{db_name}/collection/{coll_name}", response_class=HTMLResponse)
def collection_browser(request: Request, db_name: str, coll_name: str,
                       page: int = 1, page_size: int = 25):
    db = get_db(db_name)
    skip = (page - 1) * page_size
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    filter_bind_vars = {}
    filter_clause = _build_collection_constraints(
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
        bind_vars=filter_bind_vars,
    )
    htmx = request.headers.get("HX-Request") == "true"

    if not htmx:
        coll = db.collection(coll_name)
        is_edge = coll.properties().get("type") in ("edge", 3)
        active_filter_summary = _build_active_filter_summary(
            db_name=db_name,
            coll_name=coll_name,
            page_size=page_size,
            active_filters=active_filters,
            search_term=search_term,
        )
        stats_url = _build_collection_stats_url(db_name, coll_name, active_filters, search_term=search_term)
        facets_url = _build_collection_facets_url(
            db_name=db_name,
            coll_name=coll_name,
            facet_filters=active_filters,
            search_term=search_term,
            page_size=page_size,
        )
        facet_loaders = _build_collection_facet_loaders(
            db_name=db_name,
            coll_name=coll_name,
            page_size=page_size,
            category_fields=category_fields,
            active_filters=active_filters,
            search_term=search_term,
        )
        rows_url = _build_collection_url(
            db_name=db_name,
            coll_name=coll_name,
            page=page,
            page_size=page_size,
            facet_filters=active_filters,
            search_term=search_term,
        )
        download_url = _build_collection_download_url(
            db_name=db_name,
            coll_name=coll_name,
            page=page,
            page_size=page_size,
            facet_filters=active_filters,
            search_term=search_term,
        )
        clear_search_url = _build_collection_url(
            db_name=db_name,
            coll_name=coll_name,
            page=1,
            page_size=page_size,
            facet_filters=active_filters,
            search_term="",
        )
        return templates.TemplateResponse(request, "collection.html", {
            "request": request,
            "db_name": db_name,
            "coll_name": coll_name,
            "docs": [],
            "columns": [],
            "is_edge": is_edge,
            "total": None,
            "page": page,
            "page_size": page_size,
            "total_pages": 1,
            "has_facets": bool(category_fields),
            "facet_panels": [],
            "facet_loaders": facet_loaders,
            "active_filters": active_filters,
            "active_filter_summary": active_filter_summary,
            "search_term": search_term,
            "search_fields": search_fields,
            "stats_url": stats_url,
            "facets_url": facets_url,
            "download_url": download_url,
            "clear_search_url": clear_search_url,
            "rows_url": rows_url,
            "loading_table": True,
            "prev_url": None,
            "next_url": None,
        })

    # Use AQL for both count and list so they always agree
    count_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            COLLECT WITH COUNT INTO c
            RETURN c
    """
    count_cursor = db.aql.execute(count_query, bind_vars=filter_bind_vars)
    total = list(count_cursor)[0]
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    skip = (page - 1) * page_size
    list_bind_vars = {**filter_bind_vars, "skip": skip, "top": page_size}

    coll = db.collection(coll_name)
    is_edge = coll.properties().get("type") in ("edge", 3)
    preview_fields = _get_collection_preview_fields(
        is_edge=is_edge,
        facet_metadata=facet_metadata,
        search_metadata=search_metadata,
    )
    list_bind_vars["return_fields"] = preview_fields

    # Fetch documents
    query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT doc._key ASC
            LIMIT @skip, @top
            RETURN KEEP(doc, @return_fields)
    """
    cursor = db.aql.execute(query, bind_vars=list_bind_vars)
    docs = list(cursor)
    # Discover columns from this page of results
    columns = _discover_columns(docs, is_edge)
    active_filter_summary = _build_active_filter_summary(
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        active_filters=active_filters,
        search_term=search_term,
    )
    stats_url = _build_collection_stats_url(db_name, coll_name, active_filters, search_term=search_term)
    facets_url = _build_collection_facets_url(
        db_name=db_name,
        coll_name=coll_name,
        facet_filters=active_filters,
        search_term=search_term,
        page_size=page_size,
    )
    facet_loaders = _build_collection_facet_loaders(
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        category_fields=category_fields,
        active_filters=active_filters,
        search_term=search_term,
    )
    download_url = _build_collection_download_url(db_name, coll_name, page, page_size, active_filters, search_term=search_term)
    clear_search_url = _build_collection_url(
        db_name=db_name,
        coll_name=coll_name,
        page=1,
        page_size=page_size,
        facet_filters=active_filters,
        search_term="",
    )
    prev_url = None
    next_url = None
    if page > 1:
        prev_url = _build_collection_url(db_name, coll_name, page - 1, page_size, active_filters, search_term=search_term)
    if page < total_pages:
        next_url = _build_collection_url(db_name, coll_name, page + 1, page_size, active_filters, search_term=search_term)

    return templates.TemplateResponse(request, "collection_rows.html", {
        "request": request,
        "db_name": db_name,
        "coll_name": coll_name,
        "docs": docs,
        "columns": columns,
        "is_edge": is_edge,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_facets": bool(category_fields),
        "facet_panels": [],
        "facet_loaders": facet_loaders,
        "active_filters": active_filters,
        "active_filter_summary": active_filter_summary,
        "search_term": search_term,
        "search_fields": search_fields,
        "stats_url": stats_url,
        "facets_url": facets_url,
        "download_url": download_url,
        "clear_search_url": clear_search_url,
        "rows_url": "",
        "loading_table": False,
        "prev_url": prev_url,
        "next_url": next_url,
    })


@app.get("/db/{db_name}/collection/{coll_name}/download.csv")
async def collection_download(request: Request, db_name: str, coll_name: str,
                              page: int = 1, page_size: int = 25):
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    filter_bind_vars = {}
    filter_clause = _build_collection_constraints(
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
        bind_vars=filter_bind_vars,
    )

    coll = db.collection(coll_name)
    is_edge = coll.properties().get("type") in ("edge", 3)
    preview_fields = _get_collection_preview_fields(
        is_edge=is_edge,
        facet_metadata=facet_metadata,
        search_metadata=search_metadata,
    )

    # Match the export columns to the current list-page view by rediscovering
    # columns from the currently visible page, then export all filtered rows.
    page = max(page, 1)
    page_size = max(page_size, 1)
    skip = (page - 1) * page_size
    preview_bind_vars = {**filter_bind_vars, "skip": skip, "top": page_size, "return_fields": preview_fields}
    preview_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT doc._key ASC
            LIMIT @skip, @top
            RETURN KEEP(doc, @return_fields)
    """
    preview_docs = list(db.aql.execute(preview_query, bind_vars=preview_bind_vars))
    columns = _discover_columns(preview_docs, is_edge)
    if not columns:
        columns = ["_key"]

    export_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT doc._key ASC
            RETURN doc
    """
    cursor = db.aql.execute(export_query, bind_vars=filter_bind_vars)

    def generate_csv():
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for doc in cursor:
            row = {column: _normalize_csv_value(doc.get(column)) for column in columns}
            writer.writerow(row)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    filename = f"{db_name}_{coll_name}.csv"
    return StreamingResponse(
        generate_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/db/{db_name}/collection/{coll_name}/facets", response_class=HTMLResponse)
def collection_facets(request: Request, db_name: str, coll_name: str, page_size: int = 25):
    """Facet panels for a collection (loaded after the main table)."""
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    facet_panels = _build_collection_facet_panels(
        db=db,
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        category_fields=category_fields,
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
    )
    return templates.TemplateResponse(request, "collection_facets.html", {
        "request": request,
        "facet_panels": facet_panels,
    })


@app.get("/db/{db_name}/collection/{coll_name}/facet/{field}", response_class=HTMLResponse)
def collection_facet(request: Request, db_name: str, coll_name: str, field: str, page_size: int = 25):
    """Single facet panel for a collection (loaded independently via HTMX)."""
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    if field not in category_fields:
        return templates.TemplateResponse(request, "collection_facet_panel.html", {
            "request": request,
            "panel": {
                "field": field,
                "title": field.replace("_", " "),
                "values": [],
                "selected_values": [],
                "clear_href": "#",
            },
        })
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    panel = _build_collection_facet_panel(
        db=db,
        db_name=db_name,
        coll_name=coll_name,
        page_size=page_size,
        field=field,
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
    )
    return templates.TemplateResponse(request, "collection_facet_panel.html", {
        "request": request,
        "panel": panel,
    })


@app.get("/db/{db_name}/collection/{coll_name}/stats", response_class=HTMLResponse)
def collection_stats(request: Request, db_name: str, coll_name: str):
    """Field coverage stats for a collection (loaded via HTMX)."""
    db = get_db(db_name)
    facet_metadata = _get_collection_facet_metadata(db, coll_name)
    search_metadata = _get_collection_search_metadata(db, coll_name)
    category_fields = sorted(facet_metadata.get("category_fields") or [])
    search_fields = list(search_metadata.get("text_fields") or [])
    active_filters = _parse_collection_facet_filters(request, category_fields)
    search_term = _parse_collection_search_term(request)
    filter_bind_vars = {}
    filter_clause = _build_collection_constraints(
        active_filters=active_filters,
        search_fields=search_fields,
        search_term=search_term,
        bind_vars=filter_bind_vars,
    )

    count_query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            COLLECT WITH COUNT INTO c
            RETURN c
    """
    count_results = list(db.aql.execute(count_query, bind_vars=filter_bind_vars))
    total = count_results[0] if count_results else 0
    sample_size = min(total, 500)
    sample_bind_vars = {**filter_bind_vars, "sample_size": sample_size}

    # Sample documents to discover fields
    query = f"""
        FOR doc IN `{coll_name}`
            {f"FILTER {filter_clause}" if filter_clause else ""}
            SORT RAND()
            LIMIT @sample_size
            RETURN ATTRIBUTES(doc)
    """
    cursor = db.aql.execute(query, bind_vars=sample_bind_vars)
    all_attrs = list(cursor)

    field_counts = {}
    for attrs in all_attrs:
        for attr in attrs:
            if attr.startswith("_"):
                continue
            field_counts[attr] = field_counts.get(attr, 0) + 1

    stats = []
    for field, count in sorted(field_counts.items(), key=lambda x: -x[1]):
        pct = round(100 * count / sample_size, 1) if sample_size > 0 else 0
        stats.append({"field": field, "count": count, "pct": pct, "sample_size": sample_size})

    return templates.TemplateResponse(request, "stats_partial.html", {
        "request": request,
        "stats": stats,
        "sample_size": sample_size,
        "total": total,
    })


@app.get("/db/{db_name}/collection/{coll_name}/doc/{doc_key:path}/parquet-stats", response_class=HTMLResponse)
async def parquet_stats(request: Request, db_name: str, coll_name: str, doc_key: str):
    """Load parquet file stats for a Dataset document (called via HTMX)."""
    db = get_db(db_name)
    coll = db.collection(coll_name)
    error = None
    stats = None

    try:
        doc = coll.get(doc_key)
        file_ref = doc.get("file_reference") if doc else None
        if not file_ref:
            error = "No file_reference found on this document."
        else:
            import pyarrow.parquet as pq
            buf, content_length = _get_parquet_buffer(file_ref)
            if buf is None:
                error = f"Could not fetch parquet file: {file_ref}"
            else:
                pf = pq.ParquetFile(buf)
                metadata = pf.metadata

                # Read into pandas for descriptive stats
                table = pf.read()
                df = table.to_pandas()

                col_stats = []
                for col_name in df.columns:
                    col = df[col_name]
                    info = {"name": col_name, "dtype": str(col.dtype), "non_null": int(col.count())}
                    if col.dtype.kind in ("f", "i", "u"):  # numeric
                        info["min"] = f"{col.min():.4g}"
                        info["max"] = f"{col.max():.4g}"
                        info["mean"] = f"{col.mean():.4g}"
                        info["std"] = f"{col.std():.4g}"
                    else:
                        info["unique"] = int(col.nunique())
                    col_stats.append(info)

                stats = {
                    "file_path": file_ref,
                    "file_size_mb": round(content_length / (1024 * 1024), 2),
                    "num_rows": metadata.num_rows,
                    "num_columns": metadata.num_columns,
                    "num_row_groups": metadata.num_row_groups,
                    "index_name": df.index.name,
                    "index_count": len(df.index),
                    "columns": col_stats,
                    "head": df.head(5).to_html(classes="parquet-table", border=0),
                }
    except Exception as e:
        error = str(e)

    return templates.TemplateResponse(request, "parquet_stats.html", {
        "request": request,
        "stats": stats,
        "error": error,
    })


@app.get("/db/{db_name}/collection/{coll_name}/doc/{doc_key:path}", response_class=HTMLResponse)
async def document_detail(request: Request, db_name: str, coll_name: str, doc_key: str):
    db = get_db(db_name)
    coll = db.collection(coll_name)

    try:
        doc = coll.get(doc_key)
    except Exception:
        doc = None

    is_edge = coll.properties().get("type") in ("edge", 3) if doc else False

    # Parse provenance/sources for nicer display
    sources = []
    if doc and "sources" in doc and doc["sources"]:
        for src in doc["sources"]:
            if isinstance(src, str) and "\t" in src:
                parts = src.split("\t")
                sources.append({
                    "name": parts[0] if len(parts) > 0 else "",
                    "version": parts[1] if len(parts) > 1 else "",
                    "version_date": parts[2] if len(parts) > 2 else "",
                    "download_date": parts[3] if len(parts) > 3 else "",
                })
            else:
                sources.append({"name": str(src), "version": "", "version_date": "", "download_date": ""})

    # Categorize fields for display
    scalar_fields, list_fields, nested_fields = _categorize_document_fields(doc)

    # Find connected nodes via graph traversal (vertex nodes only)
    outgoing_linked_groups = []
    incoming_linked_groups = []
    linked_aql = ""
    if doc and not is_edge and db.has_graph("graph"):
        try:
            _SAMPLE_LIMIT = 20
            linked_aql = (
                f"LET outgoing_counts = (\n"
                f"    FOR v, e IN 1..1 OUTBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt\n"
                f"    RETURN {{coll, edge_coll, cnt}}\n"
                f")\n"
                f"LET incoming_counts = (\n"
                f"    FOR v, e IN 1..1 INBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt\n"
                f"    RETURN {{coll, edge_coll, cnt}}\n"
                f")\n"
                f"RETURN {{\n"
                f"    outgoing: (\n"
                f"        FOR c IN outgoing_counts\n"
                f"            SORT c.cnt DESC, c.edge_coll\n"
                f"            LET samples = (\n"
                f"                FOR v2, e2 IN 1..1 OUTBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"                FILTER SPLIT(v2._id, '/')[0] == c.coll\n"
                f"                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll\n"
                f"                LIMIT {_SAMPLE_LIMIT}\n"
                f"                RETURN {{key: v2._key, label: v2.name || v2.symbol || v2._key}}\n"
                f"            )\n"
                f"            RETURN {{collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}}\n"
                f"    ),\n"
                f"    incoming: (\n"
                f"        FOR c IN incoming_counts\n"
                f"            SORT c.cnt DESC, c.edge_coll\n"
                f"            LET samples = (\n"
                f"                FOR v2, e2 IN 1..1 INBOUND '{doc['_id']}' GRAPH 'graph'\n"
                f"                FILTER SPLIT(v2._id, '/')[0] == c.coll\n"
                f"                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll\n"
                f"                LIMIT {_SAMPLE_LIMIT}\n"
                f"                RETURN {{key: v2._key, label: v2.name || v2.symbol || v2._key}}\n"
                f"            )\n"
                f"            RETURN {{collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}}\n"
                f"    )\n"
                f"}}"
            )
            cursor = db.aql.execute(
                """
                LET outgoing_counts = (
                    FOR v, e IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt
                    RETURN {coll, edge_coll, cnt}
                )
                LET incoming_counts = (
                    FOR v, e IN 1..1 INBOUND @node_id GRAPH 'graph'
                    COLLECT coll = SPLIT(v._id, '/')[0], edge_coll = SPLIT(e._id, '/')[0] WITH COUNT INTO cnt
                    RETURN {coll, edge_coll, cnt}
                )
                RETURN {
                    outgoing: (
                        FOR c IN outgoing_counts
                            SORT c.cnt DESC, c.edge_coll
                            LET samples = (
                                FOR v2, e2 IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                                FILTER SPLIT(v2._id, '/')[0] == c.coll
                                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll
                                LIMIT @sample_limit
                                RETURN {key: v2._key, label: v2.name || v2.symbol || v2._key}
                            )
                            RETURN {collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}
                    ),
                    incoming: (
                        FOR c IN incoming_counts
                            SORT c.cnt DESC, c.edge_coll
                            LET samples = (
                                FOR v2, e2 IN 1..1 INBOUND @node_id GRAPH 'graph'
                                FILTER SPLIT(v2._id, '/')[0] == c.coll
                                FILTER SPLIT(e2._id, '/')[0] == c.edge_coll
                                LIMIT @sample_limit
                                RETURN {key: v2._key, label: v2.name || v2.symbol || v2._key}
                            )
                            RETURN {collection: c.coll, edge_type: c.edge_coll, count: c.cnt, nodes: samples}
                    )
                }
                """,
                bind_vars={"node_id": doc["_id"], "sample_limit": _SAMPLE_LIMIT},
                max_runtime=15,
            )
            linked_result = list(cursor)
            if linked_result:
                outgoing_linked_groups = linked_result[0].get("outgoing", [])
                incoming_linked_groups = linked_result[0].get("incoming", [])
        except Exception:
            pass

    template_name = _get_document_template(db_name, coll_name)

    context = {
        "request": request,
        "db_name": db_name,
        "coll_name": coll_name,
        "doc": doc,
        "doc_key": doc_key,
        "is_edge": is_edge,
        "sources": sources,
        "scalar_fields": scalar_fields,
        "list_fields": list_fields,
        "nested_fields": nested_fields,
        "outgoing_linked_groups": outgoing_linked_groups,
        "incoming_linked_groups": incoming_linked_groups,
        "linked_aql": linked_aql,
        "this_node_label": f"This {coll_name}",
    }
    if template_name == "cure_case_report_document.html":
        context.update(_get_adjacent_collection_docs(db, coll_name, doc))
        context.update(_build_cure_case_report_context(db_name, db, doc))
    return templates.TemplateResponse(request, template_name, context)


@app.get("/db/{db_name}/schema", response_class=HTMLResponse)
async def schema_view(request: Request, db_name: str):
    db = get_db(db_name)
    edge_defs = _edge_definitions_with_endpoint_pairs(db)

    # Build mermaid diagram
    mermaid_lines = ["graph LR"]
    nodes_seen = set()
    for ed in edge_defs:
        edge_name = ed["edge_collection"]
        for endpoint_pair in ed.get("endpoint_pairs") or []:
            frm = endpoint_pair["from_collection"]
            to = endpoint_pair["to_collection"]
            safe_from = frm.replace(":", "_").replace(" ", "_")
            safe_to = to.replace(":", "_").replace(" ", "_")
            if safe_from not in nodes_seen:
                mermaid_lines.append(f'    {safe_from}["{frm}"]')
                nodes_seen.add(safe_from)
            if safe_to not in nodes_seen:
                mermaid_lines.append(f'    {safe_to}["{to}"]')
                nodes_seen.add(safe_to)
            mermaid_lines.append(f"    {safe_from} -->|{edge_name}| {safe_to}")

    mermaid_text = "\n".join(mermaid_lines)

    return templates.TemplateResponse(request, "schema.html", {
        "request": request,
        "db_name": db_name,
        "edge_defs": edge_defs,
        "mermaid_text": mermaid_text,
    })


@app.get("/db/{db_name}/aql", response_class=HTMLResponse)
async def aql_page(request: Request, db_name: str):
    return templates.TemplateResponse(request, "aql.html", {
        "request": request,
        "db_name": db_name,
        "results": None,
        "query": "",
        "error": None,
        "columns": [],
    })


@app.post("/db/{db_name}/aql", response_class=HTMLResponse)
async def aql_execute(request: Request, db_name: str, query: str = Form(...)):
    results = None
    error = None
    columns = []
    try:
        db = get_db(db_name)
        cursor = db.aql.execute(query, max_runtime=60)
        results = list(cursor)
        # Auto-detect columns from results
        if results and isinstance(results[0], dict):
            col_set = set()
            for row in results[:50]:
                col_set.update(row.keys())
            columns = sorted(col_set)
    except Exception as e:
        error = str(e)

    htmx = request.headers.get("HX-Request") == "true"
    template = "aql_results.html" if htmx else "aql.html"

    return templates.TemplateResponse(request, template, {
        "request": request,
        "db_name": db_name,
        "results": results,
        "query": query,
        "error": error,
        "columns": columns,
    })


# ── MySQL Routes ─────────────────────────────────────────────────────────────

def _mysql_template_context(source_id: str, db_name: str) -> dict:
    source = _get_mysql_source(source_id)
    return {
        "mysql_source_id": source_id,
        "mysql_source_label": source["label"],
        "db_name": db_name,
    }


@app.get("/mysql/{source_id}/{db_name}", response_class=HTMLResponse)
async def mysql_dashboard(request: Request, source_id: str, db_name: str):
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    schema_meta = get_mysql_inspector(db_name, source_id=source_id)

    # SHOW TABLE STATUS returns approximate row counts — much faster than COUNT(*) for InnoDB
    with engine.connect() as conn:
        status_rows = conn.execute(text("SHOW TABLE STATUS")).mappings().all()
    row_counts = {r["Name"]: (r["Rows"] or 0) for r in status_rows}

    tables = []
    for table_name, meta in schema_meta.items():
        tables.append({
            "name": table_name,
            "count": row_counts.get(table_name, 0),
            "fk_count": len(meta["fks"]),
        })
    tables.sort(key=lambda t: t["name"])

    fk_defs = []
    for table_name, meta in schema_meta.items():
        for fk in meta["fks"]:
            fk_defs.append({
                "from_table": table_name,
                "from_columns": ", ".join(fk["constrained_columns"]),
                "to_table": fk["referred_table"],
                "to_columns": ", ".join(fk["referred_columns"]),
            })

    return templates.TemplateResponse(request, "mysql_dashboard.html", {
        "request": request,
        "tables": tables,
        "fk_defs": fk_defs,
        "table_count": len(tables),
        "total_rows": sum(t["count"] for t in tables),
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{source_id}/{db_name}/table/{table_name}", response_class=HTMLResponse)
async def mysql_table_browser(request: Request, source_id: str, db_name: str, table_name: str,
                               page: int = 1, page_size: int = 25):
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    meta = get_mysql_inspector(db_name, source_id=source_id)[table_name]

    columns_info = meta["columns"]
    pk_cols = meta["pk"]
    column_names = [c["name"] for c in columns_info]

    # Column ordering: PK first, then priority names, then FK columns, then alpha
    fks = meta["fks"]
    fk_col_names = set()
    for fk in fks:
        fk_col_names.update(fk["constrained_columns"])

    priority = list(pk_cols)
    for name in ["name", "symbol", "type", "description"]:
        if name in column_names and name not in priority:
            priority.append(name)
    fk_ordered = [c for c in column_names if c in fk_col_names and c not in priority]
    remaining = sorted(set(column_names) - set(priority) - set(fk_ordered))
    ordered_columns = priority + fk_ordered + remaining

    offset = (page - 1) * page_size
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()
        rows = conn.execute(
            text(f"SELECT * FROM `{table_name}` LIMIT :limit OFFSET :offset"),
            {"limit": page_size, "offset": offset}
        ).mappings().all()

    total_pages = max(1, (total + page_size - 1) // page_size)

    htmx = request.headers.get("HX-Request") == "true"
    template = "mysql_table_rows.html" if htmx else "mysql_table.html"

    return templates.TemplateResponse(request, template, {
        "request": request,
        "table_name": table_name,
        "rows": [dict(r) for r in rows],
        "columns": ordered_columns,
        "pk_cols": pk_cols,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{source_id}/{db_name}/table/{table_name}/stats", response_class=HTMLResponse)
async def mysql_table_stats(request: Request, source_id: str, db_name: str, table_name: str):
    """Column coverage stats for a MySQL table (loaded via HTMX)."""
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    columns = get_mysql_inspector(db_name, source_id=source_id)[table_name]["columns"]

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`")).scalar()

        stats = []
        for col in columns:
            col_name = col["name"]
            non_null = conn.execute(
                text(f"SELECT COUNT(`{col_name}`) FROM `{table_name}`")
            ).scalar()
            pct = round(100 * non_null / total, 1) if total > 0 else 0
            stats.append({"field": col_name, "count": non_null, "pct": pct})

    return templates.TemplateResponse(request, "stats_partial.html", {
        "request": request,
        "stats": stats,
        "sample_size": total,
        "total": total,
    })


@app.get("/mysql/{source_id}/{db_name}/table/{table_name}/row/{pk_value:path}", response_class=HTMLResponse)
async def mysql_row_detail(request: Request, source_id: str, db_name: str, table_name: str, pk_value: str):
    engine = get_mysql_db_engine(db_name, source_id=source_id)
    meta = get_mysql_inspector(db_name, source_id=source_id)[table_name]
    pk_cols = meta["pk"]

    # Parse pk_value: "123" for single PK, "col1=val1/col2=val2" for composite
    where_clauses = []
    bind_params = {}
    if "=" in pk_value:
        parts = pk_value.split("/")
        for part in parts:
            col, val = part.split("=", 1)
            where_clauses.append(f"`{col}` = :pk_{col}")
            bind_params[f"pk_{col}"] = val
    elif pk_cols:
        where_clauses.append(f"`{pk_cols[0]}` = :pk_val")
        bind_params["pk_val"] = pk_value

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=0"

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM `{table_name}` WHERE {where_sql}"), bind_params)
        row = result.mappings().first()

    # Build FK links for navigation
    fk_links = {}
    if row:
        for fk in meta["fks"]:
            for local_col, ref_col in zip(fk["constrained_columns"], fk["referred_columns"]):
                val = row.get(local_col)
                if val is not None:
                    fk_links[local_col] = {
                        "table": fk["referred_table"],
                        "column": ref_col,
                        "value": val,
                        "url": _app_path(f"/mysql/{source_id}/{db_name}/table/{fk['referred_table']}/row/{val}", request),
                    }

    # Get column metadata
    columns_info = {c["name"]: str(c["type"]) for c in meta["columns"]}

    return templates.TemplateResponse(request, "mysql_row.html", {
        "request": request,
        "table_name": table_name,
        "row": dict(row) if row else None,
        "pk_value": pk_value,
        "pk_cols": pk_cols,
        "fk_links": fk_links,
        "columns_info": columns_info,
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{source_id}/{db_name}/schema", response_class=HTMLResponse)
async def mysql_schema(request: Request, source_id: str, db_name: str):
    schema_meta = get_mysql_inspector(db_name, source_id=source_id)

    fk_defs = []
    mermaid_lines = ["erDiagram"]

    for table_name, meta in schema_meta.items():
        for fk in meta["fks"]:
            ref_table = fk["referred_table"]
            label = ", ".join(fk["constrained_columns"])
            safe_from = table_name.replace(" ", "_")
            safe_to = ref_table.replace(" ", "_")
            mermaid_lines.append(f'    {safe_from} }}|--|| {safe_to} : "{label}"')
            fk_defs.append({
                "from_table": table_name,
                "from_columns": ", ".join(fk["constrained_columns"]),
                "to_table": ref_table,
                "to_columns": ", ".join(fk["referred_columns"]),
            })

    mermaid_text = "\n".join(mermaid_lines)

    return templates.TemplateResponse(request, "mysql_schema.html", {
        "request": request,
        "fk_defs": fk_defs,
        "mermaid_text": mermaid_text,
        **_mysql_template_context(source_id, db_name),
    })


@app.post("/mysql/{source_id}/{db_name}/refresh-schema", response_class=HTMLResponse)
async def mysql_refresh_schema(request: Request, source_id: str, db_name: str):
    """Bust the schema cache for a database and redirect to dashboard."""
    invalidate_mysql_inspector(db_name, source_id=source_id)
    return _redirect_to(f"/mysql/{source_id}/{db_name}", request=request, status_code=303)


@app.get("/mysql/{source_id}/{db_name}/sql", response_class=HTMLResponse)
async def sql_page(request: Request, source_id: str, db_name: str):
    return templates.TemplateResponse(request, "mysql_sql.html", {
        "request": request,
        "results": None,
        "query": "",
        "error": None,
        "columns": [],
        **_mysql_template_context(source_id, db_name),
    })


@app.post("/mysql/{source_id}/{db_name}/sql", response_class=HTMLResponse)
async def sql_execute(request: Request, source_id: str, db_name: str, query: str = Form(...)):
    results = None
    error = None
    columns = []

    # Read-only guard
    query_upper = query.strip().upper()
    allowed = ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN")
    if not any(query_upper.startswith(kw) for kw in allowed):
        error = "Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are allowed."
    else:
        try:
            engine = get_mysql_db_engine(db_name, source_id=source_id)
            with engine.connect() as conn:
                result = conn.execute(text(query))
                if result.returns_rows:
                    columns = list(result.keys())
                    results = [dict(row) for row in result.mappings().all()]
                else:
                    results = []
        except Exception as e:
            error = str(e)

    htmx = request.headers.get("HX-Request") == "true"
    template = "mysql_sql_results.html" if htmx else "mysql_sql.html"

    return templates.TemplateResponse(request, template, {
        "request": request,
        "results": results,
        "query": query,
        "error": error,
        "columns": columns,
        **_mysql_template_context(source_id, db_name),
    })


@app.get("/mysql/{db_name}", response_class=HTMLResponse)
async def mysql_dashboard_default(request: Request, db_name: str):
    return _redirect_to(f"/mysql/default/{db_name}", request=request, status_code=307)


@app.get("/mysql/{db_name}/table/{table_name}", response_class=HTMLResponse)
async def mysql_table_browser_default(request: Request, db_name: str, table_name: str, page: int = 1, page_size: int = 25):
    return _redirect_to(
        f"/mysql/default/{db_name}/table/{table_name}?page={page}&page_size={page_size}",
        request=request,
        status_code=307,
    )


@app.get("/mysql/{db_name}/table/{table_name}/stats", response_class=HTMLResponse)
async def mysql_table_stats_default(request: Request, db_name: str, table_name: str):
    return _redirect_to(f"/mysql/default/{db_name}/table/{table_name}/stats", request=request, status_code=307)


@app.get("/mysql/{db_name}/table/{table_name}/row/{pk_value:path}", response_class=HTMLResponse)
async def mysql_row_detail_default(request: Request, db_name: str, table_name: str, pk_value: str):
    return _redirect_to(f"/mysql/default/{db_name}/table/{table_name}/row/{pk_value}", request=request, status_code=307)


@app.get("/mysql/{db_name}/schema", response_class=HTMLResponse)
async def mysql_schema_default(request: Request, db_name: str):
    return _redirect_to(f"/mysql/default/{db_name}/schema", request=request, status_code=307)


@app.post("/mysql/{db_name}/refresh-schema", response_class=HTMLResponse)
async def mysql_refresh_schema_default(request: Request, db_name: str):
    return _redirect_to(f"/mysql/default/{db_name}/refresh-schema", request=request, status_code=307)


@app.get("/mysql/{db_name}/sql", response_class=HTMLResponse)
async def sql_page_default(request: Request, db_name: str):
    return _redirect_to(f"/mysql/default/{db_name}/sql", request=request, status_code=307)


@app.post("/mysql/{db_name}/sql", response_class=HTMLResponse)
async def sql_execute_default(request: Request, db_name: str):
    return _redirect_to(f"/mysql/default/{db_name}/sql", request=request, status_code=307)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _discover_columns(docs: list, is_edge: bool) -> list:
    """Pick the most useful columns from a set of documents."""
    if not docs:
        return []
    col_set = set()
    for doc in docs:
        col_set.update(doc.keys())

    # Remove internal arango keys except _key
    internal = {"_id", "_rev"}
    if not is_edge:
        internal.update({"_from", "_to"})
    col_set -= internal

    # Order: _key first, then _from/_to for edges, then id, then alpha
    priority = ["_key", "_from", "_to", "id", "name", "symbol", "type", "description"]
    ordered = [c for c in priority if c in col_set]
    remaining = sorted(col_set - set(ordered))
    return ordered + remaining


def _truncate(value, max_len=80):
    """Truncate a value for table display."""
    s = str(value)
    if len(s) > max_len:
        return s[:max_len] + "..."
    return s


def _normalize_csv_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if value is None:
        return ""
    return value


def _categorize_document_fields(doc: dict | None):
    skip_keys = {"_key", "_id", "_rev", "_from", "_to", "sources", "provenance", "xref", "labels", "creation"}
    scalar_fields = []
    list_fields = []
    nested_fields = []

    if doc:
        for key, val in doc.items():
            if key in skip_keys or key.startswith("_"):
                continue
            if val is None or isinstance(val, (str, int, float, bool)):
                scalar_fields.append((key, val))
            elif isinstance(val, dict):
                nested_fields.append((key, val))
            elif isinstance(val, list):
                if val and isinstance(val[0], dict):
                    nested_fields.append((key, val))
                elif val:
                    list_fields.append((key, val))
                else:
                    scalar_fields.append((key, val))
            else:
                scalar_fields.append((key, val))

    return scalar_fields, list_fields, nested_fields


def _get_adjacent_collection_docs(db, coll_name: str, doc: dict | None) -> dict:
    if not doc:
        return {"previous_doc": None, "next_doc": None, "adjacent_error": None}

    current_key = doc.get("_key")
    if not current_key:
        return {"previous_doc": None, "next_doc": None, "adjacent_error": "missing _key"}

    try:
        cursor = db.aql.execute(
            f"""
            LET previous_doc = FIRST(
                FOR d IN `{coll_name}`
                FILTER d._key < @current_key
                SORT d._key DESC
                LIMIT 1
                RETURN KEEP(d, "_key", "id", "name")
            )
            LET next_doc = FIRST(
                FOR d IN `{coll_name}`
                FILTER d._key > @current_key
                SORT d._key ASC
                LIMIT 1
                RETURN KEEP(d, "_key", "id", "name")
            )
            RETURN {{
                previous_doc: previous_doc,
                next_doc: next_doc
            }}
            """,
            bind_vars={"current_key": current_key},
            max_runtime=10,
        )
        result = next(iter(cursor), {"previous_doc": None, "next_doc": None})
        result["adjacent_error"] = None
        return result
    except Exception as exc:
        return {"previous_doc": None, "next_doc": None, "adjacent_error": str(exc)}


def _build_cure_case_report_context(db_name: str, db, doc: dict | None) -> dict:
    if not doc or not db.has_graph("graph"):
        return {"cure_case_url": _build_cure_case_url(db_name, doc)}

    try:
        cursor = db.aql.execute(
            """
            FOR v, e IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(v._id, '/')[0] == 'Patient'
            FILTER SPLIT(e._id, '/')[0] == 'CaseReportPatientEdge'
            LIMIT 1
            RETURN v
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        patient_doc = next(iter(cursor), None)
    except Exception:
        patient_doc = None

    try:
        reporter_cursor = db.aql.execute(
            """
            FOR v, e IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(v._id, '/')[0] == 'Reporter'
            FILTER SPLIT(e._id, '/')[0] == 'CaseReportReporterEdge'
            LIMIT 1
            RETURN v
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        reporter_doc = next(iter(reporter_cursor), None)
    except Exception:
        reporter_doc = None

    try:
        clinical_context_cursor = db.aql.execute(
            """
            LET via_patient = (
              FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                FILTER SPLIT(patient._id, '/')[0] == 'Patient'
                FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
                FOR clinical_context, clinical_context_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
                  FILTER SPLIT(clinical_context._id, '/')[0] == 'ClinicalContext'
                  FILTER SPLIT(clinical_context_edge._id, '/')[0] == 'PatientClinicalContextEdge'
                  LIMIT 1
                  RETURN clinical_context
            )
            LET via_report = (
              FOR clinical_context, clinical_context_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
                FILTER SPLIT(clinical_context._id, '/')[0] == 'ClinicalContext'
                FILTER SPLIT(clinical_context_edge._id, '/')[0] == 'CaseReportClinicalContextEdge'
                LIMIT 1
                RETURN clinical_context
            )
            RETURN FIRST(APPEND(via_patient, via_report))
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        clinical_context_doc = next(iter(clinical_context_cursor), None)
    except Exception:
        clinical_context_doc = None

    try:
        primary_episode_cursor = db.aql.execute(
            """
            FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(patient._id, '/')[0] == 'Patient'
            FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
            FOR episode, patient_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
            FILTER SPLIT(episode._id, '/')[0] == 'Episode'
            FILTER SPLIT(patient_edge._id, '/')[0] == 'PersonEpisodeEdge'
            FILTER episode.role == 'primary'
            LIMIT 1
            RETURN episode
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        primary_episode_doc = next(iter(primary_episode_cursor), None)
    except Exception:
        primary_episode_doc = None

    try:
        acute_episode_cursor = db.aql.execute(
            """
            FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(patient._id, '/')[0] == 'Patient'
            FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
            FOR episode, patient_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
            FILTER SPLIT(episode._id, '/')[0] == 'Episode'
            FILTER SPLIT(patient_edge._id, '/')[0] == 'PersonEpisodeEdge'
            FILTER episode.role == 'contextual'
            FILTER LENGTH(
                FOR condition, condition_edge IN 1..1 OUTBOUND episode._id GRAPH 'graph'
                  FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                  FILTER SPLIT(condition_edge._id, '/')[0] == 'EpisodeConditionEdge'
                  FILTER condition.slug == 'acute-covid-19'
                  LIMIT 1
                  RETURN 1
            ) > 0
            LIMIT 1
            RETURN episode
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        acute_episode_doc = next(iter(acute_episode_cursor), None)
    except Exception:
        acute_episode_doc = None

    try:
        pregnancy_episode_cursor = db.aql.execute(
            """
            FOR patient, report_edge IN 1..1 OUTBOUND @node_id GRAPH 'graph'
            FILTER SPLIT(patient._id, '/')[0] == 'Patient'
            FILTER SPLIT(report_edge._id, '/')[0] == 'CaseReportPatientEdge'
            FOR episode, patient_edge IN 1..1 OUTBOUND patient._id GRAPH 'graph'
            FILTER SPLIT(episode._id, '/')[0] == 'Episode'
            FILTER SPLIT(patient_edge._id, '/')[0] == 'PersonEpisodeEdge'
            FILTER episode.role == 'contextual'
            FILTER LENGTH(
                FOR condition, condition_edge IN 1..1 OUTBOUND episode._id GRAPH 'graph'
                  FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                  FILTER SPLIT(condition_edge._id, '/')[0] == 'EpisodeConditionEdge'
                  FILTER condition.slug == 'pregnancy'
                  LIMIT 1
                  RETURN 1
            ) > 0
            LIMIT 1
            RETURN episode
            """,
            bind_vars={"node_id": doc["_id"]},
            max_runtime=10,
        )
        pregnancy_episode_doc = next(iter(pregnancy_episode_cursor), None)
    except Exception:
        pregnancy_episode_doc = None

    patient_scalar_fields, patient_list_fields, patient_nested_fields = _categorize_document_fields(patient_doc)
    reporter_scalar_fields, reporter_list_fields, reporter_nested_fields = _categorize_document_fields(reporter_doc)
    clinical_context_scalar_fields, clinical_context_list_fields, clinical_context_nested_fields = _categorize_document_fields(clinical_context_doc)
    primary_episode_scalar_fields, primary_episode_list_fields, primary_episode_nested_fields = _categorize_document_fields(primary_episode_doc)
    acute_episode_scalar_fields, acute_episode_list_fields, acute_episode_nested_fields = _categorize_document_fields(acute_episode_doc)
    pregnancy_episode_scalar_fields, pregnancy_episode_list_fields, pregnancy_episode_nested_fields = _categorize_document_fields(pregnancy_episode_doc)

    episode_relationship_cards = []
    clinical_context_condition_cards = []
    clinical_context_phenotype_cards = []
    background_context_doc = None
    background_context_scalar_fields = []
    background_context_list_fields = []
    background_context_nested_fields = []
    perinatal_context_doc = None
    perinatal_context_scalar_fields = []
    perinatal_context_list_fields = []
    perinatal_context_nested_fields = []
    perinatal_context_phenotype_cards = []
    rasopathies_diagnosis_cards = []
    rasopathies_drug_treatment_cards = []
    patient_prior_condition_cards = []
    background_regular_medicine_cards = []
    background_immunosuppressant_cards = []
    primary_episode_post_covid_condition_cards = []
    exposure_cards = []
    acute_exposure_cards = []
    pregnancy_exposure_cards = []
    acute_complication_cards = []
    acute_vaccination_card = None
    phenotype_cards = []
    therapy_cards = []
    treatment_cards = []
    outcome_cards = []

    if clinical_context_doc:
        try:
            clinical_context_condition_cursor = db.aql.execute(
                """
                FOR condition, condition_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                FILTER SPLIT(condition_edge._id, '/')[0] == 'ClinicalContextConditionEdge'
                SORT condition.name, condition._key
                RETURN {
                    condition: condition,
                    condition_edge: condition_edge
                }
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=15,
            )
            for row in clinical_context_condition_cursor:
                condition_doc = row.get("condition")
                condition_edge_doc = row.get("condition_edge")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                clinical_context_condition_cards.append({
                    "condition_doc": condition_doc,
                    "condition_edge_doc": condition_edge_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            clinical_context_condition_cards = []

        try:
            clinical_context_phenotype_cursor = db.aql.execute(
                """
                LET via_finding = (
                  FOR finding, finding_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                    FILTER SPLIT(finding._id, '/')[0] == 'Finding'
                    FILTER SPLIT(finding_edge._id, '/')[0] == 'ClinicalContextFindingEdge'
                    FOR phenotype, phenotype_edge IN 1..1 OUTBOUND finding._id GRAPH 'graph'
                      FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                      FILTER SPLIT(phenotype_edge._id, '/')[0] == 'FindingPhenotypeEdge'
                      RETURN {
                        phenotype: phenotype,
                        finding: finding,
                        finding_edge: finding_edge,
                        phenotype_edge: phenotype_edge,
                        sort_group: finding.group,
                        sort_name: phenotype.name
                      }
                )
                LET direct = (
                  FOR phenotype, phenotype_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                    FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(phenotype_edge._id, '/')[0] == 'ClinicalContextPhenotypeEdge'
                    RETURN {
                      phenotype: phenotype,
                      finding: null,
                      finding_edge: null,
                      phenotype_edge: phenotype_edge,
                      sort_group: phenotype_edge.group,
                      sort_name: phenotype.name
                    }
                )
                FOR row IN (LENGTH(via_finding) > 0 ? via_finding : direct)
                  SORT row.sort_group, row.sort_name, row.phenotype._key
                  RETURN row
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=15,
            )
            for row in clinical_context_phenotype_cursor:
                phenotype_doc = row.get("phenotype")
                finding_doc = row.get("finding")
                finding_edge_doc = row.get("finding_edge")
                phenotype_edge_doc = row.get("phenotype_edge")
                display_edge_doc = finding_doc or phenotype_edge_doc
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(phenotype_doc)
                edge_scalar_fields, edge_list_fields, edge_nested_fields = _categorize_document_fields(display_edge_doc)
                clinical_context_phenotype_cards.append({
                    "phenotype_doc": phenotype_doc,
                    "finding_doc": finding_doc,
                    "finding_edge_doc": finding_edge_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                    "edge_doc": display_edge_doc,
                    "phenotype_edge_doc": phenotype_edge_doc,
                    "edge_scalar_fields": edge_scalar_fields,
                    "edge_list_fields": edge_list_fields,
                    "edge_nested_fields": edge_nested_fields,
                })
        except Exception:
            clinical_context_phenotype_cards = []

        try:
            diagnosis_cursor = db.aql.execute(
                """
                FOR diagnosis, diagnosis_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                FILTER SPLIT(diagnosis._id, '/')[0] == 'Diagnosis'
                FILTER SPLIT(diagnosis_edge._id, '/')[0] == 'ClinicalContextDiagnosisEdge'
                LET conditions = (
                  FOR condition, condition_edge IN 1..1 OUTBOUND diagnosis._id GRAPH 'graph'
                    FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                    FILTER SPLIT(condition_edge._id, '/')[0] == 'DiagnosisConditionEdge'
                    SORT condition.name, condition._key
                    RETURN {
                      condition: condition,
                      condition_edge: condition_edge
                    }
                )
                LET genes = (
                  FOR gene, gene_edge IN 1..1 OUTBOUND diagnosis._id GRAPH 'graph'
                    FILTER SPLIT(gene._id, '/')[0] == 'Gene'
                    FILTER SPLIT(gene_edge._id, '/')[0] == 'DiagnosisGeneEdge'
                    SORT gene.symbol, gene.name, gene._key
                    RETURN {
                      gene: gene,
                      gene_edge: gene_edge
                    }
                )
                LET variants = (
                  FOR variant, variant_edge IN 1..1 OUTBOUND diagnosis._id GRAPH 'graph'
                    FILTER SPLIT(variant._id, '/')[0] == 'GeneVariant'
                    FILTER SPLIT(variant_edge._id, '/')[0] == 'DiagnosisGeneVariantEdge'
                    LET linked_genes = (
                      FOR gene, gene_variant_edge IN 1..1 INBOUND variant._id GRAPH 'graph'
                        FILTER SPLIT(gene._id, '/')[0] == 'Gene'
                        FILTER SPLIT(gene_variant_edge._id, '/')[0] == 'GeneGeneVariantEdge'
                        SORT gene.symbol, gene.name, gene._key
                        RETURN {
                          gene: gene,
                          gene_variant_edge: gene_variant_edge
                        }
                    )
                    SORT variant.source_gene_symbol, variant.variant_label, variant._key
                    RETURN {
                      variant: variant,
                      variant_edge: variant_edge,
                      linked_genes: linked_genes
                    }
                )
                SORT diagnosis._key
                RETURN {
                  diagnosis: diagnosis,
                  diagnosis_edge: diagnosis_edge,
                  conditions: conditions,
                  genes: genes,
                  variants: variants
                }
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=15,
            )
            for row in diagnosis_cursor:
                diagnosis_doc = row.get("diagnosis")
                diagnosis_scalar_fields, diagnosis_list_fields, diagnosis_nested_fields = _categorize_document_fields(diagnosis_doc)
                condition_cards = []
                for condition_row in row.get("conditions") or []:
                    condition_doc = condition_row.get("condition")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                    condition_cards.append({
                        "condition_doc": condition_doc,
                        "condition_edge_doc": condition_row.get("condition_edge"),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
                gene_cards = []
                for gene_row in row.get("genes") or []:
                    gene_doc = gene_row.get("gene")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(gene_doc)
                    gene_cards.append({
                        "gene_doc": gene_doc,
                        "gene_edge_doc": gene_row.get("gene_edge"),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
                variant_cards = []
                for variant_row in row.get("variants") or []:
                    variant_doc = variant_row.get("variant")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(variant_doc)
                    linked_gene_cards = []
                    for linked_gene_row in variant_row.get("linked_genes") or []:
                        gene_doc = linked_gene_row.get("gene")
                        linked_gene_cards.append({
                            "gene_doc": gene_doc,
                            "gene_variant_edge_doc": linked_gene_row.get("gene_variant_edge"),
                        })
                    variant_cards.append({
                        "variant_doc": variant_doc,
                        "variant_edge_doc": variant_row.get("variant_edge"),
                        "linked_gene_cards": linked_gene_cards,
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
                rasopathies_diagnosis_cards.append({
                    "diagnosis_doc": diagnosis_doc,
                    "diagnosis_edge_doc": row.get("diagnosis_edge"),
                    "diagnosis_scalar_fields": diagnosis_scalar_fields,
                    "diagnosis_list_fields": diagnosis_list_fields,
                    "diagnosis_nested_fields": diagnosis_nested_fields,
                    "condition_cards": condition_cards,
                    "gene_cards": gene_cards,
                    "variant_cards": variant_cards,
                })
        except Exception:
            rasopathies_diagnosis_cards = []

    finding_group_order = [
        "Cardiac",
        "Endocrine/Growth",
        "Gastrointestinal",
        "Hematologic/Oncologic",
        "Lymphatic/Immunologic",
        "Neurologic/Audiologic",
        "Opthalmalogic",
        "Diagnoses not listed above",
    ]
    finding_group_rank = {name: index for index, name in enumerate(finding_group_order)}
    grouped_clinical_context_phenotype_cards = []
    if clinical_context_phenotype_cards:
        grouped = {}
        for card in clinical_context_phenotype_cards:
            group_name = ((card.get("edge_doc") or {}).get("group") or "Ungrouped").strip()
            grouped.setdefault(group_name, []).append(card)
        for group_name, cards in grouped.items():
            cards.sort(
                key=lambda card: (
                    ((card.get("phenotype_doc") or {}).get("name") or "").lower(),
                    ((card.get("phenotype_doc") or {}).get("_key") or ""),
                )
            )
        grouped_clinical_context_phenotype_cards = [
            {
                "group_name": group_name,
                "cards": grouped[group_name],
            }
            for group_name in sorted(
                grouped.keys(),
                key=lambda name: (
                    finding_group_rank.get(name, 10_000),
                    name == "Ungrouped",
                    name.lower(),
                ),
            )
        ]

    if primary_episode_doc:
        if acute_episode_doc:
            try:
                episode_relationship_cursor = db.aql.execute(
                    """
                    FOR episode, episode_edge IN 1..1 OUTBOUND @acute_episode_id GRAPH 'graph'
                    FILTER SPLIT(episode._id, '/')[0] == 'Episode'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeEpisodeEdge'
                    FILTER episode_edge.relationship_type == 'precedes'
                    FILTER episode._id == @primary_episode_id
                    LIMIT 1
                    RETURN episode_edge
                    """,
                    bind_vars={
                        "acute_episode_id": acute_episode_doc["_id"],
                        "primary_episode_id": primary_episode_doc["_id"],
                    },
                    max_runtime=10,
                )
                relationship_doc = next(iter(episode_relationship_cursor), None)
                if relationship_doc:
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(relationship_doc)
                    episode_relationship_cards.append({
                        "relationship_doc": relationship_doc,
                        "label": "precedes",
                        "left_tab": "acute-covid",
                        "right_tab": "long-covid",
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                pass
        if pregnancy_episode_doc:
            try:
                overlap_relationship_cursor = db.aql.execute(
                    """
                    FOR episode, episode_edge IN 1..1 OUTBOUND @pregnancy_episode_id GRAPH 'graph'
                    FILTER SPLIT(episode._id, '/')[0] == 'Episode'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeEpisodeEdge'
                    FILTER episode_edge.relationship_type == 'overlaps'
                    FILTER episode._id == @primary_episode_id
                    LIMIT 1
                    RETURN episode_edge
                    """,
                    bind_vars={
                        "pregnancy_episode_id": pregnancy_episode_doc["_id"],
                        "primary_episode_id": primary_episode_doc["_id"],
                    },
                    max_runtime=10,
                )
                relationship_doc = next(iter(overlap_relationship_cursor), None)
                if relationship_doc:
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(relationship_doc)
                    episode_relationship_cards.append({
                        "relationship_doc": relationship_doc,
                        "label": "overlaps",
                        "left_tab": "long-covid",
                        "right_tab": "pregnancy",
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                pass

        if patient_doc:
            try:
                background_context_cursor = db.aql.execute(
                    """
                    FOR background_context, context_edge IN 1..1 OUTBOUND @person_id GRAPH 'graph'
                    FILTER SPLIT(background_context._id, '/')[0] == 'BackgroundContext'
                    FILTER SPLIT(context_edge._id, '/')[0] == 'PersonBackgroundContextEdge'
                    LIMIT 1
                    RETURN background_context
                    """,
                    bind_vars={"person_id": patient_doc["_id"]},
                    max_runtime=15,
                )
                background_context_doc = next(iter(background_context_cursor), None)
            except Exception:
                background_context_doc = None

            background_context_scalar_fields, background_context_list_fields, background_context_nested_fields = _categorize_document_fields(background_context_doc)

            if background_context_doc:
                try:
                    patient_condition_cursor = db.aql.execute(
                        """
                        FOR condition, condition_edge IN 1..1 OUTBOUND @background_context_id GRAPH 'graph'
                        FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                        FILTER SPLIT(condition_edge._id, '/')[0] == 'BackgroundContextConditionEdge'
                        FILTER condition_edge.relationship_type == 'prior_comorbidity'
                        SORT condition.name, condition._key
                        RETURN condition
                        """,
                        bind_vars={"background_context_id": background_context_doc["_id"]},
                        max_runtime=15,
                    )
                    for condition_doc in patient_condition_cursor:
                        scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                        patient_prior_condition_cards.append({
                            "condition_doc": condition_doc,
                            "scalar_fields": scalar_fields,
                            "list_fields": list_fields,
                            "nested_fields": nested_fields,
                        })
                except Exception:
                    patient_prior_condition_cards = []

                try:
                    background_exposure_cursor = db.aql.execute(
                        """
                        FOR exposure, context_edge IN 1..1 OUTBOUND @background_context_id GRAPH 'graph'
                        FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                        FILTER SPLIT(context_edge._id, '/')[0] == 'BackgroundContextExposureEdge'
                        LET drug_doc = FIRST(
                            FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                            FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                            FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                            LIMIT 1
                            RETURN drug
                        )
                        SORT context_edge.relationship_type, drug_doc.name, exposure.long_drug_name, exposure._key
                        RETURN {
                            exposure_doc: exposure,
                            drug_doc: drug_doc,
                            relationship_type: context_edge.relationship_type
                        }
                        """,
                        bind_vars={"background_context_id": background_context_doc["_id"]},
                        max_runtime=15,
                    )
                    for row in background_exposure_cursor:
                        exposure_doc = row.get("exposure_doc")
                        scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                        card = {
                            "exposure_doc": exposure_doc,
                            "drug_doc": row.get("drug_doc"),
                            "scalar_fields": scalar_fields,
                            "list_fields": list_fields,
                            "nested_fields": nested_fields,
                        }
                        if row.get("relationship_type") == "immunosuppressant":
                            background_immunosuppressant_cards.append(card)
                        else:
                            background_regular_medicine_cards.append(card)
                except Exception:
                    background_regular_medicine_cards = []
                    background_immunosuppressant_cards = []

    if patient_doc:
        try:
            perinatal_context_cursor = db.aql.execute(
                """
                FOR perinatal_context, context_edge IN 1..1 OUTBOUND @person_id GRAPH 'graph'
                FILTER SPLIT(perinatal_context._id, '/')[0] == 'PerinatalContext'
                FILTER SPLIT(context_edge._id, '/')[0] == 'PatientPerinatalContextEdge'
                LIMIT 1
                RETURN perinatal_context
                """,
                bind_vars={"person_id": patient_doc["_id"]},
                max_runtime=15,
            )
            perinatal_context_doc = next(iter(perinatal_context_cursor), None)
        except Exception:
            perinatal_context_doc = None

        perinatal_context_scalar_fields, perinatal_context_list_fields, perinatal_context_nested_fields = _categorize_document_fields(perinatal_context_doc)
        if perinatal_context_doc:
            try:
                perinatal_phenotype_cursor = db.aql.execute(
                    """
                    LET via_finding = (
                      FOR finding, finding_edge IN 1..1 OUTBOUND @perinatal_context_id GRAPH 'graph'
                        FILTER SPLIT(finding._id, '/')[0] == 'Finding'
                        FILTER SPLIT(finding_edge._id, '/')[0] == 'PerinatalContextFindingEdge'
                        FOR phenotype, phenotype_edge IN 1..1 OUTBOUND finding._id GRAPH 'graph'
                          FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                          FILTER SPLIT(phenotype_edge._id, '/')[0] == 'FindingPhenotypeEdge'
                          RETURN {
                            phenotype: phenotype,
                            finding: finding,
                            finding_edge: finding_edge,
                            phenotype_edge: phenotype_edge,
                            sort_name: phenotype.name
                          }
                    )
                    LET direct = (
                      FOR phenotype, phenotype_edge IN 1..1 OUTBOUND @perinatal_context_id GRAPH 'graph'
                        FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                        FILTER SPLIT(phenotype_edge._id, '/')[0] == 'PerinatalContextPhenotypeEdge'
                        RETURN {
                          phenotype: phenotype,
                          finding: null,
                          finding_edge: null,
                          phenotype_edge: phenotype_edge,
                          sort_name: phenotype.name
                        }
                    )
                    FOR row IN (LENGTH(via_finding) > 0 ? via_finding : direct)
                      SORT row.sort_name, row.phenotype._key
                      RETURN row
                    """,
                    bind_vars={"perinatal_context_id": perinatal_context_doc["_id"]},
                    max_runtime=15,
                )
                for row in perinatal_phenotype_cursor:
                    phenotype_doc = row.get("phenotype")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(phenotype_doc)
                    perinatal_context_phenotype_cards.append({
                        "phenotype_doc": phenotype_doc,
                        "finding_doc": row.get("finding"),
                        "finding_edge_doc": row.get("finding_edge"),
                        "edge_doc": row.get("finding") or row.get("phenotype_edge"),
                        "phenotype_edge_doc": row.get("phenotype_edge"),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                perinatal_context_phenotype_cards = []

        try:
            rasopathies_treatment_cursor = db.aql.execute(
                """
                FOR treatment, treatment_edge IN 1..1 OUTBOUND @clinical_context_id GRAPH 'graph'
                FILTER SPLIT(treatment._id, '/')[0] == 'DrugTreatment'
                FILTER SPLIT(treatment_edge._id, '/')[0] == 'ClinicalContextDrugTreatmentEdge'
                LET drug_doc = FIRST(
                  FOR drug, drug_edge IN 1..1 OUTBOUND treatment._id GRAPH 'graph'
                    FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                    FILTER SPLIT(drug_edge._id, '/')[0] == 'DrugTreatmentDrugEdge'
                    LIMIT 1
                    RETURN drug
                )
                LET responses = (
                  FOR response, response_edge IN 1..1 OUTBOUND treatment._id GRAPH 'graph'
                    FILTER SPLIT(response._id, '/')[0] == 'TreatmentResponse'
                    FILTER SPLIT(response_edge._id, '/')[0] == 'DrugTreatmentResponseEdge'
                    LET finding_doc = FIRST(
                      FOR finding, finding_edge IN 1..1 OUTBOUND response._id GRAPH 'graph'
                        FILTER SPLIT(finding._id, '/')[0] == 'Finding'
                        FILTER SPLIT(finding_edge._id, '/')[0] == 'TreatmentResponseFindingEdge'
                        LIMIT 1
                        RETURN finding
                    )
                    LET phenotype_docs = (
                      FOR phenotype, phenotype_edge IN 1..1 OUTBOUND finding_doc._id GRAPH 'graph'
                        FILTER finding_doc != null
                        FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                        FILTER SPLIT(phenotype_edge._id, '/')[0] == 'FindingPhenotypeEdge'
                        SORT phenotype.name, phenotype._key
                        RETURN {
                          phenotype: phenotype,
                          phenotype_edge: phenotype_edge
                        }
                    )
                    SORT response.source_target_index, response._key
                    RETURN {
                      response_doc: response,
                      response_edge_doc: response_edge,
                      finding_doc: finding_doc,
                      phenotype_docs: phenotype_docs
                    }
                )
                LET adverse_events = (
                  FOR phenotype, adverse_event_edge IN 1..1 OUTBOUND treatment._id GRAPH 'graph'
                    FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(adverse_event_edge._id, '/')[0] == 'DrugTreatmentAdverseEventEdge'
                    SORT adverse_event_edge.source_adverse_event_index, phenotype.name, phenotype._key
                    RETURN {
                      phenotype: phenotype,
                      adverse_event_edge: adverse_event_edge
                    }
                )
                SORT treatment.source_treatment_index, drug_doc.name, treatment._key
                RETURN {
                  treatment_doc: treatment,
                  treatment_edge_doc: treatment_edge,
                  drug_doc: drug_doc,
                  responses: responses,
                  adverse_events: adverse_events
                }
                """,
                bind_vars={"clinical_context_id": clinical_context_doc["_id"]},
                max_runtime=20,
            )
            for row in rasopathies_treatment_cursor:
                treatment_doc = row.get("treatment_doc")
                treatment_scalar_fields, treatment_list_fields, treatment_nested_fields = _categorize_document_fields(treatment_doc)
                drug_doc = row.get("drug_doc")
                drug_scalar_fields, drug_list_fields, drug_nested_fields = _categorize_document_fields(drug_doc)
                response_cards = []
                for response_row in row.get("responses") or []:
                    response_doc = response_row.get("response_doc")
                    response_scalar_fields, response_list_fields, response_nested_fields = _categorize_document_fields(response_doc)
                    finding_doc = response_row.get("finding_doc")
                    finding_scalar_fields, finding_list_fields, finding_nested_fields = _categorize_document_fields(finding_doc)
                    phenotype_cards = []
                    for phenotype_row in response_row.get("phenotype_docs") or []:
                        phenotype_doc = phenotype_row.get("phenotype")
                        phenotype_scalar_fields, phenotype_list_fields, phenotype_nested_fields = _categorize_document_fields(phenotype_doc)
                        phenotype_cards.append({
                            "phenotype_doc": phenotype_doc,
                            "phenotype_edge_doc": phenotype_row.get("phenotype_edge"),
                            "scalar_fields": phenotype_scalar_fields,
                            "list_fields": phenotype_list_fields,
                            "nested_fields": phenotype_nested_fields,
                        })
                    response_cards.append({
                        "response_doc": response_doc,
                        "response_edge_doc": response_row.get("response_edge_doc"),
                        "scalar_fields": response_scalar_fields,
                        "list_fields": response_list_fields,
                        "nested_fields": response_nested_fields,
                        "finding_doc": finding_doc,
                        "finding_scalar_fields": finding_scalar_fields,
                        "finding_list_fields": finding_list_fields,
                        "finding_nested_fields": finding_nested_fields,
                        "phenotype_cards": phenotype_cards,
                    })
                adverse_event_cards = []
                for adverse_event_row in row.get("adverse_events") or []:
                    adverse_event_doc = adverse_event_row.get("phenotype")
                    adverse_event_edge_doc = adverse_event_row.get("adverse_event_edge")
                    adverse_event_scalar_fields, adverse_event_list_fields, adverse_event_nested_fields = _categorize_document_fields(adverse_event_doc)
                    adverse_event_edge_scalar_fields, adverse_event_edge_list_fields, adverse_event_edge_nested_fields = _categorize_document_fields(adverse_event_edge_doc)
                    adverse_event_cards.append({
                        "phenotype_doc": adverse_event_doc,
                        "adverse_event_edge_doc": adverse_event_edge_doc,
                        "scalar_fields": adverse_event_scalar_fields,
                        "list_fields": adverse_event_list_fields,
                        "nested_fields": adverse_event_nested_fields,
                        "edge_scalar_fields": adverse_event_edge_scalar_fields,
                        "edge_list_fields": adverse_event_edge_list_fields,
                        "edge_nested_fields": adverse_event_edge_nested_fields,
                    })
                rasopathies_drug_treatment_cards.append({
                    "treatment_doc": treatment_doc,
                    "treatment_edge_doc": row.get("treatment_edge_doc"),
                    "drug_doc": drug_doc,
                    "treatment_scalar_fields": treatment_scalar_fields,
                    "treatment_list_fields": treatment_list_fields,
                    "treatment_nested_fields": treatment_nested_fields,
                    "drug_scalar_fields": drug_scalar_fields,
                    "drug_list_fields": drug_list_fields,
                    "drug_nested_fields": drug_nested_fields,
                    "response_cards": response_cards,
                    "adverse_event_cards": adverse_event_cards,
                })
        except Exception:
            rasopathies_drug_treatment_cards = []

        try:
            primary_post_covid_condition_cursor = db.aql.execute(
                """
                FOR condition, condition_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                FILTER SPLIT(condition_edge._id, '/')[0] == 'EpisodeConditionEdge'
                FILTER condition_edge.relationship_type == 'comorbidity'
                SORT condition.name, condition._key
                RETURN condition
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for condition_doc in primary_post_covid_condition_cursor:
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                primary_episode_post_covid_condition_cards.append({
                    "condition_doc": condition_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            primary_episode_post_covid_condition_cards = []

        try:
            exposure_cursor = db.aql.execute(
                """
                FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                LET drug_doc = FIRST(
                    FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                    FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                    FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                    LIMIT 1
                    RETURN drug
                )
                LET adverse_events = (
                    FOR ae, ae_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                    FILTER SPLIT(ae._id, '/')[0] IN ['Phenotype', 'AdverseEvent']
                    FILTER SPLIT(ae_edge._id, '/')[0] == 'ExposureAdverseEventEdge'
                    SORT ae.name
                    RETURN {
                        id: ae._id,
                        key: ae._key,
                        collection: SPLIT(ae._id, '/')[0],
                        name: ae.name,
                        outcomes: ae_edge.outcomes || []
                    }
                )
                SORT exposure.long_drug_name, exposure._key
                RETURN {
                    exposure_doc: exposure,
                    drug_doc: drug_doc,
                    adverse_events: adverse_events
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for row in exposure_cursor:
                exposure_doc = row.get("exposure_doc")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                exposure_cards.append({
                    "exposure_doc": exposure_doc,
                    "drug_doc": row.get("drug_doc"),
                    "adverse_events": row.get("adverse_events", []),
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            exposure_cards = []

        if acute_episode_doc:
            try:
                acute_exposure_cursor = db.aql.execute(
                    """
                    FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT exposure.long_drug_name, exposure._key
                    RETURN {
                        exposure_doc: exposure,
                        drug_doc: drug_doc,
                        adverse_events: []
                    }
                    """,
                    bind_vars={"episode_id": acute_episode_doc["_id"]},
                    max_runtime=15,
                )
                for row in acute_exposure_cursor:
                    exposure_doc = row.get("exposure_doc")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                    acute_exposure_cards.append({
                        "exposure_doc": exposure_doc,
                        "drug_doc": row.get("drug_doc"),
                        "adverse_events": [],
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                acute_exposure_cards = []

            try:
                acute_complication_cursor = db.aql.execute(
                    """
                    FOR condition, complication_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(condition._id, '/')[0] == 'Condition'
                    FILTER SPLIT(complication_edge._id, '/')[0] == 'EpisodeConditionEdge'
                    FILTER complication_edge.relationship_type == 'complication'
                    SORT condition.name, condition._key
                    RETURN condition
                    """,
                    bind_vars={"episode_id": acute_episode_doc["_id"]},
                    max_runtime=15,
                )
                for condition_doc in acute_complication_cursor:
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(condition_doc)
                    acute_complication_cards.append({
                        "condition_doc": condition_doc,
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                acute_complication_cards = []

            try:
                acute_vaccination_cursor = db.aql.execute(
                    """
                    FOR vaccination_event, event_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(vaccination_event._id, '/')[0] == 'VaccinationEvent'
                    FILTER SPLIT(event_edge._id, '/')[0] == 'VaccinationEventEpisodeEdge'
                    LET vaccines = (
                        FOR vaccine, vaccine_edge IN 1..1 OUTBOUND vaccination_event._id GRAPH 'graph'
                        FILTER SPLIT(vaccine._id, '/')[0] == 'Vaccine'
                        FILTER SPLIT(vaccine_edge._id, '/')[0] == 'VaccinationEventVaccineEdge'
                        SORT vaccine.name, vaccine._key
                        RETURN vaccine
                    )
                    LIMIT 1
                    RETURN {
                        vaccination_event_doc: vaccination_event,
                        event_edge: event_edge,
                        vaccines: vaccines
                    }
                    """,
                    bind_vars={"episode_id": acute_episode_doc["_id"]},
                    max_runtime=15,
                )
                acute_vaccination_row = next(iter(acute_vaccination_cursor), None)
                if acute_vaccination_row:
                    vaccination_event_doc = acute_vaccination_row.get("vaccination_event_doc")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(vaccination_event_doc)
                    acute_vaccination_card = {
                        "vaccination_event_doc": vaccination_event_doc,
                        "event_edge": acute_vaccination_row.get("event_edge") or {},
                        "vaccines": acute_vaccination_row.get("vaccines", []),
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    }
            except Exception:
                acute_vaccination_card = None

        if pregnancy_episode_doc:
            try:
                pregnancy_exposure_cursor = db.aql.execute(
                    """
                    FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                    FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT exposure.long_drug_name, exposure._key
                    RETURN {
                        exposure_doc: exposure,
                        drug_doc: drug_doc,
                        adverse_events: []
                    }
                    """,
                    bind_vars={"episode_id": pregnancy_episode_doc["_id"]},
                    max_runtime=15,
                )
                for row in pregnancy_exposure_cursor:
                    exposure_doc = row.get("exposure_doc")
                    scalar_fields, list_fields, nested_fields = _categorize_document_fields(exposure_doc)
                    pregnancy_exposure_cards.append({
                        "exposure_doc": exposure_doc,
                        "drug_doc": row.get("drug_doc"),
                        "adverse_events": [],
                        "scalar_fields": scalar_fields,
                        "list_fields": list_fields,
                        "nested_fields": nested_fields,
                    })
            except Exception:
                pregnancy_exposure_cards = []

        try:
            phenotype_cursor = db.aql.execute(
                """
                FOR phenotype, phenotype_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                FILTER SPLIT(phenotype_edge._id, '/')[0] == 'EpisodePhenotypeEdge'
                SORT phenotype.name, phenotype._key
                RETURN {
                    phenotype_doc: phenotype,
                    severity: phenotype_edge.severity
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for row in phenotype_cursor:
                phenotype_doc = row.get("phenotype_doc")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(phenotype_doc)
                phenotype_cards.append({
                    "phenotype_doc": phenotype_doc,
                    "severity": row.get("severity"),
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            phenotype_cards = []

        try:
            therapy_cursor = db.aql.execute(
                """
                FOR therapy, therapy_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(therapy._id, '/')[0] == 'Therapy'
                FILTER SPLIT(therapy_edge._id, '/')[0] == 'EpisodeTherapyEdge'
                SORT therapy.name, therapy._key
                RETURN therapy
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for therapy_doc in therapy_cursor:
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(therapy_doc)
                therapy_cards.append({
                    "therapy_doc": therapy_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            therapy_cards = []

        try:
            treatment_cursor = db.aql.execute(
                """
                FOR exposure, episode_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(exposure._id, '/')[0] == 'Exposure'
                FILTER SPLIT(episode_edge._id, '/')[0] == 'EpisodeExposureEdge'
                FOR treatment, treatment_edge IN 1..1 OUTBOUND exposure._id GRAPH 'graph'
                FILTER SPLIT(treatment._id, '/')[0] == 'Treatment'
                FILTER SPLIT(treatment_edge._id, '/')[0] == 'TreatmentExposureEdge'
                COLLECT treatment_id = treatment._id INTO grouped = {
                    treatment: treatment,
                    exposure: exposure
                }
                LET treatment_doc = FIRST(grouped[*].treatment)
                LET mapped_exposures = (
                    FOR row IN grouped
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND row.exposure._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT drug_doc.name, row.exposure.long_drug_name, row.exposure._key
                    RETURN {
                        exposure_id: row.exposure._id,
                        drug_name: drug_doc.name || row.exposure.long_drug_name || row.exposure._key
                    }
                )
                SORT treatment_doc._key
                RETURN {
                    treatment_doc: treatment_doc,
                    mapped_exposures: mapped_exposures
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=15,
            )
            for row in treatment_cursor:
                treatment_doc = row.get("treatment_doc")
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(treatment_doc)
                treatment_cards.append({
                    "treatment_doc": treatment_doc,
                    "mapped_exposures": row.get("mapped_exposures", []),
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                })
        except Exception:
            treatment_cards = []

        try:
            outcome_cursor = db.aql.execute(
                """
                FOR outcome, episode_outcome_edge IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                FILTER SPLIT(outcome._id, '/')[0] == 'Outcome'
                FILTER SPLIT(episode_outcome_edge._id, '/')[0] == 'EpisodeOutcomeEdge'
                LET treatment_doc = FIRST(
                    FOR treatment, outcome_edge IN 1..1 INBOUND outcome._id GRAPH 'graph'
                    FILTER SPLIT(treatment._id, '/')[0] == 'Treatment'
                    FILTER SPLIT(outcome_edge._id, '/')[0] == 'TreatmentOutcomeEdge'
                    LIMIT 1
                    RETURN treatment
                )
                LET phenotype_doc = FIRST(
                    FOR phenotype, phenotype_edge IN 1..1 OUTBOUND outcome._id GRAPH 'graph'
                    FILTER SPLIT(phenotype._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(phenotype_edge._id, '/')[0] == 'OutcomePhenotypeEdge'
                    LIMIT 1
                    RETURN phenotype
                )
                LET phenotype_severity = FIRST(
                    FOR phenotype2, phenotype_edge2 IN 1..1 OUTBOUND @episode_id GRAPH 'graph'
                    FILTER SPLIT(phenotype2._id, '/')[0] == 'Phenotype'
                    FILTER SPLIT(phenotype_edge2._id, '/')[0] == 'EpisodePhenotypeEdge'
                    FILTER phenotype_doc != null AND phenotype2._id == phenotype_doc._id
                    LIMIT 1
                    RETURN phenotype_edge2.severity
                )
                LET mapped_exposures = (
                    FOR exposure2, exposure_edge2 IN 1..1 INBOUND treatment_doc._id GRAPH 'graph'
                    FILTER SPLIT(exposure2._id, '/')[0] == 'Exposure'
                    FILTER SPLIT(exposure_edge2._id, '/')[0] == 'TreatmentExposureEdge'
                    LET drug_doc = FIRST(
                        FOR drug, drug_edge IN 1..1 OUTBOUND exposure2._id GRAPH 'graph'
                        FILTER SPLIT(drug._id, '/')[0] == 'Drug'
                        FILTER SPLIT(drug_edge._id, '/')[0] == 'ExposureDrugEdge'
                        LIMIT 1
                        RETURN drug
                    )
                    SORT drug_doc.name, exposure2.long_drug_name, exposure2._key
                    RETURN {
                        exposure_id: exposure2._id,
                        exposure_doc: exposure2,
                        drug_doc: drug_doc,
                        drug_name: drug_doc.name || exposure2.long_drug_name || exposure2._key
                    }
                )
                SORT outcome._key
                RETURN {
                    outcome_doc: outcome,
                    treatment_doc: treatment_doc,
                    phenotype_doc: phenotype_doc,
                    phenotype_severity: phenotype_severity,
                    mapped_exposures: mapped_exposures
                }
                """,
                bind_vars={"episode_id": primary_episode_doc["_id"]},
                max_runtime=20,
            )
            for row in outcome_cursor:
                outcome_doc = row.get("outcome_doc")
                effect_display = _get_outcome_effect_display((outcome_doc or {}).get("effect"))
                scalar_fields, list_fields, nested_fields = _categorize_document_fields(outcome_doc)
                phenotype_doc = row.get("phenotype_doc")
                phenotype_scalar_fields, phenotype_list_fields, phenotype_nested_fields = _categorize_document_fields(phenotype_doc)
                treatment_doc = row.get("treatment_doc")
                treatment_scalar_fields, treatment_list_fields, treatment_nested_fields = _categorize_document_fields(treatment_doc)
                outcome_cards.append({
                    "outcome_doc": outcome_doc,
                    "scalar_fields": scalar_fields,
                    "list_fields": list_fields,
                    "nested_fields": nested_fields,
                    "treatment_doc": treatment_doc,
                    "treatment_scalar_fields": treatment_scalar_fields,
                    "treatment_list_fields": treatment_list_fields,
                    "treatment_nested_fields": treatment_nested_fields,
                    "mapped_exposures": row.get("mapped_exposures", []),
                    "phenotype_doc": phenotype_doc,
                    "phenotype_severity": row.get("phenotype_severity"),
                    "phenotype_scalar_fields": phenotype_scalar_fields,
                    "phenotype_list_fields": phenotype_list_fields,
                    "phenotype_nested_fields": phenotype_nested_fields,
                    "effect_rank": effect_display["rank"],
                    "effect_tone": effect_display["tone"],
                    "effect_pct": effect_display["pct"],
                })
            outcome_cards.sort(
                key=lambda card: (
                    card["effect_rank"],
                    (card["outcome_doc"] or {}).get("effect") or "",
                    (card["outcome_doc"] or {}).get("raw_symptom_name") or "",
                    (card["outcome_doc"] or {}).get("_key") or "",
                )
            )
        except Exception:
            outcome_cards = []

    return {
        "cure_case_url": _build_cure_case_url(db_name, doc),
        "patient_doc": patient_doc,
        "patient_scalar_fields": patient_scalar_fields,
        "patient_list_fields": patient_list_fields,
        "patient_nested_fields": patient_nested_fields,
        "reporter_doc": reporter_doc,
        "reporter_scalar_fields": reporter_scalar_fields,
        "reporter_list_fields": reporter_list_fields,
        "reporter_nested_fields": reporter_nested_fields,
        "clinical_context_doc": clinical_context_doc,
        "clinical_context_scalar_fields": clinical_context_scalar_fields,
        "clinical_context_list_fields": clinical_context_list_fields,
        "clinical_context_nested_fields": clinical_context_nested_fields,
        "clinical_context_condition_cards": clinical_context_condition_cards,
        "clinical_context_phenotype_cards": clinical_context_phenotype_cards,
        "grouped_clinical_context_phenotype_cards": grouped_clinical_context_phenotype_cards,
        "background_context_doc": background_context_doc,
        "background_context_scalar_fields": background_context_scalar_fields,
        "background_context_list_fields": background_context_list_fields,
        "background_context_nested_fields": background_context_nested_fields,
        "perinatal_context_doc": perinatal_context_doc,
        "perinatal_context_scalar_fields": perinatal_context_scalar_fields,
        "perinatal_context_list_fields": perinatal_context_list_fields,
        "perinatal_context_nested_fields": perinatal_context_nested_fields,
        "perinatal_context_phenotype_cards": perinatal_context_phenotype_cards,
        "rasopathies_diagnosis_cards": rasopathies_diagnosis_cards,
        "rasopathies_drug_treatment_cards": rasopathies_drug_treatment_cards,
        "episode_relationship_cards": episode_relationship_cards,
        "patient_prior_condition_cards": patient_prior_condition_cards,
        "background_regular_medicine_cards": background_regular_medicine_cards,
        "background_immunosuppressant_cards": background_immunosuppressant_cards,
        "primary_episode_doc": primary_episode_doc,
        "primary_episode_scalar_fields": primary_episode_scalar_fields,
        "primary_episode_list_fields": primary_episode_list_fields,
        "primary_episode_nested_fields": primary_episode_nested_fields,
        "primary_episode_post_covid_condition_cards": primary_episode_post_covid_condition_cards,
        "acute_episode_doc": acute_episode_doc,
        "acute_episode_scalar_fields": acute_episode_scalar_fields,
        "acute_episode_list_fields": acute_episode_list_fields,
        "acute_episode_nested_fields": acute_episode_nested_fields,
        "pregnancy_episode_doc": pregnancy_episode_doc,
        "pregnancy_episode_scalar_fields": pregnancy_episode_scalar_fields,
        "pregnancy_episode_list_fields": pregnancy_episode_list_fields,
        "pregnancy_episode_nested_fields": pregnancy_episode_nested_fields,
        "pregnancy_episode_exposure_cards": pregnancy_exposure_cards,
        "acute_episode_vaccination_card": acute_vaccination_card,
        "acute_episode_exposure_cards": acute_exposure_cards,
        "acute_episode_complication_cards": acute_complication_cards,
        "primary_episode_exposure_cards": exposure_cards,
        "primary_episode_phenotype_cards": phenotype_cards,
        "primary_episode_therapy_cards": therapy_cards,
        "primary_episode_treatment_cards": treatment_cards,
        "primary_episode_outcome_cards": outcome_cards,
    }


def _build_cure_case_url(db_name: str, doc: dict | None) -> str | None:
    if not doc:
        return None
    if doc.get("case_report_url"):
        return doc.get("case_report_url")
    report_id = doc.get("id") or doc.get("_key")
    if not report_id:
        return None

    route_slug = _get_cure_route_slug(db_name, doc.get("form_type"))
    if not route_slug:
        return None
    return f"https://cure.ncats.io/explore/{route_slug}/case-reports/case-details/{report_id}"


def _is_cure_db(db_name: str) -> bool:
    normalized = (db_name or "").strip().lower()
    return normalized == "cure" or normalized.startswith("cure_")


def _get_cure_route_slug(db_name: str, form_type: str | None = None) -> str | None:
    normalized_db_name = (db_name or "").strip().lower()
    if normalized_db_name == "cure" or normalized_db_name.startswith("cure_pasc"):
        return "long-covid"
    if normalized_db_name.startswith("cure_rasopathies"):
        return "rasopathies"

    return {
        "pasc": "long-covid",
        "rasopathies": "rasopathies",
    }.get((form_type or "").strip().lower())


def _get_outcome_effect_display(effect: str | None) -> dict:
    normalized = (effect or "").strip().lower()
    mapping = {
        "complete symptom resolution": {"rank": 0, "tone": "positive", "pct": 100},
        "significant symptom improvement": {"rank": 1, "tone": "positive", "pct": 82},
        "moderate symptom improvement": {"rank": 2, "tone": "positive", "pct": 64},
        "mild symptom improvement": {"rank": 3, "tone": "positive", "pct": 46},
        "symptom was unchanged": {"rank": 4, "tone": "neutral", "pct": 0},
        "unknown": {"rank": 5, "tone": "unknown", "pct": 0},
        "mild symptom worsening": {"rank": 6, "tone": "negative", "pct": 40},
        "moderate symptom worsening": {"rank": 7, "tone": "negative", "pct": 62},
        "significant symptom worsening": {"rank": 8, "tone": "negative", "pct": 84},
    }
    default = {"rank": 9, "tone": "unknown", "pct": 0}
    return mapping.get(normalized, default)


templates.env.filters["truncate_val"] = _truncate


# ── Document template dispatch ───────────────────────────────────────────────

def _get_document_template(db_name: str, coll_name: str) -> str:
    if coll_name == "CaseReport" and _is_cure_db(db_name):
        return "cure_case_report_document.html"
    return "document.html"


# ── Demo routes ──────────────────────────────────────────────────────────────

if _demo_queries_enabled:
    import src.qa_browser.demo_routes as _demo_module  # noqa: E402
    app.include_router(_demo_module.router)
    _demo_module.set_templates(templates)

# ── POUNCE validation routes ─────────────────────────────────────────────────

import src.qa_browser.pounce_routes as _pounce_module  # noqa: E402
app.include_router(_pounce_module.router)
_pounce_module.set_templates(templates)
_pounce_module.set_mysql_engine_getter(get_mysql_db_engine)
# set_pounce_config is called in main() after args are parsed

# ── Feedback routes ───────────────────────────────────────────────────────────

import src.qa_browser.feedback_routes as _feedback_module  # noqa: E402
app.include_router(_feedback_module.router)
_feedback_module.set_templates(templates)
# set_feedback_file is called in main() after args are parsed


# ── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="QA Browser")
    parser.add_argument("--credentials", "-c",
                        default="./src/use_cases/secrets/local_arangodb.yaml",
                        help="Path to ArangoDB credentials YAML file")
    parser.add_argument("--mysql-credentials", "-m",
                        action="append",
                        default=[],
                        help="Path to a MySQL credentials YAML file; repeat to load multiple MySQL servers")
    parser.add_argument("--minio-credentials", "-s",
                        default=None,
                        help="Path to registry storage credentials YAML file (MinIO or AWS assume-role)")
    parser.add_argument("--parquet-storage-credentials",
                        default=None,
                        help="Path to object-storage credentials YAML for existing Dataset parquet files")
    parser.add_argument("--port", "-p", type=int, default=8050)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--root-path", default="", help="ASGI root path for running behind a sub-path proxy (e.g. /odin-qa)")
    parser.add_argument("--pounce-config", "-P", default="./src/use_cases/pounce/pounce.yaml",
                        help="Path to pounce.yaml — used to load resolvers for mapping coverage checks")
    parser.add_argument("--smtp-credentials", "-e",
                        default=None,
                        help="Path to SMTP credentials YAML (host, port, user, password, from_address, to_address, use_tls)")
    parser.add_argument("--feedback-file", "-f",
                        default=None,
                        help="Path to JSON file for storing feedback comments (created if missing)")
    parser.add_argument("--ramp-diagnosis-file",
                        default=os.getenv("QA_BROWSER_RAMP_DIAGNOSIS_FILE", "input_files/ramp_id_diagnoses.json"),
                        help="Path to JSON file for storing legacy RaMP merge diagnoses")
    parser.add_argument("--pounce-project-base-url",
                        default="",
                        help="Public base URL for project detail links, e.g. https://pounce-ci.ncats.nih.gov/project")
    args = parser.parse_args()

    global _credentials, _mysql_credentials, _mysql_sources, _minio_credentials, _parquet_storage_credentials
    templates.env.globals["root_path"] = args.root_path.rstrip("/")
    cred_path = Path(args.credentials)
    if cred_path.exists():
        with open(cred_path) as f:
            _credentials = yaml.safe_load(f)
        print(f"Loaded ArangoDB credentials from {cred_path}")
    else:
        print(f"Warning: {cred_path} not found, using defaults")
        _credentials = {"url": "http://localhost:8529", "user": "root", "password": "password"}

    for index, mysql_cred_path in enumerate(args.mysql_credentials, start=1):
        mysql_path = Path(mysql_cred_path)
        if not mysql_path.exists():
            print(f"Warning: MySQL credentials file {mysql_path} not found")
            continue

        with open(mysql_path) as f:
            creds = yaml.safe_load(f) or {}

        source_id = "default" if not _mysql_sources else _slugify_mysql_source(mysql_path.stem)
        if source_id in _mysql_sources:
            suffix = 2
            while f"{source_id}-{suffix}" in _mysql_sources:
                suffix += 1
            source_id = f"{source_id}-{suffix}"

        label = "default" if source_id == "default" else mysql_path.stem
        _register_mysql_source(source_id, creds, label=label)
        if index == 1:
            _mysql_credentials = creds
        print(f"Loaded MySQL credentials from {mysql_path} as source '{source_id}'")

    if _demo_queries_enabled:
        _demo_module.set_mysql_credentials(_mysql_credentials)

    if args.minio_credentials:
        minio_path = Path(args.minio_credentials)
        if minio_path.exists():
            with open(minio_path) as f:
                _minio_credentials = yaml.safe_load(f)
            print(f"Loaded registry storage credentials from {minio_path}")
        else:
            print(f"Warning: registry storage credentials file {minio_path} not found")

    if args.parquet_storage_credentials:
        parquet_storage_path = Path(args.parquet_storage_credentials)
        if parquet_storage_path.exists():
            with open(parquet_storage_path) as f:
                _parquet_storage_credentials = yaml.safe_load(f)
            print(f"Loaded parquet storage credentials from {parquet_storage_path}")
        else:
            print(f"Warning: parquet storage credentials file {parquet_storage_path} not found")

    _pounce_module.set_pounce_config(args.pounce_config)
    _pounce_module.set_smtp_config(args.smtp_credentials)
    _pounce_module.set_public_project_base_url(args.pounce_project_base_url)
    _feedback_module.set_feedback_file(args.feedback_file)
    set_ramp_diagnosis_file(args.ramp_diagnosis_file)

    print(f"Starting QA Browser at http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port, root_path=args.root_path)


if __name__ == "__main__":
    main()
