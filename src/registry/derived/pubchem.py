import csv
import gzip
import json
import re
import time
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from xml.etree import ElementTree

import requests

from src.registry.fetchers import ArtifactFile, DerivedArtifact, DerivedArtifactBuilder, ResolvedDependency
from src.registry.manifest import sha256_file, today_utc


PUBCHEM_SOURCE = "pubchem"
CID_SET_DATASET = "compound_cid_set"
COMPOUND_RECORDS_DATASET = "compound_records"
CID_MOLECULAR_INFO_DATASET = "cid_molecular_info"

PUBCHEM_COMPOUND_PREFIX = "PUBCHEM.COMPOUND"
PUBCHEM_ID_RE = re.compile(r"^(?:PUBCHEM\.COMPOUND:|pubchem:)?(?:CID)?(\d+)$", re.IGNORECASE)
WIKIPATHWAYS_PUBCHEM_RE = re.compile(
    r"(?:identifiers\.org/pubchem\.compound/|rdf\.ncbi\.nlm\.nih\.gov/pubchem/compound/CID)(\d+)"
)


def _require_dependency(
    dependencies: List[ResolvedDependency],
    *,
    source: str,
    dataset: str,
) -> ResolvedDependency:
    matches = [
        dependency
        for dependency in dependencies
        if dependency.source == source and dependency.dataset == dataset
    ]
    if not matches:
        raise LookupError(f"Missing derived artifact dependency {source}/{dataset}")
    if len(matches) > 1:
        raise ValueError(f"Multiple derived artifact dependencies match {source}/{dataset}")
    return matches[0]


def _max_version_date(dependencies: Iterable[ResolvedDependency]) -> Optional[str]:
    version_dates = [
        dependency.manifest.get("version_date") or dependency.version
        for dependency in dependencies
        if dependency.manifest.get("version_date") or dependency.version
    ]
    return max(version_dates) if version_dates else None


def _normalize_pubchem_id(value: Optional[str]) -> Optional[Tuple[str, str]]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NA", "N/A", "NONE", "NULL", "0"}:
        return None
    match = PUBCHEM_ID_RE.match(text)
    if not match:
        return None
    cid = match.group(1)
    return cid, f"{PUBCHEM_COMPOUND_PREFIX}:{cid}"


class PubchemCompoundCidSetBuilder(DerivedArtifactBuilder):
    source = PUBCHEM_SOURCE
    dataset = CID_SET_DATASET

    def build(
        self,
        *,
        config: dict,
        dependencies: List[ResolvedDependency],
        dest: Path,
        version: str,
    ) -> DerivedArtifact:
        output_path = dest / (config.get("output") or {}).get("file_name", "pubchem_compound_cids.tsv")
        rows: Set[Tuple[str, str, str, str, str]] = set()

        for dependency in dependencies:
            if dependency.source == "hmdb" and dependency.dataset == "metabolites_xml":
                rows.update(_pubchem_rows_from_hmdb(dependency.file("hmdb_metabolites.zip")))
            elif dependency.source == "wikipathways" and dependency.dataset == "rdf_wp":
                rows.update(_pubchem_rows_from_wikipathways(dependency))
            elif dependency.source == "lipidmaps" and dependency.dataset == "lmsd_sdf":
                rows.update(_pubchem_rows_from_lipidmaps(dependency.file("LMSD.sdf.zip")))
            elif dependency.source == "refmet" and dependency.dataset == "metabolites_csv":
                rows.update(_pubchem_rows_from_refmet(dependency.file("refmet.csv")))

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, delimiter="\t")
            writer.writerow(["pubchem_id", "cid", "reported_by_source", "reported_by_source_id", "source_field"])
            for row in sorted(rows, key=lambda item: (int(item[1]), item[2], item[3], item[4])):
                writer.writerow(row)

        distinct_cids = {row[1] for row in rows}
        return DerivedArtifact(
            source=self.source,
            dataset=self.dataset,
            version=version,
            version_date=_max_version_date(dependencies),
            derived_from=_dependency_manifest_refs(dependencies),
            transform=config.get("transform") or {"name": "pubchem_compound_cid_set", "version": 1},
            files=[ArtifactFile(output_path, "text/tab-separated-values")],
            stats={
                "row_count": len(rows),
                "distinct_cid_count": len(distinct_cids),
                "source_counts": _source_counts(rows),
            },
        )


class PubchemCompoundRecordsBuilder(DerivedArtifactBuilder):
    source = PUBCHEM_SOURCE
    dataset = COMPOUND_RECORDS_DATASET

    def build(
        self,
        *,
        config: dict,
        dependencies: List[ResolvedDependency],
        dest: Path,
        version: str,
    ) -> DerivedArtifact:
        dependency = _require_dependency(dependencies, source=PUBCHEM_SOURCE, dataset=CID_SET_DATASET)
        cids = _load_cids_from_cid_set(dependency.file("pubchem_compound_cids.tsv"))
        max_cids = config.get("max_cids")
        if max_cids is not None:
            cids = cids[: int(max_cids)]

        output_dir = dest
        output_dir.mkdir(parents=True, exist_ok=True)
        batch_size = int(config.get("batch_size", 100))
        delay_seconds = float(config.get("delay_seconds", 0.25))
        timeout = int(config.get("timeout", 120))

        output_config = config.get("output") or {}
        manifest_file_name = output_config.get("manifest_file_name", "pubchem_compound_records_manifest.tsv")
        batch_file_prefix = output_config.get("batch_file_prefix", "pubchem_compound_records_batch")
        manifest_path = output_dir / manifest_file_name
        artifact_files = [ArtifactFile(manifest_path, "text/tab-separated-values")]
        ok_count = 0
        not_found_count = 0
        error_count = 0

        with manifest_path.open("w", newline="", encoding="utf-8") as manifest_handle:
            writer = csv.writer(manifest_handle, delimiter="\t")
            writer.writerow(["cid", "pubchem_id", "batch_file", "status", "http_status", "payload_sha256", "error"])
            for batch_number, batch_cids in enumerate(_chunks(cids, batch_size), start=1):
                batch_file = output_dir / f"{batch_file_prefix}_{batch_number:06d}.json.gz"
                try:
                    payload, http_status = _fetch_pubchem_compound_batch(
                        batch_cids,
                        timeout=timeout,
                    )
                except Exception as exc:
                    payload, cid_statuses = _fetch_pubchem_compounds_individually(
                        batch_cids,
                        timeout=timeout,
                        delay_seconds=delay_seconds,
                        batch_error=str(exc),
                    )
                else:
                    cid_statuses = {
                        cid: {
                            "status": "ok",
                            "http_status": str(http_status),
                            "error": "",
                        }
                        for cid in batch_cids
                    }
                with gzip.open(batch_file, "wt", encoding="utf-8") as handle:
                    json.dump(payload, handle, separators=(",", ":"))
                payload_sha = sha256_file(batch_file)
                artifact_files.append(ArtifactFile(batch_file, "application/gzip"))
                for cid in batch_cids:
                    cid_status = cid_statuses[cid]
                    status = cid_status["status"]
                    if status == "ok":
                        ok_count += 1
                    elif status == "not_found":
                        not_found_count += 1
                    else:
                        error_count += 1
                    writer.writerow([
                        cid,
                        f"{PUBCHEM_COMPOUND_PREFIX}:{cid}",
                        batch_file.name,
                        cid_status["status"],
                        cid_status["http_status"],
                        payload_sha if status == "ok" else "",
                        cid_status["error"],
                    ])
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

        return DerivedArtifact(
            source=self.source,
            dataset=self.dataset,
            version=version,
            version_date=today_utc(),
            derived_from=_dependency_manifest_refs(dependencies),
            transform=config.get("transform") or {"name": "pubchem_compound_records", "version": 1},
            files=artifact_files,
            stats={
                "requested_cid_count": len(cids),
                "ok_cid_count": ok_count,
                "not_found_cid_count": not_found_count,
                "error_cid_count": error_count,
                "batch_size": batch_size,
            },
        )


class PubchemCidMolecularInfoBuilder(DerivedArtifactBuilder):
    source = PUBCHEM_SOURCE
    dataset = CID_MOLECULAR_INFO_DATASET

    def build(
        self,
        *,
        config: dict,
        dependencies: List[ResolvedDependency],
        dest: Path,
        version: str,
    ) -> DerivedArtifact:
        dependency = _require_dependency(dependencies, source=PUBCHEM_SOURCE, dataset=COMPOUND_RECORDS_DATASET)
        output_path = dest / (config.get("output") or {}).get("file_name", "cid_molecular_info.tsv")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        rows = _molecular_info_rows_from_records(dependency.local_dir)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                delimiter="\t",
                fieldnames=[
                    "pubchem_id",
                    "cid",
                    "monoisotopic_mass",
                    "inchikey",
                    "inchi_key_prefix",
                    "molecular_formula",
                    "molecular_weight",
                    "canonical_smiles",
                    "isomeric_smiles",
                    "inchi",
                    "iupac_name",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)

        return DerivedArtifact(
            source=self.source,
            dataset=self.dataset,
            version=version,
            version_date=dependency.version,
            derived_from=_dependency_manifest_refs(dependencies),
            transform=config.get("transform") or {"name": "pubchem_cid_molecular_info", "version": 1},
            files=[ArtifactFile(output_path, "text/tab-separated-values")],
            stats={
                "row_count": len(rows),
                "with_inchikey_count": sum(1 for row in rows if row.get("inchikey")),
                "with_monoisotopic_mass_count": sum(1 for row in rows if row.get("monoisotopic_mass")),
            },
        )


def _dependency_manifest_refs(dependencies: Iterable[ResolvedDependency]) -> List[Dict[str, str]]:
    return [
        {
            "snapshot_id": dependency.snapshot_id,
            "manifest_uri": dependency.manifest_uri,
        }
        for dependency in dependencies
    ]


def _source_counts(rows: Iterable[Tuple[str, str, str, str, str]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for _pubchem_id, _cid, source, _source_id, _field in rows:
        counts[source] = counts.get(source, 0) + 1
    return counts


def _pubchem_rows_from_hmdb(path: Path) -> Set[Tuple[str, str, str, str, str]]:
    rows: Set[Tuple[str, str, str, str, str]] = set()
    with zipfile.ZipFile(path) as archive:
        with archive.open("hmdb_metabolites.xml") as handle:
            accession = None
            pubchem_id = None
            for event, elem in ElementTree.iterparse(handle, events=("end",)):
                tag = elem.tag.rsplit("}", 1)[-1]
                if tag == "accession":
                    accession = (elem.text or "").strip()
                elif tag == "pubchem_compound_id":
                    pubchem_id = (elem.text or "").strip()
                elif tag == "metabolite":
                    normalized = _normalize_pubchem_id(pubchem_id)
                    if normalized and accession:
                        cid, normalized_id = normalized
                        rows.add((normalized_id, cid, "hmdb", f"HMDB:{accession}", "pubchem_compound_id"))
                    accession = None
                    pubchem_id = None
                    elem.clear()
    return rows


def _pubchem_rows_from_wikipathways(dependency: ResolvedDependency) -> Set[Tuple[str, str, str, str, str]]:
    zip_path = next(path for path in dependency.local_dir.iterdir() if path.suffix == ".zip")
    rows: Set[Tuple[str, str, str, str, str]] = set()
    with zipfile.ZipFile(zip_path) as archive:
        for name in archive.namelist():
            if name.endswith("/"):
                continue
            if not name.endswith((".ttl", ".rdf", ".nt")):
                continue
            with archive.open(name) as handle:
                for raw_line in handle:
                    line = raw_line.decode("utf-8", errors="ignore")
                    for match in WIKIPATHWAYS_PUBCHEM_RE.finditer(line):
                        cid, normalized_id = _normalize_pubchem_id(match.group(1))
                        rows.add((normalized_id, cid, "wikipathways", "", "pubchem.compound"))
    return rows


def _pubchem_rows_from_lipidmaps(path: Path) -> Set[Tuple[str, str, str, str, str]]:
    rows: Set[Tuple[str, str, str, str, str]] = set()
    with zipfile.ZipFile(path) as archive:
        with archive.open("structures.sdf") as handle:
            lm_id = ""
            pending_field = None
            for raw_line in handle:
                line = raw_line.decode("utf-8", errors="ignore").strip()
                if pending_field == "LM_ID":
                    lm_id = f"LIPIDMAPS:{line}"
                    pending_field = None
                    continue
                if pending_field == "PUBCHEM_CID":
                    normalized = _normalize_pubchem_id(line)
                    if normalized:
                        cid, normalized_id = normalized
                        rows.add((normalized_id, cid, "lipidmaps", lm_id, "PUBCHEM_CID"))
                    pending_field = None
                    continue
                if line == "> <LM_ID>":
                    pending_field = "LM_ID"
                elif line == "> <PUBCHEM_CID>":
                    pending_field = "PUBCHEM_CID"
                elif line == "$$$$":
                    lm_id = ""
                    pending_field = None
    return rows


def _pubchem_rows_from_refmet(path: Path) -> Set[Tuple[str, str, str, str, str]]:
    rows: Set[Tuple[str, str, str, str, str]] = set()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        reader.fieldnames = [field.strip() for field in reader.fieldnames or []]
        for row in reader:
            clean = {key.strip(): (value or "").strip() for key, value in row.items()}
            normalized = _normalize_pubchem_id(clean.get("pubchem_cid"))
            refmet_id = clean.get("refmet_id")
            if normalized and refmet_id:
                cid, normalized_id = normalized
                rows.add((normalized_id, cid, "refmet", f"REFMET:{refmet_id}", "pubchem_cid"))
    return rows


def _load_cids_from_cid_set(path: Path) -> List[str]:
    cids: Set[str] = set()
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            cid = (row.get("cid") or "").strip()
            if cid:
                cids.add(cid)
    return sorted(cids, key=int)


def _chunks(values: List[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _fetch_pubchem_compound_batch(cids: List[str], *, timeout: int) -> Tuple[dict, int]:
    cid_list = ",".join(cids)
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid_list}/JSON"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json(), response.status_code


def _fetch_pubchem_compounds_individually(
    cids: List[str],
    *,
    timeout: int,
    delay_seconds: float,
    batch_error: str,
) -> Tuple[dict, Dict[str, Dict[str, str]]]:
    compounds = []
    statuses: Dict[str, Dict[str, str]] = {}
    for cid in cids:
        url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{cid}/JSON"
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                payload = response.json()
                compounds.extend(payload.get("PC_Compounds") or [])
                statuses[cid] = {
                    "status": "ok",
                    "http_status": str(response.status_code),
                    "error": "",
                }
            elif response.status_code == 404:
                statuses[cid] = {
                    "status": "not_found",
                    "http_status": str(response.status_code),
                    "error": f"404 Client Error: PUGREST.NotFound for url: {url}",
                }
            else:
                statuses[cid] = {
                    "status": "error",
                    "http_status": str(response.status_code),
                    "error": response.text[:500],
                }
        except Exception as exc:
            statuses[cid] = {
                "status": "error",
                "http_status": "",
                "error": str(exc),
            }
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    return {
        "PC_Compounds": compounds,
        "recovered_from_individual_requests": True,
        "batch_error": batch_error,
    }, statuses


def _molecular_info_rows_from_records(records_dir: Path) -> List[Dict[str, str]]:
    rows: Dict[str, Dict[str, str]] = {}
    manifest_path = records_dir / "pubchem_compound_records_manifest.tsv"
    with manifest_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        ok_batches = sorted({row["batch_file"] for row in reader if row.get("status") == "ok" and row.get("batch_file")})

    for batch_file in ok_batches:
        with gzip.open(records_dir / batch_file, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        for compound in payload.get("PC_Compounds", []) or []:
            row = _molecular_info_row_from_pubchem_compound(compound)
            if row.get("cid"):
                rows[row["cid"]] = row
    return [rows[cid] for cid in sorted(rows, key=int)]


def _molecular_info_row_from_pubchem_compound(compound: dict) -> Dict[str, str]:
    cid = str(((compound.get("id") or {}).get("id") or {}).get("cid") or "")
    row = {
        "pubchem_id": f"{PUBCHEM_COMPOUND_PREFIX}:{cid}" if cid else "",
        "cid": cid,
        "monoisotopic_mass": "",
        "inchikey": "",
        "inchi_key_prefix": "",
        "molecular_formula": "",
        "molecular_weight": "",
        "canonical_smiles": "",
        "isomeric_smiles": "",
        "inchi": "",
        "iupac_name": "",
    }
    for prop in compound.get("props", []) or []:
        urn = prop.get("urn") or {}
        label = str(urn.get("label") or "").lower()
        name = str(urn.get("name") or "").lower()
        value = _pubchem_prop_value(prop.get("value") or {})
        if value is None:
            continue
        value = str(value)
        if label == "molecular formula":
            row["molecular_formula"] = value
        elif label == "molecular weight":
            row["molecular_weight"] = value
        elif (label == "weight" and name == "monoisotopic") or (label == "mass" and name == "exact"):
            row["monoisotopic_mass"] = value
        elif label == "inchi":
            row["inchi"] = value
        elif label == "inchikey":
            row["inchikey"] = value
            row["inchi_key_prefix"] = value.split("-", 1)[0]
        elif label == "smiles" and name in {"canonical", "connectivity"}:
            row["canonical_smiles"] = value
        elif label == "smiles" and name in {"isomeric", "absolute"}:
            row["isomeric_smiles"] = value
        elif label == "iupac name" and name in {"preferred", "allowed", "cas-like style"} and not row["iupac_name"]:
            row["iupac_name"] = value
    return row


def _pubchem_prop_value(value: dict):
    for key in ("sval", "fval", "ival"):
        if key in value:
            return value[key]
    return None
