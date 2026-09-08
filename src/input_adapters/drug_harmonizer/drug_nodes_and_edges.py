import csv
import json
import re
import sys
from typing import Any, Generator, List, Optional

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ligand import Ligand, ProteinLigandEdge, ActivityDetails
from src.models.protein import Protein


class DrugHarmonizerBaseAdapter(InputAdapter):
    batch_size = 25000

    @staticmethod
    def _raise_csv_field_limit() -> None:
        limit = sys.maxsize
        while True:
            try:
                csv.field_size_limit(limit)
                return
            except OverflowError:
                limit = int(limit / 10)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.DrugHarmonizer

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    @staticmethod
    def _clean(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _split_multi(value: Any) -> list[str]:
        text = DrugHarmonizerBaseAdapter._clean(value)
        if not text:
            return []
        out: list[str] = []
        for token in re.split(r"[|,]", text):
            token = token.strip()
            if token and token not in out:
                out.append(token)
        return out

    @staticmethod
    def _first_multi(value: Any) -> str:
        return next(iter(DrugHarmonizerBaseAdapter._split_multi(value)), "")

    @staticmethod
    def _bool(value: Any) -> Optional[bool]:
        text = DrugHarmonizerBaseAdapter._clean(value).lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
        return None

    @staticmethod
    def _float(value: Any) -> Optional[float]:
        text = DrugHarmonizerBaseAdapter._first_multi(value)
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    @staticmethod
    def _int(value: Any) -> Optional[int]:
        text = DrugHarmonizerBaseAdapter._first_multi(value)
        if not text or not re.fullmatch(r"\d+", text):
            return None
        return int(text)

    @staticmethod
    def _datasource(value: Any):
        text = DrugHarmonizerBaseAdapter._clean(value)
        lowered = text.lower()
        if lowered == "chembl":
            return DataSourceName.ChEMBL
        if lowered == "drugcentral":
            return DataSourceName.DrugCentral
        if lowered in {"iuphar", "iuphar/bps guide to pharmacology"}:
            return DataSourceName.IUPHAR
        if lowered in {"drug harmonizer", "ifx drug harmonizer"}:
            return DataSourceName.DrugHarmonizer
        return text or None


class DrugHarmonizerLigandNodeAdapter(DrugHarmonizerBaseAdapter):
    def __init__(self, data_source, file_name: str = "drug_nodes_full.tsv"):
        self.version_info = data_source.version_info()
        self.file_path = str(data_source.file(file_name))

    def get_all(self) -> Generator[List[Ligand], None, None]:
        self._raise_csv_field_limit()
        batch: list[Ligand] = []
        with open(self.file_path, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                drug_id = self._clean(row.get("drug_id")) or self._clean(row.get("primary_id"))
                if not drug_id:
                    continue
                ligand = Ligand(
                    id=drug_id,
                    name=self._clean(row.get("standard_name")) or drug_id,
                    smiles=self._first_multi(row.get("smiles")),
                    description=self._clean(row.get("definition")),
                    isDrug=self._is_drug(row),
                )
                batch.append(ligand)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    @classmethod
    def _is_drug(cls, row: dict[str, Any]) -> bool:
        scope_tokens = {
            token.strip().lower()
            for token in cls._clean(row.get("drug_scope")).split("|")
            if token.strip()
        }
        status_tokens = {
            token.strip().lower()
            for token in cls._clean(row.get("approval_status")).split("|")
            if token.strip()
        }
        if {"not_approved", "unapproved"} & (scope_tokens | status_tokens):
            return False
        return "approved_drug" in scope_tokens or "approved" in status_tokens


class DrugHarmonizerProteinLigandEdgeAdapter(DrugHarmonizerBaseAdapter):
    def __init__(
        self,
        data_source,
        file_name: str = "drug_edges_full.tsv",
        pchembl_cutoff: float | None = None,
    ):
        self.version_info = data_source.version_info()
        self.file_path = str(data_source.file(file_name))
        self.pchembl_cutoff = float(pchembl_cutoff) if pchembl_cutoff is not None else None

    def get_all(self) -> Generator[List[ProteinLigandEdge], None, None]:
        self._raise_csv_field_limit()
        rels: dict[tuple[str, str], ProteinLigandEdge] = {}
        with open(self.file_path, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                if self._clean(row.get("relation_kind")) != "drug_target":
                    continue
                ligand_id = self._clean(row.get("source_id"))
                protein_id = self._protein_resolver_id(row)
                if not ligand_id or not protein_id:
                    continue
                key = (protein_id, ligand_id)
                if key not in rels:
                    rels[key] = ProteinLigandEdge(
                        start_node=Protein(id=protein_id),
                        end_node=Ligand(id=ligand_id),
                        meets_idg_cutoff=self._edge_meets_cutoff(row),
                        details=[],
                    )
                rel = rels[key]
                if rel.meets_idg_cutoff is not True:
                    rel.meets_idg_cutoff = self._edge_meets_cutoff(row)
                rel.details.extend(self._activity_details(row))
                if len(rels) >= self.batch_size:
                    yield list(rels.values())
                    rels = {}
        if rels:
            yield list(rels.values())

    def _activity_details(self, row: dict[str, Any]) -> list[ActivityDetails]:
        details_json = self._clean(row.get("evidence_details"))
        details: list[dict[str, Any]] = []
        if details_json:
            try:
                parsed = json.loads(details_json)
                if isinstance(parsed, list):
                    details = [item for item in parsed if isinstance(item, dict)]
            except json.JSONDecodeError:
                details = []
        if not details:
            details = [row]
        return [self._one_activity_detail(detail) for detail in details]

    def _protein_resolver_id(self, row: dict[str, Any]) -> str:
        raw_target_id = self._clean(row.get("raw_target_id"))
        if raw_target_id:
            return raw_target_id
        uniprot_id = self._clean(row.get("target_uniprot_id"))
        if uniprot_id:
            return uniprot_id if ":" in uniprot_id else f"UniProtKB:{uniprot_id}"
        return self._clean(row.get("target_id"))

    def _one_activity_detail(self, detail: dict[str, Any]) -> ActivityDetails:
        activity_source = self._datasource(
            detail.get("activity_source")
            or detail.get("evidence_source")
            or detail.get("evidence_layer")
        )
        pmids = [pmid for pmid in (self._int(value) for value in self._split_multi(detail.get("activity_pmids"))) if pmid]
        moa_pmids = [pmid for pmid in (self._int(value) for value in self._split_multi(detail.get("mechanism_pmids"))) if pmid]
        reference = self._clean(detail.get("activity_reference")) or self._clean(detail.get("evidence_id"))
        return ActivityDetails(
            ref_id=self._int(reference),
            activity_source=activity_source,
            act_value=self._float(detail.get("activity_value")),
            act_type=self._clean(detail.get("activity_type")) or None,
            action_type=self._clean(detail.get("action_type")) or None,
            has_moa=self._bool(detail.get("has_moa")),
            reference=reference or None,
            act_pmids=pmids,
            moa_pmid=moa_pmids[0] if moa_pmids else None,
            act_source=self._clean(detail.get("activity_source")) or self._clean(detail.get("evidence_source")) or None,
            moa_source=self._clean(detail.get("mechanism_source")) or None,
            assay_type=self._clean(detail.get("assay_type")) or None,
            comment=self._clean(detail.get("activity_comment")) or None,
        )

    def _edge_meets_cutoff(self, row: dict[str, Any]) -> Optional[bool]:
        # Keep the family-aware TDL threshold calculation in the existing
        # SetLigandActivityFlagAdapter post-processing step.
        return self._bool(row.get("meets_idg_cutoff"))
