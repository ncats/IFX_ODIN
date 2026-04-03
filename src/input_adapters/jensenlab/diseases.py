import csv
import os
from datetime import date, datetime
from typing import Generator, List, Optional, Union

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, DiseaseAssociationDetail, ProteinDiseaseEdge
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein


class JensenLabDiseasesAdapter(InputAdapter):
    knowledge_fieldnames = [
        "protein_id",
        "gene_symbol",
        "disease_id",
        "disease_name",
        "source_label",
        "evidence_text",
        "confidence",
    ]
    experiments_fieldnames = knowledge_fieldnames
    textmining_fieldnames = [
        "protein_id",
        "gene_symbol",
        "disease_id",
        "disease_name",
        "zscore",
        "confidence",
        "url",
    ]

    def __init__(
        self,
        knowledge_file_path: str,
        experiments_file_path: str,
        textmining_file_path: str,
        version_file_path: Optional[str] = None,
        max_rows: Optional[int] = None,
        textmining_min_zscore: Optional[float] = None,
    ):
        self.knowledge_file_path = knowledge_file_path
        self.experiments_file_path = experiments_file_path
        self.textmining_file_path = textmining_file_path
        self.version_file_path = version_file_path
        self.max_rows = max_rows
        self.textmining_min_zscore = textmining_min_zscore

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.JensenLabDiseases

    def get_version(self) -> DatasourceVersionInfo:
        version = None
        version_date = None
        if self.version_file_path and os.path.exists(self.version_file_path):
            with open(self.version_file_path, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                row = next(reader, None)
                if row:
                    version = row.get("version") or None
                    raw_version_date = row.get("version_date") or None
                    if raw_version_date:
                        try:
                            version_date = date.fromisoformat(raw_version_date)
                        except ValueError:
                            version_date = None

        download_date = self._download_date()
        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        diseases_by_id = {}
        edges: List[ProteinDiseaseEdge] = []

        for row in self._iter_rows(
            self.knowledge_file_path,
            self.knowledge_fieldnames,
            detail_builder=self._build_knowledge_detail,
        ):
            disease = self._disease_from_row(row)
            diseases_by_id.setdefault(disease.id, disease)
            edges.append(self._edge_from_row(row, disease, self._build_knowledge_detail(row)))

        for row in self._iter_rows(
            self.experiments_file_path,
            self.experiments_fieldnames,
            detail_builder=self._build_experiments_detail,
        ):
            disease = self._disease_from_row(row)
            diseases_by_id.setdefault(disease.id, disease)
            edges.append(self._edge_from_row(row, disease, self._build_experiments_detail(row)))

        for row in self._iter_rows(
            self.textmining_file_path,
            self.textmining_fieldnames,
            detail_builder=self._build_textmining_detail,
            row_filter=self._keep_textmining_row,
        ):
            disease = self._disease_from_row(row)
            diseases_by_id.setdefault(disease.id, disease)
            edges.append(self._edge_from_row(row, disease, self._build_textmining_detail(row)))

        yield list(diseases_by_id.values())
        for i in range(0, len(edges), self.batch_size):
            yield edges[i:i + self.batch_size]

    def _iter_rows(self, file_path: str, fieldnames: List[str], detail_builder, row_filter=None):
        kept_rows = 0
        with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, fieldnames=fieldnames, delimiter="\t")
            for row in reader:
                protein_id = (row.get("protein_id") or "").strip()
                disease_id = (row.get("disease_id") or "").strip()
                disease_name = (row.get("disease_name") or "").strip()
                if not protein_id or not disease_id or not disease_name:
                    continue
                if detail_builder(row) is None:
                    continue
                if row_filter is not None and not row_filter(row):
                    continue
                yield row
                kept_rows += 1
                if self.max_rows is not None and kept_rows >= self.max_rows:
                    break

    def _edge_from_row(
        self,
        row: dict,
        disease: Disease,
        detail: DiseaseAssociationDetail,
    ) -> ProteinDiseaseEdge:
        return ProteinDiseaseEdge(
            start_node=Protein(
                id=EquivalentId(id=(row.get("protein_id") or "").strip(), type=Prefix.ENSEMBL).id_str()
            ),
            end_node=disease,
            details=[detail],
        )

    @staticmethod
    def _disease_from_row(row: dict) -> Disease:
        return Disease(
            id=(row.get("disease_id") or "").strip(),
            name=(row.get("disease_name") or "").strip(),
        )

    @staticmethod
    def _build_knowledge_detail(row: dict) -> DiseaseAssociationDetail:
        source_label = (row.get("source_label") or "").strip()
        source = f"JensenLab Knowledge {source_label}" if source_label else "JensenLab Knowledge"
        evidence_text = (row.get("evidence_text") or "").strip()
        return DiseaseAssociationDetail(
            source=source,
            source_id=(row.get("disease_id") or "").strip() or None,
            evidence_terms=[evidence_text] if evidence_text else [],
            confidence=JensenLabDiseasesAdapter._parse_float(row.get("confidence")),
        )

    @staticmethod
    def _build_experiments_detail(row: dict) -> DiseaseAssociationDetail:
        source_label = (row.get("source_label") or "").strip()
        source = f"JensenLab Experiment {source_label}" if source_label else "JensenLab Experiment"
        evidence_text = (row.get("evidence_text") or "").strip()
        return DiseaseAssociationDetail(
            source=source,
            source_id=(row.get("disease_id") or "").strip() or None,
            evidence_terms=[evidence_text] if evidence_text else [],
            confidence=JensenLabDiseasesAdapter._parse_float(row.get("confidence")),
        )

    @staticmethod
    def _build_textmining_detail(row: dict) -> DiseaseAssociationDetail:
        return DiseaseAssociationDetail(
            source="JensenLab Text Mining",
            source_id=(row.get("disease_id") or "").strip() or None,
            confidence=JensenLabDiseasesAdapter._parse_float(row.get("confidence")),
            zscore=JensenLabDiseasesAdapter._parse_float(row.get("zscore")),
            url=(row.get("url") or "").strip() or None,
        )

    def _keep_textmining_row(self, row: dict) -> bool:
        if self.textmining_min_zscore is None:
            return True
        zscore = self._parse_float(row.get("zscore"))
        return zscore is not None and zscore >= self.textmining_min_zscore

    def _download_date(self) -> Optional[date]:
        timestamps = []
        for file_path in (
            self.knowledge_file_path,
            self.experiments_file_path,
            self.textmining_file_path,
        ):
            if os.path.exists(file_path):
                timestamps.append(os.path.getmtime(file_path))
        if not timestamps:
            return None
        return datetime.fromtimestamp(max(timestamps)).date()

    @staticmethod
    def _parse_float(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
