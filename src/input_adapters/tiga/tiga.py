import csv
import json
import os
from collections import OrderedDict
from datetime import date, datetime
from typing import Generator, Iterable, List, Optional, Tuple, Union

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein
from src.models.tiga import (
    GwasAssociationDetail,
    GwasAssociationProvenance,
    GwasTrait,
    GwasTraitDiseaseEdge,
    ProteinGwasTraitEdge,
)


class TIGAAdapter(InputAdapter):
    stats_fieldnames = [
        "ensemblId",
        "efoId",
        "trait",
        "n_study",
        "n_snp",
        "n_snpw",
        "geneNtrait",
        "geneNstudy",
        "traitNgene",
        "traitNstudy",
        "pvalue_mlog_median",
        "pvalue_mlog_max",
        "or_median",
        "n_beta",
        "study_N_mean",
        "rcras",
        "geneSymbol",
        "TDL",
        "geneFamily",
        "geneIdgList",
        "geneName",
        "meanRank",
        "meanRankScore",
    ]

    provenance_fieldnames = [
        "ensemblId",
        "TRAIT_URI",
        "STUDY_ACCESSION",
        "PUBMEDID",
        "efoId",
    ]

    def __init__(
        self,
        stats_file_path: str,
        provenance_file_path: str,
        version_file_path: Optional[str] = None,
        max_rows: Optional[int] = None,
    ):
        self.stats_file_path = stats_file_path
        self.provenance_file_path = provenance_file_path
        self.version_file_path = version_file_path
        self.max_rows = max_rows

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TIGA

    def get_version(self) -> DatasourceVersionInfo:
        version = None
        version_date = None
        download_date = self._download_date()
        if self.version_file_path and os.path.exists(self.version_file_path):
            with open(self.version_file_path, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                row = next(reader, None)
                if row:
                    version = row.get("version") or None
                    raw_version_date = row.get("version_date") or None
                    raw_download_date = row.get("download_date") or None
                    if raw_version_date:
                        try:
                            version_date = date.fromisoformat(raw_version_date)
                        except ValueError:
                            version_date = None
                    if raw_download_date:
                        try:
                            download_date = date.fromisoformat(raw_download_date)
                        except ValueError:
                            pass
        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        provenance_iter = self._iter_grouped_provenance()
        current_prov_key, current_prov_rows = next(provenance_iter, (None, []))

        traits_by_id: OrderedDict[str, GwasTrait] = OrderedDict()
        trait_disease_edges_by_key: OrderedDict[str, GwasTraitDiseaseEdge] = OrderedDict()
        edges_by_key: OrderedDict[tuple[str, str], ProteinGwasTraitEdge] = OrderedDict()
        seen_detail_keys: set[tuple[str, str, str]] = set()
        emitted_rows = 0

        for row in self._iter_stats_rows():
            ensg = (row.get("ensemblId") or "").strip()
            trait_id = (row.get("efoId") or "").strip()
            key = (ensg, trait_id)
            while current_prov_key is not None and current_prov_key < key:
                current_prov_key, current_prov_rows = next(provenance_iter, (None, []))

            provenance_rows = []
            if current_prov_key == key:
                provenance_rows = current_prov_rows
                current_prov_key, current_prov_rows = next(provenance_iter, (None, []))

            traits_by_id.setdefault(
                trait_id,
                self._trait_from_row(row, provenance_rows),
            )
            trait_disease_edges_by_key.setdefault(
                trait_id,
                self._trait_disease_edge_from_trait(traits_by_id[trait_id]),
            )

            detail = self._detail_from_row(row, provenance_rows)
            edge_key = (ensg, trait_id)
            detail_key = self._detail_key(trait_id, detail)

            if edge_key not in edges_by_key:
                edges_by_key[edge_key] = ProteinGwasTraitEdge(
                    start_node=Protein(id=EquivalentId(id=ensg, type=Prefix.ENSEMBL).id_str()),
                    end_node=traits_by_id[trait_id],
                    details=[],
                )
            if detail_key not in seen_detail_keys:
                seen_detail_keys.add(detail_key)
                edges_by_key[edge_key].details.append(detail)

            emitted_rows += 1
            if self.max_rows is not None and emitted_rows >= self.max_rows:
                break

        yield list(traits_by_id.values())
        yield list(trait_disease_edges_by_key.values())
        edge_values = list(edges_by_key.values())
        for i in range(0, len(edge_values), self.batch_size):
            yield edge_values[i:i + self.batch_size]

    def _iter_stats_rows(self) -> Iterable[dict]:
        with open(self.stats_file_path, "r", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if reader.fieldnames != self.stats_fieldnames:
                raise ValueError(f"Unexpected TIGA stats header: {reader.fieldnames}")
            for row in reader:
                if not (row.get("ensemblId") or "").strip():
                    continue
                if not (row.get("efoId") or "").strip():
                    continue
                if not (row.get("trait") or "").strip():
                    continue
                yield row

    def _iter_grouped_provenance(self) -> Iterable[Tuple[Tuple[str, str], List[GwasAssociationProvenance]]]:
        current_key = None
        current_rows: List[GwasAssociationProvenance] = []
        with open(self.provenance_file_path, "r", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if reader.fieldnames != self.provenance_fieldnames:
                raise ValueError(f"Unexpected TIGA provenance header: {reader.fieldnames}")
            for row in reader:
                key = ((row.get("ensemblId") or "").strip(), (row.get("efoId") or "").strip())
                if not key[0] or not key[1]:
                    continue
                prov = GwasAssociationProvenance(
                    study_acc=(row.get("STUDY_ACCESSION") or "").strip(),
                    pubmedid=self._parse_int(row.get("PUBMEDID")),
                    trait_uri=(row.get("TRAIT_URI") or "").strip() or None,
                )
                if current_key is None:
                    current_key = key
                if key != current_key:
                    yield current_key, current_rows
                    current_key = key
                    current_rows = []
                current_rows.append(prov)
        if current_key is not None:
            yield current_key, current_rows

    @staticmethod
    def _trait_from_row(row: dict, provenance_rows: List[GwasAssociationProvenance]) -> GwasTrait:
        trait_uri = provenance_rows[0].trait_uri if provenance_rows else None
        return GwasTrait(
            id=(row.get("efoId") or "").strip(),
            name=(row.get("trait") or "").strip(),
            trait_uri=trait_uri,
        )

    @staticmethod
    def _detail_from_row(row: dict, provenance_rows: List[GwasAssociationProvenance]) -> GwasAssociationDetail:
        return GwasAssociationDetail(
            source=DataSourceName.TIGA.value,
            ensg=(row.get("ensemblId") or "").strip(),
            n_study=TIGAAdapter._parse_int(row.get("n_study")),
            n_snp=TIGAAdapter._parse_int(row.get("n_snp")),
            n_snpw=TIGAAdapter._parse_float(row.get("n_snpw")),
            geneNtrait=TIGAAdapter._parse_int(row.get("geneNtrait")),
            geneNstudy=TIGAAdapter._parse_int(row.get("geneNstudy")),
            traitNgene=TIGAAdapter._parse_int(row.get("traitNgene")),
            traitNstudy=TIGAAdapter._parse_int(row.get("traitNstudy")),
            pvalue_mlog_median=TIGAAdapter._parse_float(row.get("pvalue_mlog_median")),
            pvalue_mlog_max=TIGAAdapter._parse_float(row.get("pvalue_mlog_max")),
            or_median=TIGAAdapter._parse_float(row.get("or_median")),
            n_beta=TIGAAdapter._parse_int(row.get("n_beta")),
            study_N_mean=TIGAAdapter._parse_float(row.get("study_N_mean")),
            rcras=TIGAAdapter._parse_float(row.get("rcras")),
            gene_symbol=(row.get("geneSymbol") or "").strip() or None,
            gene_tdl=(row.get("TDL") or "").strip() or None,
            gene_family=(row.get("geneFamily") or "").strip() or None,
            gene_idg_list=TIGAAdapter._parse_bool(row.get("geneIdgList")),
            gene_name=(row.get("geneName") or "").strip() or None,
            meanRank=TIGAAdapter._parse_float(row.get("meanRank")),
            meanRankScore=TIGAAdapter._parse_float(row.get("meanRankScore")),
            provenance_details=provenance_rows,
        )

    @staticmethod
    def _detail_key(trait_id: str, detail: GwasAssociationDetail) -> tuple[str, str, str]:
        return detail.ensg or "", trait_id, json.dumps(detail.to_dict(), sort_keys=True)

    @staticmethod
    def _trait_disease_edge_from_trait(trait: GwasTrait) -> GwasTraitDiseaseEdge:
        disease_id = TIGAAdapter._trait_id_to_disease_id(trait.id)
        return GwasTraitDiseaseEdge(
            start_node=trait,
            end_node=Disease(
                id=disease_id,
                name=trait.name,
            ),
        )

    @staticmethod
    def _trait_id_to_disease_id(trait_id: str) -> str:
        raw = (trait_id or "").strip()
        if ":" in raw:
            return raw
        if "_" in raw:
            return raw.replace("_", ":", 1)
        return raw

    def _download_date(self) -> Optional[date]:
        timestamps = []
        for file_path in (self.stats_file_path, self.provenance_file_path):
            if os.path.exists(file_path):
                timestamps.append(os.path.getmtime(file_path))
        if not timestamps:
            return None
        return datetime.fromtimestamp(max(timestamps)).date()

    @staticmethod
    def _parse_int(value: Optional[str]) -> Optional[int]:
        raw = (value or "").strip()
        if not raw or raw == "NA":
            return None
        try:
            return int(float(raw))
        except ValueError:
            return None

    @staticmethod
    def _parse_float(value: Optional[str]) -> Optional[float]:
        raw = (value or "").strip()
        if not raw or raw == "NA":
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    @staticmethod
    def _parse_bool(value: Optional[str]) -> Optional[bool]:
        raw = (value or "").strip().lower()
        if not raw or raw == "na":
            return None
        if raw == "true":
            return True
        if raw == "false":
            return False
        return None
