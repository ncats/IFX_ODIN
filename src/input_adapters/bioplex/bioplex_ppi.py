import csv
from datetime import date
from pathlib import Path
from typing import Generator, List, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.ppi import PPIEdge
from src.models.protein import Protein


class BioPlexPPIAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo

    def __init__(
        self,
        file_path: str,
        version_file_path: Optional[str] = None,
        max_rows: Optional[int] = None,
    ):
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.max_rows = max_rows
        self.version_info = self._load_version_info(version_file_path)

    def _load_version_info(self, version_file_path: Optional[str]) -> DatasourceVersionInfo:
        version = None
        version_date = None
        download_date = self.download_date
        if version_file_path:
            with open(version_file_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                matching_row = None
                for row in reader:
                    if row.get("file") == Path(self.file_path).name:
                        matching_row = row
                        break
                if matching_row:
                    version_label = matching_row.get("version") or None
                    dataset_label = matching_row.get("dataset") or None
                    if version_label and dataset_label:
                        cell_line = dataset_label.replace("BioPlex", "", 1).strip().replace(version_label, "", 1).strip()
                        version = f"{version_label} ({cell_line})" if cell_line else version_label
                    else:
                        version = version_label or dataset_label or None
                    version_date_str = matching_row.get("version_date") or None
                    download_date_str = matching_row.get("download_date") or None
                    version_date = date.fromisoformat(version_date_str) if version_date_str else None
                    download_date = date.fromisoformat(download_date_str) if download_date_str else download_date
        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.BioPlex

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    @staticmethod
    def _normalize_value(value: str) -> str:
        return value.strip().strip('"')

    @classmethod
    def _protein_id(cls, uniprot_id: str, gene_id: str) -> str:
        if uniprot_id and uniprot_id != "UNKNOWN":
            return EquivalentId(id=uniprot_id, type=Prefix.UniProtKB).id_str()
        return EquivalentId(id=gene_id, type=Prefix.NCBIGene).id_str()

    def get_all(self) -> Generator[List[PPIEdge], None, None]:
        batch: List[PPIEdge] = []
        kept_rows = 0
        with open(self.file_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                if self.max_rows is not None and kept_rows >= self.max_rows:
                    break

                gene_a = self._normalize_value(row["GeneA"])
                gene_b = self._normalize_value(row["GeneB"])
                uniprot_a = self._normalize_value(row["UniprotA"])
                uniprot_b = self._normalize_value(row["UniprotB"])

                if gene_a == gene_b and uniprot_a == uniprot_b:
                    continue

                protein_a = self._protein_id(uniprot_a, gene_a)
                protein_b = self._protein_id(uniprot_b, gene_b)
                if protein_a == protein_b:
                    continue
                protein_a, protein_b = sorted((protein_a, protein_b))

                edge = PPIEdge(
                    start_node=Protein(id=protein_a),
                    end_node=Protein(id=protein_b),
                    p_wrong=[float(self._normalize_value(row["pW"]))],
                    p_ni=[float(self._normalize_value(row["pNI"]))],
                    p_int=[float(self._normalize_value(row["pInt"]))],
                )
                batch.append(edge)
                kept_rows += 1
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        yield batch
