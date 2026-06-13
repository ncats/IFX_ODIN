import csv
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
        data_source,
        max_rows: Optional[int] = None,
    ):
        file_paths = [
            str(data_source.file(entry["path"]))
            for entry in data_source.manifest.get("files", []) or []
        ]
        if not file_paths:
            raise ValueError("BioPlexPPIAdapter requires at least one registered file")
        file_path = file_paths[0]
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.file_paths = file_paths
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

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
        for file_path in self.file_paths:
            with open(file_path, "r", encoding="utf-8", newline="") as handle:
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
            if self.max_rows is not None and kept_rows >= self.max_rows:
                break
        yield batch
