import csv
import gzip
from typing import Generator, List, Optional, Union

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, DiseaseAssociationDetail, GeneDiseaseEdge
from src.models.gene import Gene
from src.models.node import EquivalentId, Node, Relationship


class CTDGeneDiseaseAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo
    fieldnames = [
        "GeneSymbol",
        "GeneID",
        "DiseaseName",
        "DiseaseID",
        "DirectEvidence",
        "OmimIDs",
        "PubMedIDs",
    ]

    def __init__(self, data_source, max_rows: Optional[int] = None):
        file_path = str(data_source.file("CTD_curated_genes_diseases.tsv.gz"))
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.CTD

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        diseases_by_id = {}
        edges: List[GeneDiseaseEdge] = []

        for row in self._iter_rows():
            disease_id = row["DiseaseID"].strip()
            disease_name = row["DiseaseName"].strip()
            gene_id = row["GeneID"].strip()

            if not disease_id or not disease_name or not gene_id:
                continue

            if disease_id not in diseases_by_id:
                diseases_by_id[disease_id] = Disease(id=disease_id, name=disease_name)

            edges.append(
                GeneDiseaseEdge(
                    start_node=Gene(id=EquivalentId(id=gene_id, type=Prefix.NCBIGene).id_str()),
                    end_node=Disease(id=disease_id, name=disease_name),
                    details=[
                        DiseaseAssociationDetail(
                            source="CTD",
                            source_id=disease_id,
                            evidence_terms=self._split_pipe_list(row.get("DirectEvidence")),
                            pmids=self._split_pipe_list(row.get("PubMedIDs")),
                        )
                    ],
                )
            )
        yield list(diseases_by_id.values())
        for i in range(0, len(edges), self.batch_size):
            yield edges[i:i + self.batch_size]

    def _iter_rows(self):
        with gzip.open(self.file_path, "rt", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(
                (line for line in handle if line and not line.startswith("#")),
                fieldnames=self.fieldnames,
                delimiter="\t",
            )
            for idx, row in enumerate(reader):
                if self.max_rows is not None and idx >= self.max_rows:
                    break
                yield row

    @staticmethod
    def _split_pipe_list(value: Optional[str]) -> List[str]:
        if not value:
            return []
        return [item.strip() for item in value.split("|") if item.strip()]
