import csv
from collections import OrderedDict
from typing import Generator, List, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.ppi import PPIEdge
from src.models.protein import Protein


class ReactomePPIAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo

    def __init__(self, data_source, max_rows: Optional[int] = None):
        file_path = str(data_source.file("reactome.homo_sapiens.interactions.tab-delimited.txt"))
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Reactome

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    @staticmethod
    def _protein_id(raw_value: str) -> Optional[str]:
        value = raw_value.strip()
        if not value.startswith("uniprotkb:"):
            return None
        return EquivalentId(id=value.split(":", 1)[1], type=Prefix.UniProtKB).id_str()

    @staticmethod
    def _parse_pmids(raw_value: str) -> List[int]:
        if not raw_value:
            return []
        pmids = []
        for token in raw_value.replace(",", "|").replace(";", "|").split("|"):
            token = token.strip()
            if token.isdigit():
                pmids.append(int(token))
        return list(dict.fromkeys(pmids))

    def get_all(self) -> Generator[List[PPIEdge], None, None]:
        edges_by_key = OrderedDict()
        kept_rows = 0
        with open(self.file_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                protein1 = self._protein_id(row["# Interactor 1 uniprot id"])
                protein2 = self._protein_id(row["Interactor 2 uniprot id"])
                if protein1 is None or protein2 is None:
                    continue
                if protein1 == protein2:
                    continue
                protein1, protein2 = sorted((protein1, protein2))

                interaction_type = row["Interaction type"].strip()
                key = (protein1, protein2, interaction_type)
                if key not in edges_by_key:
                    if self.max_rows is not None and kept_rows >= self.max_rows:
                        continue
                    edges_by_key[key] = PPIEdge(
                        start_node=Protein(id=protein1),
                        end_node=Protein(id=protein2),
                        interaction_type=[interaction_type] if interaction_type else [],
                        contexts=[],
                        pmids=[],
                    )
                    kept_rows += 1
                edge = edges_by_key[key]
                context = row["Interaction context"].strip()
                if context and context not in edge.contexts:
                    edge.contexts.append(context)
                for pmid in self._parse_pmids(row["Pubmed references"].strip()):
                    if pmid not in edge.pmids:
                        edge.pmids.append(pmid)

        batch: List[PPIEdge] = []
        for edge in edges_by_key.values():
            batch.append(edge)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        yield batch
