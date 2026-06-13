import csv
import gzip
from typing import Dict, Generator, List, Optional, Set, Tuple

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.models.publication import PublicationReference


class JensenLabPublicationAdapter(InputAdapter):
    batch_size = 300

    def __init__(
        self,
        data_source,
        max_proteins: Optional[int] = None,
    ):
        self.protein_mentions_file_path = str(data_source.file("human_textmining_mentions.tsv.gz"))
        self.max_proteins = max_proteins
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.JensenLabTextMining

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Protein], None, None]:
        batch: List[Protein] = []
        for protein_id, publications in self._iter_protein_publications():
            batch.append(
                Protein(
                    id=EquivalentId(id=protein_id, type=Prefix.ENSEMBL).id_str(),
                    publications=publications,
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_protein_publications(self) -> Generator[Tuple[str, List[PublicationReference]], None, None]:
        emitted = 0
        with self._open_text(self.protein_mentions_file_path) as handle:
            for raw_line in handle:
                row = raw_line.rstrip("\n").split("\t", 1)
                if len(row) < 2:
                    continue
                protein_id = row[0].strip()
                if not protein_id.startswith("ENSP"):
                    continue

                pmids = self._parse_pmid_field(row[1])
                if not pmids:
                    continue

                publications = [
                    PublicationReference(
                        pmid=pmid,
                        source="JensenLab",
                    )
                    for pmid in sorted(pmids, key=int)
                ]
                yield protein_id, publications
                emitted += 1
                if self.max_proteins is not None and emitted >= self.max_proteins:
                    break

    @staticmethod
    def _parse_pmid_field(raw_pmids: str) -> Set[str]:
        return {pmid for pmid in raw_pmids.strip().split() if pmid}

    @staticmethod
    def _open_text(file_path: str):
        if file_path.endswith(".gz"):
            return gzip.open(file_path, "rt", encoding="utf-8", errors="replace")
        return open(file_path, "r", encoding="utf-8", errors="replace")
