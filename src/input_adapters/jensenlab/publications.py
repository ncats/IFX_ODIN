import csv
import os
from datetime import date, datetime
from typing import Dict, Generator, List, Optional, Set, Tuple

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.models.publication import PublicationReference


class JensenLabPublicationAdapter(InputAdapter):
    batch_size = 1000

    def __init__(
        self,
        protein_mentions_file_path: str,
        version_file_path: Optional[str] = None,
        max_proteins: Optional[int] = None,
    ):
        self.protein_mentions_file_path = protein_mentions_file_path
        self.version_file_path = version_file_path
        self.max_proteins = max_proteins
        self.version_info = self._load_version_info(version_file_path)

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
        with open(self.protein_mentions_file_path, "r", encoding="utf-8", errors="replace") as handle:
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

    def _load_version_info(self, version_file_path: Optional[str]) -> DatasourceVersionInfo:
        version = None
        version_date = None
        download_date = None
        if version_file_path and os.path.exists(version_file_path):
            with open(version_file_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                row = next(reader, None)
                if row:
                    version = row.get("version") or None
                    version_date = self._parse_date(row.get("version_date"))
                    download_date = self._parse_date(row.get("download_date"))

        if download_date is None and os.path.exists(self.protein_mentions_file_path):
            download_date = datetime.fromtimestamp(os.path.getmtime(self.protein_mentions_file_path)).date()

        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

    @staticmethod
    def _parse_pmid_field(raw_pmids: str) -> Set[str]:
        return {pmid for pmid in raw_pmids.strip().split() if pmid}

    @staticmethod
    def _parse_date(raw_value: Optional[str]) -> Optional[date]:
        if raw_value is None:
            return None
        value = str(raw_value).strip()
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
