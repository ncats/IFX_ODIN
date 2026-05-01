import csv
import gzip
import os
import re
from collections import defaultdict
from datetime import date, datetime
from typing import DefaultDict, Dict, Generator, List, Optional, Tuple

from src.constants import DataSourceName, Prefix, HUMAN_TAX_ID
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId
from src.models.publication import PublicationReference, GeneRifAnnotation


class NCBIPublicationAdapter(InputAdapter):
    batch_size = 1000

    def __init__(
        self,
        gene2pubmed_file_path: str,
        generif_file_path: str,
        version_file_path: Optional[str] = None,
        max_genes: Optional[int] = None,
        max_gene2pubmed_rows: Optional[int] = None,
        max_generif_rows: Optional[int] = None,
    ):
        self.gene2pubmed_file_path = gene2pubmed_file_path
        self.generif_file_path = generif_file_path
        self.version_file_path = version_file_path
        self.max_genes = max_genes
        self.max_gene2pubmed_rows = max_gene2pubmed_rows
        self.max_generif_rows = max_generif_rows
        self.version_info = self._load_version_info(version_file_path)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCBI

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Gene], None, None]:
        publication_map = self._load_publications()

        batch: List[Gene] = []
        emitted = 0
        for gene_id, pub_map in publication_map.items():
            publications = sorted(
                pub_map.values(),
                key=lambda pub: (
                    int(pub.gene_id) if pub.gene_id is not None else -1,
                    int(pub.pmid),
                    pub.source,
                ),
            )
            batch.append(
                Gene(
                    id=EquivalentId(id=str(gene_id), type=Prefix.NCBIGene).id_str(),
                    publications=publications,
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
            emitted += 1
            if self.max_genes is not None and emitted >= self.max_genes:
                break

        if batch:
            yield batch

    def _load_publications(self) -> DefaultDict[int, Dict[Tuple[str, str, int], PublicationReference]]:
        publication_map: DefaultDict[int, Dict[Tuple[str, str, int], PublicationReference]] = defaultdict(dict)
        self._load_gene2pubmed(publication_map)
        self._load_generifs(publication_map)
        return publication_map

    def _load_gene2pubmed(
        self,
        publication_map: DefaultDict[int, Dict[Tuple[str, str, int], PublicationReference]],
    ) -> None:
        csv.field_size_limit(10_000_000)
        processed = 0
        with gzip.open(self.gene2pubmed_file_path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                if not self._is_human_row(row):
                    continue
                processed += 1
                if self.max_gene2pubmed_rows is not None and processed > self.max_gene2pubmed_rows:
                    break
                gene_id = self._parse_int(self._first_present(row, "GeneID", "Gene ID"))
                pmid = self._normalize_pmid(self._first_present(row, "PubMed_ID", "PubMed ID", "PubMed ID (PMID) list"))
                if gene_id is None or pmid is None:
                    continue
                key = (pmid, "NCBI", gene_id)
                publication_map[gene_id].setdefault(
                    key,
                    PublicationReference(
                        pmid=pmid,
                        source="NCBI",
                        gene_id=gene_id,
                    ),
                )

    def _load_generifs(
        self,
        publication_map: DefaultDict[int, Dict[Tuple[str, str, int], PublicationReference]],
    ) -> None:
        processed = 0
        with gzip.open(self.generif_file_path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                if not self._is_human_row(row):
                    continue
                processed += 1
                if self.max_generif_rows is not None and processed > self.max_generif_rows:
                    break
                gene_id = self._parse_int(self._first_present(row, "Gene ID", "GeneID"))
                updated_at = self._parse_datetime(self._first_present(row, "last update timestamp"))
                rif_text = (self._first_present(row, "GeneRIF text") or "").strip()
                if gene_id is None:
                    continue

                pmids = self._parse_pmid_list(self._first_present(row, "PubMed ID (PMID) list", "PubMed_ID", "PubMed ID"))
                if not pmids:
                    continue

                for pmid in pmids:
                    key = (pmid, "NCBI", gene_id)
                    publication = publication_map[gene_id].setdefault(
                        key,
                        PublicationReference(
                            pmid=pmid,
                            source="NCBI",
                            gene_id=gene_id,
                        ),
                    )
                    if rif_text:
                        gene_rifs = list(publication.gene_rifs or [])
                        existing = next((gene_rif for gene_rif in gene_rifs if gene_rif.text == rif_text), None)
                        if existing is None:
                            gene_rifs.append(GeneRifAnnotation(text=rif_text, updated_at=updated_at))
                            gene_rifs.sort(key=lambda gene_rif: gene_rif.text)
                            publication.gene_rifs = gene_rifs
                        elif updated_at is not None and (
                            existing.updated_at is None or updated_at > existing.updated_at
                        ):
                            existing.updated_at = updated_at

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

        if download_date is None:
            timestamps = [
                os.path.getmtime(path)
                for path in (self.gene2pubmed_file_path, self.generif_file_path)
                if os.path.exists(path)
            ]
            if timestamps:
                download_date = datetime.fromtimestamp(max(timestamps)).date()

        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

    @staticmethod
    def _first_present(row: dict, *keys: str) -> Optional[str]:
        for key in keys:
            if key in row and row[key] not in (None, ""):
                return row[key]
        return None

    @staticmethod
    def _normalize_pmid(raw_value: Optional[str]) -> Optional[str]:
        if raw_value is None:
            return None
        match = re.search(r"\d+", str(raw_value))
        return match.group(0) if match else None

    @staticmethod
    def _parse_pmid_list(raw_value: Optional[str]) -> List[str]:
        if raw_value is None:
            return []
        return sorted({token for token in re.findall(r"\d+", str(raw_value)) if token})

    @staticmethod
    def _parse_int(raw_value: Optional[str]) -> Optional[int]:
        if raw_value is None:
            return None
        try:
            return int(str(raw_value).strip())
        except ValueError:
            return None

    @staticmethod
    def _parse_datetime(raw_value: Optional[str]) -> Optional[datetime]:
        if raw_value is None:
            return None
        value = str(raw_value).strip()
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

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

    @staticmethod
    def _is_human_row(row: dict) -> bool:
        raw_tax_id = (
            row.get("#tax_id")
            or row.get("tax_id")
            or row.get("#Tax ID")
            or row.get("Tax ID")
        )
        if raw_tax_id in (None, ""):
            return True
        try:
            return int(str(raw_tax_id).strip()) == HUMAN_TAX_ID
        except ValueError:
            return False
