import csv
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, Generator, List, Optional, Tuple

from src.constants import DataSourceName, HUMAN_TAX_ID, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId
from src.models.publication import GeneRifAnnotation, PublicationReference


class TargetGraphGeneRifPublicationAdapter(InputAdapter):
    """Attach harmonizer-exported GeneRIF evidence to Gene nodes for TDL scoring."""

    batch_size = 1000

    def __init__(self, data_source, file_name: str = "generif_9606.csv"):
        self.file_path = str(data_source.file(file_name))
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCBI

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Gene], None, None]:
        publications_by_gene = self._load_generifs()
        batch: List[Gene] = []
        for gene_id in sorted(publications_by_gene):
            publications = sorted(
                publications_by_gene[gene_id].values(),
                key=lambda pub: (int(pub.pmid), pub.source),
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
        if batch:
            yield batch

    def _load_generifs(self) -> DefaultDict[int, Dict[Tuple[str, int], PublicationReference]]:
        csv.field_size_limit(10_000_000)
        publications_by_gene: DefaultDict[int, Dict[Tuple[str, int], PublicationReference]] = defaultdict(dict)
        with open(self.file_path, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not self._is_human_row(row):
                    continue
                gene_id = self._parse_int(row.get("Gene ID") or row.get("GeneID"))
                if gene_id is None:
                    continue
                rif_text = (row.get("GeneRIF text") or "").strip()
                updated_at = self._parse_datetime(row.get("last update timestamp"))
                for pmid in self._parse_pmids(row.get("PubMed ID (PMID) list") or row.get("PubMed_ID")):
                    key = (pmid, gene_id)
                    publication = publications_by_gene[gene_id].setdefault(
                        key,
                        PublicationReference(
                            pmid=pmid,
                            source="NCBI GeneRIF",
                            gene_id=gene_id,
                            gene_rifs=[],
                        ),
                    )
                    if rif_text:
                        gene_rifs = list(publication.gene_rifs or [])
                        if not any(existing.text == rif_text for existing in gene_rifs):
                            gene_rifs.append(GeneRifAnnotation(text=rif_text, updated_at=updated_at))
                            gene_rifs.sort(key=lambda entry: entry.text)
                            publication.gene_rifs = gene_rifs
        return publications_by_gene

    @staticmethod
    def _parse_pmids(value: Any) -> List[str]:
        if value is None:
            return []
        return sorted({token for token in re.findall(r"\d+", str(value)) if token})

    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip())
        except ValueError:
            return None

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if value in (None, ""):
            return None
        text = str(value).strip()
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    @staticmethod
    def _is_human_row(row: dict) -> bool:
        value = row.get("#Tax ID") or row.get("#tax_id") or row.get("Tax ID") or row.get("tax_id")
        if value in (None, ""):
            return True
        try:
            return int(str(value).strip()) == HUMAN_TAX_ID
        except ValueError:
            return False
