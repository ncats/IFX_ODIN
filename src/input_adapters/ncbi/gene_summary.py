import csv
import gzip
import os
from datetime import date, datetime
from typing import Generator, List, Optional

from src.constants import DataSourceName, HUMAN_TAX_ID, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId


class NCBIGeneSummaryAdapter(InputAdapter):
    def __init__(
        self,
        gene_summary_file_path: str,
        version_file_path: Optional[str] = None,
        tax_id: int = HUMAN_TAX_ID,
        max_rows: Optional[int] = None,
    ):
        self.gene_summary_file_path = gene_summary_file_path
        self.version_file_path = version_file_path
        self.tax_id = str(tax_id)
        self.max_rows = max_rows
        self.version_info = self._load_version_info(version_file_path)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCBI

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Gene], None, None]:
        batch: List[Gene] = []
        emitted = 0
        with gzip.open(self.gene_summary_file_path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                if row.get("#tax_id") != self.tax_id:
                    continue
                gene_id = (row.get("GeneID") or "").strip()
                summary = (row.get("Summary") or "").strip()
                if not gene_id or not summary:
                    continue

                batch.append(
                    Gene(
                        id=EquivalentId(id=gene_id, type=Prefix.NCBIGene).id_str(),
                        ncbi_gene_summary=summary,
                    )
                )
                emitted += 1
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
                if self.max_rows is not None and emitted >= self.max_rows:
                    break

        if batch:
            yield batch

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

        if download_date is None and os.path.exists(self.gene_summary_file_path):
            download_date = datetime.fromtimestamp(os.path.getmtime(self.gene_summary_file_path)).date()

        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

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
