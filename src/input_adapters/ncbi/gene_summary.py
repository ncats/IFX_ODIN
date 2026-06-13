import csv
import gzip
from typing import Generator, List, Optional

from src.constants import DataSourceName, HUMAN_TAX_ID, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId


class NCBIGeneSummaryAdapter(InputAdapter):
    def __init__(
        self,
        data_source,
        tax_id: int = HUMAN_TAX_ID,
        max_rows: Optional[int] = None,
    ):
        self.gene_summary_file_path = str(data_source.file("gene_summary.gz"))
        self.tax_id = str(tax_id)
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

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
