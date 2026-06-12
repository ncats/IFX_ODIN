from __future__ import annotations

from typing import Generator, List

import pyarrow.parquet as pq

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import Protein


class SureChEMBLPatentFamilyAdapter(InputAdapter):
    batch_size = 2_000

    def __init__(self, data_source):
        self.data_source = data_source
        self.file_path = str(data_source.file("protein_patent_family_mentions.parquet"))
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.SureChEMBL

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Protein], None, None]:
        batch: List[Protein] = []
        parquet_file = pq.ParquetFile(self.file_path)
        for record_batch in parquet_file.iter_batches(
            columns=[
                "protein_id",
                "patent_family_mentions",
                "patent_identifier_sources",
            ]
        ):
            protein_ids = record_batch.column("protein_id").to_pylist()
            mention_lists = record_batch.column("patent_family_mentions").to_pylist()
            source_lists = record_batch.column("patent_identifier_sources").to_pylist()
            for protein_id, patent_family_mentions, patent_identifier_sources in zip(
                protein_ids,
                mention_lists,
                source_lists,
            ):
                batch.append(
                    Protein(
                        id=protein_id,
                        patent_family_mentions=patent_family_mentions or [],
                        patent_identifier_sources=patent_identifier_sources or [],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch
