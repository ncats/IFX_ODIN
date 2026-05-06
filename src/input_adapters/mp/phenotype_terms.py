from datetime import date
from typing import Generator, List, Optional

import obonet

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.mouse_phenotype import MousePhenotype


class MPPhenotypeAdapter(FlatFileAdapter):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._graph = obonet.read_obo(self.file_path)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.MP

    def get_version(self) -> DatasourceVersionInfo:
        raw_version = self._graph.graph.get("data-version")
        version_string = self._extract_version_string(raw_version)
        return DatasourceVersionInfo(
            version=version_string,
            version_date=self._parse_version_date(version_string),
            download_date=self.download_date,
        )

    @staticmethod
    def _extract_version_string(raw_version: str | None) -> str | None:
        if raw_version and "/" in raw_version:
            return raw_version.split("/")[-1]
        return raw_version

    @staticmethod
    def _parse_version_date(version_string: str | None) -> date | None:
        if not version_string:
            return None
        try:
            return date.fromisoformat(version_string)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _is_mp_id(term_id: str) -> bool:
        return isinstance(term_id, str) and term_id.startswith(f"{Prefix.MP.value}:")

    def get_all(self) -> Generator[List[MousePhenotype], None, None]:
        batch = []
        for term_id, term_data in self._graph.nodes(data=True):
            if not self._is_mp_id(term_id):
                continue
            batch.append(
                MousePhenotype(
                    id=term_id,
                    name=term_data.get("name", term_id),
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
