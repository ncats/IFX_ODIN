from typing import Generator, List, Optional

import requests

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease


class RDASRareDiseaseAdapter(InputAdapter):
    graphql_url = "https://rdas.ncats.nih.gov/api/diseases/graphql"
    query = """
    query RareDiseases($limit: Int, $offset: Int) {
      gards(limit: $limit, offset: $offset) {
        GardId
        GardName
      }
    }
    """

    def __init__(
        self,
        graphql_url: Optional[str] = None,
        batch_size: int = 1000,
        request_timeout: int = 120,
        data_source=None,
        max_rows: Optional[int] = None,
    ):
        self.graphql_url = graphql_url or self.graphql_url
        self.batch_size = batch_size
        self.request_timeout = request_timeout
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.RDAS

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    @staticmethod
    def _normalize_gard_id(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        raw = value.strip()
        if not raw:
            return None
        if ":" in raw:
            prefix, local_id = raw.split(":", 1)
            if prefix.strip().lower() != "gard":
                return None
            raw = local_id.strip()
        if not raw.isdigit():
            return None
        return f"GARD:{int(raw):07d}"

    @staticmethod
    def _extract_gards(payload: dict) -> list[dict]:
        data = payload.get("data") or {}
        gards = data.get("gards")
        if isinstance(gards, list):
            return gards
        if isinstance(gards, dict):
            if isinstance(gards.get("nodes"), list):
                return gards["nodes"]
            if isinstance(gards.get("edges"), list):
                return [edge.get("node") for edge in gards["edges"] if edge.get("node")]
        return []

    def _fetch_page(self, offset: int) -> list[dict]:
        response = requests.post(
            self.graphql_url,
            json={
                "query": self.query,
                "variables": {
                    "limit": self.batch_size,
                    "offset": offset,
                },
            },
            timeout=self.request_timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(f"RDAS GraphQL returned errors: {payload['errors']}")
        return self._extract_gards(payload)

    def _iter_rare_diseases(self):
        seen = set()
        offset = 0
        emitted = 0

        while True:
            page = self._fetch_page(offset)
            if not page:
                break

            for row in page:
                gard_id = self._normalize_gard_id(row.get("GardId"))
                if gard_id is None or gard_id in seen:
                    continue
                seen.add(gard_id)
                emitted += 1
                yield Disease(
                    id=gard_id,
                    name=(row.get("GardName") or "").strip() or None,
                    rare_disease=True,
                )
                if self.max_rows is not None and emitted >= self.max_rows:
                    return

            if len(page) < self.batch_size:
                break
            offset += self.batch_size

    def get_all(self) -> Generator[List[Disease], None, None]:
        batch = []
        for disease in self._iter_rare_diseases():
            batch.append(disease)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
