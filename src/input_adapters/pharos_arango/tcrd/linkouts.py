from typing import Generator, List, Union

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.external_link import ExternalLinkProvider, ProteinExternalLinkEdge
from src.models.protein import Protein


def external_link_provider_query() -> str:
    return """
    FOR provider IN `ExternalLinkProvider`
        LET link_count = LENGTH(
            FOR rel IN `ProteinExternalLinkEdge`
                FILTER rel.end_id == provider.id
                RETURN 1
        )
        SORT provider._key
        RETURN MERGE(provider, {link_count})
    """


def protein_external_link_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinExternalLinkEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        LET provider = DOCUMENT(rel._to)
        RETURN {{
            "_key": rel._key,
            "start_id": rel.start_id,
            "end_id": rel.end_id,
            "end_node": provider,
            "source": rel.source,
            "url": rel.url,
            "source_id": rel.source_id,
            "source_id_type": rel.source_id_type,
            "details": rel.details
        }}
    """


def external_link_version_query() -> str:
    return """
    FOR provider IN `ExternalLinkProvider`
        LIMIT 1
        RETURN provider.creation
    """


class ExternalLinkProviderAdapter(PharosArangoAdapter):
    def get_all(self) -> Generator[List[ExternalLinkProvider], None, None]:
        rows = list(self.runQuery(external_link_provider_query()))
        yield [ExternalLinkProvider.from_dict(row) for row in rows]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(external_link_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class ProteinExternalLinkAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[Union[ProteinExternalLinkEdge]], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_external_link_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinExternalLinkEdge(
                    start_node=Protein(id=row["start_id"]),
                    end_node=ExternalLinkProvider.from_dict(row["end_node"]),
                    source=row.get("source"),
                    url=row.get("url"),
                    source_id=row.get("source_id"),
                    source_id_type=row.get("source_id_type"),
                    details=row.get("details") or [],
                )
                for row in rows
            ]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(external_link_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
