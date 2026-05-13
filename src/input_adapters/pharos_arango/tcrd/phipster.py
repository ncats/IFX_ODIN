from typing import Generator, List, Optional

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.protein import Protein
from src.models.virus import Virus, ViralPPIDetail, ViralPPIEdge, ViralProtein


def virus_query() -> str:
    return """FOR v IN `Virus` RETURN v"""


def viral_protein_query() -> str:
    return """FOR vp IN `ViralProtein` RETURN vp"""


def viral_ppi_query(last_key: Optional[str] = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ViralPPIEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def phipster_version_query() -> str:
    return """FOR v IN `Virus` LIMIT 1 RETURN v.creation"""


class PHIPSTERAdapter(PharosArangoAdapter):
    batch_size = 10_000

    @staticmethod
    def _build_details(row: dict) -> List[ViralPPIDetail]:
        details = row.get("details") or []
        if details:
            return [ViralPPIDetail.from_dict(detail) for detail in details]

        # Backward-compatible fallback for older graph builds before PHIPSTER details
        legacy_source = row.get("data_source")
        legacy_final_lr = row.get("final_lr")
        legacy_pdb_ids = row.get("pdb_ids") or []
        legacy_high_confidence = row.get("high_confidence")
        if (
            legacy_source is None
            and legacy_final_lr is None
            and not legacy_pdb_ids
            and legacy_high_confidence is None
        ):
            return []
        return [
            ViralPPIDetail(
                source=legacy_source,
                source_protein_id=row.get("start_id"),
                final_lr=legacy_final_lr,
                pdb_ids=legacy_pdb_ids,
                high_confidence=legacy_high_confidence,
            )
        ]

    def get_all(self) -> Generator[List[Virus | ViralProtein | ViralPPIEdge], None, None]:
        yield [Virus.from_dict(row) for row in self.runQuery(virus_query())]

        yield [ViralProtein.from_dict(row) for row in self.runQuery(viral_protein_query())]

        last_key = None
        while True:
            rows = list(self.runQuery(viral_ppi_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ViralPPIEdge(
                    start_node=Protein(id=row["start_id"]),
                    end_node=ViralProtein(id=row["end_id"]),
                    provenance=row.get("provenance"),
                    sources=row.get("sources") or [],
                    details=self._build_details(row),
                )
                for row in rows
            ]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(phipster_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
