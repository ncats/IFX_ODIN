from typing import Generator, List

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.panther_class import PantherClass, ProteinPantherClassEdge
from src.models.protein import Protein


def panther_class_query() -> str:
    return """FOR d IN `PantherClass` RETURN d"""


def panther_class_parent_query() -> str:
    return """FOR rel IN `PantherClassParentEdge` RETURN { "child": rel.start_id, "parent": rel.end_id }"""


def protein_panther_class_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinPantherClassEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def panther_version_query() -> str:
    return """FOR d IN `PantherClass` LIMIT 1 RETURN d.creation"""


class PantherClassAdapter(PharosArangoAdapter):
    def _load_parent_map(self) -> dict[str, list[str]]:
        db = self.get_db()
        if not db.has_collection("PantherClassParentEdge"):
            return {}
        parent_rows = self.runQuery(panther_class_parent_query())
        parent_map: dict[str, set[str]] = {}
        for row in parent_rows:
            child = row.get("child")
            parent = row.get("parent")
            if not child or not parent:
                continue
            parent_map.setdefault(child, set()).add(parent)
        return {
            child_id: sorted(parent_ids)
            for child_id, parent_ids in parent_map.items()
        }

    @staticmethod
    def _source_id(node_id: str) -> str:
        return node_id.split(":", 1)[1] if ":" in node_id else node_id

    def get_all(self) -> Generator[List[PantherClass], None, None]:
        parent_map = self._load_parent_map()
        rows = []
        for row in self.runQuery(panther_class_query()):
            parent_ids = parent_map.get(row["id"], [])
            rows.append(
                PantherClass(
                    id=row["id"],
                    source_id=row.get("source_id"),
                    name=row.get("name"),
                    description=row.get("description"),
                    hierarchy_code=row.get("hierarchy_code"),
                    provenance=row.get("provenance"),
                    sources=row.get("sources") or [],
                    parent_pcids="|".join(self._source_id(parent_id) for parent_id in parent_ids) or None,
                )
            )
        yield rows

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(panther_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class ProteinPantherClassAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[ProteinPantherClassEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_panther_class_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinPantherClassEdge(
                    start_node=Protein(id=row["start_id"]),
                    end_node=PantherClass(id=row["end_id"]),
                    source=row.get("source"),
                )
                for row in rows
            ]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(panther_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
