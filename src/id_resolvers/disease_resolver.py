import csv
import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from src.id_resolvers.resolver_snapshot import resolver_input
from src.id_resolvers.sqlite_cache_resolver import SqliteCacheResolver
from src.interfaces.id_resolver import IdMatch
from src.models.node import Node


@dataclass(frozen=True)
class DiseaseMatchRow:
    id: str
    match: str
    type: str
    priority: int


class DiseaseIdResolver(SqliteCacheResolver):
    name = "Disease ID Resolver"
    xref_suffix = "_xref"
    cache_schema_version = "v2"

    def __init__(self, resolver_snapshot, cache_path: str = None, **kwargs):
        self.resolver_snapshot = resolver_snapshot
        self.file_path = str(resolver_input(resolver_snapshot, "data_source").file("disease_ids.tsv"))
        self.cache_path = cache_path
        super().__init__(**kwargs)

    def cache_location(self):
        if self.cache_path:
            return self.cache_path
        return f"input_files/sqlite_resolver/{self.__class__.__name__}.sqlite"

    def get_version_info(self) -> str:
        stat = os.stat(self.file_path)
        return (
            f"{self.cache_schema_version}\t{os.path.abspath(self.file_path)}"
            f"\tsize:{stat.st_size}\tmtime_ns:{stat.st_mtime_ns}"
        )

    def matching_ids(self):
        raise NotImplementedError("DiseaseIdResolver populates its sqlite cache directly")

    def create_lookup_db(self):
        print('\tcreating sqlite lookup db')
        cur = self.connection.cursor()

        cur.execute('DROP TABLE IF EXISTS matches')
        cur.execute('DROP TABLE IF EXISTS equivalent_ids')
        cur.execute('DROP TABLE IF EXISTS file_metadata')

        cur.execute('CREATE TABLE matches (id TEXT, match TEXT, type TEXT, priority INTEGER)')
        cur.execute('CREATE INDEX match_index ON matches (match)')
        cur.execute('CREATE INDEX id_index ON matches (id)')
        cur.execute('CREATE TABLE equivalent_ids (id TEXT, equivalent_id TEXT)')
        cur.execute('CREATE INDEX equivalent_id_index ON equivalent_ids (id)')
        cur.execute('CREATE TABLE file_metadata (version_key TEXT)')

        self.connection.commit()

    def populate_lookup_db(self):
        match_rows: Set[DiseaseMatchRow] = set()
        equivalent_rows: Set[tuple[str, str]] = set()

        for canonical_id, aliases_by_type in self._iter_rows():
            for alias in aliases_by_type["equivalent"]:
                if self._is_curie(alias):
                    equivalent_rows.add((canonical_id, alias))

            for alias in aliases_by_type["standard"]:
                for variant in self._alias_variants(alias):
                    match_rows.add(DiseaseMatchRow(canonical_id, variant, "standard_id", 0))

            for alias in aliases_by_type["nn"]:
                for variant in self._alias_variants(alias):
                    match_rows.add(DiseaseMatchRow(canonical_id, variant, "nn_curie", 1))

            for alias in aliases_by_type["xref"]:
                for variant in self._alias_variants(alias):
                    match_rows.add(DiseaseMatchRow(canonical_id, variant, "xref", 2))

        cur = self.connection.cursor()
        cur.executemany(
            'INSERT INTO matches VALUES (?, ?, ?, ?)',
            [(row.id, row.match, row.type, row.priority) for row in match_rows],
        )
        cur.executemany(
            'INSERT INTO equivalent_ids VALUES (?, ?)',
            sorted(equivalent_rows),
        )
        self.store_file_metadata()
        self.connection.commit()

    def _iter_rows(self):
        with open(self.file_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            xref_columns = [column for column in reader.fieldnames or [] if column.endswith(self.xref_suffix)]

            for row in reader:
                canonical_id = self._clean_id(row.get("standard_id"))
                if not canonical_id:
                    continue

                standard_aliases = {canonical_id}
                nn_aliases = set(self._split_aliases(row.get("nn_curie")))
                xref_aliases = set()
                for column in xref_columns:
                    xref_aliases.update(self._split_aliases(row.get(column)))

                equivalent_aliases = set()
                equivalent_aliases.update(standard_aliases)
                equivalent_aliases.update(nn_aliases)
                equivalent_aliases.update(xref_aliases)

                yield canonical_id, {
                    "standard": standard_aliases,
                    "nn": nn_aliases,
                    "xref": xref_aliases,
                    "equivalent": equivalent_aliases,
                }

    @staticmethod
    def _clean_id(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @staticmethod
    def _is_curie(value: str) -> bool:
        if ":" not in value:
            return False
        prefix, local_id = value.split(":", 1)
        return bool(prefix.strip() and local_id.strip())

    @classmethod
    def _split_aliases(cls, value: Optional[str]) -> List[str]:
        cleaned = cls._clean_id(value)
        if not cleaned:
            return []
        aliases = []
        for token in cleaned.replace("|", ";").replace(",", ";").split(";"):
            alias = cls._clean_id(token)
            if alias:
                aliases.append(alias)
        return aliases

    @staticmethod
    def _alias_variants(alias: str) -> Set[str]:
        variants = {alias}
        if ":" not in alias:
            return variants

        prefix, local_id = alias.split(":", 1)
        prefix = prefix.strip()
        local_id = local_id.strip()
        if not prefix or not local_id:
            return variants

        prefix_variants = {prefix, prefix.upper(), prefix.lower()}
        if prefix.lower() == "orphanet":
            prefix_variants.add("Orphanet")
        if prefix.lower() == "gard":
            prefix_variants.add("GARD")

        for prefix_variant in prefix_variants:
            variants.add(f"{prefix_variant}:{local_id}")

        if prefix.lower() == "gard" and local_id.isdigit():
            padded = f"{int(local_id):07d}"
            unpadded = str(int(local_id))
            for prefix_variant in {"GARD", "gard"}:
                variants.add(f"{prefix_variant}:{padded}")
                variants.add(f"{prefix_variant}:{unpadded}")

        return variants

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        result_list: Dict[str, List[IdMatch]] = {}
        input_ids = sorted({node.id for node in input_nodes})
        cur = self.connection.cursor()
        max_vars = 50000

        for chunk in [input_ids[i:i + max_vars] for i in range(0, len(input_ids), max_vars)]:
            if not chunk:
                continue
            cur.execute(
                """
                SELECT id, match, type, priority
                FROM matches
                WHERE match IN ({})
                ORDER BY priority, id
                """.format(",".join("?" * len(chunk))),
                tuple(chunk),
            )
            for resolved_id, input_id, match_type, _priority in cur.fetchall():
                if input_id not in result_list:
                    result_list[input_id] = []
                if resolved_id in {match.match for match in result_list[input_id]}:
                    continue
                result_list[input_id].append(IdMatch(
                    input=input_id,
                    match=resolved_id,
                    equivalent_ids=[],
                    context=[match_type],
                ))

        resolved_ids = sorted({match.match for matches in result_list.values() for match in matches})
        equivalent_map: Dict[str, List[str]] = {}
        for chunk in [resolved_ids[i:i + max_vars] for i in range(0, len(resolved_ids), max_vars)]:
            if not chunk:
                continue
            cur.execute(
                "SELECT id, equivalent_id FROM equivalent_ids WHERE id IN ({}) ORDER BY equivalent_id".format(
                    ",".join("?" * len(chunk))
                ),
                tuple(chunk),
            )
            for resolved_id, equivalent_id in cur.fetchall():
                equivalent_map.setdefault(resolved_id, []).append(equivalent_id)

        for matches in result_list.values():
            for match in matches:
                match.equivalent_ids = equivalent_map.get(match.match, [])

        return result_list
