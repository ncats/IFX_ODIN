import argparse
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote

import yaml

from src.constants import Prefix
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials

SYSTEM_FIELDS = {"_id", "_key", "_rev"}
EDGE_RESERVED_FIELDS = SYSTEM_FIELDS | {"_from", "_to", "start_id", "end_id"}
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
SKOS_ALT_LABEL = "http://www.w3.org/2004/02/skos/core#altLabel"
SKOS_EXACT_MATCH = "http://www.w3.org/2004/02/skos/core#exactMatch"

IDENTIFIERS_ORG_PREFIX_MAP = {
    Prefix.CAS: "cas",
    Prefix.CHEMBL_COMPOUND: "chembl.compound",
    Prefix.CHEMBL_PROTEIN: "chembl.target",
    Prefix.doi: "doi",
    Prefix.DOID: "doid",
    Prefix.DRUGBANK: "drugbank",
    Prefix.EC: "ec-code",
    Prefix.EFO: "efo",
    Prefix.ENSEMBL: "ensembl",
    Prefix.GTOPDB: "gtopdb",
    Prefix.HGNC: "hgnc",
    Prefix.HMDB: "hmdb",
    Prefix.KEGG_COMPOUND: "kegg.compound",
    Prefix.KEGG_DISEASE: "kegg.disease",
    Prefix.KEGG_REACTION: "kegg.reaction",
    Prefix.MEDDRA: "meddra",
    Prefix.MESH: "mesh",
    Prefix.MGI: "mgi",
    Prefix.NCBIGene: "ncbigene",
    Prefix.NCBITaxon: "taxonomy",
    Prefix.NCIT: "ncit",
    Prefix.OMIM: "omim",
    Prefix.orphanet: "orphanet",
    Prefix.PMC: "pmc",
    Prefix.PMID: "pubmed",
    Prefix.PUBCHEM_COMPOUND: "pubchem.compound",
    Prefix.RGD: "rgd",
    Prefix.RHEA: "rhea",
    Prefix.SMPDB: "smpdb",
    Prefix.SNOMEDCT: "snomedct",
    Prefix.UMLS: "umls",
    Prefix.UNII: "unii",
    Prefix.UniProtKB: "uniprot",
    Prefix.Wikidata: "wikidata",
    Prefix.RefMet: "refmet",
    Prefix.chemspider: "chemspider",
    Prefix.kegg: "kegg.compound",
    Prefix.pubchem: "pubchem.compound",
}

OBO_PURL_PREFIXES = {
    Prefix.CHEBI,
    Prefix.CL,
    Prefix.DOID,
    Prefix.EFO,
    Prefix.EMAPA,
    Prefix.FMA,
    Prefix.GO,
    Prefix.HP,
    Prefix.MONDO,
    Prefix.MP,
    Prefix.NCIT,
    Prefix.PR,
    Prefix.UBERON,
    Prefix.ENVO,
    Prefix.CARO,
}


class NTriplesWriter:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.file_path.open("w", encoding="utf-8")

    def close(self):
        self.handle.close()

    def write_triple(self, subject: str, predicate: str, obj: str):
        self.handle.write(f"<{subject}> <{predicate}> {obj} .\n")


class ArangoToRdfConverter(ArangoAdapter):
    def __init__(
        self,
        arango_credentials: DBCredentials,
        arango_db_name: str,
        output_file: str,
        base_resource_uri: str,
        base_ontology_uri: str,
        edge_predicate_map: dict[str, str] | None = None,
        excluded_fields: set[str] | None = None,
    ):
        super().__init__(credentials=arango_credentials, database_name=arango_db_name)
        self.output_file = output_file
        self.base_resource_uri = self._ensure_trailing_slash(base_resource_uri)
        self.base_ontology_uri = self._ensure_trailing_slash(base_ontology_uri)
        self.writer = NTriplesWriter(output_file)
        self.node_iri_cache: dict[str, str] = {}
        self.edge_predicate_map = edge_predicate_map or {}
        self.excluded_fields = excluded_fields or set()
        self._planned_node_total = 0
        self._planned_edge_total = 0

    @staticmethod
    def _ensure_trailing_slash(uri: str) -> str:
        return uri if uri.endswith("/") else f"{uri}/"

    def close(self):
        self.writer.close()

    def _read_schemas(self) -> dict:
        db = self.get_db()
        if not db.has_collection(self.metadata_store_label):
            return {}
        store = db.collection(self.metadata_store_label)
        doc = store.get("collection_schemas")
        if doc is None:
            return {}
        return doc.get("collections", {})

    def export(
        self,
        include_collections: set[str] | None = None,
        exclude_collections: set[str] | None = None,
        seed_collection: str | None = None,
        seed_limit: int | None = None,
        max_hops: int = 1,
    ):
        schemas = self._read_schemas()
        if not schemas:
            raise RuntimeError("No collection_schemas found in metadata_store")

        node_collections, edge_collections = self._resolve_collections(
            schemas,
            include_collections=include_collections,
            exclude_collections=exclude_collections,
        )

        if seed_collection:
            if seed_collection not in node_collections:
                raise ValueError(f"Seed collection {seed_collection} is not in selected document collections")
            if not seed_limit:
                raise ValueError("seed_limit is required when seed_collection is provided")
            nodes, edges = self.collect_seeded_subset(
                schemas=schemas,
                node_collections=node_collections,
                edge_collections=edge_collections,
                seed_collection=seed_collection,
                seed_limit=seed_limit,
                max_hops=max_hops,
            )
        else:
            self._planned_node_total = self._count_documents(node_collections)
            self._planned_edge_total = self._count_documents(edge_collections)
            planned_total = self._planned_node_total + self._planned_edge_total
            print(
                f"Planned full export: {self._planned_node_total:,} nodes + "
                f"{self._planned_edge_total:,} edges = {planned_total:,} documents"
            )
            nodes = self._load_all_nodes(node_collections)
            edges = self._load_all_edges(edge_collections)

        self.write_graph(nodes, edges)

    def _resolve_collections(
        self,
        schemas: dict,
        include_collections: set[str] | None,
        exclude_collections: set[str] | None,
    ) -> tuple[list[str], list[str]]:
        exclude = set(exclude_collections or set()) | {self.metadata_store_label}
        selected = set(include_collections or schemas.keys())

        node_collections = []
        edge_collections = []
        for name, schema in schemas.items():
            if name in exclude or name not in selected:
                continue
            if schema.get("type") == "document":
                node_collections.append(name)
            elif schema.get("type") == "edge":
                edge_collections.append(name)

        edge_collections.sort()
        node_collections.sort()
        return node_collections, edge_collections

    def _count_documents(self, collections: list[str]) -> int:
        total = 0
        for collection in collections:
            rows = self.runQuery(f"RETURN LENGTH(`{collection}`)")
            count = rows[0] if rows else 0
            total += count
        return total

    def _load_all_nodes(self, node_collections: list[str]) -> dict[str, dict]:
        all_nodes = {}
        loaded_total = 0
        for collection in node_collections:
            rows = self.runQuery(f"FOR doc IN `{collection}` RETURN doc")
            for row in rows:
                all_nodes[row["_id"]] = row
            loaded_total += len(rows)
            self._print_progress("Loaded nodes", collection, len(rows), loaded_total, self._planned_node_total)
        return all_nodes

    def _load_all_edges(self, edge_collections: list[str]) -> list[dict]:
        all_edges = []
        loaded_total = 0
        for collection in edge_collections:
            rows = self.runQuery(f"FOR edge IN `{collection}` RETURN edge")
            all_edges.extend(rows)
            loaded_total += len(rows)
            self._print_progress("Loaded edges", collection, len(rows), loaded_total, self._planned_edge_total)
        return all_edges

    def collect_seeded_subset(
        self,
        schemas: dict,
        node_collections: list[str],
        edge_collections: list[str],
        seed_collection: str,
        seed_limit: int,
        max_hops: int,
    ) -> tuple[dict[str, dict], list[dict]]:
        selected_nodes: dict[str, dict] = {}
        selected_edges: dict[str, dict] = {}

        seed_nodes = self.runQuery(
            f"FOR doc IN `{seed_collection}` SORT doc.id LIMIT {seed_limit} RETURN doc"
        )
        frontier = {row["_id"] for row in seed_nodes}
        for row in seed_nodes:
            selected_nodes[row["_id"]] = row

        allowed_node_collections = set(node_collections)
        closure_edge_collections = [
            name for name in edge_collections if name in schemas and schemas[name].get("type") == "edge"
        ]

        for _ in range(max_hops):
            if not frontier:
                break

            new_frontier = set()
            for edge_collection in closure_edge_collections:
                edge_rows = self.runQuery(
                    f"""
                    FOR edge IN `{edge_collection}`
                        FILTER edge._from IN @frontier OR edge._to IN @frontier
                        RETURN edge
                    """,
                    bind_vars={"frontier": list(frontier)},
                )
                for edge in edge_rows:
                    selected_edges[edge["_id"]] = edge
                    for handle in (edge["_from"], edge["_to"]):
                        coll_name = handle.split("/", 1)[0]
                        if coll_name in allowed_node_collections and handle not in selected_nodes:
                            new_frontier.add(handle)

            loaded_nodes = self._fetch_nodes_by_handle(new_frontier)
            for handle, node in loaded_nodes.items():
                selected_nodes[handle] = node

            frontier = set(loaded_nodes.keys())

        self._add_internal_edges_between_selected_nodes(edge_collections, selected_nodes, selected_edges)
        return selected_nodes, list(selected_edges.values())

    def write_graph(self, nodes: dict[str, dict], edges: list[dict]):
        self._write_nodes(nodes)
        self._write_edges(edges)

    def _fetch_nodes_by_handle(self, handles: Iterable[str]) -> dict[str, dict]:
        handles = list(handles)
        if not handles:
            return {}

        grouped: dict[str, list[str]] = defaultdict(list)
        for handle in handles:
            collection, key = handle.split("/", 1)
            grouped[collection].append(key)

        loaded = {}
        db = self.get_db()
        for collection, keys in grouped.items():
            docs = db.collection(collection).get_many(keys)
            for doc in docs:
                if doc:
                    loaded[doc["_id"]] = doc
        return loaded

    def _add_internal_edges_between_selected_nodes(
        self,
        edge_collections: list[str],
        selected_nodes: dict[str, dict],
        selected_edges: dict[str, dict],
    ):
        node_handles = set(selected_nodes.keys())
        if not node_handles:
            return

        for edge_collection in edge_collections:
            rows = self.runQuery(
                f"""
                FOR edge IN `{edge_collection}`
                    FILTER edge._from IN @handles AND edge._to IN @handles
                    RETURN edge
                """,
                bind_vars={"handles": list(node_handles)},
            )
            for edge in rows:
                selected_edges[edge["_id"]] = edge

    def _write_nodes(self, nodes: dict[str, dict]):
        total = len(nodes)
        for index, node in enumerate(nodes.values(), start=1):
            collection = node["_id"].split("/", 1)[0]
            subject = self._resource_iri(collection, node)
            self.node_iri_cache[node["_id"]] = subject
            self.writer.write_triple(subject, RDF_TYPE, self._iri_object(self._class_iri(collection)))
            label_objects = set(self._unique_objects(node.get("name"), field_name="name"))

            for field_name, value in node.items():
                if field_name in SYSTEM_FIELDS or field_name in self.excluded_fields:
                    continue
                predicate = self._field_predicate(field_name)
                objects = self._unique_objects(value, field_name=field_name)
                if field_name == "synonyms":
                    objects = [obj for obj in objects if obj not in label_objects]
                for obj in objects:
                    self.writer.write_triple(subject, predicate, obj)
            if index % 50000 == 0 or index == total:
                self._print_write_progress("Wrote nodes", index, total)

    def _write_edges(self, edges: list[dict]):
        total = len(edges)
        for index, edge in enumerate(edges, start=1):
            edge_collection = edge["_id"].split("/", 1)[0]
            subject_iri = self._edge_endpoint_iri(edge["_from"])
            object_iri = self._edge_endpoint_iri(edge["_to"])
            predicate = self._edge_predicate(edge_collection)

            self.writer.write_triple(subject_iri, predicate, self._iri_object(object_iri))

            payload_fields = [
                field for field in edge.keys()
                if field not in EDGE_RESERVED_FIELDS and field not in self.excluded_fields
            ]
            if not payload_fields:
                continue

            stmt_iri = self._statement_iri(edge_collection, edge)
            self.writer.write_triple(stmt_iri, RDF_TYPE, self._iri_object(self._class_iri(edge_collection)))
            self.writer.write_triple(stmt_iri, self._field_predicate("subject"), self._iri_object(subject_iri))
            self.writer.write_triple(stmt_iri, self._field_predicate("predicate"), self._iri_object(predicate))
            self.writer.write_triple(stmt_iri, self._field_predicate("object"), self._iri_object(object_iri))

            for field_name in payload_fields:
                predicate_iri = self._field_predicate(field_name)
                for obj in self._unique_objects(edge[field_name], field_name=field_name):
                    self.writer.write_triple(stmt_iri, predicate_iri, obj)
            if index % 50000 == 0 or index == total:
                self._print_write_progress("Wrote edges", index, total)

    @staticmethod
    def _print_progress(prefix: str, collection: str, batch_count: int, loaded_total: int, planned_total: int):
        if planned_total:
            percent = loaded_total / planned_total * 100
            print(
                f"{prefix}: {collection} ({batch_count:,}) "
                f"[{loaded_total:,}/{planned_total:,}, {percent:.1f}%]"
            )
        else:
            print(f"{prefix}: {collection} ({batch_count:,})")

    @staticmethod
    def _print_write_progress(prefix: str, written: int, total: int):
        if total:
            percent = written / total * 100
            print(f"{prefix}: {written:,}/{total:,} ({percent:.1f}%)")
        else:
            print(f"{prefix}: {written:,}")

    def _resource_iri(self, collection: str, doc: dict) -> str:
        identifier = doc.get("id") or doc.get("_key")
        return f"{self.base_resource_uri}{collection}/{quote(str(identifier), safe='')}"

    def _edge_endpoint_iri(self, handle: str) -> str:
        if handle in self.node_iri_cache:
            return self.node_iri_cache[handle]
        collection, key = handle.split("/", 1)
        doc = self.get_db().collection(collection).get(key)
        if doc is None:
            return f"{self.base_resource_uri}{collection}/{quote(key, safe='')}"
        return self._resource_iri(collection, doc)

    def _statement_iri(self, edge_collection: str, edge: dict) -> str:
        key = edge.get("_key") or edge["_id"].split("/", 1)[1]
        return f"{self.base_resource_uri}statement/{edge_collection}/{quote(str(key), safe='')}"

    def _class_iri(self, class_name: str) -> str:
        return f"{self.base_ontology_uri}{class_name}"

    def _field_predicate(self, field_name: str) -> str:
        if field_name == "name":
            return RDFS_LABEL
        if field_name == "synonyms":
            return SKOS_ALT_LABEL
        if field_name == "xref":
            return SKOS_EXACT_MATCH
        return f"{self.base_ontology_uri}{field_name}"

    def _edge_predicate(self, edge_collection: str) -> str:
        predicate_name = self.edge_predicate_map.get(edge_collection, edge_collection)
        return f"{self.base_ontology_uri}{predicate_name}"

    @staticmethod
    def _iri_object(iri: str) -> str:
        return f"<{iri}>"

    def _to_objects(self, value: Any, field_name: str | None = None) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            objects = []
            for item in value:
                objects.extend(self._to_objects(item, field_name=field_name))
            return objects
        if field_name == "synonyms":
            if isinstance(value, dict):
                term = value.get("term")
                return [self._literal(str(term))] if term else []
            return [self._literal(str(value))]
        if field_name == "xref":
            if isinstance(value, dict):
                xref_value = value.get("id") or value.get("value")
                return [self._iri_object(self._xref_to_iri(str(xref_value)))] if xref_value else []
            return [self._iri_object(self._xref_to_iri(str(value)))]
        if isinstance(value, bool):
            return [f"\"{'true' if value else 'false'}\"^^<http://www.w3.org/2001/XMLSchema#boolean>"]
        if isinstance(value, int):
            return [f"\"{value}\"^^<http://www.w3.org/2001/XMLSchema#integer>"]
        if isinstance(value, float):
            return [f"\"{value}\"^^<http://www.w3.org/2001/XMLSchema#double>"]
        if isinstance(value, datetime):
            return [f"\"{value.isoformat()}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"]
        if isinstance(value, date):
            return [f"\"{value.isoformat()}\"^^<http://www.w3.org/2001/XMLSchema#date>"]
        if isinstance(value, dict):
            return [self._literal(json.dumps(value, sort_keys=True))]
        return [self._literal(str(value))]

    def _unique_objects(self, value: Any, field_name: str | None = None) -> list[str]:
        objects = self._to_objects(value, field_name=field_name)
        return list(dict.fromkeys(objects))

    @staticmethod
    def _literal(value: str) -> str:
        escaped = (
            value.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        )
        return f"\"{escaped}\""

    def _xref_to_iri(self, xref_value: str) -> str:
        xref_value = xref_value.strip()
        if ":" not in xref_value:
            return f"{self.base_resource_uri}xref/{quote(xref_value, safe='')}"

        prefix_raw, local_id = xref_value.split(":", 1)
        prefix = Prefix.parse(prefix_raw)
        if prefix is None:
            return f"{self.base_resource_uri}xref/{quote(prefix_raw, safe='')}/{quote(local_id, safe='')}"

        if prefix in OBO_PURL_PREFIXES:
            normalized = prefix.value.replace(".", "_")
            local = local_id.replace(":", "_")
            return f"http://purl.obolibrary.org/obo/{normalized}_{quote(local, safe='')}"

        identifiers_ns = IDENTIFIERS_ORG_PREFIX_MAP.get(prefix)
        if identifiers_ns:
            return f"https://identifiers.org/{identifiers_ns}:{quote(local_id, safe='')}"

        return f"{self.base_resource_uri}xref/{quote(prefix.value, safe='')}/{quote(local_id, safe='')}"


def parse_args():
    parser = argparse.ArgumentParser(description="Export Arango graph content to RDF N-Triples.")
    parser.add_argument("--arango-credentials", required=True, help="YAML file with Arango credentials")
    parser.add_argument("--arango-db", required=True, help="Arango database name")
    parser.add_argument("--output-file", required=True, help="Output .nt file")
    parser.add_argument(
        "--base-resource-uri",
        default="https://ifx.ncats.nih.gov/resource/",
        help="Base URI for exported resources",
    )
    parser.add_argument(
        "--base-ontology-uri",
        default="https://ifx.ncats.nih.gov/ontology/",
        help="Base URI for exported predicates and classes",
    )
    parser.add_argument(
        "--collection",
        action="append",
        default=[],
        help="Collection to include; may be repeated",
    )
    parser.add_argument(
        "--exclude-collection",
        action="append",
        default=[],
        help="Collection to exclude; may be repeated",
    )
    parser.add_argument("--seed-collection", help="Anchor node collection for induced subgraph export")
    parser.add_argument("--seed-limit", type=int, help="Number of seed nodes to anchor on")
    parser.add_argument("--max-hops", type=int, default=2, help="How many expansion hops to traverse from seeds")
    return parser.parse_args()


def load_credentials(path: str) -> DBCredentials:
    with open(path, "r", encoding="utf-8") as handle:
        return DBCredentials.from_yaml(yaml.safe_load(handle))


def main():
    args = parse_args()
    include_collections = set(args.collection)

    converter = ArangoToRdfConverter(
        arango_credentials=load_credentials(args.arango_credentials),
        arango_db_name=args.arango_db,
        output_file=args.output_file,
        base_resource_uri=args.base_resource_uri,
        base_ontology_uri=args.base_ontology_uri,
    )

    try:
        converter.export(
            include_collections=include_collections or None,
            exclude_collections=set(args.exclude_collection),
            seed_collection=args.seed_collection,
            seed_limit=args.seed_limit,
            max_hops=args.max_hops,
        )
    finally:
        converter.close()


if __name__ == "__main__":
    main()
