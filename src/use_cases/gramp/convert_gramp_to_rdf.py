from collections import defaultdict

from src.use_cases.arango_to_rdf import ArangoToRdfConverter, RDF_TYPE, load_credentials


BIOLINK_BASE_URI = "https://w3id.org/biolink/vocab/"

GRAMP_BIOLINK_CLASS_MAP = {
    "Protein": "Protein",
    "Pathway": "Pathway",
    "MetaboliteClass": "OntologyClass",
}

GRAMP_BIOLINK_PREDICATES = {
    "participates_in": f"{BIOLINK_BASE_URI}participates_in",
    "is_input_of": f"{BIOLINK_BASE_URI}is_input_of",
    "is_output_of": f"{BIOLINK_BASE_URI}is_output_of",
    "enabled_by": f"{BIOLINK_BASE_URI}enabled_by",
    "part_of": f"{BIOLINK_BASE_URI}part_of",
}


GRAMP_METABOHUB_COLLECTIONS = {
    "Metabolite",
    "Protein",
    "Pathway",
    "Reaction",
    "ReactionClass",
    "MetaboliteClass",
    "MetaboliteProteinEdge",
    "MetaboliteReactionEdge",
    "ProteinReactionEdge",
    "MetabolitePathwayEdge",
    "ProteinPathwayEdge",
    "ReactionReactionClassEdge",
    "ReactionClassParentEdge",
    "MetaboliteClassEdge",
}

GRAMP_EDGE_PREDICATE_MAP = {
    "MetaboliteProteinEdge": "hasProtein",
    "MetaboliteReactionEdge": "hasReaction",
    "MetabolitePathwayEdge": "hasPathway",
    "ProteinReactionEdge": "participatesInReaction",
    "ProteinPathwayEdge": "hasPathway",
    "ReactionReactionClassEdge": "hasReactionClass",
    "ReactionClassParentEdge": "hasParentReactionClass",
    "MetaboliteClassEdge": "hasMetaboliteClass",
}

GRAMP_EXCLUDED_FIELDS = {
    "creation",
    "resolved_ids",
    "updates",
}


class GrampBiolinkRdfConverter(ArangoToRdfConverter):
    def _write_nodes(self, nodes: dict[str, dict]):
        super()._write_nodes(nodes)
        for node in nodes.values():
            collection = node["_id"].split("/", 1)[0]
            biolink_class = GRAMP_BIOLINK_CLASS_MAP.get(collection)
            if not biolink_class:
                continue
            subject = self.node_iri_cache.get(node["_id"]) or self._resource_iri(collection, node)
            self.writer.write_triple(subject, RDF_TYPE, self._iri_object(f"{BIOLINK_BASE_URI}{biolink_class}"))

    def _write_edges(self, edges: list[dict]):
        super()._write_edges(edges)
        for edge in edges:
            self._write_biolink_overlay(edge)

    def _write_biolink_overlay(self, edge: dict):
        edge_collection = edge["_id"].split("/", 1)[0]
        subject_iri = self._edge_endpoint_iri(edge["_from"])
        object_iri = self._edge_endpoint_iri(edge["_to"])

        if edge_collection == "MetaboliteReactionEdge":
            self._write_biolink_triple(subject_iri, GRAMP_BIOLINK_PREDICATES["participates_in"], object_iri)
            role = edge.get("substrate_product")
            if role in (0, "0"):
                self._write_biolink_triple(subject_iri, GRAMP_BIOLINK_PREDICATES["is_input_of"], object_iri)
            elif role in (1, "1"):
                self._write_biolink_triple(subject_iri, GRAMP_BIOLINK_PREDICATES["is_output_of"], object_iri)
            return

        if edge_collection == "ProteinReactionEdge":
            self._write_biolink_triple(object_iri, GRAMP_BIOLINK_PREDICATES["enabled_by"], subject_iri)
            return

        if edge_collection in {"MetabolitePathwayEdge", "ProteinPathwayEdge"}:
            self._write_biolink_triple(subject_iri, GRAMP_BIOLINK_PREDICATES["participates_in"], object_iri)
            return

        if edge_collection == "ReactionClassParentEdge":
            self._write_biolink_triple(subject_iri, GRAMP_BIOLINK_PREDICATES["part_of"], object_iri)

    def _write_biolink_triple(self, subject_iri: str, predicate_iri: str, object_iri: str):
        self.writer.write_triple(subject_iri, predicate_iri, self._iri_object(object_iri))


def collect_gramp_metabohub_subset(converter: ArangoToRdfConverter, seed_limit: int) -> tuple[dict[str, dict], list[dict]]:
    selected_nodes: dict[str, dict] = {}
    selected_edges: dict[str, dict] = {}

    metabolite_nodes = converter.runQuery(
        """
        FOR edge IN `MetaboliteProteinEdge`
            COLLECT metabolite_handle = edge._from
            LIMIT @seed_limit
            LET doc = DOCUMENT(metabolite_handle)
            FILTER doc != null
            RETURN doc
        """,
        bind_vars={"seed_limit": seed_limit},
    )
    metabolite_handles = {row["_id"] for row in metabolite_nodes}
    for row in metabolite_nodes:
        selected_nodes[row["_id"]] = row

    edge_targets = {
        "MetaboliteProteinEdge": "Protein",
        "MetaboliteReactionEdge": "Reaction",
        "MetabolitePathwayEdge": "Pathway",
        "MetaboliteClassEdge": "MetaboliteClass",
    }
    discovered_handles: dict[str, set[str]] = defaultdict(set)
    for edge_collection, target_collection in edge_targets.items():
        rows = converter.runQuery(
            f"""
            FOR edge IN `{edge_collection}`
                FILTER edge._from IN @metabolite_handles
                RETURN edge
            """,
            bind_vars={"metabolite_handles": list(metabolite_handles)},
        )
        for edge in rows:
            selected_edges[edge["_id"]] = edge
            if edge["_to"].startswith(f"{target_collection}/"):
                discovered_handles[target_collection].add(edge["_to"])

    protein_handles = discovered_handles["Protein"]
    if protein_handles:
        protein_edges = converter.runQuery(
            """
            FOR edge IN `ProteinReactionEdge`
                FILTER edge._from IN @protein_handles
                RETURN edge
            """,
            bind_vars={"protein_handles": list(protein_handles)},
        )
        for edge in protein_edges:
            selected_edges[edge["_id"]] = edge
            discovered_handles["Reaction"].add(edge["_to"])

        protein_pathway_edges = converter.runQuery(
            """
            FOR edge IN `ProteinPathwayEdge`
                FILTER edge._from IN @protein_handles
                RETURN edge
            """,
            bind_vars={"protein_handles": list(protein_handles)},
        )
        for edge in protein_pathway_edges:
            selected_edges[edge["_id"]] = edge
            discovered_handles["Pathway"].add(edge["_to"])

    reaction_handles = discovered_handles["Reaction"]
    if reaction_handles:
        reaction_class_edges = converter.runQuery(
            """
            FOR edge IN `ReactionReactionClassEdge`
                FILTER edge._from IN @reaction_handles
                RETURN edge
            """,
            bind_vars={"reaction_handles": list(reaction_handles)},
        )
        for edge in reaction_class_edges:
            selected_edges[edge["_id"]] = edge
            discovered_handles["ReactionClass"].add(edge["_to"])

    frontier = set(discovered_handles["ReactionClass"])
    seen_reaction_classes = set(frontier)
    while frontier:
        class_edges = converter.runQuery(
            """
            FOR edge IN `ReactionClassParentEdge`
                FILTER edge._from IN @class_handles
                RETURN edge
            """,
            bind_vars={"class_handles": list(frontier)},
        )
        next_frontier = set()
        for edge in class_edges:
            selected_edges[edge["_id"]] = edge
            if edge["_to"] not in seen_reaction_classes:
                seen_reaction_classes.add(edge["_to"])
                next_frontier.add(edge["_to"])
        frontier = next_frontier
    discovered_handles["ReactionClass"] = seen_reaction_classes

    all_handles = set()
    for handles in discovered_handles.values():
        all_handles.update(handles)
    loaded_nodes = converter._fetch_nodes_by_handle(all_handles)
    for handle, node in loaded_nodes.items():
        selected_nodes[handle] = node

    return selected_nodes, list(selected_edges.values())


def main():
    converter = GrampBiolinkRdfConverter(
        arango_credentials=load_credentials("./src/use_cases/secrets/ifxdev_arangodb.yaml"),
        arango_db_name="gramp",
        output_file="./output/gramp_metabohub_subset.nt",
        base_resource_uri="https://ifx.ncats.nih.gov/resource/",
        base_ontology_uri="https://ifx.ncats.nih.gov/ontology/",
        edge_predicate_map=GRAMP_EDGE_PREDICATE_MAP,
        excluded_fields=GRAMP_EXCLUDED_FIELDS,
    )

    try:
        nodes, edges = collect_gramp_metabohub_subset(converter, seed_limit=100)
        converter.write_graph(nodes, edges)
    finally:
        converter.close()


if __name__ == "__main__":
    main()
