import gzip
from pathlib import Path

from src.input_adapters.chebi.chebi_obo_adapter import FullOboAdapter
from src.core.decorators import collect_facets, collect_indexed_fields, collect_search_fields
from src.registry.fetchers import MaterializedDataset
from src.models.chebi import (
    Application,
    BiologicalRole,
    ChemicalEntity,
    ChemicalRole,
    HasApplicationEdge,
    HasBiologicalRoleEdge,
    HasChemicalRoleEdge,
    HasFunctionalParentEdge,
    HasRoleEdge,
    IsAEdge,
    IsSubstituentGroupFromEdge,
    Role,
    Term,
)
from src.models.node import Node


def _write_chebi_obo(path: Path):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(
            "format-version: 1.2\n"
            "data-version: 252\n"
            "date: 01:05:2026 16:42\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:100\n"
            "name: (-)-medicarpin\n"
            "def: \"The (-)-enantiomer of medicarpin.\" [ChEBI]\n"
            "subset: 3:STAR\n"
            "alt_id: CHEBI:7456\n"
            "synonym: \"(-)-Medicarpin\" RELATED [kegg.compound]\n"
            "synonym: \"IUPAC name\" EXACT IUPAC:NAME [IUPAC]\n"
            "xref: cas:32383-76-9 {source=\"cas\"}\n"
            "is_a: CHEBI:16114 ! medicarpin\n"
            "is_a: CHEBI:23367 ! molecular entity\n"
            "relationship: RO:0000087 CHEBI:76924 ! has role plant metabolite\n"
            "relationship: RO:0000087 CHEBI:64047 ! has role food additive\n"
            "property_value: chemrof:charge \"0\" xsd:integer\n"
            "property_value: chemrof:generalized_empirical_formula \"C16H14O4\" xsd:string\n"
            "property_value: chemrof:smiles_string \"O=C(O)\" xsd:string\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:16114\n"
            "name: medicarpin\n"
            "is_a: CHEBI:23367 ! molecular entity\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:50320\n"
            "name: nucleoside residue\n"
            "is_a: CHEBI:33247 ! organic group\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:10014\n"
            "name: Voacamine\n"
            "is_a: CHEBI:23315 ! citraconoyl group\n"
            "property_value: chemrof:inchi_key_string \"VCMIRXRRQJNZJT-QGDNWDNJSA-N\" xsd:string\n"
            "property_value: chemrof:smiles_string \"C1CC\" xsd:string\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:23315\n"
            "name: citraconoyl group\n"
            "is_a: CHEBI:33247 ! organic group\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:33247\n"
            "name: organic group\n"
            "is_a: CHEBI:24433 ! group\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:24433\n"
            "name: group\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:99999\n"
            "alt_id: CHEBI:88888\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:141163\n"
            "name: grouped term\n"
            "is_a: CHEBI:141160 ! parent term\n"
            "relationship: RO:0018038 CHEBI:141160 ! has functional parent parent term\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:141160\n"
            "name: parent term\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:76924\n"
            "name: plant metabolite\n"
            "subset: 3:STAR\n"
            "is_a: CHEBI:75763 ! eukaryotic metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:75763\n"
            "name: eukaryotic metabolite\n"
            "is_a: CHEBI:25212 ! metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:25212\n"
            "name: metabolite\n"
            "is_a: CHEBI:52206 ! biochemical role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:52206\n"
            "name: biochemical role\n"
            "is_a: CHEBI:24432 ! biological role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:24432\n"
            "name: biological role\n"
            "is_a: CHEBI:50906 ! role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:64047\n"
            "name: food additive\n"
            "is_a: CHEBI:33232 ! application\n"
            "is_a: CHEBI:51086 ! chemical role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:33232\n"
            "name: application\n"
            "is_a: CHEBI:50906 ! role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:51086\n"
            "name: chemical role\n"
            "is_a: CHEBI:50906 ! role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:50906\n"
            "name: role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:500\n"
            "name: source substituent group\n"
            "relationship: RO:0018037 CHEBI:100 ! is substituent group from (-)-medicarpin\n"
        )


def _dataset_for_file(path: Path) -> MaterializedDataset:
    return MaterializedDataset(
        source="chebi",
        dataset="ontology_full",
        version="252",
        version_date="2026-05-01",
        download_date="2026-06-24",
        snapshot_id="chebi:ontology_full:252",
        manifest_uri="s3://ifx-registry/sources/chebi/ontology_full/252/manifest.yaml",
        manifest={"files": [{"path": path.name}]},
        local_dir=path.parent,
    )


def test_chebi_full_obo_adapter_preserves_source_annotations(tmp_path: Path):
    path = tmp_path / "chebi.obo.gz"
    _write_chebi_obo(path)
    adapter = FullOboAdapter(data_source=_dataset_for_file(path))

    records = [entry for batch in adapter.get_all() for entry in batch]
    chemical_entities = [entry for entry in records if isinstance(entry, ChemicalEntity)]
    generic_terms = [
        entry for entry in records
        if type(entry) is Term
    ]
    roles = [entry for entry in records if type(entry) is Role]
    applications = [entry for entry in records if isinstance(entry, Application)]
    biological_roles = [entry for entry in records if isinstance(entry, BiologicalRole)]
    chemical_roles = [entry for entry in records if isinstance(entry, ChemicalRole)]
    is_a_edges = [entry for entry in records if isinstance(entry, IsAEdge)]
    role_edges = [entry for entry in records if isinstance(entry, HasRoleEdge)]
    application_edges = [entry for entry in records if isinstance(entry, HasApplicationEdge)]
    biological_role_edges = [entry for entry in records if isinstance(entry, HasBiologicalRoleEdge)]
    chemical_role_edges = [entry for entry in records if isinstance(entry, HasChemicalRoleEdge)]
    functional_parent_edges = [entry for entry in records if isinstance(entry, HasFunctionalParentEdge)]
    substituent_edges = [entry for entry in records if isinstance(entry, IsSubstituentGroupFromEdge)]

    assert adapter.get_version().version == "252"
    chemical = next(term for term in chemical_entities if term.id == "CHEBI:100")
    assert chemical.name == "(-)-medicarpin"
    assert chemical.definition == "The (-)-enantiomer of medicarpin."
    assert chemical.definition_references == ["ChEBI"]
    assert chemical.subsets == ["3:STAR"]
    assert chemical.alt_ids == ["CHEBI:7456"]
    assert [syn.value for syn in chemical.synonyms] == ["(-)-Medicarpin", "IUPAC name"]
    assert chemical.synonyms[1].scope == "EXACT"
    assert chemical.synonyms[1].type == "IUPAC:NAME"
    assert chemical.xrefs[0].value == "cas:32383-76-9"
    assert chemical.xrefs[0].source == "cas"
    assert chemical.charge == "0"
    assert chemical.formula == "C16H14O4"
    assert chemical.smiles == "O=C(O)"
    assert "(-)-Medicarpin" in chemical.synonym_text
    assert chemical.xref_text == "cas:32383-76-9"
    assert next(term for term in chemical_entities if term.id == "CHEBI:50320").name == "nucleoside residue"
    assert all(term.id != "CHEBI:99999" for term in records if isinstance(term, Node))
    assert generic_terms == []
    assert next(term for term in chemical_entities if term.id == "CHEBI:10014").inchi_key == "VCMIRXRRQJNZJT-QGDNWDNJSA-N"
    assert next(term for term in chemical_entities if term.id == "CHEBI:141163").name == "grouped term"

    assert [role.id for role in roles] == ["CHEBI:50906"]
    biological_role = next(role for role in biological_roles if role.id == "CHEBI:76924")
    assert biological_role.name == "plant metabolite"
    assert next(role for role in applications if role.id == "CHEBI:64047").name == "food additive"
    assert next(role for role in chemical_roles if role.id == "CHEBI:64047").name == "food additive"
    substituent_group = next(term for term in chemical_entities if term.id == "CHEBI:500")
    assert substituent_group.name == "source substituent group"

    first_edge = next(edge for edge in is_a_edges if edge.start_node.id == "CHEBI:100" and edge.end_node.id == "CHEBI:16114")
    assert isinstance(first_edge.start_node, ChemicalEntity)
    assert isinstance(first_edge.end_node, ChemicalEntity)
    assert first_edge.source_predicate == "is_a"
    assert first_edge.target_label == "medicarpin"

    assert role_edges == []
    biological_role_edge = biological_role_edges[0]
    assert biological_role_edge.start_node.id == "CHEBI:100"
    assert biological_role_edge.end_node.id == "CHEBI:76924"
    assert biological_role_edge.source_predicate == "RO:0000087"
    assert biological_role_edge.target_label == "plant metabolite"
    application_edge = application_edges[0]
    assert application_edge.start_node.id == "CHEBI:100"
    assert application_edge.end_node.id == "CHEBI:64047"
    assert application_edge.target_label == "food additive"
    chemical_role_edge = chemical_role_edges[0]
    assert chemical_role_edge.start_node.id == "CHEBI:100"
    assert chemical_role_edge.end_node.id == "CHEBI:64047"
    assert chemical_role_edge.target_label == "food additive"

    grouped_pair_is_a_edge = next(edge for edge in is_a_edges if edge.start_node.id == "CHEBI:141163")
    assert grouped_pair_is_a_edge.end_node.id == "CHEBI:141160"
    assert grouped_pair_is_a_edge.source_predicate == "is_a"
    functional_parent_edge = functional_parent_edges[0]
    assert functional_parent_edge.start_node.id == "CHEBI:141163"
    assert functional_parent_edge.end_node.id == "CHEBI:141160"
    assert isinstance(functional_parent_edge.start_node, ChemicalEntity)
    assert isinstance(functional_parent_edge.end_node, ChemicalEntity)
    assert functional_parent_edge.source_predicate == "RO:0018038"

    substituent_edge = substituent_edges[0]
    assert isinstance(substituent_edge.start_node, ChemicalEntity)
    assert isinstance(substituent_edge.end_node, ChemicalEntity)
    assert substituent_edge.start_node.id == "CHEBI:500"
    assert substituent_edge.end_node.id == "CHEBI:100"
    assert substituent_edge.target_label == "(-)-medicarpin"


def test_chebi_term_qa_browser_metadata_keeps_high_cardinality_fields_out_of_facets_and_indexes():
    indexed_fields = collect_indexed_fields(Term)
    category_facets, _ = collect_facets(Term)
    search_fields = collect_search_fields(Term)

    assert "alt_ids" not in indexed_fields
    assert "xref_text" not in indexed_fields
    assert "inchi_key" not in indexed_fields
    assert "alt_ids" not in category_facets
    assert "xref_text" not in category_facets
    assert "inchi_key" not in category_facets
    assert "charge" not in category_facets
    assert category_facets == {"sources", "subsets", "is_obsolete"}
    assert search_fields == {"id", "name", "inchi_key"}
