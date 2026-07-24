from pathlib import Path
import json
import zipfile

from src.input_adapters.metabolite_harmonization.lipidmaps import LipidMapsMetaboliteEquivalenceAdapter
from src.input_adapters.metabolite_harmonization.lipidmaps_classes import LipidMapsMetaboliteClassificationAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.models.metabolite_harmonization import (
    MetaboliteClassificationEdge,
    MetaboliteClassificationParentEdge,
    MetaboliteClassificationTerm,
    MetaboliteIdentifier,
    MetaboliteIdentifierMappingEdge,
)
from src.shared.record_merger import FieldConflictBehavior


class _ConvertingOutputAdapter(OutputAdapter):
    def store(self, objects, single_source=False, field_conflict_behavior=FieldConflictBehavior.KeepFirst) -> bool:
        return True

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        return True


def _sdf_record(title: str, tags: dict) -> str:
    lines = [
        title,
        "  TEST",
        "",
        "  0  0  0     0  0            999 V2000",
        "M  END",
        "",
    ]
    for key, value in tags.items():
        lines.extend([f"> <{key}>", value, ""])
    lines.append("$$$$")
    return "\n".join(lines) + "\n"


def _write_lipidmaps_zip(path: Path):
    sdf = (
        _sdf_record(
            "LMFA00000001",
            {
                "LM_ID": "LMFA00000001",
                "NAME": "Serratamic acid",
                "SYSTEMATIC_NAME": "N-(3S-hydroxydecanoyl)-L-serine",
                "ABBREVIATION": "FA 16:0",
                "SYNONYMS": "Syn A; Syn B; Syn A",
                "PUBCHEM_CID": "CID42607281",
                "CHEBI_ID": "CHEBI:137783",
                "HMDB_ID": "HMDB0013655",
                "SWISSLIPIDS_ID": "SLM:000000510",
                "LIPIDBANK_ID": "DFA0002",
                "KEGG_ID": "C15989",
                "PLANTFA_ID": "10010",
                "INCHI_KEY": "NDDJIMSGSZNACM-QWRGUYRKSA-N",
                "CATEGORY": "Fatty Acyls [FA]",
                "MAIN_CLASS": "Other Fatty Acyls [FA00]",
                "SUB_CLASS": "Fatty amides [FA0803]",
                "CLASS_LEVEL4": "N-acyl amines [FA080301]",
            },
        )
        + _sdf_record(
            "LMFA00000002",
            {
                "LM_ID": "LMFA00000002",
                "NAME": "Second lipid",
                "PUBCHEM_CID": "42607282",
                "CATEGORY": "Fatty Acyls [FA]",
                "MAIN_CLASS": "Other Fatty Acyls [FA00]",
            },
        )
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("structures.sdf", sdf)


def _records(adapter):
    return [record for batch in adapter.get_all() for record in batch]


def test_lipidmaps_adapter_emits_nodes_names_synonyms_and_edges(tmp_path: Path):
    zip_path = tmp_path / "LMSD.sdf.zip"
    _write_lipidmaps_zip(zip_path)
    adapter = LipidMapsMetaboliteEquivalenceAdapter(sdf_zip_file=str(zip_path))
    assert adapter.get_datasource_name().value == "LipidMaps"

    records = _records(adapter)
    nodes = [record for record in records if isinstance(record, MetaboliteIdentifier)]
    edges = [record for record in records if isinstance(record, MetaboliteIdentifierMappingEdge)]
    nodes_by_id = {node.id: node for node in nodes}
    edge_pairs = {(edge.start_node.id, edge.end_node.id) for edge in edges}

    primary = nodes_by_id["LIPIDMAPS:LMFA00000001"]
    assert primary.prefix == "LIPIDMAPS"
    assert [name.value for name in primary.names] == [
        "Serratamic acid",
        "N-(3S-hydroxydecanoyl)-L-serine",
        "FA 16:0",
    ]
    assert [synonym.value for synonym in primary.synonyms] == ["Syn A", "Syn B"]
    assert nodes_by_id["PUBCHEM.COMPOUND:42607281"].prefix == "PUBCHEM.COMPOUND"

    expected_node_ids = {
        "LIPIDMAPS:LMFA00000001",
        "PUBCHEM.COMPOUND:42607281",
        "CHEBI:137783",
        "HMDB:HMDB0013655",
        "SwissLipids:SLM:000000510",
        "LipidBank:DFA0002",
        "KEGG.COMPOUND:C15989",
        "PlantFA:10010",
        "LIPIDMAPS:LMFA00000002",
        "PUBCHEM.COMPOUND:42607282",
    }
    assert expected_node_ids <= set(nodes_by_id)
    assert "InChIKey:NDDJIMSGSZNACM-QWRGUYRKSA-N" not in nodes_by_id

    assert ("LIPIDMAPS:LMFA00000001", "CHEBI:137783") in edge_pairs
    assert ("LIPIDMAPS:LMFA00000001", "PUBCHEM.COMPOUND:42607281") in edge_pairs
    assert ("LIPIDMAPS:LMFA00000001", "InChIKey:NDDJIMSGSZNACM-QWRGUYRKSA-N") not in edge_pairs

    details = {
        (edge.start_node.id, edge.end_node.id): edge.details[0]
        for edge in edges
    }
    assert details[("LIPIDMAPS:LMFA00000001", "CHEBI:137783")].source == "LipidMaps"
    assert details[("LIPIDMAPS:LMFA00000001", "CHEBI:137783")].source_field == "CHEBI_ID"
    assert details[("LIPIDMAPS:LMFA00000001", "CHEBI:137783")].source_id == "LIPIDMAPS:LMFA00000001"


def test_lipidmaps_records_are_json_serializable_after_output_conversion(tmp_path: Path):
    zip_path = tmp_path / "LMSD.sdf.zip"
    _write_lipidmaps_zip(zip_path)
    adapter = LipidMapsMetaboliteEquivalenceAdapter(sdf_zip_file=str(zip_path), max_records=1)
    output = _ConvertingOutputAdapter()

    converted_groups = output.sort_and_convert_objects(_records(adapter))
    converted_records = [
        record
        for group in converted_groups.values()
        for record in group[0]
    ]

    json.dumps(converted_records)
    primary = next(record for record in converted_records if record["id"] == "LIPIDMAPS:LMFA00000001")
    chebi_edge = next(record for record in converted_records if record.get("end_id") == "CHEBI:137783")
    assert primary["prefix"] == "LIPIDMAPS"
    assert primary["synonyms"] == [
        {"value": "Syn A", "source": "LipidMaps", "source_field": "SYNONYMS"},
        {"value": "Syn B", "source": "LipidMaps", "source_field": "SYNONYMS"},
    ]
    assert chebi_edge["details"] == [
        {"source": "LipidMaps", "source_field": "CHEBI_ID", "source_id": "LIPIDMAPS:LMFA00000001"}
    ]


def test_lipidmaps_adapter_honors_max_records(tmp_path: Path):
    zip_path = tmp_path / "LMSD.sdf.zip"
    _write_lipidmaps_zip(zip_path)
    adapter = LipidMapsMetaboliteEquivalenceAdapter(sdf_zip_file=str(zip_path), max_records=1)

    records = _records(adapter)
    assert any(isinstance(record, MetaboliteIdentifier) and record.id == "LIPIDMAPS:LMFA00000001" for record in records)
    assert not any(isinstance(record, MetaboliteIdentifier) and record.id == "LIPIDMAPS:LMFA00000002" for record in records)


def test_lipidmaps_classification_adapter_emits_terms_hierarchy_and_edges(tmp_path: Path):
    zip_path = tmp_path / "LMSD.sdf.zip"
    _write_lipidmaps_zip(zip_path)
    adapter = LipidMapsMetaboliteClassificationAdapter(sdf_zip_file=str(zip_path))

    records = _records(adapter)
    terms = [record for record in records if isinstance(record, MetaboliteClassificationTerm)]
    parent_edges = [record for record in records if isinstance(record, MetaboliteClassificationParentEdge)]
    member_edges = [record for record in records if isinstance(record, MetaboliteClassificationEdge)]
    term_by_name = {(term.level_name, term.name): term for term in terms}
    parent_pairs = {(edge.start_node.id, edge.end_node.id) for edge in parent_edges}
    member_pairs = {(edge.start_node.id, edge.end_node.id) for edge in member_edges}

    category = term_by_name[("LipidMaps_category", "Fatty Acyls [FA]")]
    main_class = term_by_name[("LipidMaps_main_class", "Other Fatty Acyls [FA00]")]
    sub_class = term_by_name[("LipidMaps_sub_class", "Fatty amides [FA0803]")]
    level4 = term_by_name[("LipidMaps_class_level4", "N-acyl amines [FA080301]")]

    assert category.id == "LipidMaps.CLASS:LipidMaps_category:Fatty Acyls [FA]"
    assert category.source_id == "FA"
    assert main_class.source_id == "FA00"
    assert (category.id, main_class.id) in parent_pairs
    assert (main_class.id, sub_class.id) in parent_pairs
    assert (sub_class.id, level4.id) in parent_pairs
    assert ("LIPIDMAPS:LMFA00000001", category.id) not in member_pairs
    assert ("LIPIDMAPS:LMFA00000001", main_class.id) not in member_pairs
    assert ("LIPIDMAPS:LMFA00000001", sub_class.id) not in member_pairs
    assert ("LIPIDMAPS:LMFA00000001", level4.id) in member_pairs
    assert ("LIPIDMAPS:LMFA00000002", main_class.id) in member_pairs
    assert member_edges[0].details[0].source == "LipidMaps"
