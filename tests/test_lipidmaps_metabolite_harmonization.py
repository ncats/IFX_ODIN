from pathlib import Path
import json
import zipfile

from src.input_adapters.metabolite_harmonization.lipidmaps import LipidMapsMetaboliteEquivalenceAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.models.metabolite_harmonization import MetaboliteIdentifier, MetaboliteIdentifierMappingEdge
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
            },
        )
        + _sdf_record(
            "LMFA00000002",
            {
                "LM_ID": "LMFA00000002",
                "NAME": "Second lipid",
                "PUBCHEM_CID": "42607282",
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
        "InChIKey:NDDJIMSGSZNACM-QWRGUYRKSA-N",
        "LIPIDMAPS:LMFA00000002",
        "PUBCHEM.COMPOUND:42607282",
    }
    assert expected_node_ids <= set(nodes_by_id)

    assert ("LIPIDMAPS:LMFA00000001", "CHEBI:137783") in edge_pairs
    assert ("LIPIDMAPS:LMFA00000001", "PUBCHEM.COMPOUND:42607281") in edge_pairs
    assert ("LIPIDMAPS:LMFA00000001", "InChIKey:NDDJIMSGSZNACM-QWRGUYRKSA-N") in edge_pairs

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
