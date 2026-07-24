from pathlib import Path
import json

from src.input_adapters.metabolite_harmonization.refmet import RefMetMetaboliteEquivalenceAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.models.metabolite_harmonization import MetaboliteIdentifier, MetaboliteIdentifierMappingEdge
from src.shared.record_merger import FieldConflictBehavior


class _ConvertingOutputAdapter(OutputAdapter):
    def store(self, objects, single_source=False, field_conflict_behavior=FieldConflictBehavior.KeepFirst) -> bool:
        return True

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        return True


def _write_refmet_csv(path: Path):
    path.write_text(
        " refmet_id,refmet_name,super_class,main_class,sub_class,formula,exactmass,pubchem_cid,chebi_id,hmdb_id,lipidmaps_id,kegg_id,inchi_key\n"
        "RM0108606,Acutumidine,Alkaloids,Alkaloids,Other alkaloids,C18H22ClNO6,383.113567,CID442840,CHEBI:2452,HMDB0015495,LMPK11000003,C10565,SBALNGLYQFMKPR-NQTWQHAWSA-N\n"
        "RM0108637,Adifoline,Alkaloids,Alkaloids,Other alkaloids,C22H20N2O7,424.127053,441972,2488,,,C09020,DJWXVEDJWPDUBQ-DEALGVFLSA-N\n",
        encoding="utf-8",
    )


def _records(adapter):
    return [record for batch in adapter.get_all() for record in batch]


def test_refmet_adapter_emits_nodes_names_and_edges(tmp_path: Path):
    csv_path = tmp_path / "refmet.csv"
    _write_refmet_csv(csv_path)
    adapter = RefMetMetaboliteEquivalenceAdapter(refmet_csv_file=str(csv_path))
    assert adapter.get_datasource_name().value == "RefMet"

    records = _records(adapter)
    nodes = [record for record in records if isinstance(record, MetaboliteIdentifier)]
    edges = [record for record in records if isinstance(record, MetaboliteIdentifierMappingEdge)]
    nodes_by_id = {node.id: node for node in nodes}
    edge_pairs = {(edge.start_node.id, edge.end_node.id) for edge in edges}

    primary = nodes_by_id["REFMET:RM0108606"]
    assert primary.prefix == "REFMET"
    assert primary.names[0].value == "Acutumidine"
    assert primary.names[0].source == "RefMet"
    assert nodes_by_id["PUBCHEM.COMPOUND:442840"].prefix == "PUBCHEM.COMPOUND"

    expected_node_ids = {
        "REFMET:RM0108606",
        "PUBCHEM.COMPOUND:442840",
        "CHEBI:2452",
        "HMDB:HMDB0015495",
        "LIPIDMAPS:LMPK11000003",
        "KEGG.COMPOUND:C10565",
        "REFMET:RM0108637",
        "PUBCHEM.COMPOUND:441972",
        "CHEBI:2488",
        "KEGG.COMPOUND:C09020",
    }
    assert expected_node_ids <= set(nodes_by_id)
    assert "InChIKey:SBALNGLYQFMKPR-NQTWQHAWSA-N" not in nodes_by_id
    assert "InChIKey:DJWXVEDJWPDUBQ-DEALGVFLSA-N" not in nodes_by_id

    assert ("REFMET:RM0108606", "CHEBI:2452") in edge_pairs
    assert ("REFMET:RM0108606", "PUBCHEM.COMPOUND:442840") in edge_pairs
    assert ("REFMET:RM0108606", "LIPIDMAPS:LMPK11000003") in edge_pairs
    assert ("REFMET:RM0108606", "InChIKey:SBALNGLYQFMKPR-NQTWQHAWSA-N") not in edge_pairs

    details = {
        (edge.start_node.id, edge.end_node.id): edge.details[0]
        for edge in edges
    }
    assert details[("REFMET:RM0108606", "CHEBI:2452")].source == "RefMet"
    assert details[("REFMET:RM0108606", "CHEBI:2452")].source_field == "chebi_id"
    assert details[("REFMET:RM0108606", "CHEBI:2452")].source_id == "REFMET:RM0108606"


def test_refmet_records_are_json_serializable_after_output_conversion(tmp_path: Path):
    csv_path = tmp_path / "refmet.csv"
    _write_refmet_csv(csv_path)
    adapter = RefMetMetaboliteEquivalenceAdapter(refmet_csv_file=str(csv_path), max_records=1)
    output = _ConvertingOutputAdapter()

    converted_groups = output.sort_and_convert_objects(_records(adapter))
    converted_records = [
        record
        for group in converted_groups.values()
        for record in group[0]
    ]

    json.dumps(converted_records)
    primary = next(record for record in converted_records if record["id"] == "REFMET:RM0108606")
    chebi_edge = next(record for record in converted_records if record.get("end_id") == "CHEBI:2452")
    assert primary["prefix"] == "REFMET"
    assert primary["names"] == [
        {"value": "Acutumidine", "source": "RefMet", "source_field": "refmet_name"}
    ]
    assert chebi_edge["details"] == [
        {"source": "RefMet", "source_field": "chebi_id", "source_id": "REFMET:RM0108606"}
    ]


def test_refmet_adapter_honors_max_records(tmp_path: Path):
    csv_path = tmp_path / "refmet.csv"
    _write_refmet_csv(csv_path)
    adapter = RefMetMetaboliteEquivalenceAdapter(refmet_csv_file=str(csv_path), max_records=1)

    records = _records(adapter)
    assert any(isinstance(record, MetaboliteIdentifier) and record.id == "REFMET:RM0108606" for record in records)
    assert not any(isinstance(record, MetaboliteIdentifier) and record.id == "REFMET:RM0108637" for record in records)
