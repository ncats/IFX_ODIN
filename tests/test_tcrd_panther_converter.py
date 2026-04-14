from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter


def test_panther_class_converter_maps_parent_pcids_and_source_id():
    converter = TCRDOutputConverter()

    row = converter.panther_class_converter(
        {
            "id": "PANTHER.CLASS:PC00021",
            "source_id": "PC00021",
            "parent_pcids": "PC00197",
            "name": "G-protein coupled receptor",
            "description": "GPCR class",
        }
    )

    assert row.id == 1
    assert row.pcid == "PC00021"
    assert row.parent_pcids == "PC00197"
    assert row.name == "G-protein coupled receptor"
    assert row.description == "GPCR class"


def test_p2pc_converter_uses_preloaded_protein_and_panther_ids():
    converter = TCRDOutputConverter()
    converter.id_mapping = {
        "protein": {"IFXProtein:ABC123": 7},
        "panther_class": {"PANTHER.CLASS:PC00021": 13},
    }

    row = converter.p2pc_converter(
        {
            "start_id": "IFXProtein:ABC123",
            "end_id": "PANTHER.CLASS:PC00021",
        }
    )

    assert row.protein_id == 7
    assert row.panther_class_id == 13
