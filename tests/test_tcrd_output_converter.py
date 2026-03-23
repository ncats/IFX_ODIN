from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    def query(self, *columns):
        key = tuple(getattr(column, "name", str(column)) for column in columns)
        if key == ("id", "ifx_id"):
            return _FakeQuery([
                (123, "IFX123"),
            ])
        if key == ("name",):
            return _FakeQuery([])
        raise AssertionError(f"Unexpected query columns: {key}")


def test_tcrd_output_converter_preloads_protein_ids():
    converter = TCRDOutputConverter()

    converter.preload_id_mappings(_FakeSession())

    assert converter.id_mapping["protein"] == {"IFX123": 123}


def test_pathway_converter_keeps_pwtype_without_lookup_table():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    row = converter.pathway_converter({
        "start_id": "IFX123",
        "end_node": {
            "type": "Reactome",
            "source_id": "R-HSA-199420",
            "name": "Generic Transcription Pathway",
            "url": "https://reactome.org/content/detail/R-HSA-199420",
        },
        "provenance": "Reactome\t95\t2025-11-27\t2026-03-23",
    })

    assert row.target_id == 123
    assert row.protein_id == 123
    assert row.pwtype == "Reactome"
    assert row.id_in_source == "R-HSA-199420"
