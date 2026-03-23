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
            return _FakeQuery([
                ("WikiPathways",),
                ("UniProt",),
            ])
        raise AssertionError(f"Unexpected query columns: {key}")


def test_tcrd_output_converter_preloads_existing_pathway_types():
    converter = TCRDOutputConverter()

    converter.preload_id_mappings(_FakeSession())

    assert converter.id_mapping["protein"] == {"IFX123": 123}
    assert converter._known_pathway_types == {"WikiPathways", "UniProt"}
