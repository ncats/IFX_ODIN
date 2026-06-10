from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter


class _Converter(SQLOutputConverter):
    def get_object_converters(self, obj_cls):
        return None


def test_resolve_id_allocates_after_sparse_preloaded_integer_ids():
    converter = _Converter(sql_base=None)
    converter.id_mapping["ncats_disease"] = {
        "MONDO:0000001": 1,
        "MONDO:0000003": 3,
    }

    assert converter.resolve_id("ncats_disease", "MONDO:0000004") == 4
    assert converter.resolve_id("ncats_disease", "MONDO:0000005") == 5


def test_resolve_id_reuses_existing_mapping():
    converter = _Converter(sql_base=None)
    converter.id_mapping["protein"] = {"UniProtKB:P12345": 42}

    assert converter.resolve_id("protein", "UniProtKB:P12345") == 42
