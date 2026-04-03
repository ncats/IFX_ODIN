from src.core.decorators import (
    collect_facets,
    collect_indexed_fields,
    collect_search_fields,
    facets,
    indexed,
    search,
)


@facets(category_fields=["legacy_category"], numeric_fields=["legacy_numeric"])
class LegacyFacetModel:
    pass


@indexed(fields=["base_index"])
@facets(category_fields=["base_category"])
@search(text_fields=["base_text"])
class BaseMetadataModel:
    pass


@indexed(fields=["child_index"])
@facets(category_fields=["child_category"], numeric_fields=["child_numeric"])
@search(text_fields=["child_text"])
class ChildMetadataModel(BaseMetadataModel):
    pass


def test_collect_facets_supports_category_and_numeric_declarations():
    categories, numerics = collect_facets(LegacyFacetModel)

    assert categories == {"legacy_category"}
    assert numerics == {"legacy_numeric"}


def test_collect_metadata_merges_inherited_declarations():
    indexed_fields = collect_indexed_fields(ChildMetadataModel)
    categories, numerics = collect_facets(ChildMetadataModel)
    text_fields = collect_search_fields(ChildMetadataModel)

    assert indexed_fields == {"base_index", "child_index"}
    assert categories == {"base_category", "child_category"}
    assert numerics == {"child_numeric"}
    assert text_fields == {"base_text", "child_text"}
