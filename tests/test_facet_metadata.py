from src.core.decorators import facets, collect_facets


@facets(category_fields=["legacy_category"], numeric_fields=["legacy_numeric"])
class LegacyFacetModel:
    pass


@facets(extra_indexed_fields=["base_index"], category_fields=["base_category"])
class BaseFacetModel:
    pass


@facets(
    extra_indexed_fields=["child_index"],
    category_fields=["child_category"],
    numeric_fields=["child_numeric"],
)
class ChildFacetModel(BaseFacetModel):
    pass


def test_collect_facets_supports_legacy_declarations():
    extra_indexed, categories, numerics = collect_facets(LegacyFacetModel)

    assert extra_indexed == set()
    assert categories == {"legacy_category"}
    assert numerics == {"legacy_numeric"}


def test_collect_facets_merges_inherited_metadata():
    extra_indexed, categories, numerics = collect_facets(ChildFacetModel)

    assert extra_indexed == {"base_index", "child_index"}
    assert categories == {"base_category", "child_category"}
    assert numerics == {"child_numeric"}
