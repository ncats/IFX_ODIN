from typing import Type


def facets(category_fields=None, numeric_fields=None, extra_indexed_fields=None):
    category_fields = category_fields or []
    numeric_fields = numeric_fields or []
    extra_indexed_fields = extra_indexed_fields or []

    def decorator(cls):
        cls._facet_categories = category_fields
        cls._facet_numerics = numeric_fields
        cls._facet_extra_indexed = extra_indexed_fields
        return cls

    return decorator


def collect_facets(cls: Type):
    extra_indexed = set()
    categories = set()
    numerics = set()

    for base in cls.__mro__:
        if hasattr(base, "_facet_extra_indexed"):
            extra_indexed.update(base._facet_extra_indexed)
        if hasattr(base, "_facet_categories"):
            categories.update(base._facet_categories)
        if hasattr(base, "_facet_numerics"):
            numerics.update(base._facet_numerics)

    return extra_indexed, categories, numerics
