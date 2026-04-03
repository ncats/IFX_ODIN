from typing import Type


def indexed(fields=None):
    fields = fields or []

    def decorator(cls):
        cls._indexed_fields = fields
        return cls

    return decorator


def facets(category_fields=None, numeric_fields=None):
    category_fields = category_fields or []
    numeric_fields = numeric_fields or []

    def decorator(cls):
        cls._facet_categories = category_fields
        cls._facet_numerics = numeric_fields
        return cls

    return decorator


def search(text_fields=None):
    text_fields = text_fields or []

    def decorator(cls):
        cls._search_text_fields = text_fields
        return cls

    return decorator


def collect_indexed_fields(cls: Type):
    indexed_fields = set()

    for base in cls.__mro__:
        if hasattr(base, "_indexed_fields"):
            indexed_fields.update(base._indexed_fields)

    return indexed_fields


def collect_facets(cls: Type):
    categories = set()
    numerics = set()

    for base in cls.__mro__:
        if hasattr(base, "_facet_categories"):
            categories.update(base._facet_categories)
        if hasattr(base, "_facet_numerics"):
            numerics.update(base._facet_numerics)

    return categories, numerics


def collect_search_fields(cls: Type):
    text_fields = set()

    for base in cls.__mro__:
        if hasattr(base, "_search_text_fields"):
            text_fields.update(base._search_text_fields)

    return text_fields
