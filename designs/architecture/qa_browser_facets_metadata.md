# QA Browser Facet Metadata Design

## Goal

Add first-class facet metadata to Arango collection schema metadata so the QA
browser can render generic facet panels for document lists without inferring
meaning from raw indexes alone.

The design should separate three concerns that are currently conflated:

- fields that should be indexed for query performance
- fields that are meaningful categorical facets
- fields that are meaningful numeric facets

This allows the browser to show user-facing filters for real facets while still
indexing non-facet fields such as `id` and `xref`.

---

## Problem

Today `src/core/decorators.py` exposes:

- `category_fields`
- `numeric_fields`

Those lists are used for more than one purpose:

- `src/output_adapters/arango_output_adapter.py` creates Arango indexes from
  them
- `src/interfaces/data_api_adapter.py` treats `category_fields` as the default
  facet list
- future QA browser facet UI would need to decide which of those fields should
  actually be shown to users

That overload causes two issues:

- some fields are index-worthy but not useful facets, for example `id` and
  `xref`
- the browser cannot distinguish meaningful facets from noisy internal fields
  using current metadata

---

## Design Principles

- Make facet semantics explicit in model annotations instead of relying on UI
  heuristics.
- Persist schema and facet metadata into `metadata_store` so browser behavior is
  driven by graph metadata, not live inference alone.
- Keep backward compatibility for existing model decorators during migration.
- Preserve current Arango indexing behavior unless a model opts into the new
  split metadata.
- Keep QA browser facet behavior aligned with standard faceted-search
  expectations: multi-select values, self-excluding counts, and filters that
  affect the list, count, and sibling facets.

---

## Proposed Decorator Contract

Replace the overloaded two-list model with three explicit groups:

- `extra_indexed_fields`
- `category_fields`
- `numeric_fields`

Meaning:

- `extra_indexed_fields`: create Arango indexes, but do not automatically
  expose in facet UI
- `category_fields`: categorical fields that are meaningful to facet on
- `numeric_fields`: numeric fields that are meaningful to facet on now or later
  via histogram/range UI

Target usage:

```python
@facets(
    extra_indexed_fields=["id", "xref"],
    category_fields=["sources", "type", "status"],
    numeric_fields=["score"],
)
```

### Backward Compatibility

To avoid a flag day:

- existing `@facets(category_fields=[...], numeric_fields=[...])` declarations
  remain valid
- if `extra_indexed_fields` is omitted, the index set defaults to the union of
  `category_fields` and `numeric_fields`
- existing models that currently misuse `category_fields` for indexing can be
  migrated gradually by moving those fields into `extra_indexed_fields`

This keeps current index creation stable while allowing incremental cleanup.

---

## Proposed Metadata Model

Persist facet metadata inside `metadata_store.collection_schemas`.

Current shape is roughly:

```json
{
  "_key": "collection_schemas",
  "collections": {
    "Gene": {
      "type": "document",
      "fields": {
        "symbol": "str"
      }
    }
  }
}
```

Proposed addition:

```json
{
  "_key": "collection_schemas",
  "collections": {
    "Gene": {
      "type": "document",
      "fields": {
        "symbol": "str"
      },
      "facet_metadata": {
        "extra_indexed_fields": ["id", "xref"],
        "category_fields": ["sources", "symbol"],
        "numeric_fields": []
      }
    }
  }
}
```

### Why Persist This

- QA browser can read one metadata document instead of reverse-engineering from
  indexes
- metadata survives independently of runtime class loading in the browser
- future UI work such as numeric histograms can use the same persisted contract
- API adapters can eventually use the same metadata if we want UI and API
  behavior to stay aligned

---

## Decorator / Helper Changes

`src/core/decorators.py` should evolve from storing two bare lists to storing
three explicit lists.

Expected behavior:

- `@facets(...)` stores `_facet_extra_indexed`, `_facet_categories`,
  `_facet_numerics`
- `collect_facets(cls)` returns all three sets
- all sets should include inherited values across the MRO, matching current
  behavior

Recommended return shape:

```python
extra_indexed, categories, numerics = collect_facets(cls)
```

This is a breaking helper signature internally, so call sites must be updated
in the same change.

---

## Arango Output Adapter Changes

`src/output_adapters/arango_output_adapter.py` should use the new metadata in
two places.

### 1. Index Creation

Index creation should operate on:

- all `extra_indexed_fields`
- all `category_fields`
- all `numeric_fields`

Recommended rule:

- hash index for `extra_indexed_fields` and `category_fields`
- persistent index for `numeric_fields`

If a field appears in both `extra_indexed_fields` and `category_fields`, only
one index should be created.

### 2. Persisted Collection Schema Metadata

When writing `collection_schemas`, include:

- `facet_metadata.extra_indexed_fields`
- `facet_metadata.category_fields`
- `facet_metadata.numeric_fields`

This should be merged with existing schema metadata during post-processing in
the same way current schema metadata is merged.

---

## QA Browser Behavior

The generic collection browser at
`src/qa_browser/app.py:/db/{db_name}/collection/{coll_name}` should read facet
definitions from `metadata_store.collection_schemas`.

### Facet Eligibility

For the initial browser facet panel:

- only `facet_metadata.category_fields` are rendered
- `extra_indexed_fields` are never shown as facets unless they also appear in
  `category_fields`
- `numeric_fields` are not yet rendered, but remain available for future UI

### Filtering Semantics

Facet values should be multi-select within a field.

Example:

- `type = ["protein", "gene"]`
- `sources = ["UniProt", "GTEx"]`

Filter logic:

- within a field: OR semantics
- across fields: AND semantics

This matches standard faceted navigation and user expectation.

### Count Semantics

Facet counts should be self-excluding:

- the result list uses the full active filter set
- total count uses the full active filter set
- each facet panel computes counts with all active filters except the current
  field's own selections

This is required for useful multi-select facet UX.

### Null / Missing Values

Initial behavior should mirror current Arango facet query behavior:

- missing or null categorical values may appear as a `null` bucket if present
- UI can decide later whether to label this as `missing` or hide it behind a
  flag

This should be a presentation decision, not a metadata-model decision.

---

## Query Strategy

The QA browser currently queries Arango directly rather than going through the
`ArangoAPIAdapter`.

For this feature, that is acceptable as long as the browser:

- reads persisted facet metadata from `metadata_store`
- builds AQL filters consistently for list, total count, and facet counts

Recommended implementation pattern:

- parse active facet selections from query params
- build one reusable filter-clause helper for collection pages
- use that helper for:
  - paged document query
  - total count query
  - per-facet count queries

The browser should not infer facet fields from current-page columns.

Implemented compatibility behavior:

- when persisted `facet_metadata.category_fields` is missing, the browser may
  infer categorical facets from single-field Arango hash indexes
- the fallback suppresses obvious internal or noisy fields such as `id`,
  `xref`, `_key`, `_from`, and `_to`
- explicit metadata remains the source of truth for new builds

---

## Suggested Query Parameter Shape

Use repeated query parameters keyed by field name.

Examples:

```text
/db/pharos/collection/Gene?facet_type=protein&facet_type=gene
/db/pharos/collection/Gene?facet_sources=UniProt&facet_sources=GTEx
```

Alternative acceptable shape:

```text
/db/pharos/collection/Gene?facet.type=protein&facet.type=gene
```

Recommendation:

- prefer `facet_<field>=value`

Reason:

- simple to parse from FastAPI request query params
- easy to preserve across HTMX pagination links
- no nested query parsing required

---

## Template Behavior

`src/qa_browser/templates/collection.html` should gain a left-side facet rail
similar to the existing demo explorer pages.

Expected behavior:

- one panel per categorical facet field
- each panel lists top values and counts
- active facet values are visibly selected
- a summary panel above the results shows currently applied filters grouped by
  facet
- users can toggle values on and off
- users can remove individual selected values or clear an entire facet from the
  active-filter summary
- pagination preserves active facet params
- HTMX updates should replace the result area and keep browser URL in sync

For initial scope, the browser can reload the full collection page when facet
state changes. HTMX partial optimization is optional.

### Field Coverage Stats

The existing field coverage panel should respect the active facet filters.

Expected behavior:

- total is computed against the filtered subset
- field presence percentages are sampled from the filtered subset
- this makes coverage inspection useful for facet-constrained QA workflows

---

## Migration Strategy

### Phase 1

- add `extra_indexed_fields` support to the decorator and helper
- persist facet metadata in `collection_schemas`
- keep backward compatibility so unchanged models still work

### Phase 2

- update noisy models to move non-facet fields such as `id` and `xref` out of
  `category_fields` and into `extra_indexed_fields`
- leave meaningful fields such as `sources` in `category_fields`

### Phase 3

- implement generic QA browser categorical facet panels from persisted metadata

### Phase 4

- optionally update API adapter default facet logic to prefer persisted or
  explicit `category_fields` semantics only

---

## Open Questions

### Should `sources` remain a categorical facet?

Current recommendation: yes.

Reason:

- it is often meaningful in QA workflows
- it supports provenance-oriented filtering
- it is not high-cardinality in the same way `id` or `xref` are

### Should labels/order be part of the first metadata model?

Current recommendation: no.

Persist only field names for now:

- `extra_indexed_fields`
- `category_fields`
- `numeric_fields`

If we later need field labels, ordering, or bucket controls, we can extend
`facet_metadata` without invalidating the basic model.

### Should the browser fall back to live index introspection?

Current recommendation: only as a temporary compatibility fallback, not as the
primary design.

Primary behavior should use persisted metadata from `metadata_store`.

---

## Acceptance Criteria

The design is complete when the implementation can satisfy all of the
following:

- models can declare `extra_indexed_fields`, `category_fields`, and
  `numeric_fields` separately
- Arango collection schema metadata persists all three groups in
  `metadata_store.collection_schemas`
- non-facet fields such as `id` and `xref` can remain indexed without appearing
  in QA browser facet panels
- QA browser collection pages render one facet panel per categorical field in
  persisted metadata, with index-based fallback for older graphs that do not
  yet have facet metadata
- facet values are multi-select
- active filters affect the document list, total count, and sibling facet
  counts
- each facet's counts are computed excluding that facet's own active selections
