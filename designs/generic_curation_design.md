# Generic ODIN Curation Design

## Decision

ODIN curations are reusable, typed domain streams stored independently of any
one graph. Graphs and applications select curation types; they do not require a
curator or graph author to enumerate historical batches.

The first supported target is `metabolite_annotations / MetaboliteIdentifier`.
Its first editable property is `is_generic_structure`, but the contract and UI
handle every supported scalar field on the target together.

## Storage and reproducibility

Each curation type has mutable private drafts, immutable published batches, and
an authoritative ordered manifest:

```
curation-drafts/v2/{curation_type}/{curator_hash}.json
curations/v2/{curation_type}/batches/{curation_batch_id}.json
curations/v2/{curation_type}/manifest.json
```

The manifest records an increasing revision and the ordered batch IDs, object
keys, hashes, and publication times. A consumer freezes the manifest and
verified batches before changing a graph. Build and harmonization metadata
record the revision, manifest hash, batch IDs and hashes, and resolved-operation
fingerprint.

One UI publication may contain several curation types, but it publishes one
independently auditable batch per type.

## Type and operation contracts

Types are registered in code with reviewed target models, actions, and semantic
handlers. Editable scalar properties are derived from the target dataclass and
filtered through framework and model-specific denylists. Framework identity,
provenance, source, endpoint, private, collection, and nested fields are never
editable. The initial editors support nullable or required booleans, strings,
integers, and finite numbers; unsupported shapes fail closed.

Initial types are:

- `metabolite_equivalence_edges`: `remove_edge`, `retain_edge`;
- `metabolite_annotations`: `set_properties` (with legacy read support for
  `set_property` and `unset_property`);
- `metabolite_expected_cliques`: `assert_same_clique`, `retire_assertion`.

A property operation uses a stable business identifier and may carry several
independent property decisions:

```json
{
  "action": "set_properties",
  "target": {
    "kind": "node",
    "model_type": "MetaboliteIdentifier",
    "id": "KEGG.COMPOUND:C01234"
  },
  "values": {
    "is_generic_structure": true,
    "some_nullable_string": null
  },
  "remove_overrides": ["some_number"]
}
```

Omitting a property means no change. A property in `values` with JSON `null`
explicitly overrides it with no value. A property in `remove_overrides`
removes the curation overlay and restores the source/graph value on a clean
build. These sets must be disjoint and nonempty as a whole. Resolution is
independent per type, target, and property, so a later decision for one field
does not disturb other fields; history remains immutable.

Existing published `set_property` and `unset_property` batches are normalized
into the same per-property resolution model. New publications use only
`set_properties`.

## Generic-structure semantics

`MetaboliteIdentifier.is_generic_structure` is a nullable ordinary model field:

- `true`: explicitly generic;
- `false`: explicitly non-generic;
- JSON `null`: an explicit curated no-value decision;
- removed override: restore the graph-derived value on a clean build.

A graph-dependent post adapter calculates and persists the baseline value after
all structure-bearing input adapters have merged. The curation phase then
updates that same field using the generic property-curation contract. Structure
evidence remains available for explanation, but there is no separate
generic-structure override field or special persistence path.

The classifier is tri-state: generic, specific, or unknown. A clique containing
generic and specific identifiers is a validation failure. A clique containing
generic and unknown identifiers is a classification gap. Specific and unknown
alone is not a generic-consistency failure.

## Application

Normal graph YAML selects types:

```yaml
curations:
  types:
    - metabolite_annotations
  credentials: ./src/use_cases/secrets/aws_ifx_registry.yaml
```

ODIN resolves all manifests before graph writes. One YAML may contain both
normal and graph-dependent adapter phases:

```yaml
input_adapters:
  # primary source adapters

post_adapters:
  # adapters that read the materialized graph and emit normal partial records
```

The fixed lifecycle is output pre-processing, `input_adapters`,
`post_adapters`, frozen curations, then output post-processing. Post adapters
use the normal adapter/output merge contract; they are not a second build and
do not bypass provenance or field-conflict policy. Missing curation targets and
invalid operations fail a normal build; curations never create skeletal
targets. Resume is rejected while curations are configured because removing or
changing a property decision can require a clean rebuild to restore the newly
derived baseline safely.

Metabolite harmonization uses the same snapshots through an orderable
**Apply curations** rule. It is recommended as the first rule for RaMP pipeline
comparisons so later rules share one curated starting state, but the framework
does not require it to be first. Rule order is literal: curated field values
affect a generic-structure rule only when **Apply curations** precedes that
rule. Workbench stages are immutable simulations and do not mutate the source
evidence graph.

During pre-release workbench validation, `ramp.yaml` intentionally persists the
post-adapter's derived baseline but does not also apply the curation manifests
physically. Doing both would delete curated edges and replace annotation fields
before the orderable **Apply curations** stage could show its before/after
effect. Once that experiment is retired, enabling graph-build curations is a
separate cutover decision, not an additive duplicate of the stage rule.

### Follow-up: effective curated records

The graph build now applies generic property curations to records after post
adapters. The harmonization workbench still simulates selected, frozen
curations as an orderable stage rule. Do not assume that adding another
editable field automatically makes every harmonization rule consume its staged
value. Supporting fields such as
SMILES, formula, mass, or nested chemical properties requires a shared
effective-record layer that applies all active property decisions before any
downstream rule reads the record. That layer must preserve the source value,
effective value, curation provenance, explicit null semantics, and restoration
behavior. Until it exists, each newly curatable field needs an explicit
consumer audit and should fail closed rather than appear supported while rules
continue using the uncurated graph value.

### Follow-up: migrate Pharos post builds

Pharos currently models graph-dependent TDL calculation with a separate base
YAML and post-YAML build. That makes ordering an entrypoint convention rather
than a property of one declarative build. Migrate those TDL adapters into the
base configuration's `post_adapters` section after validating output and resume
behavior. This feature does not change the current Pharos production configs.

## QA workflow

The shared selected-node details render an editor from property schema. Each
editable property shows these states independently:

- source/graph value;
- published override, including an explicit no-value override;
- effective value;
- pending private decision, clearly marked not applied.

The actions are **No change**, **Set value**, **Set no value** when nullable,
and **Restore graph value** when an override exists. Input controls follow the
declared scalar type. All changes for one target are added as one review item.
Generic-structure detection and its evidence remain a separate read-only
section. Draft changes are grouped by curation type in **Review changes**.
Publishing makes them active; existing materialized stages still require
synchronization.

## Migration

The v1 graph-scoped metabolite objects are migrated once into the three v2
types. Legacy batch and operation IDs are retained in provenance, and resolved
v1 and v2 state must compare equal before cutover. V1 objects remain immutable
for audit, but production consumers do not indefinitely dual-read both formats.
