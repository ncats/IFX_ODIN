# Generic ODIN Curation Design

## Decision

ODIN curations are reusable, typed domain streams stored independently of any
one graph. Graphs and applications select curation types; they do not require a
curator or graph author to enumerate historical batches.

The first supported target is
`metabolite_record_properties / MetaboliteIdentifier`. Its first common
classification field is `is_generic_structure`, while the same typed path
contract and document editor handle the other supported fields on the record.
Record-property streams are model-family specific so graph builders can select
metabolite properties independently of future protein or drug properties.

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
handlers. `operation_contract: record_properties` supplies common validation,
resolution, cart, projection, and UI behavior without creating one global
property stream. Framework identity, provenance, source-locator, endpoint,
private, collection, and calculated fields are never editable. Declared scalar
fields and declared leaves inside stably selectable source records are editable;
unsupported or ambiguous shapes fail closed.

Initial types are:

- `metabolite_equivalence_edges`: `remove_edge`, `retain_edge`;
- `metabolite_record_properties`: typed `set_properties` decisions for
  `MetaboliteIdentifier`;
- `metabolite_record_suppressions`: `suppress_record`, `restore_record`;
- `metabolite_expected_cliques`: `assert_same_clique`, `retire_assertion`.

A property operation uses a stable business identifier and may carry several
independent property decisions:

```json
{
  "action": "set_properties",
  "target": {
    "kind": "node",
    "curation_set": "metabolite_harmonization",
    "model_type": "MetaboliteIdentifier",
    "id": "KEGG.COMPOUND:C01234"
  },
  "decisions": [
    {
      "path": ["is_generic_structure"],
      "mode": "set",
      "value": true,
      "observed_exists": false,
      "observed_value": null
    }
  ],
  "note": "Reviewed the source structure evidence."
}
```

Each decision is `set` or `remove_override`. A `set` value may be JSON `null`
when the declared field allows it. Its observed value and presence form a
stale-data guard. `remove_override` restores the loaded graph value. Resolution
is independent per curation set, model, target, and canonical path, so a later
decision for one field does not disturb other fields; history remains immutable.

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
    - metabolite_record_properties
  credentials: ./src/use_cases/secrets/aws_ifx_registry.yaml
  allow_missing: true  # useful before the first batch is published
```

ODIN resolves all manifests before graph writes. One YAML may contain both
normal and graph-dependent adapter phases:

```yaml
input_adapters:
  # primary source adapters

post_adapters:
  # adapters that read the materialized graph and emit normal partial records
```

The fixed lifecycle is output pre-processing, `input_adapters`, source-field
curations, `post_adapters`, the same source-field curations again, then output
post-processing. Applying twice lets post adapters consume curated source values
while ensuring a post adapter cannot accidentally replace the published
effective value. Post adapters
use the normal adapter/output merge contract; they are not a second build and
do not bypass provenance or field-conflict policy. Missing curation targets and
invalid operations fail a normal build; curations never create skeletal
targets. Resume is rejected while curations are configured because removing or
changing a property decision can require a clean rebuild to restore the newly
derived baseline safely.

Metabolite harmonization uses the same snapshots through an orderable
**Apply curations** rule. The rule includes typed
`metabolite_record_properties` corrections as well as metabolite equivalence
edge and record-suppression streams. It is recommended as the first rule for RaMP pipeline
comparisons so later rules share one curated starting state, but the framework
does not require it to be first. Rule order is literal: curated field values
affect generic-structure pruning, InChIKey merging, and molecular-weight
validation only after **Apply curations**. Workbench stages are immutable
simulations and do not mutate the source evidence graph.

`ramp.yaml` intentionally persists the post-adapter's derived baseline but does
not apply curation manifests physically. The metabolite harmonization database
is an evidence graph: source assertions and calculated baselines remain intact,
while the selected immutable curation snapshots form a stage-local effective
record view. Doing both would replace evidence fields before the orderable
**Apply curations** stage could show its before/after effect. A future curated
RaMP export must consume the final stage's effective record view rather than
requiring mutation of the evidence graph.

The effective curated value is stored in the ordinary field. Its loaded value
is retained in the sibling `_curation_original` object at the same nesting
level, and `updates` plus a `Manual Curation` source identify the published
batch. This keeps ordinary graph queries simple while retaining enough baseline
state to explain or restore the correction on a clean build.

### Follow-up: migrate Pharos post builds

Pharos currently models graph-dependent TDL calculation with a separate base
YAML and post-YAML build. That makes ordering an entrypoint convention rather
than a property of one declarative build. Migrate those TDL adapters into the
base configuration's `post_adapters` section after validating output and resume
behavior. This feature does not change the current Pharos production configs.

## QA workflow

The generic document page renders the canonical editor from property schema. Each
editable property shows these states independently:

- source/graph value;
- published override, including an explicit no-value override;
- effective value;
- pending private decision, clearly marked not applied.

The actions are **No change**, **Set value**, **Set no value** when nullable,
and **Restore graph value** when an override exists. Input controls follow the
declared scalar type. All changes for one target are added as one review item.
The metabolite harmonizer reuses the same cart and backend, but provides a
compact **Change classification** control beside generic-structure evidence.
Its labels are **Generic**, **Specific / non-generic**, **Unknown /
unclassified**, and **Use evidence graph value**. It creates exactly the same
typed record-property operation as the generic document editor. Draft changes
are grouped by curation type in **Review changes**. Publishing makes them
active; existing materialized stages still require synchronization.

## Migration

On 2026-09-29, the pre-release `metabolite_annotations` and global
`record_properties` histories were migrated into
`metabolite_record_properties`. Twelve replacement batches preserve source
batch/object/hash and operation provenance. The migration converted 90
operations and verified that the replacement resolves to the same 88 active
field decisions (86 generic-structure targets plus the existing RefMet record
corrections). The destination batches were written and read back before its
manifest was activated. Both legacy streams remain immutable for audit and a
later explicit cleanup; runtime code has no aliases or dual reads.
