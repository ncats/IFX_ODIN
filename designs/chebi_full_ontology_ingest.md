# ChEBI FULL Ontology Ingest

## Source

- Registry snapshot: `chebi:ontology_full:252`
- Registry artifact: `s3://ifx-registry/sources/chebi/ontology_full/252/chebi.obo.gz`
- Upstream file: `https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo.gz`
- Upstream metadata: `https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/README`
- Version strategy: use the ChEBI README release as `version`, the README
  update date as `version_date`, and validate that the downloaded OBO header
  `data-version` matches the README release.

Current snapshot:

- `version`: `252`
- `version_date`: `2026-05-01`
- `download_date`: `2026-06-24`

## Discovery

The ChEBI FULL OBO payload contains:

- `205,593` terms
- `285,589` `is_a` relationships
- `94,902` typed `relationship` rows
- `507,982` synonyms
- `389,458` xrefs
- `1,352,583` property values

Observed OBO term tags include `id`, `name`, `def`, `subset`, `synonym`,
`xref`, `alt_id`, `is_a`, `relationship`, `property_value`, and obsolete term
markers.

The source uses CHEBI CURIEs directly (`CHEBI:...`). Chemical properties are
represented as `chemrof:*` property values, including charge, formula, mass,
monoisotopic mass, SMILES, InChI, InChIKey, and WURCS.

## Final Model

The ingest intentionally represents ChEBI as ChEBI. It does not map ChEBI terms
into existing IFX `Metabolite`, `Ligand`, or generic ontology classes.

Output node collections:

- `ChemicalEntity`
- `Application`
- `BiologicalRole`
- `ChemicalRole`
- `Role`

`Term` remains the shared source annotation base class in code, but it is not
used as a normal ChEBI output collection. During review, the remaining generic
`Term` records were either chemical-ish concepts, such as particles, groups,
residues, and family terms, or empty alt-id-only stubs. The final behavior is:

- Named non-role fallback terms emit as `ChemicalEntity`.
- Empty unnamed alt-id-only stubs are skipped.
- The ChEBI role root and uncategorized role terms emit as `Role`.

This leaves the graph with a broad `ChemicalEntity` bucket rather than trying
to draw hard lines between concrete chemicals, residues, groups, particles, and
chemical classes. That matches the practical shape of ChEBI better than a
`Chemical` versus `SubstituentGroup` split.

## ChemicalEntity Classification

Terms emit as `ChemicalEntity` when they are not role/application terms and any
of the following are true:

- The term descends from ChEBI `atom` (`CHEBI:33250`), `molecular entity`
  (`CHEBI:23367`), or `chemical substance` (`CHEBI:59999`).
- The term has source structure properties:
  `chemrof:inchi_key_string`, `chemrof:inchi_string`, or
  `chemrof:smiles_string`.
- The term is connected to a chemical entity by chemical relationship
  predicates: conjugate acid/base, enantiomer, tautomer, parent hydride,
  functional parent, substituent-group-from, or `has part`.
- The term is an `is_a` parent of an already-classified chemical entity, unless
  the parent is a role/application term.
- The term is a named non-role fallback term.

`is_a` promotion is intentionally directional. A chemical entity can promote
its parent class to `ChemicalEntity`, but a chemical-ish parent does not promote
all of its children. This avoids pulling broad abstract subtrees into the
chemical bucket through a single shared parent.

## Role Classification

ChEBI stores roles as ordinary terms in the OBO file. A source chemical has
role assertions via:

- `RO:0000087` = `has role`

The role target is classified by walking its `is_a` ancestry. The roots are:

- `CHEBI:33232` = `application`
- `CHEBI:24432` = `biological role`
- `CHEBI:51086` = `chemical role`
- `CHEBI:50906` = `role`

The adapter emits:

- `Application` for descendants of `CHEBI:33232`
- `BiologicalRole` for descendants of `CHEBI:24432`
- `ChemicalRole` for descendants of `CHEBI:51086`
- `Role` for the role root or role targets that do not land in one of the
  known role categories

Some role terms have multiple category ancestors. In that case the adapter
emits the same CHEBI ID into each applicable role collection and emits one
corresponding edge collection per category. This matches the way the ChEBI
website separates Chemical Roles, Biological Roles, and Applications.

Example for caffeine (`CHEBI:27732`):

```obo
relationship: RO:0000087 CHEBI:35337 ! has role central nervous system stimulant
relationship: RO:0000087 CHEBI:64047 ! has role food additive
relationship: RO:0000087 CHEBI:85234 ! has role human blood serum metabolite
```

Each target term is bucketed by its role ancestry, not by the English label.

## Edge Collections

The graph preserves ChEBI OBO predicates as source-faithful edge collections:

- `is_a` -> `IsAEdge`
- `RO:0000087` -> categorized role edges:
  `HasApplicationEdge`, `HasBiologicalRoleEdge`, `HasChemicalRoleEdge`, or
  fallback `HasRoleEdge`
- `RO:0018038` -> `HasFunctionalParentEdge`
- `RO:0018033` -> `IsConjugateBaseOfEdge`
- `RO:0018034` -> `IsConjugateAcidOfEdge`
- `BFO:0000051` -> `HasPartEdge`
- `RO:0018039` -> `IsEnantiomerOfEdge`
- `RO:0018036` -> `IsTautomerOfEdge`
- `RO:0018040` -> `HasParentHydrideEdge`
- `RO:0018037` -> `IsSubstituentGroupFromEdge`

`IsSubstituentGroupFromEdge` is retained as the source relationship between
chemical entities. We no longer create a separate `SubstituentGroup` node type.

Conjugate acid/base, enantiomer, and tautomer edges are preserved as directed
source assertions. They are useful for finding connected chemical-form
components, but the ingest does not invent missing reciprocals or collapse the
forms.

## Node Fields And QA Browser Metadata

Source annotations are preserved on concrete node collections:

- `definition` and `definition_references`
- `subsets`
- `alt_ids`
- `synonyms`
- `xrefs`
- `properties`
- `is_obsolete`

Convenience fields copied from source annotations:

- `synonym_text`
- `xref_text`
- `charge`
- `formula`
- `mass`
- `monoisotopic_mass`
- `smiles`
- `inchi`
- `inchi_key`
- `wurcs`

These fields do not perform identifier reconciliation. Search metadata is
limited to `id`, `name`, and `inchi_key`. Facets are limited to `subsets` and
`is_obsolete` plus inherited source metadata. High-cardinality fields such as
`alt_ids`, `xref_text`, `inchi_key`, and `charge` are intentionally not facets.

QA Browser schema display was updated to show observed endpoint pairs with
counts instead of the cartesian product of Arango's unioned edge-definition
`from` and `to` collection sets. This avoids misleading schema edges such as an
edge collection appearing to connect collection pairs that have no stored edge
rows.

## Use-Case Configuration

The promoted ChEBI use case is:

- YAML: `src/use_cases/chebi.yaml`
- build script: `src/use_cases/build_chebi.py`
- database: `chebi`
- adapter: `FullOboAdapter`
- data source: `chebi:ontology_full:252`
- resolvers: none

Run the full ChEBI ETL with:

```bash
uv run python src/use_cases/build_chebi.py --yes
```

Use `--resume` to resume a prior build without truncating the datastore.

## Validation

Focused tests:

```bash
uv run pytest tests/test_chebi_obo_adapter.py -q
```

The test fixture validates:

- registry `MaterializedDataset` version propagation
- term annotation parsing
- synonym/xref/property parsing
- QA Browser search and facet metadata
- semantic ChEBI edge classes
- categorized role nodes and role edges
- broad `ChemicalEntity` classification
- suppression of empty unnamed alt-id-only stubs

Full adapter parse after final classification emitted:

- `ChemicalEntity`: `203,657`
- `Application`: `504`
- `BiologicalRole`: `1,271`
- `ChemicalRole`: `128`
- `Role`: `1`

No normal `Term` collection is expected in the ChEBI graph.
