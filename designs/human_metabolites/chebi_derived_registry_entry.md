# ChEBI Endogenous Human Metabolites Derived Artifact

## Goal

Register a derived ChEBI artifact for the human metabolites workflow. The
artifact is built only from the registry source snapshot `chebi:ontology_full`
and does not change graph builds, input adapters, models, resolvers, or ETL
configuration.

## Registry Entry

Dataset:

- `chebi:endogenous_human_metabolites`

Dependency:

- `chebi:ontology_full:<version>`

Configured builder:

```yaml
sources:
  chebi:
    datasets:
      endogenous_human_metabolites:
        derived:
          module: src.registry.derived.chebi
          class: ChebiEndogenousHumanMetabolitesBuilder
          dependencies:
            - source: chebi
              dataset: ontology_full
          output:
            file_name: chebi_endogenous_human_metabolites.tsv
            full_file_name: chebi_full.tsv
            data_dictionary_file_name: chebi_endogenous_human_metabolites_data_dictionary.tsv
          transform:
            name: chebi_endogenous_human_metabolites
            version: 1
            code_ref: src/registry/derived/chebi.py
```

## Outputs

- `chebi_endogenous_human_metabolites.tsv`: filtered first-pass endogenous
  human metabolite list.
- `chebi_full.tsv`: full ChEBI full table with one row per ChEBI term.
- `chebi_endogenous_human_metabolites_data_dictionary.tsv`: column definitions
  shared by both data files.

The filtered file and full ChEBI file have the same columns. The filtered file
contains rows where:

- `is_chemical_entity_descendant=true`
- `is_obsolete=false`
- `has_drug_xref=false`
- `is_exogenous_descendant=false`
- `has_forbidden_label_text=false`

## Evidence Columns

- `chebi_id`, `name`, `definition`, `synonyms`, `xrefs`
- `formula`, `monoisotopic_mass`, `smiles`, `inchi`, `inchi_key`
- `is_chemical_entity_descendant`
- `has_human_metabolite_role`
- `is_pharmaceutical_descendant`
- `is_obsolete`
- `has_drug_xref`, `drug_xrefs`
- `is_exogenous_descendant`, `exogenous_ancestor_ids`
- `has_forbidden_label_text`, `forbidden_label_terms`

## Transform Rules

The builder parses `chebi.obo.gz` directly and does not call OLS, RaMP, or any
external service.

Evidence flags:

- `is_chemical_entity_descendant`: term is `CHEBI:24431` or descends from it.
- `has_human_metabolite_role`: term has a direct `RO:0000087` role target of
  `CHEBI:77746` or a descendant of it.
- `is_pharmaceutical_descendant`: term or direct role target descends from
  `CHEBI:52217`.
- `is_obsolete`: term has `is_obsolete: true`.
- `has_drug_xref`: term has an xref with prefix `drugbank`, `drugcentral`,
  `rxnorm`, `dailymed`, `ttd`, or `pharmgkb`.
- `is_exogenous_descendant`: term or direct role target is or descends from one
  of the historical R-script exclusion roots:
  `CHEBI:35703`, `CHEBI:23888`, `CHEBI:35610`, or `CHEBI:33281`.
- `has_forbidden_label_text`: name, definition, or synonym text contains one
  of the historical R-script forbidden terms.

`CHEBI:35610` is labeled `antineoplastic agent` in ChEBI 252, although the old
R comment called it pharmaceutical. The broader ChEBI pharmaceutical root is
`CHEBI:52217`.

## Validation

Focused tests:

```bash
uv run pytest tests/test_chebi_endogenous_human_metabolites.py -q
```

Local build only, no upload:

```bash
uv run python -c "from src.core.data_registry import DataRegistry; registry = DataRegistry.from_registry_credentials('src/use_cases/secrets/aws_ifx_registry.yaml'); registry.build_derived_artifact('chebi', 'endogenous_human_metabolites', dest='/private/tmp/ifx-registry-cache')"
```
