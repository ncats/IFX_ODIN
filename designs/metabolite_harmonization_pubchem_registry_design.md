# PubChem Context Registry Design

## Scope

Register PubChem chemical context for the `metabolite_harmonization` graph without creating one registry entry per compound.

The PubChem context chain is registry-only in this pass. Graph adapters and model changes are deferred.

## Derived Artifact Chain

The registry represents PubChem context as three coarse derived datasets:

1. `pubchem:compound_cid_set`
   - Derived from HMDB metabolite XML, WikiPathways RDF, LipidMaps SDF, and RefMet CSV snapshots.
   - Output: `pubchem_compound_cids.tsv`
   - Columns: `pubchem_id`, `cid`, `reported_by_source`, `reported_by_source_id`, `source_field`
   - Purpose: preserve the set of requested PubChem compound CIDs and why each CID was requested.

2. `pubchem:compound_records`
   - Derived from `pubchem:compound_cid_set`.
   - Output: `pubchem_compound_records_manifest.tsv` plus gzipped batch JSON payload files.
   - Purpose: preserve the expensive PubChem fetch payloads as reusable registry artifacts.
   - Registry granularity stays one dataset snapshot; compound-level status is tracked inside the dataset manifest TSV.

3. `pubchem:cid_molecular_info`
   - Derived from `pubchem:compound_records`.
   - Output: `cid_molecular_info.tsv`
   - Purpose: adapter-facing flattened molecular context.

## Registry Decisions

- Store these under `derived/pubchem/...`, not `sources/pubchem/...`, because the CID set is induced from other inputs and the payload batch is scoped to that induced set.
- Use derived-on-derived dependencies for the payload and flattened TSV stages.
- Keep payloads batched as files within one snapshot, rather than registering individual compounds.
- Preserve more PubChem fields than RaMP currently exports so future context fields can be derived without refetching.

## Deferred

- Adding `MetaboliteChemProps` to graph nodes.
- Adding chemprops/context adapters.

## Implementation Update

The PubChem registry chain has been built and registered for dependency version `deps-31c7bedefb74`.

Final registered counts:

- `pubchem:compound_records:deps-31c7bedefb74`
  - requested CIDs: 169,669
  - PubChem records: 169,666
  - not found: 3
  - errors: 0
- `pubchem:cid_molecular_info:deps-31c7bedefb74`
  - rows: 169,666
  - fields populated for every row: monoisotopic mass, InChIKey, InChIKey prefix, molecular formula, molecular weight, canonical SMILES, isomeric SMILES, InChI
  - IUPAC name populated for 169,527 rows

Graph context adapters now attach chemprops directly to `MetaboliteIdentifier.chem_props`.

Implemented adapter-facing sources:

- `hmdb:structures_sdf:5.0`
- `chebi:three_star_sdf:2026-05-01`
- `lipidmaps:lmsd_sdf:2026-06-30`
- `pubchem:cid_molecular_info:deps-31c7bedefb74`

The ChEBI version is the already registered `chebi:three_star_sdf` snapshot.

## Still Deferred

- Full graph validation after running `src/use_cases/working.py`.
