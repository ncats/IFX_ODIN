# RaMP RDF Export Note

This RDF file is an IFX export derived from the RaMP graph staged in ArangoDB.

## Namespaces

- Resource IRIs: `https://ifx.ncats.nih.gov/resource/...`
- Ontology / predicate IRIs: `https://ifx.ncats.nih.gov/ontology/...`
- Biolink overlay terms: `https://w3id.org/biolink/vocab/...`

## What The File Contains

- RaMP-derived `Metabolite`, `Protein`, `Pathway`, `Reaction`, `ReactionClass`, and `MetaboliteClass` resources
- direct relationship triples such as:
  - `ifx:hasReaction`
  - `ifx:hasPathway`
  - `ifx:hasProtein`
  - `ifx:hasReactionClass`
  - `ifx:hasParentReactionClass`
- additive Biolink overlay triples where the mapping was straightforward

## Labels And Cross-References

- primary names use `rdfs:label`
- synonyms use `skos:altLabel`
- xrefs use `skos:exactMatch`

## Edge Properties

When an edge has additional payload, the export preserves that payload using a statement resource typed with the corresponding IFX edge class, for example `ifx:MetaboliteReactionEdge`.

Those statement resources use:

- `ifx:subject`
- `ifx:predicate`
- `ifx:object`

plus any edge payload fields such as `ifx:substrate_product`.

## Biolink Overlay

The export keeps IFX predicates as the base representation and adds Biolink terms where a clean mapping was available.

Examples:

- `ifx:Protein` is also typed as `biolink:Protein`
- `ifx:Pathway` is also typed as `biolink:Pathway`
- `ifx:MetaboliteClass` is also typed as `biolink:OntologyClass`
- metabolite to reaction edges also emit `biolink:participates_in`
- protein to reaction edges also emit `biolink:enabled_by` in the reaction-to-protein direction

## RaMP IDs

Instance IRIs may use RaMP identifiers such as `RAMP_C_*`, `RAMP_P_*`, `RAMP_R_*`, and `RAMP_RC_*` because RaMP is the source graph being exported.
