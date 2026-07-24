# Metabolite Harmonization Graph Design

## Purpose

The `metabolite_harmonization` graph is the evidence layer for the next
generation of RaMP metabolite harmonization.

The graph should preserve what source systems say about metabolite identifiers,
names, chemistry, pathways, proteins, and ontology context. It should not decide
the final harmonized metabolite groups during ingest. Those decisions belong in
explicit harmonization rules and saved snapshots that can be inspected in the
Harmonization Workbench.

## High-Level Model

The central node is `MetaboliteIdentifier`. Examples include `HMDB:...`,
`CHEBI:...`, `PUBCHEM.COMPOUND:...`, `LIPIDMAPS:...`, `REFMET:...`,
`InChIKey:...`, and other source-provided IDs.

Important supporting nodes include:

- `PathwayIdentifier` for source pathway IDs.
- `ProteinIdentifier` and `GeneIdentifier` for source pathway/protein context.
- Rhea-specific reaction nodes and EC reaction class nodes.
- ChEBI ontology nodes such as `ChemicalEntity` and role nodes.
- `MetaboliteHarmonizationClique*` nodes created by saved workbench snapshots.

Important edges include:

- source-reported metabolite ID equivalence edges;
- metabolite/protein/gene pathway membership edges;
- HMDB metabolite-protein association edges;
- Rhea metabolite-reaction, protein-reaction, reaction-class, reaction-direction,
  and EC class-parent edges;
- Reactome pathway hierarchy edges;
- ChEBI ontology edges;
- ChEBI `ChemicalEntity` to `MetaboliteIdentifier` bridge edges;
- snapshot clique membership edges.

## What We Keep

Adapters should be source-preserving by default. If a source provides an
identifier or association and we can normalize it into a stable CURIE-like ID,
we generally keep it.

Current retained evidence includes:

- metabolite identifier mappings from HMDB, WikiPathways, LipidMaps, and RefMet;
- source-attributed metabolite names and synonyms;
- chemical properties from HMDB, ChEBI, LipidMaps, and PubChem-derived files;
- pathway context from HMDB/SMPDB, WikiPathways, Reactome, and PFOCR;
- HMDB metabolite-protein associations;
- active Rhea reactions, using Rhea's native reaction/direction/participant
  model;
- Rhea participant IDs as `MetaboliteIdentifier` nodes, including ChEBI-backed
  participants, Rhea generic compounds, and Rhea polymer compounds;
- human-filtered Rhea UniProt protein mappings;
- ExPASy enzyme classes used as Rhea reaction class metadata;
- Reactome pathway hierarchy;
- the ChEBI full ontology graph;
- bridge edges linking matching ChEBI `ChemicalEntity` and
  `MetaboliteIdentifier` nodes;
- saved harmonization snapshots and their clique memberships.

Names, synonyms, and chemical properties are stored on the relevant identifier
node as source-attributed nested data. We do not copy a name from one identifier
to another just because a source reports an equivalence.

InChIKey can appear in two ways:

- as source-reported identifier evidence, for example `InChIKey:...`;
- as chemical-property evidence inside `chem_props`.

Rules must be explicit about which InChIKey evidence they use.

Rhea participant names, HTML names, and formula strings are stored as
reaction-participant edge context. They are not stored as `chem_props`, because
Rhea does not provide structure-derived SMILES, InChI, or InChIKey values in the
reaction bundle.

## What We Skip

The graph is broad, but it is not “everything from everywhere.”

Current intentional skips:

- non-human WikiPathways RDF pathway files;
- non-human PFOCR pathway files;
- pathway sources that are not part of this RaMP-focused graph yet, such as the
  current Pathway Commons usage;
- cross-identifier reconciliation inside adapters;
- final metabolite granularity decisions during ingest;
- implicit RaMP legacy prefix allowlists or denylists inside adapters.
- Rhea metabolite equivalence edges. Rhea generic and polymer accessions become
  `MetaboliteIdentifier` nodes and participate in reactions, but Rhea does not
  make them equivalent to ChEBI or other metabolite IDs.

RefMet-only orphan handling is not an ingest concern. If we need a RaMP-like
rule that ignores RefMet-only results, it should be modeled as an explicit
harmonization/export rule.

Rhea `has_human_protein`, `only_human_metabolites`, and `is_cofactor` style
flags are treated as derived annotations rather than source fields for the
first-pass ingest.

## Harmonization Rules

The graph stores evidence. Harmonization rules decide how to group IDs.

The workbench can create saved snapshots from rule pipelines such as:

- use all source-reported equivalence edges;
- apply the RaMP mapping deny list;
- ignore generic/non-generic structure equivalences;
- merge by InChIKey prefix;
- merge by InChIKey first two components;
- switch between InChIKey strategies by molecular-weight cutoff.

Rules should be independently configurable, ordered, and inspectable. A saved
snapshot should record which rules ran, their order, parameters, and summary
statistics.

## Known Granularity Problems

The graph intentionally does not solve metabolite granularity at ingest time.
These are rule and curation questions:

- generic structures with R groups;
- generic IDs linked to specific structures;
- salts and parent compounds;
- open-chain and cyclic carbohydrate forms;
- stereochemistry that matters in some contexts and not others;
- lipids where analytical and biological granularity differ;
- source cliques that share InChIKey evidence but should not always merge.

The goal is to keep enough evidence for experts to inspect these cases, compare
rules, and decide which behavior should become RaMP release behavior.

## Current Configuration

The promoted RaMP metabolite harmonization graph lives in:

- `src/use_cases/ramp/ramp.yaml`
- `src/use_cases/ramp/build_ramp.py`

`src/use_cases/working.yaml` remains a scratch configuration for experiments
before changes are promoted.

For structural model changes, prefer a full truncate/rebuild. Use `--resume`
only when the graph shape has not changed.

## Deferred Decisions

- How to export a backward-compatible RaMP SQLite database from this graph.
- Which rule profile becomes the released RaMP harmonization policy.
- How expert curation decisions should be stored and replayed.
- Whether additional class systems from HMDB, LipidMaps, RefMet, or ChEBI should
  become first-class graph context.
- Whether broader pathway sources belong in this graph later.
