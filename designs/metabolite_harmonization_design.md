# Metabolite Harmonization Graph Design

## Purpose

The `metabolite_harmonization` graph is a source-ID-first backing graph for a
future metabolite resolver and for a cleaner RaMP-DB rebuild path.

The graph intentionally does not resolve metabolite identifiers as they enter.
Instead, it preserves what each source says about identifiers, names, chemistry,
proteins, and pathways. Harmonized metabolite concepts can be created later from
this evidence graph, with room for automated rules and human curation.

## Current Scope

The first implementation covers four kinds of context:

- metabolite identifier equivalencies
- source-attributed names and synonyms
- chemical properties
- pathway/protein/gene context linked to source identifiers

The current graph is not yet the final harmonized-metabolite layer. Source ID
nodes such as `HMDB:...`, `CHEBI:...`, `LIPIDMAPS:...`, and
`PUBCHEM.COMPOUND:...` are the evidence layer. Later work can add coarser
`HarmonizedMetabolite` nodes once the granularity rules are better understood.

## Design Principles

- Keep adapters source-native and avoid resolver behavior.
- Preserve source assertions even when some are likely wrong.
- Store source-specific edge evidence in `details` lists so multiple sources can
  assert the same edge without overwriting each other.
- Attach names, synonyms, and chemical properties to identifier nodes as
  source-attributed nested objects, not as separate graph nodes.
- Use shared source-ID context node types for pathways, proteins, and genes where
  possible.
- Start broad enough to preserve the data needed for RaMP compatibility, then
  refine harmonization rules after the graph contains enough context.

## Core Graph Model

Main node types:

- `MetaboliteIdentifier`: source metabolite identifier node with optional
  source-attributed names, synonyms, prefix, and chemical properties.
- `ProteinIdentifier`: source protein identifier node, currently used for HMDB,
  Reactome, and WikiPathways protein context.
- `GeneIdentifier`: source gene identifier node, currently used for
  WikiPathways and PFOCR pathway context.
- `PathwayIdentifier`: source pathway identifier node for HMDB/SMPDB, Reactome,
  WikiPathways, and PFOCR.

Main edge types:

- `MetaboliteIdentifierMappingEdge`: source-reported metabolite ID equivalency.
- `HmdbMetaboliteProteinAssociationEdge`: HMDB metabolite-protein association.
- `MetabolitePathwayEdge`: metabolite identifier to pathway.
- `ProteinPathwayEdge`: protein identifier to pathway.
- `GenePathwayEdge`: gene identifier to pathway.
- `ReactomePathwayParentEdge`: Reactome pathway hierarchy.

## Source Inventory

| Source | Registered dataset | Main graph contribution |
| --- | --- | --- |
| HMDB metabolites | `hmdb:metabolites_xml:5.0` | metabolite IDs, names, synonyms, HMDB pathway context |
| HMDB proteins | `hmdb:proteins_xml:5.0` | protein nodes, metabolite-protein links, protein-pathway links |
| WikiPathways | `wikipathways:rdf_wp:2026-06-10` | metabolite ID mappings, human pathway memberships |
| LipidMaps | `lipidmaps:lmsd_sdf:2026-06-30` | metabolite ID mappings, names, synonyms, chemprops |
| RefMet | `refmet:metabolites_csv:sha256-f40f14165725` | metabolite ID mappings and names |
| Reactome | `reactome:pathways:97` | human metabolite/protein pathway memberships and pathway hierarchy |
| PFOCR | `pfocr:human_pathways:2025-07-01` | human figure-level metabolite/gene pathway memberships |
| HMDB structures | `hmdb:structures_sdf:5.0` | chemprops |
| ChEBI structures | `chebi:three_star_sdf:2026-05-01` | chemprops |
| ChEBI ontology | `chebi.obo.gz` registry source TBD | ChEBI classes, ontology relationships, and roles |
| PubChem derived context | `pubchem:cid_molecular_info:deps-31c7bedefb74` | chemprops derived from fetched PubChem payloads |

## RaMP Coverage Checklist

This checklist tracks the current working goal: capture the source-native data
needed to rebuild RaMP from the graph before deciding final harmonization and
SQLite export rules.

Done:

- Metabolite identifiers and source-reported equivalencies from HMDB,
  WikiPathways, LipidMaps, and RefMet.
- Metabolite names and synonyms from the same ID/name sources where the source
  provides them.
- Production chemical properties from HMDB structures, ChEBI structures,
  LipidMaps, and derived PubChem molecular info.
- HMDB protein nodes and HMDB metabolite-protein associations.
- Pathway nodes and metabolite/protein/gene pathway links from HMDB/SMPDB,
  Reactome, WikiPathways, and PFOCR.
- Reactome pathway hierarchy.
- Registry coverage for the major current inputs and the expensive PubChem
  derived artifacts.

Left:

- Rhea reaction context.
- HMDB ontology/classification context from the metabolite XML.
- Chemical class and ontology context:
  - LipidMaps classes from the LipidMaps SDF.
  - ChEBI ontology classes, relationships, and roles from `chebi.obo.gz`.
- A final audit against RaMP build code and schema to catch smaller remaining
  sources or fields not covered by the current graph.
- Graph-to-RaMP SQLite export mapping.
- Harmonized metabolite aggregate nodes and curation workflow.

Likely not needed in this graph yet:

- Pathway Commons, because it is Pharos/TCRD-oriented in the current IFX_ODIN
  registry usage and is not part of the RaMP source set we are matching now.
- The old `curation_mapping_issues_list.txt` flow, because curation should be
  redesigned against this evidence graph rather than ported directly.

## Identifier Equivalencies

The graph captures source-reported mappings from HMDB, WikiPathways, LipidMaps,
and RefMet. These adapters emit one node per reported ID and one
`MetaboliteIdentifierMappingEdge` per source assertion.

Representative identifier families include:

- HMDB
- ChEBI
- KEGG compound
- PubChem compound
- ChemSpider
- CAS
- DrugBank
- FoodDB
- BioCyc
- BiGG
- METLIN
- LipidMaps
- SwissLipids
- LipidBank
- PlantFA
- RefMet
- InChIKey
- Wikidata

Mappings are directional in graph storage because Arango edges are directional,
but semantically these are equivalency assertions. Downstream resolver logic
should treat them as evidence, not as final truth.

## Names And Synonyms

Names and synonyms are nested source-attributed objects on
`MetaboliteIdentifier` nodes. They are not separate nodes.

Current sources:

- HMDB: primary HMDB accession gets HMDB name and synonyms.
- WikiPathways: source RDF metabolite subject gets `rdfs:label`.
- LipidMaps: source LipidMaps ID gets name, systematic name, abbreviation, and
  synonyms.
- RefMet: source RefMet ID gets RefMet name.

Names are only attached to the identifier the source directly names. The adapter
does not copy an HMDB name onto a ChEBI or PubChem node just because HMDB reports
that xref.

## Chemical Properties

Chemical properties are stored as `MetaboliteChemProps` objects on
`MetaboliteIdentifier.chem_props`.

Current chemprop sources:

- HMDB structures SDF
- ChEBI 3-star SDF
- LipidMaps SDF
- PubChem derived molecular info TSV

The PubChem context is registry-managed as coarse derived datasets, not one
registry entry per compound. The expensive fetched payloads are preserved as
batched files under `pubchem:compound_records`, and
`pubchem:cid_molecular_info` is the flattened adapter-facing artifact.

The current PubChem flattened dataset has `169,666` rows and includes molecular
weight, monoisotopic mass, formula, InChI, InChIKey, canonical SMILES, isomeric
SMILES, and IUPAC names where available.

## Protein Context

HMDB protein XML is needed for RaMP parity because it contains substantially more
metabolite-protein associations than the HMDB metabolite XML alone.

Current HMDB protein handling:

- `ProteinIdentifier` nodes use `UniProtKB:<accession>` because HMDB protein
  accessions and UniProt IDs are 1:1 in the observed HMDB 5.0 snapshot.
- HMDB protein accession and source-provided protein metadata are preserved on
  the node.
- Metabolite-protein edges connect `MetaboliteIdentifier(HMDB:<id>)` to
  `ProteinIdentifier(UniProtKB:<id>)`.
- No human filtering or UniProt canonicalization is applied in the adapter.

Observed HMDB protein scale:

- `8,292` protein records
- `1,592,074` HMDB metabolite-protein association edges before graph merge
- protein XML contributes `728,315` associations not present in the metabolite
  XML association block

## Pathway Context

Pathway context is source-native. The current graph does not harmonize pathways
across sources.

Current pathway sources:

- HMDB/SMPDB and HMDB KEGG-style pathway links
- Reactome human pathway mappings and hierarchy
- WikiPathways human RDF pathway memberships
- PFOCR human figure-level pathway memberships

Reactome is the only current pathway source with parent-child pathway hierarchy
modeled in this pass.

PFOCR is intentionally represented as figure-level pathways. It uses the same
human gene and chemical GMT file types that RaMP used.

Pathway Commons is intentionally not included yet because the current relevant
registered usage is Pharos/TCRD-oriented and not part of RaMP's current source
set for this graph.

## Adapter Validation Counts

These are adapter-level counts before final graph merge effects.

| Adapter area | Key counts |
| --- | --- |
| HMDB metabolite pathway context | `49,658` pathways, `816,646` metabolite-pathway edges |
| HMDB protein pathway context | `48,922` pathways, `310,861` protein-pathway edges |
| HMDB protein associations | `8,292` proteins, `1,592,074` metabolite-protein edges |
| Reactome pathway context | `2,883` pathways, `3,223` metabolites, `12,155` proteins, `38,329` metabolite-pathway edges, `143,723` protein-pathway edges, `2,899` hierarchy edges |
| WikiPathways pathway context | `1,015` pathways, `21,938` metabolites, `38,279` genes, `49,038` proteins, `57,338` metabolite-pathway edges, `147,441` gene-pathway edges, `242,241` protein-pathway edges |
| PFOCR pathway context | `75,103` pathways, `7,192` metabolites, `15,801` genes, `141,124` metabolite-pathway edges, `477,667` gene-pathway edges |
| PubChem molecular info | `169,666` flattened molecular info rows |

## Current Configuration

The working graph is configured in:

- `src/use_cases/working.yaml`

This is still a working graph configuration. It should not be promoted into a
production RaMP or Pharos build until the validation and export rules are
settled.

Because the graph now has multiple source adapters emitting the same node and
edge types, these adapters should run with normal merge behavior. Do not set
`single_source=True` for the metabolite harmonization source adapters.

When new node or edge collections are added, use a full truncate/rebuild rather
than `--resume`.

## Known Granularity Problem

The graph is intentionally not solving metabolite granularity yet.

Known issues include:

- generic metabolites with R groups
- specified molecules linked to generic forms
- salts and parent compounds
- stereochemistry that matters in some use cases and not others
- different source cliques sharing the same InChIKey
- lipids where biological and analytical granularity may differ

The current strategy is to keep the source-ID evidence graph rich enough to
study these cases before creating harmonized metabolite aggregate nodes.

## Deferred Decisions

- When and how to create `HarmonizedMetabolite` aggregate nodes.
- How to represent multiple metabolite granularity levels for different use
  cases.
- How human curation should be stored and replayed.
- Whether class and role systems from HMDB, LipidMaps, RefMet, and ChEBI should
  become nodes, nested attributes, or separate context edges.
- Whether to include Pathway Commons in this graph later.
- How to export a backward-compatible RaMP SQLite database from this evidence
  graph.
- How QA Browser curation should display and modify equivalence decisions.
