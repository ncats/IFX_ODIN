# RaMP SQLite 4.0.0 vs IFX_ODIN Graph

## Comparison

- SQLite: `RaMP_SQLite_v4.0.0.sqlite`
- Graph database: `metabolite_harmonization` on `ifxdev`
- Pipeline: `20260730T192253Z-649c7c4d` (`RaMP-ish - new cleanup, with wikidata`)
- Final stage: `stage-06-f66c806a55f65266`
- Pipeline completed: `2026-07-30T19:47:26Z`

This pipeline ignores WikiPathways `bdbKeggCompound` mappings but retains
`bdbWikidata`. Biological associations in the rebuilt graph are attached only
to the identifier used by the source; equivalent identifiers are connected by
mapping edges instead of receiving copied associations.

## Source ID coverage

| Measure | Count |
|---|---:|
| SQLite distinct compound source IDs | 631,543 |
| Graph active IDs | 633,890 |
| IDs shared by both | 631,289 |
| SQLite IDs missing from graph | 254 |
| Graph IDs absent from SQLite | 2,601 |

The graph contains **99.96%** of the normalized source IDs in SQLite.

### SQLite IDs missing from graph

| Prefix | Count | Current explanation |
|---|---:|---|
| CHEBI | 237 | All exist as ChEBI `ChemicalEntity` nodes. Of these, 195 never become `MetaboliteIdentifier` nodes, primarily because they occur only in preliminary Rhea reactions; 42 become metabolite nodes but are inactive after harmonization rules. |
| REFMET | 14 | RefMet-only identifiers removed by cleanup after their mapping support is severed by earlier rules. |
| RHEA.COMP | 2 | Generic Rhea identifiers present in the graph but inactive in the final stage. |
| RHEA.POLYMER | 1 | Polymer Rhea identifier present in the graph but inactive in the final stage. |

No SQLite Wikidata or KEGG compound IDs are missing from the final graph stage.

### Graph IDs absent from SQLite

| Prefix | Count |
|---|---:|
| LIPIDMAPS | 1,465 |
| PUBCHEM.COMPOUND | 523 |
| REFMET | 249 |
| CHEBI | 188 |
| KEGG.COMPOUND | 71 |
| Wikidata | 71 |
| ChemSpider | 31 |
| CAS | 3 |

### LIPIDMAPS review

The 1,465 graph-only LIPIDMAPS IDs are not caused by source-version drift. Both
builds used the 2026-06-30 LIPID MAPS release.

| Group | Count | Explanation |
|---|---:|---|
| Current LIPID MAPS records with no external-ID mapping | 1,434 | The graph keeps the primary LIPIDMAPS ID and its classification edge. The legacy SDF export wrote only external xrefs, so a record with none contributed no source ID to SQLite. |
| Current LIPID MAPS records also mapped by RefMet | 8 | The graph has both LIPID MAPS and RefMet support. Legacy RaMP still failed to write the primary LIPIDMAPS ID from the SDF and later treated the assembled metabolite as RefMet-only. |
| IDs introduced by RefMet's `lipidmaps_id` field | 23 | These IDs are not in the current LIPID MAPS SDF. In the graph they merge into broader, classification-connected cliques; legacy RaMP removed them with its RefMet-only scrub. |

The first two groups account for the 1,442 IDs present in the current LIPID MAPS
SDF. All 1,442 have direct classification edges. The 23 RefMet-only identifiers
do not have direct classification edges, but every one belongs to a final clique
that does. Across all 1,465 IDs, 1,387 are final-stage singletons and 78 belong
to non-singleton cliques.

The legacy behavior is visible in `ChemWrangler.readLipidMapsSDF()` and
`Molecule.toSourceString()`: the primary `LM_ID` is stored as the molecule ID,
but the source export loops only over the external-xref dictionary. Legacy RaMP
also explicitly removes metabolites whose only assembled source is RefMet.

This makes the LIPIDMAPS difference an expected coverage improvement in the
graph, not evidence that the graph is retaining unsupported identifiers. The
remaining graph-only prefixes still need the same focused review.

## WikiPathways findings

The legacy RaMP parser reads and writes `bdbWikidata`, so it must not be ignored
for SQLite-compatible harmonization. In the current WikiPathways snapshot,
3,419 identifiers are introduced only through `bdbWikidata`: 3,348 occur in
SQLite and 71 are graph-only.

The legacy parser does not read `bdbKeggCompound`, so that field remains ignored
by default. KEGG identifiers supported by other sources remain active; this is
why the final graph has no missing SQLite KEGG IDs even with that mapping field
disabled.

## Harmonized metabolite differences

The total metabolite populations are also close:

| Measure | SQLite | Graph |
|---|---:|---:|
| Total metabolite count | 256,814 | 254,584 |
| Singleton count | 109,165 | 110,801 |
| Maximum clique size | 62 | 66 |

On the 631,289 IDs shared by SQLite and the final graph stage:

- 248,717 cliques match exactly;
- 224 SQLite cliques are split across graph groups;
- 3,728 graph cliques contain IDs assigned to multiple SQLite cliques.

### SQLite cliques split by the graph

Most splits are small peel-offs rather than competing large cliques:

| Shape | SQLite cliques |
|---|---:|
| One graph clique plus singleton IDs | 126 |
| All IDs become singletons | 92 |
| Multiple graph cliques plus singletons | 4 |
| Multiple graph cliques without singletons | 2 |

The generic/non-generic structure filter creates 217 of the 224 splits. The
other seven are already split using the unfiltered raw mapping edges. Later
WikiPathways, InChIKey, deny-list, and cleanup stages do not create the main
split population.

One representative large split is SQLite `RAMP_C_000000110`. SQLite combines
D-mannose identifiers with an HMDB sterol record and generic LIPID MAPS
identifiers. The graph separates the D-mannose and sterol groups and leaves the
unsupported generic-to-specific links disconnected. This is consistent with
the intended generic-structure protection.

### SQLite cliques merged by the graph

Of the 3,728 graph cliques spanning multiple SQLite cliques:

| Cause in the graph pipeline | Graph cliques |
|---|---:|
| InChIKey merge with the molecular-weight cutoff | 3,719 |
| Already connected by raw mapping evidence | 9 |

Graph-only IDs occur in only 38 of the 3,719 structural-merge cliques, adding
59 IDs total. Recomputing those 38 cliques with only SQLite-shared IDs leaves
every clique connected. The graph-only IDs are passengers, not bridges, in the
observed SQLite grouping differences.

The largest structural example combines 11 SQLite carotenoid stereoisomer
groups. Their InChIKeys have different stereochemistry blocks but the same
first block, and their mass is about 568.4. The graph therefore applies the
greater-than-500 prefix rule and merges them. Legacy SQLite has molecular
weight missing for the relevant LIPID MAPS and PubChem chemical-property rows;
only monoisotopic mass is populated. Its implementation consequently used the
stereochemistry-sensitive InChIKey duplex and kept the groups separate.

The main remaining decision is not about graph-only bridges. It is whether the
greater-than-500 InChIKey-prefix policy should use the graph's current chemical
properties, or reproduce the behavior produced by missing molecular weights in
the legacy build. If the policy itself is still desired, many graph merges are
expected improvements over SQLite rather than errors.
