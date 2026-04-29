# TCRD / Pharos Pipeline TODO

Two-stage tracker: (1) get data into the Pharos ArangoDB graph, (2) convert it to the TCRD MySQL schema.
Status: [ ] not started | [~] in progress | [x] done

---

## Code Style TODOs

- [x] Standardize edge/relationship class names — pick one convention (e.g. always end with `Edge`, drop `Relationship`)
- [ ] `ProteinAdapter` (UniProt) yields `Pathway` nodes and `ProteinPathwayRelationship` edges in addition to `Protein` — consider splitting into separate adapters to make intent clear
- [ ] Normalize old `pharos_mysql` adapters to use `EquivalentId(...).id_str()` consistently instead of manual `f"{Prefix...}:{...}"` string construction where they emit graph IDs.

## POUNCE Code Style TODOs

- [ ] Replace `CategoryValue` with `Dict[str, str]` — `CategoryValue` is a dataclass with only `name` and `value` fields (plus an `id` derived from them), used wherever a category label + value pair is stored (e.g. `demographics.categories`, `exposure.category`, `biospecimen.organism_category`). Plain dicts would be simpler, eliminate the class entirely, and map naturally to the `dict_table` pattern in `arango_to_mysql.py` (which already handles `Dict[str, str]` fields as linked key-value tables in MySQL).

---

## Pipeline Status Table

Each row is a protein-facing Pharos/TCRD concept. Data source checkboxes = ingested into the Pharos graph or side-lifted into the protein-oriented Pharos view. MySQL table checkboxes = outputs written during the TCRD build, whether graph-derived or loaded directly from source files.

| Concept | Data Sources (→ graph) | Arango Type | MySQL Tables (graph → TCRD)                                                                                 |
|---------|-----------------------|-------------|-------------------------------------------------------------------------------------------------------------|
| **Protein** | [x] target_graph CSV<br>[x] UniProt reviewed<br>[x] JensenLab *(pm_score)*<br>[x] TIN-X *(novelty)*<br>[x] Antibodypedia *(antibody_count)*<br>[x] old Pharos MySQL *(idg_family)* | `Protein` | [x] `protein`<br>[x] `target`<br>[x] `t2tc`<br>[x] `alias`<br>[x] `xref`<br>[x] `tdl_info`<br>[x] `pmscore` |
| **GeneRif** | [x] target_graph generif CSV | `GeneRif` | [x] `generif`                                                                                               |
| **GeneGeneRifEdge** | [x] target_graph generif CSV | `GeneGeneRifEdge` | [x] `generif`<br>[x] `generif2pubmed`<br>[x] `protein2pubmed`                                               |
| **Tissue** | [x] Uberon OBO | `Tissue` | [x] `uberon`                                                                                                |
| **TissueParentEdge** | [x] Uberon OBO | `TissueParentEdge` | [x] `uberon_parent`                                                                                         |
| **ProteinTissueExpressionEdge** | [x] GTEx<br>[x] HPA protein (IHC)<br>[x] HPA RNA<br>[x] HPM<br>[x] JensenLab TISSUES | `ProteinTissueExpressionEdge` | [x] `tissue`<br>[x] `expression`<br>[x] `gtex`                                                              |
| **GoTerm** | [x] GO OBO | `GoTerm` | [x] `go`                                                                                                    |
| **GoTermHasParent** | [x] GO OBO | `GoTermHasParent` | [x] `go_parent`                                                                                             |
| **ProteinGoTermEdge** | [x] UniProt GAF<br>[x] GO GAF | `ProteinGoTermEdge` | [x] `goa`                                                                                                   |
| **Ligand** | [x] IUPHAR<br>[x] ChEMBL<br>[x] DrugCentral | `Ligand` | [x] `ncats_ligands`                                                                                         |
| **ProteinLigandEdge** | [x] IUPHAR<br>[x] ChEMBL<br>[x] DrugCentral | `ProteinLigandEdge` | [x] `ncats_ligand_activity`                                                                                 |
| **Disease** | [x] MONDO<br>[x] Disease Ontology<br>[x] UniProt curated<br>[x] CTD<br>[x] JensenLab DISEASES <br>[x] DrugCentral Indication<br>[x] TIN-X *(novelty only)* | `Disease` | [x] `ncats_disease` *(graph-backed disease rows; currently includes TIN-X novelty diseases needed by direct `tinx_importance` load)* |
| **DiseaseParentEdge** | [x] MONDO | `DiseaseParentEdge` | not exported from merged graph; source-file MONDO tables populate `mondo_parent` / `ancestry_mondo` below   |
| **DODiseaseParentEdge** | [x] Disease Ontology | `DODiseaseParentEdge` | not exported from merged graph; source-file DO tables populate `do_parent` / `ancestry_do` below            |
| **ProteinDiseaseEdge** | [x] UniProt curated<br>[x] CTD <br>[x] JensenLab DISEASES <br>[x] DrugCentral Indication | `ProteinDiseaseEdge` | [x] `disease_type`<br>[x] `disease`<br>[x] `ncats_d2da`                                                     |
| **TIN-X Importance** | [x] TIN-X *(protein-disease importance; Jensen-derived)* | not materialized in graph; loaded directly from source files in `tcrd.yaml` | [x] `tinx_importance`                                                                                        |
| **GwasTrait** | [x] TIGA | `GwasTrait` | no standalone TCRD table; trait content is duplicated via `ProteinGwasTraitEdge` into `tiga`                 |
| **ProteinGwasTraitEdge** | [x] TIGA *(GWAS gene/trait associations; best-effort disease projection via `GwasTraitDiseaseEdge`)* | `ProteinGwasTraitEdge` | [x] `tiga`<br>[x] `tiga_provenance`                                                                          |
| **Pathway** | [x] UniProt<br>[x] Reactome<br>[x] WikiPathways<br>[x] PathwayCommons | `Pathway` | no standalone TCRD table; pathway content is duplicated via `ProteinPathwayEdge` into `pathway`             |
| **PathwayParentEdge** | [x] Reactome | `PathwayParentEdge` | not exported to legacy TCRD MySQL                                                                           |
| **ProteinPathwayEdge** | [x] UniProt<br>[x] Reactome<br>[x] WikiPathways <br>[x] PathwayCommons  | `ProteinPathwayEdge` | [x] `pathway`                                                                                               |
| **PPIEdge** | [x] STRING<br>[x] BioPlex<br>[x] Reactome | `PPIEdge` | [x] `ncats_ppi`                                                                                             |
| **PantherClass** | [x] PANTHER Classes | `PantherClass` | [x] `panther_class`                                                                                         |
| **ProteinPantherClassEdge** | [x] PANTHER Classes  | `ProteinPantherClassEdge` | [x] `p2pc`                                                                                                  |
| **DTOClass** | [x] old Pharos MySQL | `DTOClass` | current converter supports `dto`, but DTO is not wired in active `tcrd.yaml`                                |
| **DTOClassParentEdge** | [x] old Pharos MySQL | `DTOClassParentEdge` | current converter supports `dto_parent`, but DTO is not wired in active `tcrd.yaml`                         |
| **ProteinDTOClassEdge** | [x] old Pharos MySQL | `ProteinDTOClassEdge` | current converter supports `p2dto`, but DTO is not wired in active `tcrd.yaml`                              |
| **Keyword** | [x] UniProt | `Keyword` | no standalone TCRD table; keyword content is duplicated via `ProteinKeywordEdge` into `xref`                |
| **ProteinKeywordEdge** | [x] UniProt | `ProteinKeywordEdge` | [x] `xref`                                                                                                  | | |
| **SetPreferredSymbolAdapter** | [x] computed from graph | updates `preferred_symbol` on `Protein` | *(via Protein → `protein.preferred_symbol`)*                                                                |
| **SetLigandActivityFlagAdapter** | [x] computed from graph | updates `meets_idg_cutoff` on `ProteinLigandEdge` | *(via ProteinLigandEdge)*                                                                                   |
| **SetGoTermLeafFlagAdapter** | [x] computed from graph | updates `is_leaf` on `GoTerm` | *(via GoTerm)*                                                                                              |
| **TDLInputAdapter** | [x] computed from graph | updates `tdl`, `tdl_meta` on `Protein` | *(via Protein)*                                                                                             |
| **TDLOverrideAdapter** | [x] manual CSV | updates `tdl` on `Protein` | *(via Protein)*                                                                                             |

### Source-File Ontology Tables

These tables are populated directly from ontology source files during the TCRD build, not reconstructed from the merged graph disease hierarchy.

| Source Concept | Source Files | TCRD Tables |
|---------|------------------------|-------------|
| **MONDO ontology** | [x] `input_files/auto/mondo/mondo.json` | [x] `mondo`<br>[x] `mondo_xref`<br>[x] `mondo_parent`<br>[x] `ancestry_mondo` *(post-processing from `mondo_parent`)* |
| **Disease Ontology** | [x] `input_files/auto/disease_ontology/doid.json` | [x] `do`<br>[x] `do_parent`<br>[x] `ancestry_do` *(post-processing from `do_parent`)* |

---

## Planned Data Sources
### Target Disease Associations
- maybe ClinGen - old pharos didn't have it, but maybe it's useful

### New Concepts
- Publications — NCBI, JensenLab
- IDG Resources
- NIH Target Lists
- Other Publication Statistics (PubTator)
- Orthologs — OMA, EggNOG, Inparanoid
- Phenotype — IMPC, JAX/MGI
- P-HIPSTer Viral PPIs
- Nearest Tclin (computed from graph)

### Simple Linkouts
- Dark Kinase Knowledgebase — understudied kinases compendium
- RESOLUTE — solute carrier (SLC) target class resource
- ProKinO — protein kinase ontology (sequence, structure, function, mutation, pathway)
- TIGA — GWAS-based target-disease importance scores
- GENEVA — RNA-seq datasets from GEO for gene expression variance analysis
- LinkedOmicsKB — CPTAC multi-omics data for cancer research
- GlyGen — carbohydrate and glycoconjugate data
- ARCHS4 — RNA-seq co-expression gene-function predictions and expression levels
- PubChem — open chemistry database with protein/gene cross-references

### Requires License
- DisGeNET Disease Associations
- KEGG Pathway
- OMIM investigation follow-up *(low priority: legacy TCRD/Pharos loaded licensed OMIM files into `omim`, `omim_ps`, and `phenotype` with `ptype='OMIM'`, and the frontend did not surface that content. Revisit only if we explicitly want a phenotype/trait ingest, not as a target-disease source.)*

### Punted / Not Doing Right Now
- ERAM *(punt for now: public download appears stale/legacy; if we need ERAM coverage, prefer copying or migrating the legacy `eRAM` rows from `pharos319` rather than building a fresh ingest from the public files)*
- Expression Atlas *(punt for now: old TCRD used a bulk Atlas export plus custom preprocessing, but current Atlas appears to require per-experiment harvesting from FTP; revisit only as a larger dedicated project, not a quick ingest)*
- Monarch as a standalone disease-association source *(do not ingest the current dump as `Monarch`; the public file is a Translator-style aggregate whose primary sources are `infores:omim` and `infores:clingen`)*
- Harmonizome: pharos shows high-level summary stats for different types of data - it's basically a summary of relations in their KG, when we should probalby just use summary stats from our own KG
- GWAS - gwas data is essentially duplicated in the old pharos, as it shows up in TIGA as well, and the UI exclusively uses the TIGA data, not the legacy direct GWAS data that was in there before TIGA was a thing
 
### Findings From Investigation
- OMIM is not a legacy Pharos target-disease association source
  - old `load-OMIM.py` populated `omim`, `omim_ps`, and `phenotype`, not `disease`
  - `pharos319` currently has `14147` `phenotype` rows with `ptype='OMIM'`
  - the old frontend did not surface that OMIM phenotype content
- Current Monarch disease dump is not a clean standalone `Monarch` source
  - the current public file is a Translator-style aggregate
  - `infores:monarchinitiative` is the aggregator, while the primary sources are `infores:omim` and `infores:clingen`
  - do not ingest it as `Monarch`; revisit direct OMIM or direct ClinGen instead

---

## Working Graph -> TCRD Gap TODOs

Use `src/use_cases/working.yaml` and `src/use_cases/working_mysql.yaml` to validate these gaps before promoting changes into the full Pharos configs.

### Disease Ontology Tables

- [x] Stop populating `mondo`, `mondo_parent`, `do`, and `do_parent` from merged graph `Disease` nodes
  - These tables now come from source-file adapters instead of the canonicalized graph disease layer.
- [x] Add source-file MONDO adapters to the working/full TCRD YAMLs
  - `MondoTableAdapter` and `MondoTableParentEdgeAdapter` now feed `mondo` / `mondo_parent`.
- [x] Add source-file Disease Ontology adapters to the working/full TCRD YAMLs
  - `DOTableAdapter` and `DOTableParentEdgeAdapter` now feed `do` / `do_parent`.
- [x] Verify `mondo.def` is populated from source-file MONDO data
  - `MondoTableAdapter` maps `Disease.mondo_description` into `MondoTerm.mondo_description`, and `TCRDOutputConverter.mondo_table_converter()` writes that into `mondo.def`.
- [x] Verify `do.def` is populated from source-file Disease Ontology data
  - `DOTableAdapter` maps `Disease.do_description` into `DOTerm.do_description`, and `TCRDOutputConverter.do_table_converter()` writes that into `do.def`.
- [x] Compare `mondo.comment` population against `pharos319`
  - Current source-file path already maps MONDO comments into `mondo.comment` through `MondoTableAdapter` and `TCRDOutputConverter.mondo_table_converter()`.
  - Current source-file adapter output includes `1210` commented MONDO terms versus `773` in `pharos319`; the difference appears to reflect source-version drift, not a missing comment mapping.

### Disease Association Table

- [x] Fix `disease.name` population in working MySQL
- [x] Fix `disease.ncats_name` population in working MySQL
- [x] Map `ProteinDiseaseEdge.details` into source-specific `disease` association rows
  - Current converter emits one `disease` row per edge detail rather than one row per merged graph edge.
- [x] Populate `disease.evidence` from disease association details
- [x] Decide whether any disease-detail text should populate `disease.description`
  - Keep the generic converter behavior as-is: do not map edge-detail text into `disease.description`.
  - Legacy `pharos319` only populated `disease.description` for `UniProt Disease`, and that text is disease-level descriptive content rather than generic edge-detail payload.
  - In the current graph model, source-native disease descriptions belong on the `Disease` node (for example `Disease.uniprot_description`), not in per-source association detail rows.
- [x] Decide whether disease association detail metadata should populate `disease.source`
  - Keep `disease.source` intentionally empty in the generic working converter for now.
  - In legacy `pharos319`, `disease.source` was source-specific subsource metadata for only some ingests (for example `DisGeNET` values like `CTD_human` / `PSYGENET`, and `eRAM` source bundles like `CLINVAR|CTD_human|GHR|ORPHANET|UNIPROT`).
  - That does not map cleanly to a generic cross-source field, so any future population should be source-specific and explicit rather than inferred from `details`.
- [x] Document source-specific fields that remain intentionally unsupported in the working converter
  - Intentionally supported already:
    - `drug_name` for `DrugCentral Indication`
    - `zscore`, `conf`, and `reference` for Jensen text-mining / knowledge rows
  - Intentionally unsupported in the generic converter:
    - `description` as an edge-detail sink
    - `source` as a generic subsource field
    - `log2foldchange` and `pvalue` from `Expression Atlas`
    - `score` and `source` from `DisGeNET`
    - `source` from `eRAM`
    - `S2O` / `O2S` from legacy `Monarch`
    - `updated` flags from legacy source-specific rows
- [x] Populate Jensen-compatible disease association fields in working/full MySQL
  - `disease.did` now preserves source disease IDs, while `mondoid` remains a best-effort FK-backed resolved MONDO mapping.
  - `disease.conf` and `disease.evidence` now populate for Jensen Knowledge / Experiment rows.
  - `disease.zscore`, `disease.conf`, and `disease.reference` now populate for Jensen Text Mining rows.
- [x] Add `ncats_d2da` links from `ncats_disease` to `disease` association rows
  - Current converter emits one direct link per disease association row.

### Disease Node Coverage

- [x] Add `ncats_disease` output for canonical disease nodes from the graph
- [x] Limit `ncats_disease` output to diseases that have target associations
  - `DiseaseAdapter` now supports `associated_only: true` for the TCRD build path.
- [x] Decide how non-`MONDO:` / non-`DOID:` associated disease nodes should be represented downstream
  - Current graph includes associated diseases with prefixes such as `UMLS`, `OMIM`, `HP`, `EFO`, `NCIT`, and `MESH`.
  - the IdResolver handles the mappings
