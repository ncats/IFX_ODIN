# TCRD / Pharos Pipeline TODO

Two-stage tracker: (1) get data into the Pharos ArangoDB graph, (2) convert it to the TCRD MySQL schema.
Status: [ ] not started | [~] in progress | [x] done

---

## Code Style TODOs

- [x] Standardize edge/relationship class names — pick one convention (e.g. always end with `Edge`, drop `Relationship`)
- [ ] `ProteinAdapter` (UniProt) yields `Pathway` nodes and `ProteinPathwayRelationship` edges in addition to `Protein` — consider splitting into separate adapters to make intent clear
- [ ] `ProteinDiseaseEdgeAdapter` (UniProt) yields `Disease` nodes in addition to edges — name already implies edges only, consider making the Disease node creation explicit

## POUNCE Code Style TODOs

- [ ] Replace `CategoryValue` with `Dict[str, str]` — `CategoryValue` is a dataclass with only `name` and `value` fields (plus an `id` derived from them), used wherever a category label + value pair is stored (e.g. `demographics.categories`, `exposure.category`, `biospecimen.organism_category`). Plain dicts would be simpler, eliminate the class entirely, and map naturally to the `dict_table` pattern in `arango_to_mysql.py` (which already handles `Dict[str, str]` fields as linked key-value tables in MySQL).

---

## Pipeline Status Table

Each row is a protein-facing Pharos/TCRD concept. Data source checkboxes = ingested into the Pharos graph or side-lifted into the protein-oriented Pharos view. MySQL table checkboxes = graph-derived converter output written to TCRD.

| Concept | Data Sources (→ graph) | Arango Type | MySQL Tables (graph → TCRD) |
|---------|------------------------|-------------|------------------------------|
| **Protein** | [x] target_graph CSV<br>[x] UniProt reviewed<br>[x] JensenLab *(pm_score)*<br>[x] Antibodypedia *(antibody_count)*<br>[x] old Pharos MySQL *(idg_family)* | `Protein` | [x] `protein`<br>[x] `target`<br>[x] `t2tc`<br>[x] `alias`<br>[x] `xref`<br>[x] `tdl_info` |
| **GeneRif** | [x] target_graph generif CSV | `GeneRif` | [x] `generif` |
| **GeneGeneRifEdge** | [x] target_graph generif CSV | `GeneGeneRifEdge` | [x] `generif`<br>[x] `generif2pubmed`<br>[x] `protein2pubmed` |
| **Tissue** | [x] Uberon OBO | `Tissue` | [x] `uberon` |
| **TissueParentEdge** | [x] Uberon OBO | `TissueParentEdge` | [x] `uberon_parent` |
| **ProteinTissueExpressionEdge** | [x] GTEx<br>[x] HPA protein (IHC)<br>[x] HPA RNA<br>[x] HPM<br>[x] JensenLab TISSUES | `ProteinTissueExpressionEdge` | [x] `tissue`<br>[x] `expression`<br>[x] `gtex` |
| **GoTerm** | [x] GO OBO | `GoTerm` | [x] `go` |
| **GoTermHasParent** | [x] GO OBO | `GoTermHasParent` | [x] `go_parent` |
| **ProteinGoTermEdge** | [x] UniProt GAF<br>[x] GO GAF | `ProteinGoTermEdge` | [x] `goa` |
| **Ligand** | [x] IUPHAR<br>[x] ChEMBL<br>[x] DrugCentral | `Ligand` | [x] `ncats_ligands` |
| **ProteinLigandEdge** | [x] IUPHAR<br>[x] ChEMBL<br>[x] DrugCentral | `ProteinLigandEdge` | [x] `ncats_ligand_activity` |
| **Disease** | [x] MONDO<br>[x] Disease Ontology<br>[x] UniProt curated<br>[x] CTD<br>[x] JensenLab DISEASES *(promoted in `pharos.yaml` / `target_graph.yaml`)* | `Disease` | [x] `ncats_disease` |
| **DiseaseParentEdge** | [x] MONDO | `DiseaseParentEdge` | [x] `mondo_parent`<br>[x] `ancestry_mondo` |
| **DODiseaseParentEdge** | [x] Disease Ontology | `DODiseaseParentEdge` | [x] `do_parent`<br>[x] `ancestry_do` |
| **ProteinDiseaseEdge** | [x] UniProt curated<br>[x] CTD *(side-lifted from gene associations by the TCRD target resolver)*<br>[x] JensenLab DISEASES *(Knowledge, Experiment/TIGA, and Text Mining; promoted in `pharos.yaml` / `target_graph.yaml`; working/full configs apply `textmining_min_zscore: 6.0` to stay close to historical Pharos text-mining scope)* | `ProteinDiseaseEdge` | [x] `disease_type`<br>[x] `disease`<br>[x] `ncats_d2da` |
| **Pathway** | [x] UniProt<br>[x] Reactome<br>[x] WikiPathways<br>[x] PathwayCommons | `Pathway` | [x] `pathway` |
| **PathwayParentEdge** | [x] Reactome | `PathwayParentEdge` | not exported to legacy TCRD MySQL |
| **ProteinPathwayEdge** | [x] UniProt<br>[x] Reactome<br>[x] WikiPathways *(side-lifted from gene associations by the TCRD target resolver)*<br>[x] PathwayCommons *(side-lifted from gene associations by the TCRD target resolver)* | `ProteinPathwayEdge` | [x] `pathway` |
| **Keyword** | [x] UniProt | `Keyword` | [x] `xref` *(UniProt Keyword xtype)* |
| **ProteinKeywordEdge** | [x] UniProt | `ProteinKeywordEdge` | [x] `xref` *(UniProt Keyword xtype)* |
| | *— post-processing (pharos_aql_post.yaml) —* | | |
| **SetLigandActivityFlagAdapter** | [x] computed from graph | updates `meets_idg_cutoff` on `ProteinLigandEdge` | *(via ProteinLigandEdge)* |
| **SetGoTermLeafFlagAdapter** | [x] computed from graph | updates `is_leaf` on `GoTerm` | *(via GoTerm)* |
| **TDLInputAdapter** | [x] computed from graph | updates `tdl`, `tdl_meta` on `Protein` | *(via Protein)* |
| **TDLOverrideAdapter** | [x] manual CSV | updates `tdl` on `Protein` | *(via Protein)* |

### Source-File Ontology Tables

These tables are populated directly from ontology source files during the TCRD build, not reconstructed from the merged graph disease hierarchy.

| Source Concept | Source Files | TCRD Tables |
|---------|------------------------|-------------|
| **MONDO ontology** | [x] `input_files/auto/mondo/mondo.json` | [x] `mondo`<br>[x] `mondo_parent`<br>[x] `ancestry_mondo` *(post-processing from `mondo_parent`)* |
| **Disease Ontology** | [x] `input_files/auto/disease_ontology/doid.json` | [x] `do`<br>[x] `do_parent`<br>[x] `ancestry_do` *(post-processing from `do_parent`)* |

---

## Planned Data Sources

### Additional Disease Associations (ProteinDiseaseEdge)
- DrugCentral Indication
- ERAM *(punt for now: public download appears stale/legacy; if we need ERAM coverage, prefer copying or migrating the legacy `eRAM` rows from `pharos319` rather than building a fresh ingest from the public files)*
- Expression Atlas *(punt for now: old TCRD used a bulk Atlas export plus custom preprocessing, but current Atlas appears to require per-experiment harvesting from FTP; revisit only as a larger dedicated project, not a quick ingest)*
- Monarch
- OMIM

### New Concepts
- Protein-Protein Interactions — STRING, BioPlex, Reactome PPI
- Orthologs — OMA, EggNOG, Inparanoid
- Protein Classes - PANTHER, DTO
- Phenotype — IMPC, JAX/MGI
- GWAS
- Protein & Disease Novelty (this might be TINx, I'm not sure)
- Harmonizome - updated version maybe
- P-HIPSTer Viral PPIs
- Publications — NCBI, JensenLab
- NIH Target Lists
- IDG Resources
- Nearest Tclin (computed from graph)
- Publication Statistics (PubMed Score, PubTator)

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
- [ ] Compare `mondo.comment` population against `pharos319`
  - Current source-file path preserves MONDO comments separately from the merged disease graph.

### Disease Association Table

- [x] Fix `disease.name` population in working MySQL
- [x] Fix `disease.ncats_name` population in working MySQL
- [x] Map `ProteinDiseaseEdge.details` into source-specific `disease` association rows
  - Current converter emits one `disease` row per edge detail rather than one row per merged graph edge.
- [x] Populate `disease.evidence` from disease association details
- [ ] Decide whether any disease-detail text should populate `disease.description`
  - `pharos319` has disease descriptions for some sources, but current working MySQL leaves `disease.description` empty.
- [ ] Decide whether disease association detail metadata should populate `disease.source`
  - `pharos319` uses `disease.source` for some sources; current working MySQL leaves it empty.
- [ ] Document source-specific fields that remain intentionally unsupported in the working converter
  - Examples from `pharos319`: `drug_name`, `log2foldchange`, `pvalue`, `score`, `S2O`, `updated`.
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
- [ ] Decide how non-`MONDO:` / non-`DOID:` associated disease nodes should be represented downstream
  - Current graph includes associated diseases with prefixes such as `UMLS`, `OMIM`, `HP`, `EFO`, `NCIT`, and `MESH`.
- [ ] Compare disease ID normalization expectations against `pharos319`
  - Especially for legacy `MIM:` / `OMIM:` / `UMLS:` disease identifiers that appear in association rows.
