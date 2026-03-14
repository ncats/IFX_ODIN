# TCRD / Pharos Pipeline TODO

Two-stage tracker: (1) get data into the Pharos ArangoDB graph, (2) convert it to the TCRD MySQL schema.
Status: [ ] not started | [~] in progress | [x] done

---

## Code Style TODOs

- [ ] Standardize edge/relationship class names — pick one convention (e.g. always end with `Edge`, drop `Relationship`)
- [ ] `ProteinAdapter` (UniProt) yields `Pathway` nodes and `ProteinPathwayRelationship` edges in addition to `Protein` — consider splitting into separate adapters to make intent clear
- [ ] `ProteinDiseaseEdgeAdapter` (UniProt) yields `Disease` nodes in addition to edges — name already implies edges only, consider making the Disease node creation explicit

## POUNCE Code Style TODOs

- [ ] Replace `CategoryValue` with `Dict[str, str]` — `CategoryValue` is a dataclass with only `name` and `value` fields (plus an `id` derived from them), used wherever a category label + value pair is stored (e.g. `demographics.categories`, `exposure.category`, `biospecimen.organism_category`). Plain dicts would be simpler, eliminate the class entirely, and map naturally to the `dict_table` pattern in `arango_to_mysql.py` (which already handles `Dict[str, str]` fields as linked key-value tables in MySQL).

---

## Pipeline Status Table

Each row is a concept. Data source checkboxes = ingested into Pharos graph. MySQL table checkboxes = converter written to TCRD.

| Concept | Data Sources (→ graph) | Arango Type | MySQL Tables (graph → TCRD) |
|---------|------------------------|-------------|------------------------------|
| **Protein** | [x] target_graph CSV<br>[x] UniProt reviewed<br>[x] Antibodypedia *(antibody_count)*<br>[x] JensenLab *(pm_score)*<br>[x] old Pharos MySQL *(idg_family)* | `Protein` | [x] `protein`<br>[x] `target`<br>[x] `t2tc`<br>[x] `alias`<br>[x] `xref`<br>[x] `tdl_info` |
| **GeneRif** | [x] target_graph generif CSV | `GeneRif`<br>`GeneGeneRifRelationship` | [x] `generif`<br>[x] `generif2pubmed`<br>[x] `protein2pubmed` |
| **Tissue** | [x] Uberon OBO<br>[x] GTEx<br>[x] HPA<br>[x] HPM<br>[x] JensenLab | `Tissue`<br>`TissueParentEdge` | [ ] TBD |
| **GoTerm** | [x] GO OBO | `GoTerm`<br>`GoTermHasParent` | [x] `go`<br>[x] `go_parent` |
| **ProteinGoTermRelationship** | [x] UniProt GAF<br>[x] GO GAF | `ProteinGoTermRelationship` | [x] `goa` |
| **Ligand** | [x] IUPHAR<br>[x] ChEMBL<br>[x] DrugCentral | `Ligand` | [x] `ncats_ligands` |
| **ProteinLigandRelationship** | [x] IUPHAR<br>[x] ChEMBL<br>[x] DrugCentral | `ProteinLigandRelationship` | [x] `ncats_ligand_activity` |
| **ProteinTissueExpressionEdge** | [x] GTEx<br>[x] HPA protein (IHC)<br>[x] HPA RNA<br>[x] HPM<br>[x] JensenLab TISSUES | `ProteinTissueExpressionEdge` | [ ] TBD |
| **Disease** | [x] MONDO<br>[x] UniProt | `Disease`<br>`DiseaseParentEdge` | [ ] TBD |
| **ProteinDiseaseEdge** | [x] UniProt curated | `ProteinDiseaseEdge` | [ ] TBD |
| **Pathway** | [x] Reactome<br>[x] UniProt | `Pathway`<br>`PathwayParentEdge` | [ ] TBD |
| **ProteinPathwayRelationship** | [x] Reactome<br>[x] UniProt | `ProteinPathwayRelationship` | [ ] TBD |
| **Keyword** | [x] UniProt | `Keyword`<br>`ProteinKeywordEdge` | [ ] TBD |
| | *— post-processing (pharos_aql_post.yaml) —* | | |
| **SetLigandActivityFlagAdapter** | [x] computed from graph | updates `meets_idg_cutoff` on `ProteinLigandRelationship` | *(via ProteinLigandRelationship)* |
| **SetGoTermLeafFlagAdapter** | [x] computed from graph | updates `is_leaf` on `GoTerm` | *(via GoTerm)* |
| **TDLInputAdapter** | [x] computed from graph | updates `tdl`, `tdl_meta` on `Protein` | *(via Protein)* |
| **TDLOverrideAdapter** | [x] manual CSV | updates `tdl` on `Protein` | *(via Protein)* |

---

## Planned Data Sources

### Additional Disease Associations (ProteinDiseaseEdge)
- CTD
- DrugCentral Indication
- ERAM
- Expression Atlas
- Monarch
- OMIM
- JensenLab DISEASES
- JensenLab Knowledge UniProtKB-KW
- JensenLab Text Mining

### Additional Disease Ontologies (Disease)
- Disease Ontology (DO)

### Additional Pathways (Pathway / ProteinPathwayRelationship)
- WikiPathways
- PathwayCommons

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