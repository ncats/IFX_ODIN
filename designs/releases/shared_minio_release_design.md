# Shared Data Investigation

## Goal

Before designing shared MinIO storage, inventory Jessica's source inputs and the derived artifact layers produced by `entity_resolvers`.

## Jessica Pipeline Artifact Layers

This table distinguishes Jessica's producer layers from the subset currently consumed by the `build_pharos.py` path.

| Domain | Raw sources Jessica pulls | First derived layer | Harmonized / merged layer | Final canonical artifacts | Used In `build_pharos.py` |
| --- | --- | --- | --- | --- | --- |
| TARGETS | Ensembl, NCBI Gene, HGNC, RefSeq, UniProt, NodeNorm/Babel | cleaned source tables such as `ensembl_data_with_isoforms.csv`, `ncbi_gene_info.csv`, `hgnc_complete_set.csv`, `refseq_transformed.csv`, `uniprotkb_mapping.csv`, `uniprotkb_info.csv`, `nodenorm_genes.csv`, `nodenorm_proteins.csv` | `gene_mapping_provenance.csv`, `transcript_mapping_provenance.csv`, `protein_provenance_mapping.csv` | `gene_ids.tsv`, `transcript_ids.tsv`, `protein_ids.tsv` | `gene_ids.tsv`, `transcript_ids.tsv`, `protein_ids.tsv`, `uniprotkb_mapping.csv` |
| DISEASES | MONDO, DOID, MedGen, Orphanet, OMIM, UMLS, NodeNorm disease, Jensen disease associations | cleaned source tables such as `mondo_ids.csv`, `doid.csv`, `medgen_id_mappings.csv`, `orphanet_disease_ids.csv`, `orphanet_gene_associations.csv`, `OMIM_diseases.csv`, `umls_diseases.csv`, `nodenorm_disease.csv`, `jensen_disease_gene_associations.csv` | `disease_mapping_provenance.csv`, `disease_name_clusters.tsv` | no single final disease ID file is obvious from config; current visible merged outputs are provenance- and cluster-oriented | none directly from `entity_resolvers` |
| DRUGS | GSRS public dump | `gsrs_drug_ids.tsv` | no separate merge layer visible in current config | `gsrs_drug_ids.tsv` | none directly |
| GO | GO OBO, GOA human GAF, NCBI `gene2go` | `go_obo_dataframe.csv`, `goa.csv`, `gene2go.csv` | no explicit cross-source merge file visible; GO transform also depends on target gene/protein mappings | `GOterms.csv` plus resolved GO edge outputs under `cleaned/resolved_edges/` | none directly from `entity_resolvers` |
| PPI | STRING human PPI download | filtered / transformed STRING table | no separate merge layer visible in current config | `string_ppi.csv` | none directly |
| PHENOTYPES | HPO OBO, `phenotype.hpoa`, `genes_to_phenotype.txt`, `phenotype_to_genes.txt` | cleaned phenotype edge tables | no separate merge layer visible in current config | `hpoa.tsv`, `hpo_ids.tsv`, plus resolved phenotype edge outputs | none directly |
| PATHWAYS | Pathway Commons, Panther, Reactome, WikiPathways | cleaned source tables such as `pathwaycommons_uniprot.csv`, `pathwaycommons_hgnc.csv`, `pathwaycommons_pathways.csv`, `panther2uniprot.csv`, `reactome_pathways.csv`, `reactome_to_uniprot.csv`, `wikipathways_human_pathways.tsv` | `pathway_provenance.tsv` | `pathway_ids.tsv` | none directly from `entity_resolvers` |

## Consumer Note

| Consumer | What it mainly uses |
| --- | --- |
| IFX_ODIN `src/id_resolvers` and graph builds | Mostly the final canonical artifacts, not the full raw or intermediate producer layers |

## Usage Basis

`Used In build_pharos.py` means the artifact is part of the current Pharos build path through:

- [pharos.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/pharos/pharos.yaml)
- [build_pharos.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/pharos/build_pharos.py)

That column is intentionally narrower than "used somewhere in ODIN."

## Files Reviewed

- [entity_resolvers/src/workflows/targets.Snakefile](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/workflows/targets.Snakefile)
- [entity_resolvers/src/code/main.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/main.py)
- [entity_resolvers/config/targets_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/targets_config.yaml)
- [entity_resolvers/config/diseases_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/diseases_config.yaml)
- [entity_resolvers/config/drugs_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/drugs_config.yaml)
- [entity_resolvers/config/GO_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/GO_config.yaml)
- [entity_resolvers/config/ppi_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/ppi_config.yaml)
- [entity_resolvers/config/phenotypes_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/phenotypes_config.yaml)
- [entity_resolvers/config/pathways_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/pathways_config.yaml)
- [entity_resolvers/src/code/publicdata/target_data/gene_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/target_data/gene_ids.py)
- [entity_resolvers/src/code/publicdata/target_data/transcript_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/target_data/transcript_ids.py)
- [entity_resolvers/src/code/publicdata/target_data/protein_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/target_data/protein_ids.py)
- [entity_resolvers/src/code/publicdata/pathway_data/pathways_merge.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/pathway_data/pathways_merge.py)
- [entity_resolvers/src/code/publicdata/pathway_data/pathway_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/pathway_data/pathway_ids.py)
