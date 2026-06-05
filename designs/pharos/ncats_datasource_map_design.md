# Pharos `ncats_dataSource_map` Design

## Goal

Rebuild `ncats_dataSource` and `ncats_dataSource_map` for `pharos400` from
current graph/TCRD materialization signals, one datasource at a time.

The previous legacy-style post-processing attempt was removed because broad
table-derived rules inflated counts after the graph and MySQL schemas drifted
from Pharos 3.x. This design records only mappings that have been validated
against current `pharos400` contents.

## Destination Semantics

`ncats_dataSource_map` is a Pharos API compatibility table. Each row should mean
that a datasource directly contributes to one exposed entity:

- target datasource membership: `dataSource`, `protein_id`
- ligand datasource membership: `dataSource`, `ncats_ligand_id`
- disease datasource membership: `dataSource`, `disease_name`

Rows should be distinct by datasource and entity key. Avoid using generic node
existence, canonical identifiers, or broad populated columns as a proxy for
source contribution unless that exactly matches the source-specific semantics.

`ncats_dataSource` should be populated from the distinct datasource labels after
`ncats_dataSource_map` is built, with datasource metadata where available.

## Implementation

Use the normal TCRD adapter/converter path rather than hidden MySQL
post-processing. `DataSourceMapAdapter` emits compatibility row models for:

- `ncats_dataSource`
- `ncats_dataSource_map`

The adapter reads current materialized `pharos400` tables for table-backed
rules and reads the Pharos graph for activity-source rules that are no longer
represented in legacy activity tables. The SQL converter resolves stable
protein IFX IDs and ligand identifiers to the destination MySQL primary keys at
write time, so production `tcrd.yaml` can run after the relevant protein,
target, ligand, disease, and supporting compatibility tables have already been
materialized.

The working smoke test confirmed that the adapter/converter path writes rows
through the ETL framework. The temporary table-seeding workaround used for that
test was removed; production builds should depend on normal adapter ordering.

Current implemented constraints:

- Graph-backed DrugCentral and Guide to Pharmacology activity rules are enabled.
- Native ChEMBL activity coverage emits distinct proteins and distinct exported
  ligand nodes from graph ChEMBL activity details.
- Ligand identifier rows require `ncats_ligands` to be populated before this
  adapter runs.
- Disease datasource rows are source coverage by disease page/name, not the
  disease-association provenance facet used by the disease list API.

For each datasource, document:

- `Entity`: protein, ligand, disease, or multiple
- `Signal`: graph field or materialized MySQL table/query used as source of truth
- `Rows`: current validated `pharos400` count
- `Legacy`: matching `pharos319` count when useful
- `Status`: accepted, needs discovery, or deferred
- `Notes`: caveats and source-specific rationale

## Mapping Matrix

| Datasource | Entity | Signal | Rows | Legacy | Status | Notes |
| --- | --- | --- | ---: | ---: | --- | --- |
| Antibodypedia | Protein | `SELECT DISTINCT protein_id FROM tdl_info WHERE integer_value > 0 AND itype = 'Ab Count'` | 18,633 | 18,496 | Accepted | This legacy predicate still maps cleanly in `pharos400`; current rows are one per protein and `provenance` is not needed. |
| Animal TFDB | Protein | `SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'Is Transcription Factor' AND boolean_value = 1` | 0 | 1,630 | Deferred | The legacy Animal TFDB signal is not present in `pharos400`. Do not backfill this from unrelated transcription-factor flags unless a current Animal TFDB source is restored. |
| ARCHS4 | Protein | `SELECT DISTINCT protein_id FROM extlink WHERE source = 'ARCHS4'` | 20,361 | 20,238 | Accepted | Current `PharosExternalLinkAdapter` emits ARCHS4 protein linkouts from target-graph symbols. Legacy datasource-map also treated ARCHS4 linkouts as target datasource membership. |
| BioPlex Protein-Protein Interactions | Protein | `SELECT DISTINCT protein_id FROM (SELECT protein_id FROM ncats_ppi WHERE ppitypes LIKE '%BioPlex%' UNION SELECT other_id AS protein_id FROM ncats_ppi WHERE ppitypes LIKE '%BioPlex%') x` | 10,358 | 12,005 | Accepted | Current `ncats_ppi` stores BioPlex in `ppitypes`; both endpoints have the same 10,358-protein coverage, so the union is duplicate-safe. Legacy datasource-map coverage was larger than legacy `ncats_ppi.ppitypes = 'BioPlex'` endpoint coverage, so compare current output to current PPI contents rather than forcing the old count. |
| CCLE | Protein | `SELECT DISTINCT protein_id FROM expression WHERE etype = 'CCLE'` | 0 | 18,750 | Deferred | The legacy Pharos target-expression signal is not present in `pharos400`. Current CCLE code in this repo is POUNCE/API-oriented and should not be treated as Pharos target datasource coverage. |
| Cell Surface Protein Atlas | Protein | `SELECT DISTINCT protein_id FROM expression WHERE etype = 'Cell Surface Protein Atlas'` | 0 | 1,038 | Deferred | The legacy expression signal is not present in `pharos400`. |
| ChEMBL Activities | Protein | Graph `ProteinLigandEdge` detail where `activity_source = 'ChEMBL'` | 4,312 | TBD | Accepted | Activity-evidence datasource membership from native ChEMBL activity edges. Legacy used `cmpd_activity.catype = 'ChEMBL'` for proteins, but `cmpd_activity` is no longer present; graph activity details are the current source of truth. |
| ChEMBL Activities | Ligand | Distinct graph ligand nodes from `ProteinLigandEdge` details where `activity_source = 'ChEMBL'`, resolved to exported `ncats_ligands` IDs | TBD | TBD | Accepted | Activity-evidence datasource membership from native ChEMBL activity edges. Use discrete exported ligand nodes, not raw activity rows. Keep separate from `ChEMBL IDs`, because identifier coverage and activity-evidence coverage answer different questions. |
| ChEMBL IDs | Ligand | `SELECT DISTINCT id FROM ncats_ligands WHERE ChEMBL IS NOT NULL AND ChEMBL != ''` | 429,759 | TBD | Accepted | Identifier datasource membership for ligands with explicit ChEMBL IDs. Keep separate from `ChEMBL Activities`, because identifier coverage and activity-evidence coverage answer different questions. |
| ClinVar | Protein | legacy `SELECT DISTINCT protein_id FROM clinvar` | 0 | 2,947 | Deferred | No current `pharos400.clinvar` table/signal exists for Pharos target datasource membership. Do not infer from Harmonizome ClinVar metadata. |
| Consensus Expression Values | Protein | `SELECT DISTINCT protein_id FROM expression WHERE etype = 'Consensus'` | 0 | 19,008 | Deferred | Current `pharos400.expression` has no `Consensus` rows. |
| CTD | Protein | `SELECT DISTINCT protein_id FROM disease WHERE dtype = 'CTD'` | 8,140 | 7,837 | Accepted | Current disease rows preserve `dtype = 'CTD'`; use `dtype`, not row `provenance`, because sampled CTD rows have misleading provenance. |
| CTD | Disease | `SELECT DISTINCT ncats_name FROM disease WHERE dtype = 'CTD'` | 5,720 | 5,748 | Accepted | Current rows have non-null `ncats_name`; distinct disease membership remains direct from `disease.dtype`. |
| Dark Kinase Knowledgebase | Protein | `SELECT DISTINCT protein_id FROM extlink WHERE source = 'Dark Kinome'` | 159 | 161 | Accepted | Current `extlink.source` is `Dark Kinome`, but provider/display name and legacy datasource-map label are `Dark Kinase Knowledgebase`; use the display label in `ncats_dataSource_map`. |
| Disease Ontology | Disease | `SELECT DISTINCT name AS disease_name FROM do` | 12,021 | 9,233 | Accepted | Compatibility mapping by disease page/name, not disease-association provenance. This is a legacy-style hack, but it remains useful because DO terms can have Pharos disease pages by name. Backend list/facet APIs should decide how to reconcile full source coverage with list-visible associated diseases. |
| DisGeNET | Protein/Disease | legacy `disease WHERE dtype = 'DisGeNET'` | 0 | 19,218 | Deferred | Intentionally absent from this Pharos version because of licensing. Do not backfill. |
| DRGC Resources | Protein | `SELECT DISTINCT t2tc.protein_id FROM drgc_resource JOIN t2tc ON t2tc.target_id = drgc_resource.target_id` | 315 | 316 | Accepted | Legacy migration defined datasource metadata only, but current `drgc_resource` rows map cleanly to proteins through `target_id -> t2tc`. This should populate protein datasource membership. |
| Drug Central Indication | Protein | `SELECT DISTINCT protein_id FROM disease WHERE dtype = 'DrugCentral Indication'` | 1,753 | 1,118 | Accepted | Current disease rows preserve `dtype = 'DrugCentral Indication'` and have non-null `protein_id`; row-count increase reflects current DrugCentral payload. |
| Drug Central Indication | Disease | `SELECT DISTINCT ncats_name FROM disease WHERE dtype = 'DrugCentral Indication'` | 2,044 | 1,452 | Accepted | Current disease rows have non-null `ncats_name`; use `dtype` for datasource membership. |
| Drug Central - Scientific Literature | Protein/Ligand | Graph `ProteinLigandEdge` detail with `activity_source = 'DrugCentral'` and `act_source` or `moa_source` = `SCIENTIFIC LITERATURE` | 454 proteins / 451 ligands | 302 proteins / 277 ligands | Accepted | DrugCentral adapter preserves the legacy sub-source in `ActivityDetails.act_source` and `moa_source`; use either field. |
| Drug Central - Drug Label | Protein/Ligand | Graph `ProteinLigandEdge` detail with `activity_source = 'DrugCentral'` and `act_source` or `moa_source` = `DRUG LABEL` | 356 proteins / 433 ligands | 324 proteins / 200 ligands | Accepted | DrugCentral adapter preserves the legacy sub-source in `ActivityDetails.act_source` and `moa_source`; use either field. |
| Drug Central - Kegg Drug | Protein/Ligand | Graph `ProteinLigandEdge` detail with `activity_source = 'DrugCentral'` and `act_source` or `moa_source` = `KEGG DRUG` | 50 proteins / 45 ligands | 19 proteins / 17 ligands | Accepted | DrugCentral adapter preserves the legacy sub-source in `ActivityDetails.act_source` and `moa_source`; use either field. |
| Drug Central - GtoPdb | Protein/Ligand | Graph `ProteinLigandEdge` detail with `activity_source = 'DrugCentral'` and `act_source` or `moa_source` = `IUPHAR` | 418 proteins / 502 ligands | 157 proteins / 156 ligands | Accepted | This preserves the legacy DrugCentral sub-source label. Keep separate from native IUPHAR/BPS `activity_source` rows unless the product decides to collapse labels. |
| Drug Central - ChEMBL | Protein/Ligand | Graph `ProteinLigandEdge` detail with `activity_source = 'DrugCentral'` and `act_source` or `moa_source` = `CHEMBL` | 1,490 proteins / 1,717 ligands | 886 proteins / 1,095 ligands | Accepted | This preserves the legacy DrugCentral sub-source label. Keep separate from native ChEMBL `activity_source` rows unless the product decides to collapse labels. |
| Drug Target Ontology IDs and Classifications | Protein | `SELECT id AS protein_id FROM protein WHERE dtoid IS NOT NULL` | 9,120 | 9,232 | Accepted | Current `protein.dtoid` remains the direct target-level DTO classification signal. |
| SureChEMBL Patent Family Count | Protein | `SELECT DISTINCT protein_id FROM patent_count` | 20,081 | n/a | Accepted | Replaces legacy `EBI Patent Counts`. Current `patent_count` is exported from SureChEMBL patent family mentions, not the frozen EBI patent-count file. |
| EBI Patent Counts | Protein | legacy `SELECT DISTINCT protein_id FROM patent_count` | n/a | 1,710 | Replaced | Do not use this legacy label for current `pharos400`; the underlying source changed to SureChEMBL. |
| Ensembl Gene IDs | Protein | `SELECT DISTINCT protein_id FROM xref WHERE xtype = 'Ensembl' AND value LIKE 'ENSG%'` | 19,732 | 19,452 | Accepted | Legacy used `xtype = 'ENSG'`; current `pharos400` stores Ensembl gene/transcript/protein IDs under `xtype = 'Ensembl'`, so filter values to `ENSG%` to preserve gene-ID semantics. |
| eRAM | Protein | `SELECT DISTINCT protein_id FROM disease WHERE dtype = 'eRAM'` | 5,057 | 5,139 | Accepted | Current disease rows preserve `dtype = 'eRAM'`; use `dtype`, not row `provenance`, because sampled eRAM rows have misleading provenance. |
| eRAM | Disease | `SELECT DISTINCT ncats_name FROM disease WHERE dtype = 'eRAM'` | 1,347 | 1,362 | Accepted | Current rows have non-null `ncats_name`; distinct disease membership remains direct from `disease.dtype`. |
| Expression Atlas | Protein/Disease | legacy `disease WHERE dtype = 'Expression Atlas'` | 0 | 16,891 | Deferred | Punted for this version; current `pharos400.disease` has no `Expression Atlas` rows. |
| Gene Ontology | Protein | `SELECT DISTINCT protein_id FROM goa` | 19,697 | 7,107 | Accepted | Legacy used a narrow `tdl_info` leaf-term summary row that is no longer populated. Current semantics are datasource membership for proteins with any GO annotation in `goa`. |
| GTEx | Protein | `SELECT DISTINCT protein_id FROM gtex` | 19,580 | 19,241 | Accepted | Current `gtex` table is populated from GTEx expression details. Use table membership, not row `provenance`, because sampled rows show UBERON provenance from tissue resolution. |
| Guide to Pharmacology | Protein | Graph `ProteinLigandEdge` detail where `activity_source = 'IUPHAR/BPS Guide to PHARMACOLOGY'` | 1,483 | 1,321 | Accepted | Legacy used `cmpd_activity.catype = 'Guide to Pharmacology'`, but `cmpd_activity` is gone. Current graph activity-source details are the closest equivalent for target activity. |
| Guide to Pharmacology | Ligand | `SELECT DISTINCT id FROM ncats_ligands WHERE \`Guide to Pharmacology\` IS NOT NULL AND \`Guide to Pharmacology\` != ''` | 6,559 | 5,136 | Accepted | The legacy ligand identifier query still works against the exported ligand table. Graph activity details cover 6,107 ligands, but datasource membership follows the explicit ligand identifier column. |
| GWAS Catalog | Protein | legacy `SELECT DISTINCT protein_id FROM gwas` | 0 | 13,116 | Replaced by TIGA | Current `pharos400.gwas` exists but is empty. Populate current GWAS-derived datasource membership as `Target Illumination GWAS Analytics (TIGA)` rather than keeping the legacy `GWAS Catalog` label. |
| GlyGen | Protein | `SELECT DISTINCT protein_id FROM extlink WHERE source = 'GlyGen'` | 20,437 | 20,175 | Accepted | Current `PharosExternalLinkAdapter` emits GlyGen protein linkouts from UniProt accessions. Legacy datasource-map also treated GlyGen linkouts as target datasource membership. |
| Harmonizome | Protein | `SELECT DISTINCT protein_id FROM hgram_cdf` | 18,731 | 18,789 | Accepted | Current `pharos400` intentionally materializes Harmonizome compatibility summaries (`gene_attribute_type`, `hgram_cdf`) but not the 65M-row `gene_attribute` table. Legacy datasource-map membership matched CDF protein coverage, so use `hgram_cdf`, not `gene_attribute`. |
| HGNC | Protein | `SELECT DISTINCT protein_id FROM xref WHERE xtype = 'HGNC'` | 19,743 | 20,206 | Accepted | The legacy predicate still maps cleanly in `pharos400`. There are multiple HGNC rows for some proteins, so insert distinct protein IDs only. |
| HomoloGene | Protein | legacy `SELECT DISTINCT protein_id FROM homologene` | 0 | 18,806 | Deferred | No current `pharos400.homologene` table/signal exists. Do not infer HomoloGene membership from HCOP or generic ortholog coverage unless product semantics intentionally replace the retired NCBI HomoloGene source. |
| Human Cell Atlas Compartments | Protein | legacy `SELECT DISTINCT protein_id FROM compartment WHERE ctype = 'Human Cell Atlas'` | 0 | 11,166 | Deferred | No current `pharos400.compartment` table/signal exists. Current Harmonizome catalog still has COMPARTMENTS-like metadata, but that should not be treated as direct Human Cell Atlas compartment datasource membership. |
| Human Cell Atlas Expression | Protein | legacy `SELECT DISTINCT protein_id FROM expression WHERE etype = 'HCA RNA'` | 0 | 19,070 | Deferred | Current `pharos400.expression` was intentionally trimmed and has no `HCA RNA` or other HCA-like `etype` values. Do not backfill from generic expression coverage. |
| Human Protein Atlas | Protein | legacy `SELECT DISTINCT protein_id FROM expression WHERE etype = 'HPA'` | n/a | 10,513 | Replaced | The old listed predicate is stale; both legacy and current expression rows are split into `HPA Protein` and `HPA RNA`. Use the two explicit current labels below instead of a combined HPA datasource-map label. |
| Human Protein Atlas Protein | Protein | `SELECT DISTINCT protein_id FROM expression WHERE etype = 'HPA Protein'` | 11,066 | 11,023 | Accepted | Current protein-expression membership from HPA protein rows. Legacy count shown is legacy `expression` coverage, not the old combined datasource-map count. |
| Human Protein Atlas RNA | Protein | `SELECT DISTINCT protein_id FROM expression WHERE etype = 'HPA RNA'` | 19,654 | 19,203 | Accepted | Current RNA-expression membership from HPA RNA rows. This is intentionally broader than the old combined `Human Protein Atlas` datasource-map label. |
| Human Proteome Map | Protein | `SELECT DISTINCT protein_id FROM expression WHERE etype IN ('HPM Protein', 'HPM Gene')` | 16,736 | 16,855 | Accepted | Current trimmed expression rows retain `HPM Protein`; no current `HPM Gene` rows were found, but keeping the legacy predicate is harmless and preserves intended semantics. |
| IDG Eligible Targets List | Protein | legacy `SELECT DISTINCT protein_id FROM ncats_idg_list` | 0 | 1,301 | Deferred | No current `pharos400.ncats_idg_list` table/signal exists. Do not infer this from general IDG family or TDL status membership; the old eligible-target list was a narrower list. |
| IDG Families | Protein | `SELECT DISTINCT t2tc.protein_id FROM target JOIN t2tc ON t2tc.target_id = target.id WHERE target.fam IS NOT NULL` | 8,138 | 8,147 | Accepted | The legacy predicate still maps cleanly. Current family labels are expanded names for some families (`Transcription Factor`, `Ion Channel`, `Nuclear Receptor`, `TF-Epigenetic`) compared with legacy abbreviations, but protein coverage is nearly unchanged. |
| IMPC Mouse Clones | Protein | legacy `SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'IMPC Clones'` | 0 | 270 | Deferred | The old clone-count summary row is no longer populated in `pharos400.tdl_info`. Do not infer clone membership from phenotype rows. |
| IMPC Phenotypes | Protein | `SELECT DISTINCT protein_id FROM phenotype WHERE ptype = 'IMPC'` | 7,604 | 5,787 | Accepted | Current `phenotype` rows already carry `protein_id`, so the simpler direct predicate matches the old join result and avoids depending on `nhprotein`/`ortholog` shape. The legacy join is still equivalent in current data. |
| JAX/MGI Mouse/Human Orthology Phenotypes | Protein | `SELECT DISTINCT protein_id FROM phenotype WHERE ptype = 'JAX/MGI Human Ortholog Phenotype'` | 13,951 | 10,204 | Accepted | Current phenotype export preserves the legacy `ptype` and protein IDs directly. |
| JensenLab COMPARTMENTS | Protein | legacy `SELECT DISTINCT protein_id FROM compartment WHERE ctype LIKE 'JensenLab%'` | 0 | 18,491 | Deferred | No current `pharos400.compartment` table/signal exists. Do not infer this from current JensenLab TISSUES expression rows; COMPARTMENTS was a separate subcellular-localization source. |
| JensenLab Experiment COSMIC | Protein/Disease | legacy `disease WHERE dtype = 'JensenLab Experiment COSMIC'` | 0 | 0 | Deferred | This old label is not present in current or legacy materialized disease rows. Current JensenLab experiment rows use `JensenLab Experiment TIGA`. |
| JensenLab Experiment DistiLD | Protein/Disease | legacy `disease WHERE dtype = 'JensenLab Experiment DistiLD'` | 0 | 0 | Deferred | This old label is not present in current or legacy materialized disease rows. Current JensenLab experiment rows use `JensenLab Experiment TIGA`. |
| JensenLab Experiment TIGA | Protein/Disease | `SELECT DISTINCT protein_id / ncats_name FROM disease WHERE dtype = 'JensenLab Experiment TIGA'` | 11,642 proteins / 279 diseases | 8,673 proteins / 286 diseases | Accepted | Current JensenLab experiment disease source preserved by `disease.dtype`. This is the current experiment label emitted by the adapter. |
| JensenLab Knowledge AmyCo | Protein/Disease | `SELECT DISTINCT protein_id / ncats_name FROM disease WHERE dtype = 'JensenLab Knowledge AmyCo'` | 76 proteins / 75 diseases | 78 proteins / 75 diseases | Accepted | Current JensenLab knowledge disease source preserved by `disease.dtype`. |
| JensenLab Knowledge GHR | Protein/Disease | legacy `disease WHERE dtype = 'JensenLab Knowledge GHR'` | 0 | 0 | Deferred | This old label is not present in current or legacy materialized disease rows. Current related knowledge labels are `AmyCo`, `MedlinePlus`, and `UniProtKB-KW`. |
| JensenLab Knowledge MedlinePlus | Protein/Disease | `SELECT DISTINCT protein_id / ncats_name FROM disease WHERE dtype = 'JensenLab Knowledge MedlinePlus'` | 2,542 proteins / 1,001 diseases | 2,516 proteins / 993 diseases | Accepted | Current JensenLab knowledge disease source preserved by `disease.dtype`; this appears to replace the old GHR-style knowledge source in the present payload. |
| JensenLab Knowledge UniProtKB-KW | Protein/Disease | `SELECT DISTINCT protein_id / ncats_name FROM disease WHERE dtype = 'JensenLab Knowledge UniProtKB-KW'` | 2,583 proteins / 119 diseases | 2,418 proteins / 118 diseases | Accepted | Current and legacy disease rows preserve this exact `dtype`; existing datasource-map rows use distinct protein/disease membership rather than all association rows. |
| JensenLab Text Mining | Protein/Disease | `SELECT DISTINCT protein_id / ncats_name FROM disease WHERE dtype = 'JensenLab Text Mining'` | 2,770 proteins / 1,255 diseases | 2,785 proteins / 1,268 diseases | Accepted | Current and legacy disease rows preserve this exact `dtype`; existing datasource-map rows use distinct protein/disease membership rather than all association rows. |
| JensenLab TISSUES | Protein | `SELECT DISTINCT protein_id FROM expression WHERE etype = 'JensenLab TISSUES'` | 18,727 | 17,987 | Accepted | Current tissue-expression rows preserve the source label. The legacy predicate `etype LIKE 'JENSENLAB %'` also matches under MySQL collation, but use the exact current `etype` for clarity. |
| JensenLab PubMed Text-mining Scores | Protein | `SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'JensenLab PubMed Score' AND number_value > 0` | 18,733 | 19,052 | Accepted | Current `tdl_info` still carries the JensenLab publication text-mining score. `protein2pubmed.source = 'JensenLab'` is also populated, but the score row exactly matches the legacy datasource-map rule. |
| NCBI GeneRIFs | Protein | `SELECT DISTINCT protein_id FROM generif` | 16,824 | 17,043 | Accepted | New compatibility datasource-map label. GeneRIF-specific membership should come from `generif`; broader `protein2pubmed.source = 'NCBI'` includes NCBI gene2pubmed links as well. |
| KEGG Pathways | Protein | legacy `SELECT DISTINCT protein_id FROM pathway WHERE pwtype = 'KEGG'` | 0 | 7,686 | Deferred | Current `pharos400.pathway` exists, but has no `pwtype = 'KEGG'` rows. KEGG pathway compatibility is punted for this version. |
| KEGG Distances | Protein | legacy `SELECT pid1 AS protein_id FROM kegg_distance UNION SELECT pid2 AS protein_id FROM kegg_distance` | 0 | 4,896 | Deferred | Current `pharos400.kegg_distance` exists but is empty. Reactome nearest-Tclin work may replace this concept, but do not populate the KEGG label from Reactome-derived data. |
| KEGG Nearest Tclins | Protein | legacy `SELECT protein_id FROM kegg_nearest_tclin UNION SELECT t2tc.protein_id FROM kegg_nearest_tclin JOIN t2tc ON tclin_id = t2tc.target_id` | 0 | 2,977 | Deferred | Current `pharos400.kegg_nearest_tclin` exists but is empty. Keep this punted unless a KEGG-derived nearest-Tclin computation is restored. |
| LINCS | Protein | legacy `SELECT DISTINCT protein_id FROM lincs` | 0 | 980 | Deferred | No current `pharos400.lincs` table/signal exists. |
| LINCS L1000 XRefs | Protein | legacy `SELECT DISTINCT protein_id FROM xref WHERE xtype = 'L1000 ID'` | 0 | 978 | Deferred | No current `xref` rows with `L1000`/`LINCS`-like `xtype` values exist. |
| LinkedOmicsKB | Protein | `SELECT DISTINCT protein_id FROM extlink WHERE source = 'LinkedOmicsKB'` | 18,889 | 19,183 | Accepted | Current `PharosExternalLinkAdapter` emits LinkedOmicsKB protein linkouts from symbol mappings. Legacy datasource-map also treated LinkedOmicsKB linkouts as target datasource membership. |
| LocSigDB | Protein | legacy `SELECT DISTINCT protein_id FROM locsig` | 0 | 18,916 | Deferred | No current `pharos400.locsig` table/signal exists. |
| Monarch | Protein/Disease | legacy `SELECT DISTINCT protein_id / ncats_name FROM disease WHERE dtype = 'Monarch'` | 0 | 3,825 proteins / 5,096 diseases | Deferred | No current `disease.dtype = 'Monarch'` rows exist. Current Monarch work should not be ingested as a standalone `Monarch` source because the public dump is an aggregate with primary sources such as OMIM and ClinGen. |
| Monarch Ortholog Disease Associations | Protein/Disease | legacy `SELECT DISTINCT protein_id / name FROM ortholog_disease` | 0 | 3,827 proteins / 5,614 diseases | Deferred | No current `pharos400.ortholog_disease` table/signal exists. Do not infer from generic ortholog or disease data. |
| MONDO | Disease | `SELECT DISTINCT name AS disease_name FROM mondo` | 26,660 | n/a | Accepted | Current `MondoTableAdapter` contributes disease terms and MONDO-backed `ncats_disease` pages. Use full MONDO source coverage for datasource-map membership; backend list/facet APIs should decide how to reconcile this with list-visible associated diseases. |
| NCBI Gene | Protein | `SELECT DISTINCT protein_id FROM xref WHERE xtype = 'NCBIGene'` | 19,745 | 20,153 | Accepted | Current NCBI Gene membership is simpler than the old multi-table union. `alias.type = 'NCBI Gene ID'` has the same coverage, and adding `protein2pubmed.source = 'NCBI'` does not add more proteins in `pharos400`. |
| NCBI GI Numbers | Protein | legacy `SELECT DISTINCT protein_id FROM xref WHERE xtype = 'NCBI GI'` | 0 | 20,402 | Deferred | GI numbers are retired/legacy NCBI sequence identifiers and are not present in current `pharos400.xref`. Do not replace them with NCBI Gene IDs; those are a different identifier family and are covered by `NCBI Gene`. |
| OMIM | Protein | legacy `SELECT DISTINCT protein_id FROM phenotype WHERE ptype = 'OMIM'` | 0 | 13,856 | Deferred | Current `pharos400` has no OMIM phenotype rows and the `omim` table is empty. OMIM disease identifiers may appear through disease resolution/xrefs, but the old datasource-map label was phenotype-based and did not model OMIM as a target-disease source. |
| Orthologs | Protein | `SELECT DISTINCT protein_id FROM ortholog` | 18,055 | 18,056 | Accepted | Current `ortholog` is populated from HCOP-derived ortholog edges and preserves the legacy table semantics. Do not conflate this with retired `HomoloGene` or Monarch ortholog-disease labels. |
| P-HIPSTer Viral PPIs | Protein | `SELECT DISTINCT protein_id FROM viral_ppi` | 5,634 | 5,719 | Accepted | Current `viral_ppi` remains populated from P-HIPSTer legacy-lifted viral PPI edges. |
| PANTHER Protein Classes | Protein | `SELECT DISTINCT protein_id FROM p2pc` | 13,949 | 8,070 | Accepted | Current `p2pc` is populated from PANTHER protein class edges. Increased coverage reflects current PANTHER class ingest rather than a predicate change. |
| PathwayCommons | Protein | `SELECT DISTINCT protein_id FROM pathway WHERE pwtype LIKE 'PathwayCommons%'` | 5,990 | 5,001 | Accepted | Current rows use `pwtype = 'PathwayCommons'`; legacy rows used subtype labels such as `PathwayCommons: panther`, but the old `LIKE` predicate still captures current membership. |
| PubChem | Protein | `SELECT DISTINCT protein_id FROM extlink WHERE source = 'PubChem'` | 20,654 | 0 | Accepted | Current `PharosExternalLinkAdapter` emits protein-level PubChem linkouts from UniProt IDs. Include this in `ncats_dataSource_map` for consistency with the other protein linkout providers, even though legacy datasource-map only exposed ligand `PubChem CIDs`. |
| PubChem CIDs | Ligand | `SELECT DISTINCT id FROM ncats_ligands WHERE PubChem IS NOT NULL AND PubChem != ''` | 405,567 | 329,851 | Accepted | Use explicit PubChem identifier membership on ligands. Existing datasource-map counts are higher than this predicate in both current and legacy, so treat map counts as broad/legacy artifacts rather than the rule to reproduce. |
| PubMed | Protein | `SELECT DISTINCT protein_id FROM protein2pubmed` | 19,714 | 19,790 | Accepted | Broad publication membership across NCBI and JensenLab publication links. Current `protein2pubmed` has both `NCBI` and `JensenLab` source rows. |
| PubTator Text-mining Scores | Protein | `SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'PubTator Score'` | 19,692 | 18,310 | Accepted | Current `tdl_info` still carries PubTator publication text-mining scores and current datasource-map rows match this predicate. |
| Reactome Pathways | Protein | `SELECT DISTINCT protein_id FROM pathway WHERE pwtype = 'Reactome'` | 11,332 | 10,781 | Accepted | Current Reactome pathway rows are populated in `pathway` with `pwtype = 'Reactome'`. |
| Reactome Protein-Protein Interactions | Protein | `SELECT DISTINCT protein_id FROM (SELECT protein_id FROM ncats_ppi WHERE ppitypes LIKE '%Reactome%' UNION SELECT other_id AS protein_id FROM ncats_ppi WHERE ppitypes LIKE '%Reactome%') x` | 5,002 | 4,465 | Accepted | Current and legacy materialized PPI table is `ncats_ppi`, not `ppi`; use both endpoints because datasource membership is target-level. |
| RESOLUTE | Protein | `SELECT DISTINCT protein_id FROM extlink WHERE source = 'RESOLUTE'` | 449 | 451 | Accepted | Current `PharosExternalLinkAdapter` emits RESOLUTE protein linkouts from symbol mappings. Legacy datasource-map also treated RESOLUTE linkouts as target datasource membership. |
| RDAS | Disease | `SELECT DISTINCT name AS disease_name FROM ncats_disease WHERE gard_rare = 1` | 10,354 | n/a | Accepted | Current `RDASRareDiseaseAdapter` contributes rare-disease metadata to disease pages. Use full rare-disease source coverage for datasource-map membership; backend list/facet APIs should decide how to reconcile this with list-visible associated diseases. |
| RGD | Protein | legacy `rat_qtl` / `nhprotein` / `ortholog` join | 0 | 431 | Deferred | No current `pharos400.rat_qtl` table/signal exists, and no current `RGD` datasource-map rows exist. HCOP may carry RGD identifiers for rat ortholog records, but that should not be treated as the old RGD QTL datasource. |
| STRING IDs | Protein | legacy `SELECT id AS protein_id FROM protein WHERE stringid IS NOT NULL` | 20 | 19,121 | Replaced | Current `protein.stringid` is barely populated and represents STRING's Ensembl-protein-style identifier rather than source contribution. Do not use this as broad STRING datasource membership. |
| STRINGDB | Protein | `SELECT DISTINCT protein_id FROM (SELECT protein_id FROM ncats_ppi WHERE ppitypes LIKE '%STRING%' UNION SELECT other_id AS protein_id FROM ncats_ppi WHERE ppitypes LIKE '%STRING%') x` | 18,718 | 19,057 | Accepted | Current STRING contribution is represented by PPI edges in `ncats_ppi`; use endpoint union for target datasource membership. This preserves the legacy datasource label better than the deprecated `STRING IDs` alias field. |
| TIN-X Data | Protein/Disease | `SELECT DISTINCT protein_id FROM tinx_importance`; `SELECT DISTINCT d.name FROM tinx_importance ti JOIN ncats_disease d ON d.id = ti.ncats_disease_id` | 18,717 proteins / 5,167 diseases | 18,982 proteins / 8,960 diseases | Accepted | Current simplified schema stores TIN-X as `tinx_importance` keyed by `protein_id` and `ncats_disease_id`. Use distinct foreign keys; legacy also used `tinx_novelty` and `tinx_disease`, which are no longer current tables. |
| Target Illumination GWAS Analytics (TIGA) | Protein | `SELECT DISTINCT protein_id FROM tiga` | 18,781 | 18,005 | Accepted | Current `TIGAAdapter` materializes GWAS-derived TIGA scores in `tiga`, and this should replace the legacy `GWAS Catalog` datasource-map label for current builds. Use the legacy/display datasource label, not bare `TIGA`, to avoid duplicate facet labels. |
| TMHMM Predictions | Protein | legacy `SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'TMHMM Prediction'` | 0 | 5,350 | Deferred | The legacy TMHMM summary rows are not populated in current `pharos400.tdl_info`. |
| Transcription Factor Flags | Protein | legacy `SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'Is Transcription Factor'` | 0 | 1,630 | Deferred | The legacy transcription-factor summary rows are not populated in current `pharos400.tdl_info`; same missing signal as the deferred Animal TFDB row. |
| UniProt | Protein | `SELECT DISTINCT id AS protein_id FROM protein WHERE uniprot IS NOT NULL AND uniprot != ''` | 20,654 | 20,412 | Accepted | Use explicit UniProt identifier membership. The old keyword predicate happened to cover all legacy UniProt proteins, but current keyword xrefs miss some proteins and are a narrower annotation signal. |
| UniProt Disease | Protein/Disease | `SELECT DISTINCT protein_id / ncats_name FROM disease WHERE dtype = 'UniProt'` | 4,903 proteins / 6,680 diseases | 3,766 proteins / 4,972 diseases | Accepted | Current UniProt disease associations are present, but the materialized `dtype` is `UniProt` rather than legacy `UniProt Disease`. Keep the datasource-map label as `UniProt Disease` if preserving the old facet label, but source membership should use current `dtype = 'UniProt'`. |
| WikiPathways | Protein | `SELECT DISTINCT protein_id FROM pathway WHERE pwtype = 'WikiPathways'` | 8,708 | 6,411 | Accepted | Current WikiPathways pathway rows are populated in `pathway` with `pwtype = 'WikiPathways'`, and current datasource-map rows match the predicate exactly. |

## Projected Count Summary

Projected counts are the planned `pharos400` datasource-map entity counts.
Legacy counts are from `pharos319` when available. Repeated protein, disease,
and ligand rows under the same datasource label are collapsed into one summary
row.

| Data source | Protein projected | Protein legacy | Disease projected | Disease legacy | Ligand projected | Ligand legacy | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Antibodypedia | 18,633 | 18,496 | - | - | - | - | Accepted |
| ARCHS4 | 20,361 | 20,238 | - | - | - | - | Accepted |
| BioPlex Protein-Protein Interactions | 10,358 | 12,005 | - | - | - | - | Accepted |
| ChEMBL Activities | 4,312 | TBD | - | - | TBD | TBD | Accepted |
| ChEMBL IDs | - | - | - | - | 429,759 | TBD | Accepted |
| CTD | 8,140 | 7,837 | 5,720 | 5,748 | - | - | Accepted |
| Dark Kinase Knowledgebase | 159 | 161 | - | - | - | - | Accepted |
| Disease Ontology | - | - | 12,021 | 9,233 | - | - | Accepted |
| DRGC Resources | 315 | 316 | - | - | - | - | Accepted |
| Drug Central - ChEMBL | 1,490 | 886 | - | - | 1,717 | 1,095 | Accepted |
| Drug Central - Drug Label | 356 | 324 | - | - | 433 | 200 | Accepted |
| Drug Central - GtoPdb | 418 | 157 | - | - | 502 | 156 | Accepted |
| Drug Central - Kegg Drug | 50 | 19 | - | - | 45 | 17 | Accepted |
| Drug Central - Scientific Literature | 454 | 302 | - | - | 451 | 277 | Accepted |
| Drug Central Indication | 1,753 | 1,118 | 2,044 | 1,452 | - | - | Accepted |
| Drug Target Ontology IDs and Classifications | 9,120 | 9,232 | - | - | - | - | Accepted |
| Ensembl Gene IDs | 19,732 | 19,452 | - | - | - | - | Accepted |
| eRAM | 5,057 | 5,139 | 1,347 | 1,362 | - | - | Accepted |
| Gene Ontology | 19,697 | 7,107 | - | - | - | - | Accepted |
| GlyGen | 20,437 | 20,175 | - | - | - | - | Accepted |
| GTEx | 19,580 | 19,241 | - | - | - | - | Accepted |
| Guide to Pharmacology | 1,483 | 1,321 | - | - | 6,559 | 5,136 | Accepted |
| Harmonizome | 18,731 | 18,789 | - | - | - | - | Accepted |
| HGNC | 19,743 | 20,206 | - | - | - | - | Accepted |
| Human Protein Atlas Protein | 11,066 | 11,023 | - | - | - | - | Accepted |
| Human Protein Atlas RNA | 19,654 | 19,203 | - | - | - | - | Accepted |
| Human Proteome Map | 16,736 | 16,855 | - | - | - | - | Accepted |
| IDG Families | 8,138 | 8,147 | - | - | - | - | Accepted |
| IMPC Phenotypes | 7,604 | 5,787 | - | - | - | - | Accepted |
| JAX/MGI Mouse/Human Orthology Phenotypes | 13,951 | 10,204 | - | - | - | - | Accepted |
| JensenLab Experiment TIGA | 11,642 | 8,673 | 279 | 286 | - | - | Accepted |
| JensenLab Knowledge AmyCo | 76 | 78 | 75 | 75 | - | - | Accepted |
| JensenLab Knowledge MedlinePlus | 2,542 | 2,516 | 1,001 | 993 | - | - | Accepted |
| JensenLab Knowledge UniProtKB-KW | 2,583 | 2,418 | 119 | 118 | - | - | Accepted |
| JensenLab PubMed Text-mining Scores | 18,733 | 19,052 | - | - | - | - | Accepted |
| JensenLab Text Mining | 2,770 | 2,785 | 1,255 | 1,268 | - | - | Accepted |
| JensenLab TISSUES | 18,727 | 17,987 | - | - | - | - | Accepted |
| LinkedOmicsKB | 18,889 | 19,183 | - | - | - | - | Accepted |
| MONDO | - | - | 26,660 | n/a | - | - | Accepted |
| NCBI Gene | 19,745 | 20,153 | - | - | - | - | Accepted |
| NCBI GeneRIFs | 16,824 | 17,043 | - | - | - | - | Accepted |
| Orthologs | 18,055 | 18,056 | - | - | - | - | Accepted |
| P-HIPSTer Viral PPIs | 5,634 | 5,719 | - | - | - | - | Accepted |
| PANTHER Protein Classes | 13,949 | 8,070 | - | - | - | - | Accepted |
| PathwayCommons | 5,990 | 5,001 | - | - | - | - | Accepted |
| PubChem | 20,654 | 0 | - | - | - | - | Accepted |
| PubChem CIDs | - | - | - | - | 405,567 | 329,851 | Accepted |
| PubMed | 19,714 | 19,790 | - | - | - | - | Accepted |
| PubTator Text-mining Scores | 19,692 | 18,310 | - | - | - | - | Accepted |
| RDAS | - | - | 10,354 | n/a | - | - | Accepted |
| Reactome Pathways | 11,332 | 10,781 | - | - | - | - | Accepted |
| Reactome Protein-Protein Interactions | 5,002 | 4,465 | - | - | - | - | Accepted |
| RESOLUTE | 449 | 451 | - | - | - | - | Accepted |
| STRINGDB | 18,718 | 19,057 | - | - | - | - | Accepted |
| SureChEMBL Patent Family Count | 20,081 | n/a | - | - | - | - | Accepted |
| Target Illumination GWAS Analytics (TIGA) | 18,781 | 18,005 | - | - | - | - | Accepted |
| TIN-X Data | 18,717 | 18,982 | 5,167 | 8,960 | - | - | Accepted |
| UniProt | 20,654 | 20,412 | - | - | - | - | Accepted |
| UniProt Disease | 4,903 | 3,766 | 6,680 | 4,972 | - | - | Accepted |
| WikiPathways | 8,708 | 6,411 | - | - | - | - | Accepted |

## Deferred / Rejected Broad Rules

Do not use these as global rules:

- all `Protein.sources`
- all `Node.sources`
- all generic non-null identifier columns unless the datasource label is exactly
  that identifier source, for example the accepted `UniProt` rule above
- all non-null `protein.geneid`
- all non-null ligand identifier columns for source-specific activity labels
- all rows in large fact tables unless the source label is exactly the table's
  semantic scope

These may still be useful source-by-source after profiling, but they should not
be assumed to mean datasource membership.

## Current YAML Coverage Audit

The matrix above now covers all current `pharos.yaml` and `tcrd.yaml` adapters
that directly contribute target, ligand, or disease datasource membership, with
these caveats:

- ChEMBL is split into `ChEMBL IDs` for explicit ligand identifiers and
  `ChEMBL Activities` for activity-evidence membership from graph
  `ProteinLigandEdge` details.
- Disease-list `Disease Data Source` is association-level provenance from
  `disease.dtype`; this datasource-map design is entity/page source coverage.
  Those are intentionally different backend concepts.
- Linkout provider datasource rows belong in this compatibility project rather
  than a separate hidden post-processing step, including protein-level
  `PubChem` linkouts.
- `MONDO`, `RDAS`, and `Disease Ontology` are disease datasource rows because
  they create or annotate disease pages. Keep datasource-map rows true to source
  coverage. The backend should make Disease list/facet counts consistent with
  whichever disease universe the API is exposing.
  `UBERON` has no protein, ligand, or disease membership target for this table.
- `Mammalian Phenotype Ontology` is the vocabulary behind mouse phenotype
  terms. The datasource-map rows are the protein phenotype sources already
  listed as `IMPC Phenotypes` and `JAX/MGI Mouse/Human Orthology Phenotypes`.
- Raw GO term nodes are vocabulary. The datasource-map row is `Gene Ontology`,
  which already maps proteins with GO annotations through `goa`.
- Target graph CSVs, resolvers, and word-count processing are support sources
  rather than datasource-map membership rules.
- RefSeq and other target-graph identifier families are intentionally not broad
  datasource-map rules. They can become explicit identifier-source rows only if
  the product defines them that way.

## Implementation Validation

Working MySQL smoke test against a partially built `pharos400` completed through
`DataSourceMapAdapter` and inserted datasource-map rows. Because the source
`pharos400` was incomplete at test time, this validates wiring rather than final
coverage.

Observed aggregate after the smoke test:

```sql
SELECT dataSource,
       COUNT(protein_id) AS protein_count,
       COUNT(ncats_ligand_id) AS ligand_count,
       COUNT(disease_name) AS disease_count
FROM pharos400_working.ncats_dataSource_map
GROUP BY dataSource;
```

Key rows:

- `UniProt`: `20,654` proteins
- `IDG Families`: `8,138` proteins
- `Disease Ontology`: `12,021` diseases
- `MONDO`: `26,660` diseases
- DrugCentral graph activity rows were present for protein counts.
- Ligand counts were `0` in this smoke test because
  `pharos400.ncats_ligands` was empty at the time.

Final validation after a complete `pharos400` build should compare aggregate
counts from `ncats_dataSource_map` against the projected count summary above,
with special attention to ligand rows and ChEMBL activity ligand coverage.

## Validation Queries

Antibodypedia validation:

```sql
-- pharos400
SELECT
  COUNT(*) AS rows_n,
  COUNT(DISTINCT protein_id) AS proteins,
  MIN(integer_value) AS min_v,
  MAX(integer_value) AS max_v
FROM tdl_info
WHERE integer_value > 0
  AND itype = 'Ab Count';
```

Result:

- rows: `18,633`
- distinct proteins: `18,633`
- antibody count range: `1` to `11,258`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE integer_value > 0
  AND itype = 'Ab Count';

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Antibodypedia';
```

Both legacy queries return:

- rows: `18,496`
- distinct proteins: `18,496`

Animal TFDB validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'Is Transcription Factor'
  AND boolean_value = 1;
```

Result:

- rows: `0`
- distinct proteins: `0`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Animal TFDB';
```

Result:

- rows: `1,630`
- distinct proteins: `1,630`

BioPlex validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n
FROM ncats_ppi
WHERE ppitypes LIKE '%BioPlex%';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM ncats_ppi
WHERE ppitypes LIKE '%BioPlex%';

SELECT COUNT(DISTINCT other_id) AS proteins
FROM ncats_ppi
WHERE ppitypes LIKE '%BioPlex%';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%BioPlex%'
  UNION
  SELECT other_id AS protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%BioPlex%'
) x;
```

Result:

- `ncats_ppi` BioPlex rows: `175,804`
- distinct `protein_id`: `10,358`
- distinct `other_id`: `10,358`
- union distinct proteins: `10,358`
- proteins only seen as `other_id`: `0`
- proteins only seen as `protein_id`: `0`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'BioPlex Protein-Protein Interactions';
```

Result:

- rows: `12,005`
- distinct proteins: `12,005`

CCLE and Cell Surface Protein Atlas validation:

```sql
-- pharos400
SELECT etype, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype IN ('CCLE', 'Cell Surface Protein Atlas')
GROUP BY etype;
```

Result:

- no rows

Legacy comparison:

```sql
-- pharos319
SELECT dataSource, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('CCLE', 'Cell Surface Protein Atlas')
GROUP BY dataSource;
```

Result:

- `CCLE`: `18,750` rows / `18,750` proteins
- `Cell Surface Protein Atlas`: `1,038` rows / `1,038` proteins

ChEMBL split:

- `ChEMBL Activities`:
  - select distinct resolved protein and ligand endpoints from graph
    `ProteinLigandEdge` details where `activity_source = 'ChEMBL'`
  - materialize protein endpoints to `protein.id`
  - materialize ligand endpoints to `ncats_ligands.id`
- `ChEMBL IDs`:
  - use the exported `ncats_ligands.ChEMBL` identifier field

The old protein-side query should not be reused because `cmpd_activity` is not
present in `pharos400`.

ClinVar and Consensus Expression validation:

```sql
-- pharos400 schema check
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'pharos400'
  AND table_name IN ('clinvar', 'expression');

-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype = 'Consensus';
```

Result:

- `clinvar` table: absent
- `Consensus` expression rows: `0`

Legacy comparison:

```sql
-- pharos319
SELECT dataSource, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('ClinVar', 'Consensus Expression Values')
GROUP BY dataSource;
```

Result:

- `ClinVar`: `2,947` rows / `2,947` proteins
- `Consensus Expression Values`: `19,008` rows / `19,008` proteins

CTD validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n
FROM disease
WHERE dtype = 'CTD';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM disease
WHERE dtype = 'CTD'
  AND protein_id IS NOT NULL;

SELECT COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'CTD'
  AND ncats_name IS NOT NULL;
```

Result:

- disease association rows: `32,281`
- distinct proteins: `8,140`
- distinct diseases: `5,720`
- null `protein_id`: `0`
- null/blank `ncats_name`: `0`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'CTD';

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'CTD'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'CTD'
  AND disease_name IS NOT NULL;
```

Result:

- legacy CTD disease rows: `35,187`
- legacy distinct proteins: `7,837`
- legacy distinct diseases: `5,748`
- legacy map protein rows: `7,837`
- legacy map disease rows: `5,748`

Disease Ontology validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT name) AS diseases,
       SUM(name IS NULL OR name = '') AS blank_names
FROM do;
```

Result:

- rows: `12,021`
- distinct disease names: `12,021`
- blank names: `0`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT name) AS diseases
FROM do;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'Disease Ontology'
  AND disease_name IS NOT NULL;
```

Result:

- legacy `do` rows: `9,233`
- legacy map disease rows: `9,233`

DisGeNET validation:

```sql
-- pharos400
SELECT dtype, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'DisGeNET'
GROUP BY dtype;
```

Result:

- no rows

Legacy comparison:

```sql
-- pharos319
SELECT dataSource, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'DisGeNET'
GROUP BY dataSource;
```

Result:

- rows: `19,218`
- distinct proteins: `9,025`
- distinct diseases: `10,193`

DRGC Resources validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT target_id) AS targets
FROM drgc_resource;

SELECT COUNT(DISTINCT t2tc.protein_id) AS proteins
FROM drgc_resource
JOIN t2tc ON t2tc.target_id = drgc_resource.target_id;
```

Result:

- resource rows: `2,847`
- distinct targets: `315`
- distinct proteins: `315`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT target_id) AS targets
FROM drgc_resource;

SELECT COUNT(DISTINCT t2tc.protein_id) AS proteins
FROM drgc_resource
JOIN t2tc ON t2tc.target_id = drgc_resource.target_id;
```

Result:

- legacy resource rows: `2,849`
- legacy distinct targets: `316`
- legacy distinct proteins: `316`

Drug Central Indication validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n
FROM disease
WHERE dtype = 'DrugCentral Indication';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM disease
WHERE dtype = 'DrugCentral Indication'
  AND protein_id IS NOT NULL;

SELECT COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'DrugCentral Indication'
  AND ncats_name IS NOT NULL;
```

Result:

- disease association rows: `62,140`
- distinct proteins: `1,753`
- distinct diseases: `2,044`
- null `protein_id`: `0`
- null/blank `ncats_name`: `0`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'DrugCentral Indication';

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Drug Central Indication'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'Drug Central Indication'
  AND disease_name IS NOT NULL;
```

Result:

- legacy disease rows: `13,919`
- legacy distinct proteins: `1,118`
- legacy distinct diseases: `1,452`
- legacy map protein rows: `1,118`
- legacy map disease rows: `1,452`

DrugCentral activity sub-source validation:

Current graph top-level activity-source buckets:

```aql
FOR rel IN ProteinLigandEdge
  FOR detail IN (rel.details || [])
    COLLECT source = detail.activity_source WITH COUNT INTO n
    SORT n DESC
    RETURN {source, n}
```

Result:

- `ChEMBL`: `4,052,775` detail rows
- `DrugCentral`: `17,445` detail rows
- `IUPHAR/BPS Guide to PHARMACOLOGY`: `11,800` detail rows

Distinct endpoint counts from current graph details:

- `ChEMBL`: `4,312` proteins / `914,007` ligands
- `DrugCentral`: `1,844` proteins / `2,492` ligands
- `IUPHAR/BPS Guide to PHARMACOLOGY`: `1,483` proteins / `6,107` ligands

DrugCentral adapter sub-source fields:

- `src/input_adapters/drug_central/protein_drug_edge.py` reads
  `ActTableFull.act_source` and `ActTableFull.moa_source`
- those values are copied into `ActivityDetails.act_source` and
  `ActivityDetails.moa_source`

Current DrugCentral sub-source endpoint counts, using either `act_source` or
`moa_source`:

```aql
FOR rel IN ProteinLigandEdge
  FOR detail IN (rel.details || [])
    FILTER detail.activity_source == "DrugCentral"
    LET sources = UNIQUE([detail.act_source, detail.moa_source])
    FOR source IN sources
      FILTER source IN [
        "SCIENTIFIC LITERATURE",
        "DRUG LABEL",
        "KEGG DRUG",
        "IUPHAR",
        "CHEMBL"
      ]
      COLLECT source = source INTO group_rows
      RETURN {
        source,
        detail_rows: LENGTH(group_rows),
        proteins: LENGTH(UNIQUE(group_rows[*].rel._from)),
        ligands: LENGTH(UNIQUE(group_rows[*].rel._to))
      }
```

Result:

- `CHEMBL`: `10,408` detail rows / `1,490` proteins / `1,717` ligands
- `DRUG LABEL`: `796` detail rows / `356` proteins / `433` ligands
- `IUPHAR`: `1,054` detail rows / `418` proteins / `502` ligands
- `KEGG DRUG`: `84` detail rows / `50` proteins / `45` ligands
- `SCIENTIFIC LITERATURE`: `1,035` detail rows / `454` proteins / `451` ligands

Legacy comparison:

```sql
-- pharos319
SELECT dataSource, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_ligand_id) AS ligands
FROM ncats_dataSource_map
WHERE dataSource IN (
  'Drug Central - Scientific Literature',
  'Drug Central - Drug Label',
  'Drug Central - Kegg Drug',
  'Drug Central - GtoPdb',
  'Drug Central - ChEMBL'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- `Drug Central - ChEMBL`: `1,981` rows / `886` proteins / `1,095` ligands
- `Drug Central - Drug Label`: `524` rows / `324` proteins / `200` ligands
- `Drug Central - GtoPdb`: `313` rows / `157` proteins / `156` ligands
- `Drug Central - Kegg Drug`: `36` rows / `19` proteins / `17` ligands
- `Drug Central - Scientific Literature`: `579` rows / `302` proteins / `277` ligands

The old sub-source labels came from `drug_activity.source`; current graph
details preserve the same source family in `act_source` and/or `moa_source`.

DTO validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT id) AS proteins
FROM protein
WHERE dtoid IS NOT NULL;
```

Result:

- rows: `9,120`
- distinct proteins: `9,120`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Drug Target Ontology IDs and Classifications';
```

Result:

- rows: `9,232`
- distinct proteins: `9,232`

Patent count validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       MIN(year) AS min_year, MAX(year) AS max_year, SUM(count) AS total_mentions
FROM patent_count;
```

Result:

- yearly rows: `544,895`
- distinct proteins: `20,081`
- year range: `1973` to `2026`
- total family mentions: `36,703,916`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'EBI Patent Counts';
```

Result:

- rows: `1,710`
- distinct proteins: `1,710`

Current patent rows are generated from the SureChEMBL patent-family ingest. Use
the current datasource label `SureChEMBL Patent Family Count`, not legacy
`EBI Patent Counts`.

Ensembl Gene IDs validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'ENSG'
  AND protein_id IS NOT NULL;

SELECT LEFT(value, 4) AS prefix, COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'Ensembl'
  AND protein_id IS NOT NULL
GROUP BY LEFT(value, 4);

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'Ensembl'
  AND value LIKE 'ENSG%'
  AND protein_id IS NOT NULL;
```

Result:

- `xtype = 'ENSG'`: `0` rows / `0` proteins
- `xtype = 'Ensembl'` value prefixes:
  - `ENSP`: `121,229` rows / `19,734` proteins
  - `ENST`: `120,903` rows / `19,714` proteins
  - `ENSG`: `23,214` rows / `19,732` proteins
- accepted `ENSG%` mapping: `23,214` rows / `19,732` proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'ENSG'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Ensembl Gene IDs'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `xref` rows: `21,608`
- legacy distinct proteins: `19,452`
- legacy map rows: `19,452`
- legacy map proteins: `19,452`

eRAM validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n
FROM disease
WHERE dtype = 'eRAM';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM disease
WHERE dtype = 'eRAM'
  AND protein_id IS NOT NULL;

SELECT COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'eRAM'
  AND ncats_name IS NOT NULL;
```

Result:

- disease association rows: `13,572`
- distinct proteins: `5,057`
- distinct diseases: `1,347`
- null `protein_id`: `0`
- null/blank `ncats_name`: `0`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'eRAM';

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'eRAM'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'eRAM'
  AND disease_name IS NOT NULL;
```

Result:

- legacy disease rows: `14,660`
- legacy distinct proteins: `5,139`
- legacy distinct diseases: `1,362`
- legacy map protein rows: `5,139`
- legacy map disease rows: `1,362`

Expression Atlas validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'Expression Atlas';
```

Result:

- rows: `0`
- distinct proteins: `0`
- distinct diseases: `0`

Legacy comparison:

```sql
-- pharos319
SELECT dataSource, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'Expression Atlas'
GROUP BY dataSource;
```

Result:

- rows: `16,891`
- distinct proteins: `16,784`
- distinct diseases: `107`

Gene Ontology validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'Experimental MF/BP Leaf Term GOA'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM goa
WHERE protein_id IS NOT NULL;

SELECT go_type, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM goa
GROUP BY go_type;
```

Result:

- old `tdl_info` signal: `0` rows / `0` proteins
- current all-`goa` signal: `507,769` rows / `19,697` proteins
- `goa` by type:
  - `Process`: `190,105` rows / `17,726` proteins
  - `Component`: `163,383` rows / `18,992` proteins
  - `Function`: `154,281` rows / `18,403` proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'Experimental MF/BP Leaf Term GOA'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Gene Ontology'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `tdl_info` rows: `7,107`
- legacy map rows: `7,107`

GTEx validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM gtex
WHERE protein_id IS NOT NULL;
```

Result:

- rows: `1,213,960`
- distinct proteins: `19,580`

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'GTEx'
  AND protein_id IS NOT NULL;
```

Result:

- rows: `19,241`
- distinct proteins: `19,241`

Guide to Pharmacology validation:

```aql
-- pharos graph
FOR rel IN ProteinLigandEdge
  FOR detail IN (rel.details || [])
    FILTER detail.activity_source == "IUPHAR/BPS Guide to PHARMACOLOGY"
    COLLECT AGGREGATE proteins = UNIQUE(rel._from), ligands = UNIQUE(rel._to)
    RETURN {proteins: LENGTH(proteins), ligands: LENGTH(ligands)}
```

Result:

- graph activity-source proteins: `1,483`
- graph activity-source ligands: `6,107`

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT id) AS ligands
FROM ncats_ligands
WHERE `Guide to Pharmacology` IS NOT NULL
  AND `Guide to Pharmacology` != '';
```

Result:

- ligand identifier rows: `6,559`
- distinct ligands: `6,559`

Legacy comparison:

```sql
-- pharos319
SELECT dataSource, COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_ligand_id) AS ligands
FROM ncats_dataSource_map
WHERE dataSource = 'Guide to Pharmacology'
GROUP BY dataSource;
```

Result:

- rows: `6,457`
- distinct proteins: `1,321`
- distinct ligands: `5,136`

GWAS Catalog validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM gwas
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM tiga
WHERE protein_id IS NOT NULL;
```

Result:

- current `gwas`: `0` rows / `0` proteins
- current `tiga`: `764,015` rows / `18,781` proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM gwas
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'GWAS Catalog'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n, COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Target Illumination GWAS Analytics (TIGA)'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `gwas`: `124,149` rows / `13,116` proteins
- legacy `GWAS Catalog` map: `13,116` rows / `13,116` proteins
- legacy `TIGA` map: `18,005` rows / `18,005` proteins

Harmonizome validation:

```sql
-- pharos400
SHOW TABLES LIKE '%gene_attribute%';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT type) AS types
FROM hgram_cdf;

SELECT COUNT(*) AS rows_n
FROM gene_attribute_type;
```

Result:

- current Harmonizome tables matching `%gene_attribute%`: `gene_attribute_type`
- current `hgram_cdf`: `1,166,434` rows / `18,731` proteins / `110` types
- current `gene_attribute_type`: `113` rows

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT gat_id) AS types
FROM gene_attribute;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT type) AS types
FROM hgram_cdf;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Harmonizome'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `gene_attribute`: `65,549,760` rows / `18,789` proteins / `113` types
- legacy `hgram_cdf`: `1,167,880` rows / `18,789` proteins / `110` types
- legacy `Harmonizome` map: `18,789` rows / `18,789` proteins

Conclusion:

- Use `hgram_cdf` protein membership for `Harmonizome`.
- Do not recreate or query `gene_attribute` for datasource-map membership in
  `pharos400`; that table is intentionally skipped and absent from the new
  table model.

HGNC validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'HGNC'
  AND protein_id IS NOT NULL;

SELECT xtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype LIKE '%HGNC%'
GROUP BY xtype;
```

Result:

- current `xref.xtype = 'HGNC'`: `19,933` rows / `19,743` proteins
- no alternate current HGNC-like `xtype` values were found

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'HGNC'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'HGNC'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `xref.xtype = 'HGNC'`: `20,374` rows / `20,206` proteins
- legacy `HGNC` map: `20,206` rows / `20,206` proteins

Conclusion:

- Use distinct `protein_id` membership from `xref.xtype = 'HGNC'`.
- Do not insert one datasource-map row per `xref` row; the compatibility map
  should preserve the legacy distinct-protein behavior.

HomoloGene and Human Cell Atlas validation:

```sql
-- pharos400
SHOW TABLES LIKE 'homologene';
SHOW TABLES LIKE 'compartment';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype = 'HCA RNA'
  AND protein_id IS NOT NULL;

SELECT etype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype LIKE '%HCA%'
   OR etype LIKE '%Human Cell%'
GROUP BY etype;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'HomoloGene',
  'Human Cell Atlas Compartments',
  'Human Cell Atlas Expression'
)
GROUP BY dataSource;
```

Result:

- current `homologene` table: absent
- current `compartment` table: absent
- current `expression.etype = 'HCA RNA'`: `0` rows / `0` proteins
- current HCA-like expression `etype` values: none
- current datasource-map rows for all three labels: none

Legacy comparison:

```sql
-- pharos319
SHOW TABLES LIKE '%homolo%';
SHOW TABLES LIKE 'compartment';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype = 'HCA RNA'
  AND protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'HomoloGene',
  'Human Cell Atlas Compartments',
  'Human Cell Atlas Expression'
)
GROUP BY dataSource;
```

Result:

- legacy `homologene` table: absent from `pharos319`
- legacy `compartment` table: absent from `pharos319`
- legacy `expression.etype = 'HCA RNA'`: `0` rows / `0` proteins
- legacy `HomoloGene` map: `18,807` rows / `18,806` proteins
- legacy `Human Cell Atlas Compartments` map: `11,166` rows / `11,166`
  proteins
- legacy `Human Cell Atlas Expression` map: `19,070` rows / `19,070`
  proteins

Conclusion:

- Defer all three labels for `pharos400`.
- The old migration/source predicates are no longer backed by current or legacy
  exposed MySQL tables, so any replacement would need an explicit product
  decision and a new current-source mapping.

Human Protein Atlas expression and Human Proteome Map validation:

```sql
-- pharos400
SELECT etype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype LIKE 'HPA%'
GROUP BY etype
ORDER BY etype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype IN ('HPA Protein', 'HPA RNA')
  AND protein_id IS NOT NULL;

SELECT etype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype LIKE 'HPM%'
GROUP BY etype
ORDER BY etype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype IN ('HPM Protein', 'HPM Gene')
  AND protein_id IS NOT NULL;
```

Result:

- current `HPA Protein`: `976,676` rows / `11,066` proteins
- current `HPA RNA`: `793,600` rows / `19,654` proteins
- current HPA protein-or-RNA union: `1,770,276` rows / `19,654` proteins
- current `HPM Protein`: `871,590` rows / `16,736` proteins
- current `HPM Gene`: no rows

Legacy comparison:

```sql
-- pharos319
SELECT etype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype LIKE 'HPA%'
GROUP BY etype
ORDER BY etype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype IN ('HPM Protein', 'HPM Gene')
  AND protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'Human Protein Atlas',
  'Human Proteome Map'
)
GROUP BY dataSource;
```

Result:

- legacy `HPA Protein`: `1,006,268` rows / `11,023` proteins
- legacy `HPA RNA`: `4,887,454` rows / `19,203` proteins
- legacy `Human Protein Atlas` map: `10,513` rows / `10,513`
  proteins
- legacy `HPM Protein` / `HPM Gene` predicate: `497,010` rows / `16,567`
  proteins
- legacy `Human Proteome Map` map: `16,855` rows / `16,855` proteins

Conclusion:

- Replace the old combined `Human Protein Atlas` datasource-map label with two
  explicit labels: `Human Protein Atlas Protein` from `HPA Protein` rows and
  `Human Protein Atlas RNA` from `HPA RNA` rows.
- Use the legacy HPM predicate for `Human Proteome Map`; current rows only
  include `HPM Protein`.

IDG Eligible Targets List and IDG Families validation:

```sql
-- pharos400
SHOW TABLES LIKE 'ncats_idg_list';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'IDG Eligible Targets List'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT t2tc.protein_id) AS proteins
FROM target
JOIN t2tc ON t2tc.target_id = target.id
WHERE target.fam IS NOT NULL;

SELECT target.fam,
       COUNT(DISTINCT t2tc.protein_id) AS proteins
FROM target
JOIN t2tc ON t2tc.target_id = target.id
WHERE target.fam IS NOT NULL
GROUP BY target.fam
ORDER BY proteins DESC;
```

Result:

- current `ncats_idg_list` table: absent
- current `IDG Eligible Targets List` map: `0` rows / `0` proteins
- current IDG family query: `8,138` rows / `8,138` proteins
- current family breakdown:
  - `Enzyme`: `4,136`
  - `Transcription Factor`: `1,400`
  - `Kinase`: `634`
  - `Transporter`: `471`
  - `oGPCR`: `421`
  - `GPCR`: `406`
  - `Ion Channel`: `342`
  - `Epigenetic`: `242`
  - `Nuclear Receptor`: `48`
  - `TF-Epigenetic`: `38`

Legacy comparison:

```sql
-- pharos319
SHOW TABLES LIKE 'ncats_idg_list';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'IDG Eligible Targets List'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT t2tc.protein_id) AS proteins
FROM target
JOIN t2tc ON t2tc.target_id = target.id
WHERE target.fam IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'IDG Families'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `ncats_idg_list` table: absent from `pharos319`
- legacy `IDG Eligible Targets List` map: `1,301` rows / `1,301`
  proteins
- legacy IDG family query: `8,147` rows / `8,147` proteins
- legacy `IDG Families` map: `8,147` rows / `8,147` proteins

Conclusion:

- Defer `IDG Eligible Targets List`; the old narrow eligible-list source is not
  represented by a current table or graph signal.
- Use `target.fam IS NOT NULL` through `t2tc` for `IDG Families`.

IMPC and JAX/MGI mouse phenotype validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'IMPC Clones'
  AND protein_id IS NOT NULL;

SELECT ptype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS phenotype_proteins,
       COUNT(DISTINCT nhprotein_id) AS nhproteins
FROM phenotype
WHERE ptype LIKE '%IMPC%'
   OR ptype LIKE '%JAX%'
   OR ptype LIKE '%MGI%'
GROUP BY ptype
ORDER BY rows_n DESC;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein.id) AS proteins
FROM nhprotein
JOIN phenotype ON nhprotein.id = phenotype.nhprotein_id
JOIN ortholog ON ortholog.geneid = nhprotein.geneid
             AND ortholog.taxid = nhprotein.taxid
JOIN protein ON protein.id = ortholog.protein_id
WHERE phenotype.ptype = 'IMPC';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM phenotype
WHERE ptype = 'JAX/MGI Human Ortholog Phenotype'
  AND protein_id IS NOT NULL;
```

Result:

- current `IMPC Clones`: `0` rows / `0` proteins
- current `phenotype.ptype = 'IMPC'`: `60,126` rows / `7,604`
  proteins / `7,566` non-human proteins
- current legacy IMPC join: `61,062` rows / `7,604` proteins
- current `JAX/MGI Human Ortholog Phenotype`: `85,593` rows / `13,951`
  proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'IMPC Clones'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein.id) AS proteins
FROM nhprotein
JOIN phenotype ON nhprotein.id = phenotype.nhprotein_id
JOIN ortholog ON ortholog.geneid = nhprotein.geneid
             AND ortholog.taxid = nhprotein.taxid
JOIN protein ON protein.id = ortholog.protein_id
WHERE phenotype.ptype = 'IMPC';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM phenotype
WHERE ptype = 'JAX/MGI Human Ortholog Phenotype'
  AND protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'IMPC Mouse Clones',
  'IMPC Phenotypes',
  'JAX/MGI Mouse/Human Orthology Phenotypes'
)
GROUP BY dataSource;
```

Result:

- legacy `IMPC Clones`: `270` rows / `270` proteins
- legacy IMPC phenotype join: `135,493` rows / `5,787` proteins
- legacy `JAX/MGI Human Ortholog Phenotype`: `58,398` rows / `10,204`
  proteins
- legacy datasource-map rows:
  - `IMPC Mouse Clones`: `270` rows / `270` proteins
  - `IMPC Phenotypes`: `5,787` rows / `5,787` proteins
  - `JAX/MGI Mouse/Human Orthology Phenotypes`: `10,204` rows /
    `10,204` proteins

Conclusion:

- Defer `IMPC Mouse Clones`; the old clone summary signal is gone.
- Use current `phenotype.ptype = 'IMPC'` protein membership for `IMPC
  Phenotypes`.
- Use current `phenotype.ptype = 'JAX/MGI Human Ortholog Phenotype'` protein
  membership for `JAX/MGI Mouse/Human Orthology Phenotypes`.

JensenLab COMPARTMENTS validation:

```sql
-- pharos400
SHOW TABLES LIKE 'compartment';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'JensenLab COMPARTMENTS'
  AND protein_id IS NOT NULL;
```

Result:

- current `compartment` table: absent
- current `JensenLab COMPARTMENTS` map: `0` rows / `0` proteins

Legacy comparison:

```sql
-- pharos319
SHOW TABLES LIKE 'compartment';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'JensenLab COMPARTMENTS'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `compartment` table: absent from `pharos319`
- legacy `JensenLab COMPARTMENTS` map: `18,491` rows / `18,491`
  proteins

Conclusion:

- Defer `JensenLab COMPARTMENTS`; the current JensenLab ingest has TISSUES,
  DISEASES, and publication/text-mining signals, but no COMPARTMENTS
  subcellular-localization compatibility table.

JensenLab DISEASES validation:

```sql
-- pharos400
SELECT dtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype LIKE 'JensenLab%'
GROUP BY dtype
ORDER BY dtype;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource IN (
  'JensenLab Experiment TIGA',
  'JensenLab Knowledge AmyCo',
  'JensenLab Knowledge MedlinePlus',
  'JensenLab Knowledge UniProtKB-KW',
  'JensenLab Text Mining',
  'JensenLab Experiment COSMIC',
  'JensenLab Experiment DistiLD',
  'JensenLab Knowledge GHR'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- current `JensenLab Experiment TIGA`: `37,370` rows / `11,642`
  proteins / `279` diseases
- current `JensenLab Knowledge AmyCo`: `255` rows / `76` proteins / `75`
  diseases
- current `JensenLab Knowledge MedlinePlus`: `3,791` rows / `2,542`
  proteins / `1,001` diseases
- current `JensenLab Knowledge UniProtKB-KW`: `3,597` rows / `2,583`
  proteins / `119` diseases
- current `JensenLab Text Mining`: `7,790` rows / `2,770` proteins /
  `1,255` diseases
- current datasource-map rows exist only for:
  - `JensenLab Knowledge UniProtKB-KW`: `2,702` rows / `2,583` proteins /
    `119` diseases
  - `JensenLab Text Mining`: `4,025` rows / `2,770` proteins / `1,255`
    diseases

Legacy comparison:

```sql
-- pharos319
SELECT dtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype LIKE 'JensenLab%'
GROUP BY dtype
ORDER BY dtype;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource IN (
  'JensenLab Experiment TIGA',
  'JensenLab Knowledge AmyCo',
  'JensenLab Knowledge MedlinePlus',
  'JensenLab Knowledge UniProtKB-KW',
  'JensenLab Text Mining',
  'JensenLab Experiment COSMIC',
  'JensenLab Experiment DistiLD',
  'JensenLab Knowledge GHR'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- legacy `JensenLab Experiment TIGA`: `26,330` rows / `8,673` proteins /
  `286` diseases
- legacy `JensenLab Knowledge AmyCo`: `260` rows / `78` proteins / `75`
  diseases
- legacy `JensenLab Knowledge MedlinePlus`: `4,113` rows / `2,516`
  proteins / `993` diseases
- legacy `JensenLab Knowledge UniProtKB-KW`: `3,380` rows / `2,418`
  proteins / `118` diseases
- legacy `JensenLab Text Mining`: `7,404` rows / `2,785` proteins /
  `1,268` diseases
- legacy datasource-map rows exist only for:
  - `JensenLab Knowledge UniProtKB-KW`: `2,536` rows / `2,418` proteins /
    `118` diseases
  - `JensenLab Text Mining`: `4,053` rows / `2,785` proteins / `1,268`
    diseases

Conclusion:

- Use the actual current `disease.dtype` labels from JensenLab DISEASES.
- Defer old labels that are not materialized: `JensenLab Experiment COSMIC`,
  `JensenLab Experiment DistiLD`, and `JensenLab Knowledge GHR`.
- Add datasource-map coverage for current JensenLab labels even if the old
  compatibility map only populated `UniProtKB-KW` and `Text Mining`.

JensenLab TISSUES, JensenLab PubMed scores, and NCBI GeneRIFs validation:

```sql
-- pharos400
SELECT etype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype LIKE 'JensenLab%'
   OR etype LIKE 'JENSENLAB%'
GROUP BY etype
ORDER BY etype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'JensenLab PubMed Score'
  AND number_value > 0
  AND protein_id IS NOT NULL;

SELECT source,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM protein2pubmed
GROUP BY source
ORDER BY rows_n DESC;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM generif
WHERE protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'JensenLab TISSUES',
  'JensenLab PubMed Text-mining Scores',
  'NCBI GeneRIFs'
)
GROUP BY dataSource;
```

Result:

- current `JensenLab TISSUES`: `4,379,071` expression rows / `18,727`
  proteins
- current `JensenLab PubMed Score`: `18,733` rows / `18,733` proteins
- current `protein2pubmed.source = 'JensenLab'`: `46,132,625` rows /
  `18,733` proteins
- current `protein2pubmed.source = 'NCBI'`: `1,953,875` rows / `19,666`
  proteins
- current `generif`: `899,976` rows / `16,824` proteins
- current datasource-map rows:
  - `JensenLab TISSUES`: `18,727` rows / `18,727` proteins
  - `JensenLab PubMed Text-mining Scores`: `18,733` rows / `18,733`
    proteins
  - `NCBI GeneRIFs`: none

Legacy comparison:

```sql
-- pharos319
SELECT etype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM expression
WHERE etype LIKE 'JensenLab%'
   OR etype LIKE 'JENSENLAB%'
GROUP BY etype
ORDER BY etype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'JensenLab PubMed Score'
  AND number_value > 0
  AND protein_id IS NOT NULL;

SELECT source,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM protein2pubmed
GROUP BY source
ORDER BY rows_n DESC;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM generif
WHERE protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'JensenLab TISSUES',
  'JensenLab PubMed Text-mining Scores',
  'NCBI GeneRIFs'
)
GROUP BY dataSource;
```

Result:

- legacy `JensenLab TISSUES`: `4,173,249` expression rows / `17,973`
  proteins
- legacy `JensenLab TISSUES` map: `17,987` rows / `17,987` proteins
- legacy `JensenLab PubMed Score`: `19,052` rows / `19,052` proteins
- legacy `JensenLab PubMed Text-mining Scores` map: `19,052` rows /
  `19,052` proteins
- legacy `protein2pubmed.source = 'JensenLab'`: `41,854,850` rows /
  `17,992` proteins
- legacy `protein2pubmed.source = 'NCBI'`: `1,333,543` rows / `19,900`
  proteins
- legacy `generif`: `888,732` rows / `17,043` proteins
- legacy `NCBI GeneRIFs` datasource-map rows: none

Conclusion:

- Use exact `expression.etype = 'JensenLab TISSUES'` for JensenLab tissue
  expression membership.
- Keep the legacy `tdl_info` score predicate for `JensenLab PubMed
  Text-mining Scores`.
- Add `NCBI GeneRIFs` from `generif` as a new datasource-map label; do not use
  all `protein2pubmed.source = 'NCBI'` rows for this label because that also
  includes NCBI gene2pubmed links.

KEGG compatibility validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype = 'KEGG'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT pid1) AS pid1_proteins,
       COUNT(DISTINCT pid2) AS pid2_proteins
FROM kegg_distance;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT tclin_id) AS tclins
FROM kegg_nearest_tclin;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'KEGG Pathways',
  'KEGG Distances',
  'KEGG Nearest Tclins'
)
GROUP BY dataSource;
```

Result:

- current `pathway.pwtype = 'KEGG'`: `0` rows / `0` proteins
- current `kegg_distance`: `0` rows
- current `kegg_nearest_tclin`: `0` rows
- current datasource-map rows for KEGG labels: none

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype = 'KEGG'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT pid1) AS pid1_proteins,
       COUNT(DISTINCT pid2) AS pid2_proteins
FROM kegg_distance;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT tclin_id) AS tclins
FROM kegg_nearest_tclin;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'KEGG Pathways',
  'KEGG Distances',
  'KEGG Nearest Tclins'
)
GROUP BY dataSource;
```

Result:

- legacy `KEGG Pathways`: `32,325` pathway rows / `7,686` proteins
- legacy `kegg_distance`: `208,238` rows / `4,323` `pid1` proteins /
  `4,071` `pid2` proteins
- legacy `kegg_nearest_tclin`: `15,911` rows / `2,574` proteins / `403`
  Tclin targets
- legacy datasource-map rows:
  - `KEGG Pathways`: `7,686` rows / `7,686` proteins
  - `KEGG Distances`: `4,896` rows / `4,896` proteins
  - `KEGG Nearest Tclins`: `2,977` rows / `2,977` proteins

Conclusion:

- Defer all KEGG compatibility labels for `pharos400`.
- Do not populate KEGG labels from Reactome or other pathway-distance
  replacements unless the product intentionally changes the datasource
  semantics.

LINCS, LocSigDB, and Monarch validation:

```sql
-- pharos400
SHOW TABLES WHERE Tables_in_pharos400 IN (
  'lincs',
  'locsig',
  'ortholog_disease'
);

SELECT xtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype LIKE '%L1000%'
   OR xtype LIKE '%LINCS%'
GROUP BY xtype;

SELECT dtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype LIKE '%Monarch%'
GROUP BY dtype;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource IN (
  'LINCS',
  'LINCS L1000 XRefs',
  'LocSigDB',
  'Monarch',
  'Monarch Ortholog Disease Associations'
)
GROUP BY dataSource;
```

Result:

- current `lincs`, `locsig`, and `ortholog_disease` tables: absent
- current L1000/LINCS `xref.xtype` rows: none
- current Monarch-like `disease.dtype` rows: none
- current datasource-map rows for these labels: none

Legacy comparison:

```sql
-- pharos319
SHOW TABLES WHERE Tables_in_pharos319 IN (
  'lincs',
  'locsig',
  'ortholog_disease'
);

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM lincs
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'L1000 ID'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM locsig
WHERE protein_id IS NOT NULL;

SELECT dtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'Monarch'
GROUP BY dtype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT name) AS diseases
FROM ortholog_disease
WHERE protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource IN (
  'LINCS',
  'LINCS L1000 XRefs',
  'LocSigDB',
  'Monarch',
  'Monarch Ortholog Disease Associations'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- legacy `lincs`: `84,097,720` rows / `980` proteins
- legacy `LINCS` map: `980` rows / `980` proteins
- legacy `L1000 ID` xrefs: `978` rows / `978` proteins
- legacy `LINCS L1000 XRefs` map: `978` rows / `978` proteins
- legacy `locsig`: `106,521` rows / `18,916` proteins
- legacy `LocSigDB` map: `18,916` rows / `18,916` proteins
- legacy `Monarch` disease rows: `9,509` rows / `3,825` proteins /
  `5,096` diseases
- legacy `Monarch` map: `8,921` rows / `3,825` proteins / `5,096`
  diseases
- legacy `ortholog_disease`: `37,852` rows / `3,827` proteins / `5,614`
  diseases
- legacy `Monarch Ortholog Disease Associations` map: `9,441` rows /
  `3,827` proteins / `5,614` diseases

Conclusion:

- Defer all five labels for `pharos400`.
- Do not repurpose current Monarch aggregate data as the legacy `Monarch`
  datasource; direct source-specific disease associations should be handled
  separately if restored.

NCBI Gene validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'NCBIGene'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM alias
WHERE type = 'NCBI Gene ID'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'NCBI Gene Summary'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM protein2pubmed
WHERE source = 'NCBI'
  AND protein_id IS NOT NULL;

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id FROM xref WHERE xtype = 'NCBIGene'
  UNION
  SELECT protein_id FROM alias WHERE type = 'NCBI Gene ID'
  UNION
  SELECT protein_id FROM tdl_info WHERE itype = 'NCBI Gene Summary'
  UNION
  SELECT protein_id FROM protein2pubmed WHERE source = 'NCBI'
) x
WHERE protein_id IS NOT NULL;
```

Result:

- current `xref.xtype = 'NCBIGene'`: `20,007` rows / `19,745` proteins
- current `alias.type = 'NCBI Gene ID'`: `20,007` rows / `19,745`
  proteins
- current `NCBI Gene Summary`: `19,198` rows / `19,198` proteins
- current `protein2pubmed.source = 'NCBI'`: `1,953,875` rows / `19,666`
  proteins
- current union across NCBI Gene ID, summary, and NCBI publications: `19,745`
  proteins
- current `NCBI Gene` datasource-map rows: `19,745` rows / `19,745`
  proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM alias
WHERE dataset_id = 7
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'NCBI Gene Summary'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'NCBI Gene PubMed Count'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'PubMed'
  AND protein_id IS NOT NULL;

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id FROM alias WHERE dataset_id = 7
  UNION
  SELECT protein_id FROM tdl_info WHERE itype = 'NCBI Gene Summary'
  UNION
  SELECT protein_id FROM tdl_info WHERE itype = 'NCBI Gene PubMed Count'
  UNION
  SELECT protein_id FROM xref WHERE xtype = 'PubMed'
) x
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'NCBI Gene'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `alias.dataset_id = 7`: `53,920` rows / `17,344` proteins
- legacy `NCBI Gene Summary`: `12,900` rows / `12,900` proteins
- legacy `NCBI Gene PubMed Count`: `20,153` rows / `20,153` proteins
- legacy `xref.xtype = 'PubMed'`: `1,205,815` rows / `20,044` proteins
- legacy old union: `20,153` proteins
- legacy `NCBI Gene` map: `20,153` rows / `20,153` proteins

Conclusion:

- Use `xref.xtype = 'NCBIGene'` for current `NCBI Gene` datasource-map
  membership.
- Publication rows remain part of the NCBI evidence surface, but in
  `pharos400` they do not expand protein coverage beyond the NCBI Gene ID
  xrefs. Keep `NCBI GeneRIFs` as a separate GeneRIF-specific label.

NCBI GI Numbers validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'NCBI GI'
  AND protein_id IS NOT NULL;

SELECT xtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype LIKE '%GI%'
   OR xtype LIKE '%NCBI%'
GROUP BY xtype
ORDER BY rows_n DESC;
```

Result:

- current `xref.xtype = 'NCBI GI'`: `0` rows / `0` proteins
- only current NCBI-like xref in this search is `NCBIGene`: `20,007` rows /
  `19,745` proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'NCBI GI'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'NCBI GI Numbers'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `xref.xtype = 'NCBI GI'`: `257,171` rows / `20,402` proteins
- legacy `NCBI GI Numbers` map: `20,402` rows / `20,402` proteins

Conclusion:

- Defer `NCBI GI Numbers`; GI identifiers are not present in current xrefs.
- Do not backfill from `NCBIGene`; gene IDs and GI sequence identifiers are
  different identifier families.

OMIM validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM phenotype
WHERE ptype = 'OMIM'
  AND protein_id IS NOT NULL;

SELECT ptype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM phenotype
WHERE ptype LIKE '%OMIM%'
GROUP BY ptype;

SHOW TABLES WHERE Tables_in_pharos400 IN ('omim', 'omim_ps');

SELECT COUNT(*) AS rows_n
FROM omim;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'OMIM';
```

Result:

- current `phenotype.ptype = 'OMIM'`: `0` rows / `0` proteins
- current OMIM-like phenotype `ptype` values: none
- current `omim` table exists but has `0` rows
- current `omim_ps` table: absent
- current `OMIM` datasource-map rows: none

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM phenotype
WHERE ptype = 'OMIM'
  AND protein_id IS NOT NULL;

SELECT ptype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM phenotype
WHERE ptype LIKE '%OMIM%'
GROUP BY ptype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'OMIM';
```

Result:

- legacy `phenotype.ptype = 'OMIM'`: `14,147` rows / `13,856` proteins
- legacy `OMIM` map: `13,856` rows / `13,856` proteins / `0` diseases

Conclusion:

- Defer the old `OMIM` datasource-map label for `pharos400`.
- Do not use OMIM disease IDs or disease xrefs as a replacement; the legacy
  label was populated from OMIM phenotype rows, and old Pharos did not treat it
  as a disease-association datasource.

Orthologs validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ortholog
WHERE protein_id IS NOT NULL;

SELECT taxid,
       species,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ortholog
GROUP BY taxid, species
ORDER BY rows_n DESC
LIMIT 20;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Orthologs'
  AND protein_id IS NOT NULL;
```

Result:

- current `ortholog`: `164,732` rows / `18,055` proteins
- current `Orthologs` map: `18,055` rows / `18,055` proteins
- top current species include mouse, cow, chimp, dog, horse, pig, opossum, rat,
  Xenopus, and zebrafish

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ortholog
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'Orthologs'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `ortholog`: `178,759` rows / `18,056` proteins
- legacy `Orthologs` map: `18,056` rows / `18,056` proteins

Conclusion:

- Use `SELECT DISTINCT protein_id FROM ortholog` for `Orthologs`.
- Keep this separate from dead or deferred ortholog-adjacent labels such as
  `HomoloGene` and `Monarch Ortholog Disease Associations`.

P-HIPSTer, PANTHER, and PathwayCommons validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM viral_ppi
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM p2pc
WHERE protein_id IS NOT NULL;

SELECT pwtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype LIKE 'PathwayCommons%'
GROUP BY pwtype
ORDER BY pwtype;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'P-HIPSTer Viral PPIs',
  'PANTHER Protein Classes',
  'PathwayCommons'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- current `viral_ppi`: `280,601` rows / `5,634` proteins
- current `P-HIPSTer Viral PPIs` map: `5,634` rows / `5,634` proteins
- current `p2pc`: `21,894` rows / `13,949` proteins
- current `PANTHER Protein Classes` map: `13,949` rows / `13,949`
  proteins
- current `PathwayCommons`: `44,765` pathway rows / `5,990` proteins
- current `PathwayCommons` map: `5,990` rows / `5,990` proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM viral_ppi
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM p2pc
WHERE protein_id IS NOT NULL;

SELECT pwtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype LIKE 'PathwayCommons%'
GROUP BY pwtype
ORDER BY pwtype;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'P-HIPSTer Viral PPIs',
  'PANTHER Protein Classes',
  'PathwayCommons'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- legacy `viral_ppi`: `280,585` rows / `5,719` proteins
- legacy `P-HIPSTer Viral PPIs` map: `5,720` rows / `5,719`
  proteins
- legacy `p2pc`: `22,520` rows / `8,070` proteins
- legacy `PANTHER Protein Classes` map: `8,070` rows / `8,070`
  proteins
- legacy PathwayCommons subtype rows total: `25,066` rows / `5,001`
  proteins
- legacy `PathwayCommons` map: `5,001` rows / `5,001` proteins

Conclusion:

- Use `viral_ppi`, `p2pc`, and `pathway.pwtype LIKE 'PathwayCommons%'`
  directly for these three labels.
- PathwayCommons subtype labels collapsed in current output, but the legacy
  `LIKE` predicate remains compatible.

PubChem CIDs validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT id) AS ligands
FROM ncats_ligands
WHERE PubChem IS NOT NULL
  AND PubChem != '';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT ncats_ligand_id) AS ligands
FROM ncats_dataSource_map
WHERE dataSource = 'PubChem CIDs'
  AND ncats_ligand_id IS NOT NULL;
```

Result:

- current ligands with explicit PubChem IDs: `405,567`
- current `PubChem CIDs` map: `591,532` rows / `591,532` ligands

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT id) AS ligands
FROM ncats_ligands
WHERE PubChem IS NOT NULL
  AND PubChem != '';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT ncats_ligand_id) AS ligands
FROM ncats_dataSource_map
WHERE dataSource = 'PubChem CIDs'
  AND ncats_ligand_id IS NOT NULL;
```

Result:

- legacy ligands with explicit PubChem IDs: `329,851`
- legacy `PubChem CIDs` map: `355,932` rows / `355,932` ligands

Conclusion:

- Use explicit `ncats_ligands.PubChem` identifier membership for `PubChem CIDs`.
- Do not copy the inflated current or legacy datasource-map counts; they exceed
  the explicit PubChem identifier coverage.

PubMed and PubTator validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM protein2pubmed
WHERE protein_id IS NOT NULL;

SELECT source,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM protein2pubmed
GROUP BY source
ORDER BY rows_n DESC;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       MIN(number_value) AS min_score,
       MAX(number_value) AS max_score
FROM tdl_info
WHERE itype = 'PubTator Score'
  AND protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('PubMed', 'PubTator Text-mining Scores')
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- current `protein2pubmed`: `48,086,500` rows / `19,714` proteins
- current `protein2pubmed.source = 'JensenLab'`: `46,132,625` rows /
  `18,733` proteins
- current `protein2pubmed.source = 'NCBI'`: `1,953,875` rows / `19,666`
  proteins
- current `PubMed` map: `19,714` rows / `19,714` proteins
- current `PubTator Score`: `19,692` rows / `19,692` proteins
- current `PubTator Text-mining Scores` map: `19,692` rows / `19,692`
  proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM protein2pubmed
WHERE protein_id IS NOT NULL;

SELECT source,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM protein2pubmed
GROUP BY source
ORDER BY rows_n DESC;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       MIN(number_value) AS min_score,
       MAX(number_value) AS max_score
FROM tdl_info
WHERE itype = 'PubTator Score'
  AND protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('PubMed', 'PubTator Text-mining Scores')
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- legacy `protein2pubmed`: `43,188,393` rows / `20,007` proteins
- legacy `protein2pubmed.source = 'JensenLab'`: `41,854,850` rows /
  `17,992` proteins
- legacy `protein2pubmed.source = 'NCBI'`: `1,333,543` rows / `19,900`
  proteins
- legacy `PubMed` map: `19,790` rows / `19,790` proteins
- legacy `PubTator Score`: `18,310` rows / `18,310` proteins
- legacy `PubTator Text-mining Scores` map: `18,310` rows / `18,310`
  proteins

Conclusion:

- Use `protein2pubmed` for broad `PubMed` membership.
- Use `tdl_info.itype = 'PubTator Score'` for `PubTator Text-mining Scores`.

Reactome validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype = 'Reactome'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS protein1s,
       COUNT(DISTINCT other_id) AS protein2s
FROM ncats_ppi
WHERE ppitypes LIKE '%Reactome%';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%Reactome%'
  UNION
  SELECT other_id AS protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%Reactome%'
) x
WHERE protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'Reactome Pathways',
  'Reactome Protein-Protein Interactions'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- current `Reactome` pathway rows: `133,822` rows / `11,332` proteins
- current Reactome `ncats_ppi`: `43,696` rows / `5,002` first-endpoint
  proteins / `5,002` second-endpoint proteins
- current Reactome PPI endpoint union: `5,002` proteins
- current datasource-map rows:
  - `Reactome Pathways`: `11,332` rows / `11,332` proteins
  - `Reactome Protein-Protein Interactions`: `10,004` rows / `5,002`
    proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype = 'Reactome'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS protein1s,
       COUNT(DISTINCT other_id) AS protein2s
FROM ncats_ppi
WHERE ppitypes LIKE '%Reactome%';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%Reactome%'
  UNION
  SELECT other_id AS protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%Reactome%'
) x
WHERE protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN (
  'Reactome Pathways',
  'Reactome Protein-Protein Interactions'
)
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- legacy `Reactome` pathway rows: `110,872` rows / `10,781` proteins
- legacy Reactome `ncats_ppi`: `36,474` rows / `4,465` first-endpoint
  proteins / `4,465` second-endpoint proteins
- legacy Reactome PPI endpoint union: `4,465` proteins
- legacy datasource-map rows:
  - `Reactome Pathways`: `10,781` rows / `10,781` proteins
  - `Reactome Protein-Protein Interactions`: `4,465` rows / `4,465`
    proteins

Conclusion:

- Use `pathway.pwtype = 'Reactome'` for Reactome pathway membership.
- Use `ncats_ppi.ppitypes LIKE '%Reactome%'` endpoint union for Reactome PPI
  membership; the old `ppi` table is not present in the current materialized
  schema.

RGD validation:

```sql
-- pharos400
SHOW TABLES WHERE Tables_in_pharos400 IN (
  'rat_qtl',
  'nhprotein',
  'ortholog',
  'protein'
);

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'RGD'
  AND protein_id IS NOT NULL;
```

Result:

- current `rat_qtl` table: absent
- current `nhprotein`, `ortholog`, and `protein` tables: present
- current `RGD` datasource-map rows: `0` rows / `0` proteins

Legacy comparison:

```sql
-- pharos319
SHOW TABLES WHERE Tables_in_pharos319 IN (
  'rat_qtl',
  'nhprotein',
  'ortholog',
  'protein'
);

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'RGD'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `rat_qtl` table: absent from exposed `pharos319`
- legacy `RGD` map: `431` rows / `431` proteins

Conclusion:

- Defer `RGD`; the old rat-QTL-derived source is not represented in current
  `pharos400`.
- Do not infer this from HCOP rat ortholog records or RGD-prefixed ortholog
  IDs.

STRING validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT id) AS proteins,
       COUNT(DISTINCT stringid) AS stringids
FROM protein
WHERE stringid IS NOT NULL
  AND stringid != '';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS protein1s,
       COUNT(DISTINCT other_id) AS protein2s
FROM ncats_ppi
WHERE ppitypes LIKE '%STRING%';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%STRING%'
  UNION
  SELECT other_id AS protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%STRING%'
) x
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('STRING IDs', 'STRINGDB')
  AND protein_id IS NOT NULL;
```

Result:

- current `protein.stringid`: `20` rows / `20` proteins / `20` string IDs
- sampled current `stringid` values are Ensembl protein IDs such as
  `ENSP00000413121`
- current STRING `ncats_ppi`: `1,769,760` rows / `18,718` first-endpoint
  proteins / `18,718` second-endpoint proteins
- current STRING PPI endpoint union: `18,718` proteins
- current `STRING IDs` map: `20` rows / `20` proteins
- current `STRINGDB` map: no rows yet

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT id) AS proteins,
       COUNT(DISTINCT stringid) AS stringids
FROM protein
WHERE stringid IS NOT NULL
  AND stringid != '';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS protein1s,
       COUNT(DISTINCT other_id) AS protein2s
FROM ncats_ppi
WHERE ppitypes LIKE '%STRING%';

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%STRING%'
  UNION
  SELECT other_id AS protein_id
  FROM ncats_ppi
  WHERE ppitypes LIKE '%STRING%'
) x
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('STRING IDs', 'STRINGDB')
  AND protein_id IS NOT NULL;
```

Result:

- legacy `protein.stringid`: `19,121` rows / `19,121` proteins / `18,960`
  string IDs
- legacy STRING `ncats_ppi`: `11,600,496` rows / `19,057` first-endpoint
  proteins / `19,057` second-endpoint proteins
- legacy STRING PPI endpoint union: `19,057` proteins
- legacy `STRING IDs` map: `19,121` rows / `19,121` proteins
- legacy `STRINGDB` map: no rows in the checked compatibility map

Conclusion:

- Do not use `protein.stringid` as broad STRING membership in `pharos400`; it
  is only 20 proteins and is an identifier alias. The TCRD converter currently
  populates it from the first `ENSEMBL:ENSP...` protein xref only when no NCBI
  Gene ID is present.
- Use the `STRINGDB` label with STRING PPI endpoint union for actual STRING
  source contribution.

TIN-X validation:

```sql
-- pharos400
SHOW TABLES WHERE Tables_in_pharos400 IN (
  'tinx_novelty',
  'tinx_importance',
  'tinx_disease'
);

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_disease_id) AS diseases
FROM tinx_importance
WHERE protein_id IS NOT NULL;

SELECT COUNT(DISTINCT d.name) AS diseases
FROM tinx_importance ti
JOIN ncats_disease d ON d.id = ti.ncats_disease_id;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'TIN-X Data';
```

Result:

- current TIN-X tables: `tinx_importance`
- current `tinx_importance`: `6,159,643` rows / `18,717` proteins /
  `5,168` disease IDs
- current disease names reached through `ncats_disease`: `5,167`
- current `TIN-X Data` map: `18,717` rows / `18,717` proteins / `0`
  diseases

Legacy comparison:

```sql
-- pharos319
SHOW TABLES WHERE Tables_in_pharos319 IN (
  'tinx_novelty',
  'tinx_importance',
  'tinx_disease'
);

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT doid) AS diseases
FROM tinx_importance
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT name) AS diseases
FROM tinx_disease
WHERE name IS NOT NULL;

SELECT COUNT(DISTINCT protein_id) AS proteins
FROM (
  SELECT protein_id FROM tinx_novelty
  UNION
  SELECT protein_id FROM tinx_importance
) x
WHERE protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'TIN-X Data';
```

Result:

- legacy TIN-X tables: `tinx_novelty`, `tinx_importance`, `tinx_disease`
- legacy `tinx_importance`: `13,800,890` rows / `18,949` proteins /
  `8,508` diseases
- legacy `tinx_disease`: `8,960` rows / `8,960` diseases
- legacy novelty-or-importance protein union: `18,982` proteins
- legacy `TIN-X Data` map: `27,942` rows / `18,982` proteins / `8,960`
  diseases

Conclusion:

- Use distinct foreign keys from current `tinx_importance` for `TIN-X Data`.
- Do not reference old `tinx_novelty` or `tinx_disease` in current rules; the
  schema has been simplified.

TMHMM and transcription-factor flag validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'TMHMM Prediction'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'Is Transcription Factor'
  AND protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('TMHMM Predictions', 'Transcription Factor Flags')
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- current `TMHMM Prediction`: `0` rows / `0` proteins
- current `Is Transcription Factor`: `0` rows / `0` proteins
- current datasource-map rows for both labels: none

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'TMHMM Prediction'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM tdl_info
WHERE itype = 'Is Transcription Factor'
  AND protein_id IS NOT NULL;

SELECT dataSource,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource IN ('TMHMM Predictions', 'Transcription Factor Flags')
GROUP BY dataSource
ORDER BY dataSource;
```

Result:

- legacy `TMHMM Prediction`: `5,350` rows / `5,350` proteins
- legacy `TMHMM Predictions` map: `5,350` rows / `5,350` proteins
- legacy `Is Transcription Factor`: `1,632` rows / `1,630` proteins
- legacy `Transcription Factor Flags` map: `1,630` rows / `1,630`
  proteins

Conclusion:

- Defer both labels for `pharos400`; their legacy `tdl_info` summary rows are
  not populated.

UniProt validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'UniProt Keyword'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT id) AS proteins,
       COUNT(DISTINCT uniprot) AS uniprots
FROM protein
WHERE uniprot IS NOT NULL
  AND uniprot != '';

SELECT xtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype LIKE '%UniProt%'
GROUP BY xtype
ORDER BY rows_n DESC;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'UniProt'
  AND protein_id IS NOT NULL;
```

Result:

- current `UniProt Keyword` xrefs: `229,440` rows / `20,419` proteins
- current `protein.uniprot`: `20,654` rows / `20,654` proteins /
  `20,654` UniProt IDs
- current `xref.xtype = 'UniProtKB'`: `265,596` rows / `20,654` proteins
- current `UniProt` map: `41,073` rows / `20,654` proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM xref
WHERE xtype = 'UniProt Keyword'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT id) AS proteins,
       COUNT(DISTINCT uniprot) AS uniprots
FROM protein
WHERE uniprot IS NOT NULL
  AND uniprot != '';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'UniProt'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `UniProt Keyword` xrefs: `230,530` rows / `20,412` proteins
- legacy `protein.uniprot`: `20,412` rows / `20,412` proteins /
  `20,412` UniProt IDs
- legacy `UniProt` map: `20,412` rows / `20,412` proteins

Conclusion:

- Use `protein.uniprot` for UniProt datasource membership.
- Do not use `UniProt Keyword` xrefs as the source rule; keywords are an
  annotation signal and no longer cover every UniProt-backed protein.

UniProt Disease validation:

```sql
-- pharos400
SELECT dtype,
       COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype LIKE '%UniProt%'
GROUP BY dtype
ORDER BY dtype;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'UniProt Disease';
```

Result:

- current `disease.dtype = 'UniProt Disease'`: no rows
- current `disease.dtype = 'UniProt'`: `7,143` rows / `4,903` proteins /
  `6,680` diseases
- current `UniProt Disease` datasource-map rows: none

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT ncats_name) AS diseases
FROM disease
WHERE dtype = 'UniProt Disease';

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins,
       COUNT(DISTINCT disease_name) AS diseases
FROM ncats_dataSource_map
WHERE dataSource = 'UniProt Disease';
```

Result:

- legacy `UniProt Disease`: `5,651` rows / `3,766` proteins / `4,972`
  diseases
- legacy `UniProt Disease` map: `8,738` rows / `3,766` proteins / `4,972`
  diseases

Conclusion:

- Use current `disease.dtype = 'UniProt'` for the `UniProt Disease`
  datasource-map label.
- The current source label changed in the materialized disease table, but the
  source semantics still match UniProt curated disease associations.

WikiPathways validation:

```sql
-- pharos400
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype = 'WikiPathways'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'WikiPathways'
  AND protein_id IS NOT NULL;
```

Result:

- current `WikiPathways` pathway rows: `39,426` rows / `8,708` proteins
- current `WikiPathways` map: `8,708` rows / `8,708` proteins

Legacy comparison:

```sql
-- pharos319
SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM pathway
WHERE pwtype = 'WikiPathways'
  AND protein_id IS NOT NULL;

SELECT COUNT(*) AS rows_n,
       COUNT(DISTINCT protein_id) AS proteins
FROM ncats_dataSource_map
WHERE dataSource = 'WikiPathways'
  AND protein_id IS NOT NULL;
```

Result:

- legacy `WikiPathways` pathway rows: `162,449` rows / `6,411` proteins
- legacy `WikiPathways` map: `6,411` rows / `6,411` proteins

Conclusion:

- Use `pathway.pwtype = 'WikiPathways'` for WikiPathways membership.
