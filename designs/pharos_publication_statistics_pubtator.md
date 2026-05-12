# Pharos Publication Statistics: PubTator

## Status

- Implemented
- Promoted to Pharos graph YAMLs
- Exported through TCRD MySQL conversion

## Source

- Bulk file: `input_files/auto/pubtator/gene2pubtator3.gz`
- README: `input_files/auto/pubtator/README.txt`
- Version metadata: `input_files/auto/pubtator/pubtator_version.tsv`
- Pub year lookup source: local PubMed mirror `ifx_pubmed.pubmed`

## What Landed

### Download and versioning

- Snakemake rule in `workflows/pharos.Snakefile` downloads:
  - `gene2pubtator3.gz`
  - `README.txt`
  - `pubtator_version.tsv`

### Graph model

- `Gene.pt_score`
- `Gene.pt_score_by_year`
- `Protein.pt_score`
- `Protein.pt_score_by_year`
- shared yearly record structure in `src/models/year_score.py`

### Adapter

- `src/input_adapters/pubtator/publication_statistics.py`
- adapter class: `PubTatorPublicationStatisticsAdapter`
- datasource name: `PubTator`

### YAML wiring

- `src/use_cases/pharos/target_graph.yaml`
- `src/use_cases/pharos/pharos.yaml`

### MySQL export

- yearly PubTator rows export to `ptscore`
- aggregate PubTator total exports to `tdl_info`
  - `itype = 'PubTator Score'`

## Implemented Semantics

### Input interpretation

- use only `PMID` and `NCBIGene` from PubTator
- do not use `mentions` text for weighting or cleanup
- do not attempt to repair PubTator normalization errors in the first pass

### Score calculation

For each PMID:

1. count PubTator rows per `NCBIGene`
2. compute fractional contribution:
   - `gene_row_count_in_pmid / total_pubtator_gene_rows_in_pmid`
3. join PMID to `pub_year` through the local PubMed mirror
4. add the fractional score to `(gene_id, pub_year)`

Then:

- `pt_score_by_year` stores yearly fractional totals
- `pt_score` stores the aggregate total across years

## Important Observations

- `gene2pubtator3.gz` is a single gene/protein-normalized stream exposed as `Type = Gene`
- the file does not distinguish separate `Gene` versus `Protein` entity types
- sampled rows showed that PubTator normalization is noisy for ambiguous symbols
- first-pass implementation intentionally accepts that noise rather than trying to filter or repair it

## Validation Outcome

Validated on a partial working run:

- graph-side `Protein.pt_score` and `Protein.pt_score_by_year` landed in `test_pharos`
- MySQL export wrote:
  - `ptscore` yearly rows
  - `tdl_info` aggregate rows with `itype = 'PubTator Score'`

## Files Touched

- `workflows/pharos.Snakefile`
- `src/models/gene.py`
- `src/models/protein.py`
- `src/models/year_score.py`
- `src/input_adapters/pubtator/publication_statistics.py`
- `src/output_adapters/sql_converters/tcrd.py`
- `src/output_adapters/mysql_output_adapter.py`
- `src/use_cases/pharos/target_graph.yaml`
- `src/use_cases/pharos/pharos.yaml`

## Out Of Scope

- publication nodes
- publication edges
- PubTator API usage during ETL
- normalization cleanup of ambiguous gene/protein names
- exact reproduction of legacy mention-text weighting beyond what current row counts support
