# Pharos Publication Property Model, May 2026

## Goal

Simplify Pharos publication ingest by:

- removing `GeneRif` as a graph node
- removing `GeneGeneRifEdge` as a graph edge
- storing publications directly on `Gene` / `Protein`
- keeping GeneRIF text and GeneRIF date together
- preserving enough structure to populate legacy MySQL tables:
  - `protein2pubmed`
  - `generif`
  - `generif2pubmed`

This design is now implemented and validated in the working graph and working MySQL build.

## Final Model

### `PublicationReference`

One publication association per `(pmid, source, gene_id)`.

Fields:

- `pmid: str`
- `source: str`
- `gene_id: Optional[int] = None`
- `gene_rifs: Optional[List[GeneRifAnnotation]] = None`

### `GeneRifAnnotation`

One GeneRIF statement attached to a publication association.

Fields:

- `text: str`
- `updated_at: Optional[datetime] = None`

### Node fields

Both classes support:

- `publications: Optional[List[PublicationReference]] = None`

In practice:

- NCBI emits source-native publication updates on `Gene`
- JensenLab emits source-native publication updates on `Protein`

## Why This Shape

The important correction from the earlier draft is that GeneRIF date does not belong on the outer publication record.

`pharos319` kept GeneRIF text and date together. If multiple GeneRIF statements land on the same `(gene_id, pmid)` publication association, storing one publication-level `updated_at` is misleading.

So the correct shape is:

- publication association outside
- GeneRIF statements inside

This preserves the real semantics:

- `protein2pubmed` is publication-centric
- `generif` is statement-centric
- `generif2pubmed` links statements to supporting PMIDs

## Sources

### NCBI

Raw downloads:

- `https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2pubmed.gz`
- `https://ftp.ncbi.nlm.nih.gov/gene/GeneRIF/generifs_basic.gz`

Current workflow behavior:

- Snakemake downloads the raw `.gz` files and version metadata only
- the adapter reads the gz files directly
- human filtering (`9606`) happens in the adapter

This is better than trying to pre-filter the files in Snakemake because `gene2pubmed.gz` has very wide fields and is easier to handle in adapter code.

### JensenLab

Raw publication source:

- `input_files/auto/jensenlab/human_textmining_mentions.tsv`

Behavior:

- keep ENSP-backed rows
- skip non-protein identifiers like `18S_rRNA`
- emit one JensenLab publication association per PMID

## Merge Rules

### Publication identity

Within a node, publications dedupe by:

- `pmid`
- `source`
- `gene_id`

This means overlap between:

- `gene2pubmed`
- GeneRIF-backed NCBI PMIDs

collapses into one NCBI publication object, with optional `gene_rifs` enrichment attached.

### GeneRIF identity

Within one `PublicationReference`, GeneRIF annotations dedupe by:

- `text`

If the same GeneRIF text appears again for the same publication association:

- keep one annotation
- keep `updated_at = max(updated_at)`

### Protein-level TDL counting

`tdl_generif_count` should count unique GeneRIF statements, not PMIDs.

The current property-backed count uses unique:

- `(gene_id, normalized GeneRIF text)`

This matches the old intent well and compares closely to current `pharos` values.

## Resolver Behavior

No resolver behavior changes were made.

That turned out to be the right call.

Observed working behavior in `working.yaml` / `test_pharos`:

- JensenLab emits `Protein.publications`
- NCBI emits `Gene.publications`
- `TCRDTargetResolver` with `canonical_type: Protein` retypes the NCBI gene-side payload onto proteins
- Arango merge combines those publication lists on the final protein nodes

So no explicit new side-lift code was needed for the first pass.

## Implemented Adapters

### `NCBIPublicationAdapter`

Current behavior:

- reads `gene2pubmed.gz`
- reads `generifs_basic.gz`
- filters to `9606`
- builds one NCBI publication map keyed by `(gene_id, pmid)`
- enriches those publication records with `gene_rifs`
- emits `Gene(publications=[...])`

Location:

- `src/input_adapters/ncbi/publications.py`

### `JensenLabPublicationAdapter`

Current behavior:

- reads `human_textmining_mentions.tsv`
- emits `Protein(publications=[...])`

Location:

- `src/input_adapters/jensenlab/publications.py`

## MySQL Mapping

### `protein2pubmed`

Emit one row per `PublicationReference`.

Expected semantics:

- all NCBI PMIDs go here
- all JensenLab PMIDs go here
- GeneRIF-backed NCBI PMIDs also go here

So GeneRIF PMIDs appear in:

- `protein2pubmed`
- `generif2pubmed`

That is correct because those tables answer different questions.

### `generif`

Expand from `PublicationReference.gene_rifs`.

One row per:

- protein
- gene_id
- GeneRIF text
- GeneRIF date

### `generif2pubmed`

Link each expanded GeneRIF row back to the parent publication PMID.

## Validation Results

### Working graph: `test_pharos`

Observed after rebuild:

- `146` proteins with any `publications`
- `98` proteins with JensenLab publications
- `48` proteins with NCBI publications
- `48` proteins with GeneRIF-bearing publications
- `4675` publication records carrying `gene_rifs`

Sample payload shape on `Protein.publications`:

- `pmid`
- `source='NCBI'`
- `gene_id`
- `gene_rifs=[{text, updated_at}]`

This confirms:

- the publication-centric model works
- GeneRIF text/date pairing is preserved
- existing resolver/retype behavior is enough to land NCBI publications on proteins

### TDL comparison against current `pharos`

A sample comparison of old `pharos.tdl_meta.tdl_generif_count` against the new publication-derived count in `test_pharos` showed values that were usually identical and otherwise off by only `1-3`.

Examples:

- `ABCA1`: old `673`, new `676`
- `ABL1`: old `558`, new `558`
- `NAT2`: old `440`, new `441`
- `ABCA4`: old `262`, new `265`
- `A2M`: old `129`, new `128`

This is close enough to treat the new TDL counting approach as correct for this migration.

### Working MySQL: `pharos400_working`

Observed:

- `protein2pubmed`
  - `581,474` total rows
  - `JensenLab`: `575,038`
  - `NCBI`: `6,436`
- `generif`
  - `4,041` rows
- `generif2pubmed`
  - `4,998` rows

Sample `generif` rows correctly carry:

- `protein_id`
- `gene_id`
- `date`
- `text`

So the main graph-to-MySQL mapping is working.

## One Known Schema Fix Still Needed

`protein2pubmed.gene_id` in `pharos400_working` is currently wrong for JensenLab.

Observed behavior:

- JensenLab rows should have `gene_id = NULL`
- current schema forced them to `0`

Cause:

- `pharos400_working.protein2pubmed.gene_id` is `NOT NULL`
- it is part of a composite primary key

But in `pharos319`:

- `gene_id` is nullable
- `protein2pubmed` has no primary key

Conclusion:

- the new schema over-constrained the table
- this is a table-definition issue, not a converter issue

Recommended fix:

- make `protein2pubmed.gene_id` nullable again
- remove the composite PK requirement on `(protein_id, pubmed_id, gene_id, source)`

Keep ETL-side dedupe logic as the main dedupe mechanism for this table, as `pharos319` effectively did.

## Files Changed

Primary implementation files:

- `src/models/publication.py`
- `src/models/gene.py`
- `src/models/protein.py`
- `src/input_adapters/ncbi/publications.py`
- `src/input_adapters/jensenlab/publications.py`
- `src/output_adapters/sql_converters/tcrd.py`
- `src/input_adapters/pharos_arango/tdl_input_adapter.py`
- `workflows/pharos.Snakefile`
- `src/use_cases/working.yaml`

## Final Status

This design is effectively complete for the first pass.

Completed:

- publication-centric graph model
- GeneRIF text/date pairing preserved
- NCBI bulk ingest without API crawling
- JensenLab publication ingest
- existing resolver/retype path validated
- TDL counting updated and spot-validated
- MySQL export path validated structurally

Remaining follow-up:

- fix `protein2pubmed.gene_id` nullability / PK schema to match actual source semantics
