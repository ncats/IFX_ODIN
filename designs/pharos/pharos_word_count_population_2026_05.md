# Pharos Word Count Population, May 2026

## Goal

Repopulate `pharos400.word_count` for the Pharos target publication word cloud.

The table is a corpus-level background distribution used by the backend to score
target-local words for overrepresentation. It is not the final top-100 word-cloud
payload.

## Status

- Legacy Airflow population code recovered
- Backend word-cloud scoring code reviewed
- Legacy `pharos319` data inspected
- Implemented as a resumable TCRD input adapter in `tcrd.yaml`

## Core Contract

`word_count.count` is abstract document frequency:

- one count per word per abstract
- repeated occurrences inside the same abstract count once
- counts are lowercased normalized word forms
- `__ABSTRACT_COUNT__` stores the number of abstracts in the background corpus

This is the same unit used by the backend word-cloud scorer. The scorer builds
target-local document frequencies, retrieves matching global document frequencies
from `word_count`, and computes a Fisher exact test over abstracts.

The table should include broad background counts for all matched words, including
common stop words. Stop-word and display filtering happen after scoring, not while
populating `word_count`.

## Destination Table

Schema:

- table: `word_count`
- columns:
  - `word varchar(128) primary key`
  - `count int`

Schema definitions:

- `src/shared/sqlalchemy_tables/pharos_tables_old.py`
- `src/shared/sqlalchemy_tables/pharos_tables_new.py`

## Legacy Population Behavior

Recovered Airflow code populated `word_count` from:

1. distinct PMIDs in `{schemaname}.protein2pubmed`
2. restricted to `source = "NCBI"`
3. joined to `ncats_pubmed.pubmed`
4. using rows where `abstract is not NULL`

Legacy tokenization:

```python
wordPattern = r"\b[A-Za-z]+[A-Za-z0-9\-\./\+]{1,}\b|\b[0-9]+[A-Za-z]+[0-9]+\b"
matches = re.findall(wordPattern, abstract)
wordsInAbstract = {word.lower(): 1 for word in matches}
```

Then each key in `wordsInAbstract` increments the global count by `1`.

Important details:

- JensenLab publication links were not included.
- GeneRIF text was not included.
- Titles, journal names, authors, and PubTator mention text were not included.
- No stop-word denylist was applied during table population.
- The legacy code initialized `abstract_count = 1`, so its sentinel row is one
  greater than the number of returned abstracts.

## Legacy Data Checks

Observed in `pharos319.word_count`:

- total rows: `986,271`
- sum of word document counts: `66,481,202`
- `__ABSTRACT_COUNT__ = 724,492`
- top rows include stop words such as `the`, `of`, `in`, `and`, `to`, `with`,
  `is`, `that`, and `we`

Observed in `pharos319.protein2pubmed`:

- NCBI:
  - `1,333,543` rows
  - `744,187` distinct PMIDs
  - `19,900` proteins
  - `gene_id` populated
- JensenLab:
  - much larger PMID sets for common targets
  - `gene_id` is `NULL`

The NCBI distinct PMID count is close to the `__ABSTRACT_COUNT__` sentinel after
allowing for missing PubMed rows and records without abstracts. That matches the
recovered source code.

Target spot checks also showed that JensenLab would massively expand the target
abstract sets. For example, TP53 had `10,522` NCBI PMIDs but `294,195` JensenLab
PMIDs in `pharos319`. Including JensenLab would change the word-cloud semantics
and should be treated as a separate future feature.

## Scoring Model

The existing backend naturally forms a 2x2 table over abstracts:

```text
                         contains word   does not contain word
target abstracts             k                n - k
background outside target    K - k            N - K - (n - k)
```

Where:

- `k` = target abstracts containing the word
- `n` = target abstract count
- `K` = all background abstracts containing the word
- `N` = `__ABSTRACT_COUNT__`

This document-frequency model is simpler and more stable than raw token counts:

- long abstracts do not dominate because they repeat a term many times
- abstract length normalization is unnecessary
- Fisher exact / hypergeometric enrichment applies directly
- the top-100 UI payload can be selected after enrichment scoring

## Proposed `pharos400` Semantics

Populate `pharos400.word_count` from:

1. `pharos400.protein2pubmed`
2. restricted to `source = 'NCBI'`
3. distinct `pubmed_id`
4. joined to `ifx_pubmed.pubmed`
5. using records with non-empty `abstract`

Use the recovered legacy regex and document-frequency counting:

- lowercase tokens
- count each normalized word once per abstract
- skip or truncate words longer than `128` characters
- insert every global word count
- insert `__ABSTRACT_COUNT__`

Recommended sentinel behavior:

- Prefer the corrected exact abstract count, initialized from `0`.
- Do not preserve the legacy off-by-one unless strict old-count compatibility is
  explicitly required.

## Denylist And Display Filtering

Do not apply a denylist when populating `word_count`.

The table is the statistical background. It should retain stop words because the
backend may need background counts for any candidate word emitted by the target
abstract tokenizer.

Apply filtering at scoring/display time. At minimum, display filtering should
exclude:

- common English stop words
- malformed tokens, if any are emitted
- `__ABSTRACT_COUNT__`

## Implementation Plan

Implemented path: a real TCRD input adapter, not a post-processing step.

- adapter: `src/input_adapters/pharos_source_tcrd/word_count.py`
- model: `src/models/word_count.py`
- converter: `TCRDOutputConverter.word_count_converter`
- YAML wiring: `src/use_cases/pharos/tcrd.yaml`

Responsibilities:

1. connect to the target Pharos MySQL schema, for example `pharos400`
2. connect to the PubMed mirror schema, `ifx_pubmed`
3. stream distinct NCBI PMIDs from `protein2pubmed`
4. fetch PubMed abstracts in batches
5. build document-frequency counts using the recovered regex
6. insert or update global word counts plus `__ABSTRACT_COUNT__`

The adapter is wired immediately after `ProteinAdapter` in `tcrd.yaml`, because
`ProteinAdapter` populates `protein2pubmed` from protein publication properties.
Once `WordCountAdapter` completes, normal ETL adapter-run metadata marks it
completed in `etl_adapter_run`; a later `--resume` run skips it instead of
recomputing the corpus.

`word_count.word` is the primary key, and the MySQL output adapter treats
`WordCount` rows as idempotent upserts. This allows a failed run to retry the
adapter without colliding with partial rows from the previous attempt.

Do not implement this as output post-processing. Post-processing would rerun on
every resumed build, while a normal adapter participates in checkpointing.

## Validation Plan

After populating `pharos400.word_count`:

1. verify shape:
   - vocabulary row count is on the same order as `pharos319`
   - `__ABSTRACT_COUNT__` exists
   - top rows include common stop words
   - no word count exceeds `__ABSTRACT_COUNT__`
2. verify corpus boundary:
   - `__ABSTRACT_COUNT__` should equal distinct NCBI-linked PubMed abstracts
     with non-empty abstracts in `ifx_pubmed.pubmed`
3. verify scoring compatibility:
   - pick a few targets such as ABCA1, ABL1, GCK, NAT2, and TP53
   - compute target-local document frequencies with the same regex
   - confirm backend contingency tables have non-negative cells
4. compare top background words to `pharos319` for broad sanity, not exact
   equality

Exact equality is not expected if:

- the PubMed mirror is newer
- `pharos400.protein2pubmed` uses refreshed NCBI files
- the corrected `__ABSTRACT_COUNT__` starts from `0`

## Decision Summary

- Use `ifx_pubmed.pubmed.abstract`.
- Include only PMIDs linked through `protein2pubmed.source = 'NCBI'`.
- Exclude JensenLab publication links.
- Store abstract document frequencies, not token frequencies.
- Count each word at most once per abstract.
- Use the recovered legacy regex.
- Do not apply stop-word filtering during table population.
- Add `__ABSTRACT_COUNT__` as a metadata row.
