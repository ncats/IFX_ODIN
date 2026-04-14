# Monarch Diseases Discovery Note

## Status
Rejected as a new standalone ingest source.

## Why
- The current public Monarch disease-association file is a Translator-style aggregated release, not a source-native Monarch assertion set.
- `infores:monarchinitiative` appears as the aggregator, not the primary source.
- The only `primary_knowledge_source` values in the current dump are:
  - `infores:omim` (`6830` rows)
  - `infores:clingen` (`218` rows)
- So treating the file as a single `Monarch` ingest would blur provenance and mostly duplicate planned OMIM work.

## What We Found
- Current file: `https://data.monarchinitiative.org/monarch-kg-dev/latest/tsv/gene_associations/gene_disease.9606.tsv.gz`
- Current dump is human-only and already normalized to `HGNC -> MONDO`.
- Edge payload is slim: mostly predicate plus provenance.
- Old `pharos319` Monarch rows came from a much older Monarch/MySQL export and are not reproduced by the current public file.
- The ClinGen-backed slice adds some distinct pairs, but that is better thought of as ClinGen content than as an independent Monarch source.

## Decision
- Do not ingest this file as `Monarch`.
- Move on to direct OMIM discovery.
- Optionally revisit ClinGen later using a direct ClinGen download surface rather than the aggregated Monarch release.

## References
- Monarch KG downloads: `https://monarchinitiative.org/kg/downloads`
- Release artifact root: `https://data.monarchinitiative.org/monarch-kg-dev/latest/`
- Old loaders:
  - `load-MonarchDiseases.py`
  - `load-MonarchOrthologDiseases.py`
