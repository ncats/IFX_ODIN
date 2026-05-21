# Designs

This directory holds source discovery notes, implementation designs, migration plans, and validation artifacts. Keep source-specific ingest work in the closest topical folder and avoid leaving new documents at the root.

## Folders

- `architecture/` - cross-cutting ETL, graph, QA browser, and metadata designs.
- `cure/` - CURE form and ingest discovery notes.
- `diseases/` - disease-source ingest designs.
- `drugs/` - drug, ligand, and indication source designs.
- `dto/` - DTO investigation notes.
- `expression/` - expression and tissue source designs.
- `genes/` - gene-source ingest designs.
- `idg_resources/` - IDG resource migration notes and legacy loader references.
- `migration/` - broad graph/model migration plans.
- `orthologs/` - ortholog source designs.
- `patents/` - patent source designs and supporting figures.
- `pathways/` - pathway source designs.
- `pharos/` - Pharos output, publication, target graph, and Pharos-specific source investigation designs.
- `pounce/` - POUNCE ingest and validation designs.
- `ppi/` - protein interaction source designs.
- `protein_classes/` - protein classification source designs.
- `rdf/` - RDF export, ontology, and SPARQL handoff notes.
- `releases/` - release and shared artifact distribution designs.
- `tiga/` - TIGA ingest design notes.

## Conventions

- Name ingest documents as `<source>_ingest_design.md` or `<source>_ingest_discovery_YYYY_MM.md`.
- Keep generated previews or figure data next to the design that uses them.
- Do not commit local metadata files such as `.DS_Store`.
