# Adapter Dependency And Execution Order

## Problem

Some adapters are not truly self-contained. They emit nodes or edges that only make sense when other adapters and resolver paths are also present in the same build.

This creates hidden dependencies:

- a source adapter may appear valid in isolation
- but its output only lands correctly if some other node-producing adapter is also configured
- and if the right resolver is present with the right `no_match_behavior`

When that contract is implicit, the build is harder to reason about, harder to validate, and easier to break during refactors.

## TIGA as the current example

The TIGA ingest now has an explicit cross-source dependency pattern.

It emits:

- `GwasTrait`
- `ProteinGwasTraitEdge`
- `GwasTraitDiseaseEdge`

The `GwasTraitDiseaseEdge` is a projection from raw GWAS trait space into canonical disease space:

- `GwasTrait.id` keeps the raw TIGA trait ID, for example `EFO_0007990`
- the projected disease endpoint is seeded as `Disease(id="EFO:0007990")`
- the adapter itself does not emit canonical `Disease` nodes

For that projection to work, the build also needs:

- a `Disease` resolver
- disease node input from somewhere else, currently MONDO in the working build
- resolver behavior that drops unresolved disease projections cleanly

Without that surrounding configuration, the TIGA adapter still runs, but the disease projection does not land as intended.

## This is not unique to TIGA

The same general pattern already exists elsewhere in less explicit form.

Examples:

- many edge adapters assume `Protein` identity normalization is available
- some graph-to-MySQL export paths assume certain node tables were loaded earlier in the same build
- some associations only produce useful foreign keys after a resolver-backed canonical node path has run

So the real issue is broader than disease projection. It is about declaring build-time dependencies between:

- emitted node and edge types
- resolver expectations
- post-processing expectations
- downstream export expectations

## Current failure mode

The main failure mode is not usually a crash. It is partial success:

- the adapter emits data
- the ETL completes
- but some intended connections are missing
- and the reason is configuration coupling, not source-data absence

That is worse than a hard failure because it is easy to miss.

## What should be made explicit

For any adapter or post-processing step with non-local requirements, we should eventually be able to state:

- what node and edge types it emits
- what other node types must already exist somewhere in the build
- what resolver types must be configured
- whether unmatched endpoints should be skipped, allowed through, or treated as errors
- whether downstream export depends on additional graph content beyond the adapter’s own output

In other words, each adapter should have an explicit dependency contract, even if the framework does not yet enforce one.

## Short-term recommendation

Document these dependencies in design docs and YAML-adjacent comments whenever they matter for correctness.

At minimum, for dependency-bearing adapters we should record:

- required companion adapters
- required resolvers
- required post-processing steps
- whether the dependency is for graph correctness, MySQL correctness, or both

TIGA should now be treated as one of those adapters.

## Longer-term direction

If these dependencies become explicit and machine-readable, the build system could eventually use them for planning.

That opens two useful possibilities:

1. validation before runtime

- detect missing companion adapters or resolvers up front
- fail early when an adapter’s dependency contract is unsatisfied

2. better execution scheduling

- independent adapter groups could run in parallel
- dependency-linked groups could run in a defined order
- post-processing could declare which collections it consumes and produces

That would turn current “deep knowledge of the process” into something the build can reason about directly.

## Open design questions

- Should dependency contracts live in YAML, in adapter class metadata, or both?
- Should dependencies be declared at the level of adapter classes, emitted model types, or named graph collections?
- Should unresolved dependency-linked edges be silently skipped, warned on, or counted in a QA report?
- Should working builds enforce dependency checks more strictly than full production builds?

## Practical takeaway

The TIGA disease projection is a valid graph model, but it exposed a framework-level issue:

- some ingest components are only correct in the presence of other ingest components

That is manageable today with documentation and careful build composition, but it should eventually become an explicit part of the ETL framework contract.
