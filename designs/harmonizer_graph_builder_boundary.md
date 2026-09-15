# Harmonizer and Graph Builder Boundary

## Status

This document is the default architectural rule for harmonizer handoffs into
graph-building code. Departures require an explicit design decision because
they change ownership, provenance, and the ability to configure sources.

## The boundary in one sentence

**The harmonizer determines who an entity is; adapters determine what each
source contributes; graph configuration determines what belongs in a
particular graph.**

An ID resolver answers **"who is this?"** by mapping source identifiers to a
preferred entity ID. An adapter answers **"what does this source say?"** by
parsing a payload into graph fields and relationships.

Keeping these responsibilities separate allows either side to evolve without
forcing an unrelated rebuild or code change.

## Preferred handoff

A harmonizer output may contain two useful kinds of information:

1. Identity mappings, such as DrugCentral and ChEMBL IDs that refer to the same
   preferred `IFXDrug`.
2. Harmonizer-owned assertions, such as mapping scores, methods, confidence,
   component membership, and review decisions.

Use that output twice, through explicit consumers:

```text
harmonizer identity output ──> ID resolver ───────────────┐
              │                                           │
              └──────────> harmonizer adapter ────────────┤
                                                          ├─> graph merger
DrugCentral snapshot ───────> DrugCentral adapter ────────┤
ChEMBL snapshot ────────────> ChEMBL adapter ─────────────┤
IUPHAR snapshot ────────────> IUPHAR adapter ─────────────┘
```

The resolver maps all emitted source IDs onto preferred entities. The
harmonizer adapter adds only the harmonizer's own results. Primary-source
adapters remain responsible for primary-source fields and evidence. The graph
merger can then retain an audit trail showing which source contributed each
value.

The existing gene, transcript, and protein TSV flow is the model: the files
support resolvers and also contribute harmonizer-owned scores and methods,
while sources such as UniProt remain independently ingestible.

## Preferred annotations are not evidence selection

Annotation harmonization and evidence-edge selection carry different risks.

A harmonizer may compare several candidate values and publish a preferred
protein name, gene symbol, chromosome location, drug label, or similar
consensus annotation. That preferred value is a new harmonizer-owned assertion
when the output also makes its method, score or confidence, and supporting
evidence auditable. The graph builder can ingest it alongside the original
source assertions.

Evidence-bearing relationships are different. Filtering, sampling, ranking,
capping, or collapsing drug-target activities and other evidence edges changes
the scientific contents available to the graph. It can affect downstream
scores, classifications, and conclusions. These decisions must remain visible
and configurable at the graph-building boundary.

| Operation | Default owner |
| --- | --- |
| Map several source IDs to one preferred entity | Harmonizer and ID resolver |
| Select a preferred name or symbol with method, score, and evidence trail | Harmonizer annotation output |
| Normalize a source table without changing row meaning | Source transformer |
| Decide which evidence providers belong in a graph | Graph configuration |
| Apply activity or confidence thresholds | Source adapter or explicit graph policy |
| Sample, prioritize, or cap evidence edges | Presentation layer, not an ingest contract |
| Collapse provider evidence into one edge | Graph merger, after source-specific evidence is represented |

An upstream derived-evidence table can be an ingest contract only when it is
source-faithful: provider identity, raw endpoint identifiers, row-level
evidence, source-specific fields, and independently selectable providers must
remain available. Every filtering or collapse rule must be explicit and
versioned. A presentation-oriented edge export does not meet this standard
merely because it retains a combined source label or an evidence summary.

## What not to substitute

An application, presentation, QA, or explorer graph is usually a downstream
view. It may already have:

- chosen preferred identifiers;
- selected or omitted fields;
- combined several providers;
- collapsed source rows or edges;
- applied thresholds, prioritization, or other graph policy;
- reshaped provenance for display.

Registering such an artifact in the Registry makes it versioned and
distributable. It does not make the artifact source-faithful or suitable as a
replacement for primary-source adapters.

If the graph builder consumes this combined output through one adapter, it
loses important controls:

| Lost capability | Consequence |
| --- | --- |
| Independent source selection | Providers bundled upstream can no longer be enabled or disabled separately. |
| Independent source versioning | Updating one provider requires a release of the combined product. |
| Field-level provenance | The framework sees the harmonizer as the source instead of the provider that supplied each value. |
| Access to omitted payload | A downstream adapter cannot select a field that was not exported. |
| Change isolation | Adding a source field requires a harmonizer change and rebuild even when identity is unchanged. |
| Use-case policy | Different graph builders inherit upstream filtering and modeling choices. |
| Failure isolation | A problem in one provider or transform can block the entire combined release. |

A combined `source_namespaces` field or nested evidence list can be useful for
display and investigation, but it does not restore independent configuration
or a complete field-level audit trail.

## Use-case ownership

Target Graph and Pharos may be separate graph-building use cases with different
owners, included sources, filtering, resolver policies, and outputs. That does
not require them to collapse identity resolution into adapters.

The adapter/resolver boundary is valuable within each use case:

- Target Graph can change which fields it takes from ChEMBL without changing
  drug identity harmonization.
- Pharos can use the same identity mappings while choosing a different set of
  source fields or canonicalization policy.
- The harmonizer team can improve identity logic without owning every graph
  field required by downstream consumers.

Use-case autonomy governs **which** components are composed. The shared
architectural boundary governs **what responsibility** each component owns.

## Artifact classification

Classify every derived handoff before implementing its consumer:

| Artifact type | Normal consumer | Requirement |
| --- | --- | --- |
| Raw source snapshot | Source adapter | Preserve source identity and payload. |
| Source-faithful normalized derivative | Source adapter | Preserve provider discriminator, raw IDs, relevant fields, and row semantics. |
| Identity map | ID resolver | Map source IDs to preferred IDs with versioned lineage. |
| Harmonizer annotations | Harmonizer adapter | Contain assertions actually calculated or curated by the harmonizer. |
| Application/presentation graph | UI, QA, or downstream application | Do not use as a primary-source replacement without an explicit ownership transfer. |

One physical file may support both an identity resolver and a harmonizer
adapter, but those are separate semantic uses and should be wired explicitly.

## Review questions

Before accepting a harmonizer artifact as an adapter input, answer:

1. Can the graph configuration still select and version each primary source
   independently?
2. Does each adapter report the datasource that actually owns its fields and
   evidence?
3. Can a new primary-source field be added without changing identity logic?
4. Are raw source identifiers available to the resolver?
5. Are provider rows, evidence, and source-specific semantics preserved until
   the graph builder applies its policies?
6. Are harmonizer-owned scores and decisions distinguishable from facts copied
   from primary sources?
7. Is this artifact intended as a reusable data contract, or was it shaped for
   an application or QA view?

If the answer to any of the first six questions is no, or the last answer is an
application view, pause before implementation and make the ownership transfer
an explicit architectural decision.
