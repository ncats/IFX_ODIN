# Metabolite Harmonization Curation Design

## Purpose

The Harmonization Studio needs a durable way for experts to record decisions
that survive browser, graph, and container rebuilds. The initial scope is
intentionally small:

- collect removals of specific metabolite-equivalence edges in a persistent
  curation cart;
- publish the cart as an immutable curation batch;
- apply published batches as a late harmonization rule.
- record expected-clique assertions and evaluate them against every materialized
  stage, with an explicit optional fallback rule that materializes synthetic
  assertion edges.

General node-field and other-graph curations remain follow-up work.

## Durable Curation Store

The QA Browser stores one mutable draft cart per curator per graph and one JSON
object per published batch:

```
curation-drafts/v1/{graph}/{curator_hash}.json
curations/v1/{graph}/{curation_batch_id}.json
```

These prefixes are separate from registry datasets even when they use the same
AWS bucket and credentials. Drafts are deliberately outside `curations/`, so a
partially assembled cart can never be applied by the harmonization pipeline.
The draft key hashes the normalized curator identity rather than exposing it in
the S3 object name.

Adding or removing a cart item immediately rewrites the draft in S3. A browser
refresh therefore reloads the draft rather than losing it. Publishing writes an
immutable batch under `curations/` and then clears that curator's draft. Only
published batches change active state. Graph-changing edge decisions and
validation-only assertions have separate fingerprints, so publishing only an
assertion does not invalidate pipeline snapshots.

## Batch Shape

Published batches contain operations using stable graph business identifiers
rather than Arango document handles:

```
{
    "format_version": 1,
    "curation_batch_id": "qa-browser-<draft-uuid>",
    "graph": "metabolite_harmonization",
    "name": "Haley's batch 2026-08-24",
    "description": "Reviewed molecular-weight conflicts.",
    "created_at": "2026-08-24T18:04:00.123456Z",
    "created_by": {"id": "haley@example.org", "name": "Haley"},
    "source": {"type": "qa_browser_curation_cart", "draft_id": "<uuid>"},
    "operations": [{
        "operation_id": "<content hash>",
        "action": "remove_edge",
        "edge_type": "MetaboliteIdentifierMappingEdge",
        "start_id": "CHEBI:1234",
        "end_id": "HMDB:HMDB0000123",
        "symmetric": true,
        "added_at": "2026-08-24T17:55:00.123456Z",
        "added_by": {"id": "haley@example.org", "name": "Haley"}
    }]
}
```

The operation ID is a deterministic content hash, making repeated additions of
the same edge removal idempotent. The batch ID is derived from the cart's draft
UUID, so retrying a publish cannot create a second logical batch.

Direction remains explicit because different edge types and directed
relationships can connect the same pair of nodes. Metabolite equivalence-edge
removals are symmetric and their endpoints are stored in sorted order.

The same batch may contain an `assert_same_clique` operation. It stores a
deterministic assertion ID, a sorted set of at least two metabolite business
IDs, a name, and an optional rationale. A later immutable batch can contain a
`retire_assertion` operation referencing that ID. Published operations are
applied chronologically; retirement removes an assertion from the active set
without deleting its audit history.

## Curator Identity

The API prefers identity supplied by an identity-aware reverse proxy through
common user or email headers. When none is available, the curator enters a name
or email in the Studio. That value is remembered locally by the browser while
the authoritative cart and recorded identity are stored in S3.

The entered-identity fallback provides attribution but is not authentication.
Deployment behind an identity-aware proxy is required before attribution should
be treated as verified identity.

## Applying Curations

Metabolite harmonization loads all published batches and resolves symmetric
`remove_edge` and `retain_edge` decisions chronologically. The latest published
decision for a pair controls its effective state. The rule removes selected
equivalence edges before clique-building without modifying the evidence graph.

Publishing and applying are separate actions. Curators may publish several
batches before a user selects **Sync curations**. A sync reuses unaffected early
pipeline stages, recalculates curation-dependent stages, and removes superseded
unreferenced stage artifacts.

## Expected-Clique Assertions

Assertions are validation requirements, not implicit merge instructions. An
assertion passes when all listed IDs are active in a stage and belong to the
same clique; additional clique members are allowed. It is `split` when all IDs
are active across multiple cliques or singletons, and `missing` when any listed
ID is inactive or absent. Split and missing both count as failures.

The QA Browser evaluates the current published assertion set directly against
materialized stage memberships. It shows a cross-stage matrix, identifies the
first failing rule, and includes assertion details on each stage page. Draft
assertions remain private and are not evaluated until published. Because this
is a live validation lens over existing snapshots, assertion-only batches do
not require **Sync curations** unless a pipeline selects the assertion fallback
rule described below.

### Optional assertion fallback

The optional **Give up: Make Assertions True** rule converts every active
assertion into a deterministic star of N−1 stage evidence edges. Each edge is
marked `synthetic` and `fallback` and retains the assertion ID, rationale,
curation batch, publication time, and curator attribution. These edges are
materialized only in the harmonization stage; they do not modify the base
`MetaboliteIdentifierMappingEdge` collection.

The deliberately conspicuous name treats use of the rule as a measurable
failure to find a general chemistry-based rule. Assertions containing an ID
that is not active at that stage are skipped and remain failed rather than
resurrecting filtered identifiers. The rule is intended immediately before
cleanup, so cleanup can still remove identifiers and cause a later assertion
failure. Assertion fingerprint changes invalidate this rule's stage and later
stages while leaving earlier stages reusable.

## Considered but Not Included

- Storing durable drafts or curations in the target Arango database was rejected
  because graph rebuilds and Docker-volume loss can erase them.
- MySQL remains a possible future curation-store implementation.
- GitHub batch export, approval workflows, and automated rollback are deferred.
- `remove_node`, general-purpose `add_edge`, and `set_field` are
  deferred.
