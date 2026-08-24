# Metabolite Harmonization Curation Design

## Purpose

The Harmonization Studio needs a durable way for experts to record decisions
that survive browser, graph, and container rebuilds. The initial scope is
intentionally small:

- collect removals of specific metabolite-equivalence edges in a persistent
  curation cart;
- publish the cart as an immutable curation batch;
- apply published batches as a late harmonization rule.

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
published batches change the active curation fingerprint.

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

## Curator Identity

The API prefers identity supplied by an identity-aware reverse proxy through
common user or email headers. When none is available, the curator enters a name
or email in the Studio. That value is remembered locally by the browser while
the authoritative cart and recorded identity are stored in S3.

The entered-identity fallback provides attribution but is not authentication.
Deployment behind an identity-aware proxy is required before attribution should
be treated as verified identity.

## Applying Curations

Metabolite harmonization loads all published batches and unions their symmetric
`remove_edge` operations. Because these operations are idempotent, batch order
does not affect the result. The rule removes selected equivalence edges before
clique-building without modifying the evidence graph.

Publishing and applying are separate actions. Curators may publish several
batches before a user selects **Sync curations**. A sync reuses unaffected early
pipeline stages, recalculates curation-dependent stages, and removes superseded
unreferenced stage artifacts.

## Considered but Not Included

- Storing durable drafts or curations in the target Arango database was rejected
  because graph rebuilds and Docker-volume loss can erase them.
- MySQL remains a possible future curation-store implementation.
- GitHub batch export, approval workflows, and automated rollback are deferred.
- `remove_node`, `add_edge`, forced metabolite merges, and `set_field` are
  deferred.
- Conflicting ordered operations require an explicit deterministic precedence
  policy before they are enabled.
