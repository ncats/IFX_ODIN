# Target Harmonizer Explorer Data

The Target Harmonizer Explorer consumes app-graph bundles with this structure:

```text
target_app_graph/
  v1.0.0/
    manifest.json
    target_nodes.tsv
    target_edges.tsv
  current/
    manifest.json
    target_nodes.tsv
    target_edges.tsv
```

Startup prefers the highest semantic `v*` directory that contains
`manifest.json` and `target_nodes.tsv`. The `current/` directory remains a
backward-compatible fallback for existing local and deployed environments.

The source of truth for generating versioned target app graph bundles is the
TargetGraph pipeline module:

```bash
python src/code/main.py TARGETS --modules target_app_graph_export
```

Copy a generated `v*/app_graph/` bundle from TargetGraph into this directory
only when the deployment should serve that target release.
