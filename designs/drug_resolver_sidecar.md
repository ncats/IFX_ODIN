# Complete Drug Resolver Sidecar

The drug graph display bundle remains intentionally bounded for browser
performance. Complete local identity lookup uses the version-matched
`drug_resolver_index.sqlite` emitted by IFX Harmonizers.

## Release and staging contract

1. Run the versioned drug harmonizer release workflow and its QC/release diff.
   Do not generate this sidecar by copying or editing an older SQLite file.
2. Publish `drug_resolver_index.sqlite` beside the corresponding versioned drug
   app-graph release in release/object storage. It is a generated artifact and
   is not intended for normal Git storage.
3. Preserve the matching `manifest.json`. Its `counts` object records resolver
   schema version, node count, byte size, and SHA-256.
4. Stage the graph directory and sidecar from the same harmonizer version. Run:

   ```bash
   python -m src.qa_browser.app \
     --drug-graph-dir /data/drug_data/v1.2.0/app_graph \
     --drug-resolver-index /data/drug_data/v1.2.0/app_graph/drug_resolver_index.sqlite \
     --verify-drug-resolver-checksum
   ```

5. Production may omit `--verify-drug-resolver-checksum` after a verified,
   immutable staging promotion to avoid hashing a multi-gigabyte file on every
   restart. Byte size, SQLite schema, and harmonizer version are always checked.

If a manifest declares the sidecar but the file is missing, or an explicitly
configured sidecar is missing/mismatched, the QA Browser fails visibly instead
of silently claiming complete local coverage.

The sidecar is read-only at query time. Live PubChem candidates never modify it
or mint IFXDrug identifiers.
