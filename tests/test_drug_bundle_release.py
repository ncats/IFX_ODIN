import hashlib
import json
from pathlib import Path


def test_bundled_drug_release_is_hydrated_and_matches_manifest():
    bundle = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "qa_browser"
        / "data"
        / "drug_app_graph"
        / "v1.4.0"
    )
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))

    upstream_path = bundle / manifest["deployment_bundle"]["upstream_manifest"]
    upstream_sha256 = hashlib.sha256(upstream_path.read_bytes()).hexdigest()
    assert upstream_sha256 == manifest["deployment_bundle"]["upstream_manifest_sha256"]

    for file_name, count_key in (("drug_nodes.tsv", "nodes"), ("drug_edges.tsv", "edges")):
        path = bundle / file_name
        with path.open("rb") as stream:
            assert not stream.read(128).startswith(b"version https://git-lfs.github.com/spec")
        with path.open(encoding="utf-8", newline="") as stream:
            row_count = sum(1 for _ in stream) - 1
        assert row_count == manifest["counts"][count_key]
