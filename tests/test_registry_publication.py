from datetime import date, datetime
from pathlib import Path
import sys

import pytest

from src.core.registry_publication import publish_derived_file, publish_source_file
from src.use_cases.pharos import fetch_disease_harmonizer_ids
from src.use_cases.pharos import fetch_drug_harmonizer_ids
from src.use_cases.pharos import fetch_target_harmonizer_ids


def test_publish_source_file_uses_standalone_registry_client(monkeypatch, tmp_path: Path):
    path = tmp_path / "records.tsv"
    path.write_text("id\n1\n", encoding="utf-8")
    captured = {}

    class Client:
        def publish_source(self, snapshot_id, **kwargs):  # type: ignore[no-untyped-def]
            captured["snapshot_id"] = snapshot_id
            captured.update(kwargs)
            return object()

    monkeypatch.setattr(
        "src.core.registry_publication.RegistryClient.connect",
        lambda credentials: Client(),
    )

    publish_source_file(
        "example:records:2026-09-11",
        path,
        "registry.yaml",
        capture_method="provider_export",
        source_url="https://example.org/records.tsv",
        metadata={"producer": "test"},
    )

    assert captured["snapshot_id"] == "example:records:2026-09-11"
    assert captured["files"] == {"records.tsv": path}
    assert captured["version_date"] == date(2026, 9, 11)
    assert isinstance(captured["captured_at"], datetime)
    assert captured["captured_at"].tzinfo is not None
    assert captured["capture_method"] == "provider_export"
    assert captured["file_sources"] == {"records.tsv": "https://example.org/records.tsv"}
    assert captured["upstream_urls"] == ("https://example.org/records.tsv",)


def test_publish_derived_file_requires_kind_qualified_inputs(monkeypatch, tmp_path: Path):
    path = tmp_path / "records.tsv"
    path.write_text("id\n1\n", encoding="utf-8")
    captured = {}

    class Derived:
        def publish(self, snapshot_id, **kwargs):  # type: ignore[no-untyped-def]
            captured["snapshot_id"] = snapshot_id
            captured.update(kwargs)
            return object()

    class Client:
        derived = Derived()

    monkeypatch.setattr(
        "src.core.registry_publication.RegistryClient.connect",
        lambda credentials: Client(),
    )

    publish_derived_file(
        "target_graph:gene_ids:2026-09-11",
        path,
        "registry.yaml",
        inputs=["source=ensembl:genes:116", "external=nodenorm:api:2026jul22"],
        producer_release="2.2.0",
        producer_repository="https://github.com/ncats/IFX_Harmonizers",
        producer_revision="a" * 40,
        transform_name="target_harmonizer_export",
        validation={"rows": 1},
    )

    assert captured["snapshot_id"] == "target_graph:gene_ids:2026-09-11"
    assert [ref.kind.value for ref in captured["inputs"]] == [
        "source_snapshot",
        "external_dataset_version",
    ]
    assert captured["producer"].name == "ifx_harmonizers"
    assert captured["transform"] == {
        "name": "target_harmonizer_export",
        "version": "2.2.0",
    }


@pytest.mark.parametrize(
    ("module", "selection_args"),
    [
        (fetch_target_harmonizer_ids, ["--entity-types", "gene", "--skip-uniprot-mapping"]),
        (fetch_drug_harmonizer_ids, ["--file-types", "nodes"]),
        (fetch_disease_harmonizer_ids, ["--file-types", "concepts"]),
    ],
)
def test_harmonizer_cli_fails_when_a_requested_publication_fails(
    monkeypatch,
    tmp_path: Path,
    module,
    selection_args: list[str],
):
    monkeypatch.setattr(module, "_make_session", lambda: object())
    if module is fetch_target_harmonizer_ids:
        monkeypatch.setattr(module, "fetch_entity_ids", lambda *args: "id\n")
    elif module is fetch_drug_harmonizer_ids:
        monkeypatch.setattr(
            module,
            "fetch_drug_file",
            lambda api_url, filename, session, output_path: output_path.write_text(
                "id\n", encoding="utf-8"
            ),
        )
    else:
        monkeypatch.setattr(module, "fetch_disease_file", lambda *args: "id\n")
    monkeypatch.setattr(
        module,
        "publish_to_registry",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("upload failed")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            module.__name__,
            "--output-dir",
            str(tmp_path),
            "--version",
            "2026-09-11",
            "--input",
            "source=example:records:1",
            "--producer-release",
            "1.0.0",
            "--producer-revision",
            "a" * 40,
            *selection_args,
        ],
    )

    with pytest.raises(RuntimeError, match="Registry publication failed"):
        module.main()


def test_drug_harmonizer_cli_prints_only_successfully_selected_refs(
    monkeypatch,
    tmp_path: Path,
    capsys,
):
    monkeypatch.setattr(fetch_drug_harmonizer_ids, "_make_session", lambda: object())
    monkeypatch.setattr(
        fetch_drug_harmonizer_ids,
        "fetch_drug_file",
        lambda api_url, filename, session, output_path: output_path.write_text(
            "id\n", encoding="utf-8"
        ),
    )
    monkeypatch.setattr(fetch_drug_harmonizer_ids, "publish_to_registry", lambda *args: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            fetch_drug_harmonizer_ids.__name__,
            "--output-dir",
            str(tmp_path),
            "--version",
            "2026-09-11",
            "--input",
            "source=example:records:1",
            "--producer-release",
            "1.0.0",
            "--producer-revision",
            "a" * 40,
            "--file-types",
            "nodes",
        ],
    )

    fetch_drug_harmonizer_ids.main()

    output = capsys.readouterr().out
    assert "drug_graph:drug_nodes:2026-09-11" in output
    assert "drug_graph:drug_edges:2026-09-11" not in output
