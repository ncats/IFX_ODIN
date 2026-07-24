import gzip
from pathlib import Path

from src.input_adapters.metabolite_harmonization.chebi_bridge import (
    ChebiMetaboliteIdentifierBridgeAdapter,
)
from src.models.chebi import ChemicalEntity
from src.models.metabolite_harmonization import (
    ChebiChemicalEntityMetaboliteIdentifierEdge,
    MetaboliteIdentifier,
)
from src.registry.fetchers import MaterializedDataset


def _write_chebi_obo(path: Path):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(
            "format-version: 1.2\n"
            "data-version: 252\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:100\n"
            "name: example chemical\n"
            "is_a: CHEBI:23367 ! molecular entity\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:23367\n"
            "name: molecular entity\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:25212\n"
            "name: metabolite\n"
            "is_a: CHEBI:24432 ! biological role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:24432\n"
            "name: biological role\n"
            "is_a: CHEBI:50906 ! role\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:50906\n"
            "name: role\n"
        )


def _dataset_for_file(path: Path) -> MaterializedDataset:
    return MaterializedDataset(
        source="chebi",
        dataset="ontology_full",
        version="252",
        version_date="2026-05-01",
        download_date="2026-06-24",
        snapshot_id="chebi:ontology_full:252",
        manifest_uri="s3://ifx-registry/sources/chebi/ontology_full/252/manifest.yaml",
        manifest={"files": [{"path": path.name}]},
        local_dir=path.parent,
    )


def test_chebi_bridge_emits_chemical_entity_to_metabolite_identifier_edges(tmp_path: Path):
    path = tmp_path / "chebi.obo.gz"
    _write_chebi_obo(path)
    adapter = ChebiMetaboliteIdentifierBridgeAdapter(data_source=_dataset_for_file(path))

    records = [entry for batch in adapter.get_all() for entry in batch]

    assert all(isinstance(record, ChebiChemicalEntityMetaboliteIdentifierEdge) for record in records)
    assert {record.start_node.id for record in records} == {"CHEBI:100", "CHEBI:23367"}
    assert {record.end_node.id for record in records} == {"CHEBI:100", "CHEBI:23367"}
    assert all(isinstance(record.start_node, ChemicalEntity) for record in records)
    assert all(isinstance(record.end_node, MetaboliteIdentifier) for record in records)
    assert next(record for record in records if record.start_node.id == "CHEBI:100").source_label == "example chemical"
    assert all(record.start_node.id != "CHEBI:25212" for record in records)
