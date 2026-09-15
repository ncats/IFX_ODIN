from pathlib import Path

from src.constants import DataSourceName
from src.input_adapters.go.go_gaf import ProteinGoTermEdgeAdapter
from tests.registry_fakes import registry_dataset


def test_go_gaf_adapter_uses_goa_uniprot_datasource_name(tmp_path: Path):
    gaf_path = tmp_path / "goa_human_uniprot.gaf.gz"
    gaf_path.write_bytes(b"")

    adapter = ProteinGoTermEdgeAdapter(
        registry_dataset(tmp_path, gaf_path.name), source="UniProt"
    )

    assert adapter.get_datasource_name() == DataSourceName.GOA_UniProt


def test_go_gaf_adapter_uses_goa_go_datasource_name(tmp_path: Path):
    gaf_path = tmp_path / "goa_human_go.gaf.gz"
    gaf_path.write_bytes(b"")

    adapter = ProteinGoTermEdgeAdapter(
        registry_dataset(tmp_path, gaf_path.name), source="GO"
    )

    assert adapter.get_datasource_name() == DataSourceName.GOA_GO
