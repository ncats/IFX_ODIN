from pathlib import Path

from src.constants import DataSourceName
from src.input_adapters.go.go_gaf import ProteinGoTermEdgeAdapter


def test_go_gaf_adapter_uses_goa_uniprot_datasource_name(tmp_path: Path):
    gaf_path = tmp_path / "goa_human-uniprot.gaf.gz"
    gaf_path.write_bytes(b"")

    adapter = ProteinGoTermEdgeAdapter(gaf_file_name=str(gaf_path), source="UniProt")

    assert adapter.get_datasource_name() == DataSourceName.GOA_UniProt


def test_go_gaf_adapter_uses_goa_go_datasource_name(tmp_path: Path):
    gaf_path = tmp_path / "goa_human-go.gaf.gz"
    gaf_path.write_bytes(b"")

    adapter = ProteinGoTermEdgeAdapter(gaf_file_name=str(gaf_path), source="GO")

    assert adapter.get_datasource_name() == DataSourceName.GOA_GO
