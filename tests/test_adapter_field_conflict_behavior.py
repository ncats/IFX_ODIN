from src.constants import DataSourceName
from src.core.etl import ETL
from src.input_adapters.manual.tdl_override import TDLOverrideAdapter
from src.interfaces.input_adapter import InputAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import Protein
from src.shared.record_merger import FieldConflictBehavior


class _OneProteinAdapter(InputAdapter):
    field_conflict_behavior = FieldConflictBehavior.KeepLast

    def get_all(self):
        yield [Protein(id="IFXProtein:P1", name="new")]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(version="test")


class _RecordingOutputAdapter(OutputAdapter):
    def __init__(self):
        self.store_calls = []

    def store(self, objects, single_source=False,
              field_conflict_behavior: FieldConflictBehavior = FieldConflictBehavior.KeepFirst) -> bool:
        self.store_calls.append({
            "objects": objects,
            "single_source": single_source,
            "field_conflict_behavior": field_conflict_behavior,
        })
        return True

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        return True


def test_etl_passes_adapter_field_conflict_behavior_to_output_adapter():
    output = _RecordingOutputAdapter()
    etl = ETL(input_adapters=[_OneProteinAdapter()], output_adapters=[output])

    etl.do_etl(do_post_processing=False, run_id="test-run")

    assert len(output.store_calls) == 1
    assert output.store_calls[0]["field_conflict_behavior"] == FieldConflictBehavior.KeepLast


def test_tdl_override_adapter_requests_keep_last_conflict_behavior(tmp_path):
    tdl_file = tmp_path / "tdl_updates.csv"
    tdl_file.write_text(
        "UniProt,Symbol,Name,Target Development Level,new TDLs\n"
        "O00255,MEN1,Menin,Tclin,Tchem\n"
    )
    adapter = TDLOverrideAdapter(file_path=str(tdl_file))

    proteins = next(adapter.get_all())

    assert adapter.get_field_conflict_behavior() == FieldConflictBehavior.KeepLast
    assert proteins == [Protein(id="UniProtKB:O00255", tdl="Tclin")]
