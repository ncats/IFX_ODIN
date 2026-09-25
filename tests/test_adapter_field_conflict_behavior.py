from src.constants import DataSourceName
from src.core.etl import ETL
from src.input_adapters.manual.tdl_override import TDLOverrideAdapter
from src.interfaces.input_adapter import InputAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import Protein
from src.shared.record_merger import FieldConflictBehavior
from tests.registry_fakes import registry_dataset


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
    adapter = TDLOverrideAdapter(
        data_source=registry_dataset(tmp_path, "tdl_updates.csv", version="2026-09-14")
    )

    proteins = next(adapter.get_all())

    assert adapter.get_field_conflict_behavior() == FieldConflictBehavior.KeepLast
    assert proteins == [Protein(id="UniProtKB:O00255", tdl="Tclin")]


def test_etl_runs_input_then_post_adapters_then_curations_then_output_postprocessing():
    events = []

    class InputPhaseAdapter(_OneProteinAdapter):
        def get_all(self):
            events.append("input")
            yield [Protein(id="IFXProtein:P1", name="source")]

    class PostPhaseAdapter(_OneProteinAdapter):
        def get_all(self):
            events.append("post")
            yield [Protein(id="IFXProtein:P1", name="derived")]

    class LifecycleOutput(_RecordingOutputAdapter):
        def apply_curation_snapshots(self, snapshots):
            assert snapshots == {"test": "snapshot"}
            events.append("curations")

        def do_post_processing(self, clean_edges=True):
            events.append("output_postprocessing")

    output = LifecycleOutput()
    etl = ETL(
        input_adapters=[InputPhaseAdapter()],
        post_adapters=[PostPhaseAdapter()],
        output_adapters=[output],
        curation_snapshots={"test": "snapshot"},
    )

    etl.do_etl(run_id="lifecycle-test")

    assert events == ["input", "post", "curations", "output_postprocessing"]
    assert [call["field_conflict_behavior"] for call in output.store_calls] == [
        FieldConflictBehavior.KeepLast,
        FieldConflictBehavior.KeepLast,
    ]


def test_etl_rejects_resume_when_curations_need_a_clean_baseline():
    etl = ETL(
        input_adapters=[],
        output_adapters=[_RecordingOutputAdapter()],
        curation_snapshots={"metabolite_annotations": object()},
    )

    try:
        etl.do_etl(do_post_processing=False, resume=True, run_id="unsafe-resume")
    except RuntimeError as exc:
        assert "Run a clean build" in str(exc)
    else:
        raise AssertionError("Expected resume with curations to fail safely")


def test_etl_resume_accepts_legacy_input_checkpoint_and_reruns_post_adapter():
    events = []

    class ShouldBeSkipped(_OneProteinAdapter):
        def get_all(self):
            raise AssertionError("completed input adapter should be skipped")

    class RerunPost(_OneProteinAdapter):
        def get_all(self):
            events.append("post")
            yield [Protein(id="IFXProtein:P1", name="recalculated")]

    input_adapter = ShouldBeSkipped()

    class CheckpointOutput(_RecordingOutputAdapter):
        def get_completed_adapter_names(self, run_id):
            assert run_id == "resume-test"
            return {input_adapter.get_name(), f"post:{RerunPost().get_name()}"}

    etl = ETL(
        input_adapters=[input_adapter],
        post_adapters=[RerunPost()],
        output_adapters=[CheckpointOutput()],
    )

    etl.do_etl(do_post_processing=False, resume=True, run_id="resume-test")

    assert events == ["post"]
