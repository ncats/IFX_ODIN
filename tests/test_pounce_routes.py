import io
import os
import tempfile

import pytest
from starlette.datastructures import UploadFile

from src.qa_browser.pounce_routes import _append_new_pairs, _build_session, _summarize_edit_submission


def _upload(filename: str, content: bytes = b"test") -> UploadFile:
    return UploadFile(filename=filename, file=io.BytesIO(content))


def test_append_new_pairs_appends_complete_pairs():
    with tempfile.TemporaryDirectory() as tmp:
        exp_paths = []
        stats_paths = []

        _append_new_pairs(
            [_upload("exp.xlsx")],
            [_upload("stats.xlsx")],
            tmp,
            exp_paths,
            stats_paths,
        )

        assert len(exp_paths) == 1
        assert len(stats_paths) == 1
        assert os.path.basename(exp_paths[0]) == "exp_new0_exp.xlsx"
        assert os.path.basename(stats_paths[0]) == "stats_new0_stats.xlsx"


def test_append_new_pairs_rejects_half_filled_pair():
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(ValueError, match="must include both"):
            _append_new_pairs(
                [_upload("exp.xlsx")],
                [],
                tmp,
                [],
                [],
            )


def test_build_session_tracks_all_filenames():
    with tempfile.TemporaryDirectory() as tmp:
        project_path = os.path.join(tmp, "project.xlsx")
        exp_path = os.path.join(tmp, "exp.xlsx")
        stats_path = os.path.join(tmp, "stats.xlsx")
        for path in [project_path, exp_path, stats_path]:
            with open(path, "wb") as handle:
                handle.write(b"x")

        session_id, session = _build_session(project_path, [exp_path], [stats_path])

        assert session_id
        assert session["file_basenames"] == ["project.xlsx", "exp.xlsx", "stats.xlsx"]
        assert session["file_paths"] == [project_path, exp_path, stats_path]


def test_summarize_edit_submission_tracks_replaced_unchanged_and_added():
    prior_session = {
        "project_path": "/tmp/project.xlsx",
        "exp_paths": ["/tmp/exp1.xlsx"],
        "stats_paths": ["/tmp/stats1.xlsx"],
    }

    summary = _summarize_edit_submission(
        prior_session,
        _upload("project-new.xlsx"),
        [_upload("")],
        [_upload("stats1-new.xlsx")],
        [_upload("exp2.xlsx")],
        [_upload("stats2.xlsx")],
    )

    assert "project.xlsx -> project-new.xlsx" in summary["replaced"]
    assert "stats1.xlsx -> stats1-new.xlsx" in summary["replaced"]
    assert "exp1.xlsx" in summary["unchanged"]
    assert "exp2.xlsx" in summary["added"]
    assert "stats2.xlsx" in summary["added"]
