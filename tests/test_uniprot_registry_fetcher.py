import gzip
import json
from pathlib import Path

import pytest

from src.registry.sources.uniprot import _validate_full_human_includes_reviewed


def _write_uniprot_json(path: Path, records: list[dict]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump({"results": records}, handle)


def _reviewed_record(accession: str, secondary: list[str] | None = None) -> dict:
    return {
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "primaryAccession": accession,
        "secondaryAccessions": secondary or [],
    }


def _unreviewed_record(accession: str, secondary: list[str] | None = None) -> dict:
    return {
        "entryType": "UniProtKB unreviewed (TrEMBL)",
        "primaryAccession": accession,
        "secondaryAccessions": secondary or [],
    }


def test_uniprot_human_validation_accepts_full_file_with_all_reviewed_accessions(tmp_path: Path):
    full_path = tmp_path / "uniprot-human.json.gz"
    reviewed_path = tmp_path / "uniprot-human-reviewed.json.gz"
    _write_uniprot_json(
        full_path,
        [
            _reviewed_record("P1", ["P1OLD"]),
            _reviewed_record("P2"),
            _unreviewed_record("Q1"),
        ],
    )
    _write_uniprot_json(
        reviewed_path,
        [
            _reviewed_record("P1", ["P1OLD"]),
            _reviewed_record("P2"),
        ],
    )

    stats = _validate_full_human_includes_reviewed(full_path, reviewed_path)

    assert stats["full_records"] == 3
    assert stats["reviewed_records"] == 2
    assert stats["missing_reviewed_primary_accessions"] == 0
    assert stats["missing_reviewed_secondary_accessions"] == 0


def test_uniprot_human_validation_rejects_full_file_missing_reviewed_accessions(tmp_path: Path):
    full_path = tmp_path / "uniprot-human.json.gz"
    reviewed_path = tmp_path / "uniprot-human-reviewed.json.gz"
    _write_uniprot_json(full_path, [_reviewed_record("P1", ["P1OLD"])])
    _write_uniprot_json(
        reviewed_path,
        [
            _reviewed_record("P1", ["P1OLD"]),
            _reviewed_record("P2", ["P2OLD"]),
        ],
    )

    with pytest.raises(ValueError, match="P2"):
        _validate_full_human_includes_reviewed(full_path, reviewed_path)
