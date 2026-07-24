from datetime import date

import pytest

from src.models.datasource_version_info import DataSourceDetails


def test_parse_tsv_accepts_full_datasource_details():
    details = DataSourceDetails.parse_tsv("HMDB\t5.0\t2021-11-17\tNone")

    assert details.name == "HMDB"
    assert details.version == "5.0"
    assert details.version_date == date(2021, 11, 17)
    assert details.download_date is None


def test_parse_tsv_accepts_name_only_source_labels():
    details = DataSourceDetails.parse_tsv("HMDB")

    assert details.name == "HMDB"
    assert details.version is None
    assert details.version_date is None
    assert details.download_date is None


def test_parse_tsv_rejects_partial_datasource_details():
    with pytest.raises(ValueError, match="1 or 4 tab-separated fields"):
        DataSourceDetails.parse_tsv("HMDB\t5.0")
