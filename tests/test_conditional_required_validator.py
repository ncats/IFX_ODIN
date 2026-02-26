"""Tests for ConditionalRequiredValidator."""

import pytest
from dataclasses import dataclass
from typing import List, Optional

from src.core.validator import ConditionalRequiredValidator


@dataclass
class MockExperiment:
    platform_type: Optional[str] = None
    metabolite_identification_description: Optional[str] = None


@dataclass
class MockData:
    experiments: List[MockExperiment] = None

    def __post_init__(self):
        if self.experiments is None:
            self.experiments = []


def make_validator():
    return ConditionalRequiredValidator(
        entity="experiments",
        field="metabolite_identification_description",
        when_field="platform_type",
        when_values=["metabolomics", "lipidomics"],
        message="Sheet ExperimentMeta is missing required field: metabolite_identification_description",
        sheet="ExperimentMeta",
        column="metabolite_identification_description",
    )


class TestConditionNotMet:
    def test_no_error_when_platform_type_is_transcriptomics(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="bulk_rnaseq", metabolite_identification_description=None)
        ])
        assert make_validator().validate(data) == []

    def test_no_error_when_platform_type_is_none(self):
        data = MockData(experiments=[
            MockExperiment(platform_type=None, metabolite_identification_description=None)
        ])
        assert make_validator().validate(data) == []

    def test_no_error_when_platform_type_is_unrecognized(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="proteomics", metabolite_identification_description=None)
        ])
        assert make_validator().validate(data) == []


class TestConditionMet:
    def test_error_when_metabolomics_and_field_missing(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="metabolomics", metabolite_identification_description=None)
        ])
        errors = make_validator().validate(data)
        assert len(errors) == 1
        assert errors[0].field == "metabolite_identification_description"
        assert errors[0].row == 0

    def test_error_when_lipidomics_and_field_missing(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="lipidomics", metabolite_identification_description=None)
        ])
        errors = make_validator().validate(data)
        assert len(errors) == 1

    def test_no_error_when_metabolomics_and_field_present(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="metabolomics", metabolite_identification_description="LC-MS/MS")
        ])
        assert make_validator().validate(data) == []

    def test_error_when_field_is_empty_string(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="metabolomics", metabolite_identification_description="")
        ])
        assert len(make_validator().validate(data)) == 1


class TestCaseInsensitive:
    def test_uppercase_platform_type_matches(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="Metabolomics", metabolite_identification_description=None)
        ])
        assert len(make_validator().validate(data)) == 1

    def test_mixed_case_platform_type_matches(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="LIPIDOMICS", metabolite_identification_description=None)
        ])
        assert len(make_validator().validate(data)) == 1


class TestMultipleExperiments:
    def test_only_flagged_experiments_produce_errors(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="bulk_rnaseq", metabolite_identification_description=None),
            MockExperiment(platform_type="metabolomics", metabolite_identification_description=None),
            MockExperiment(platform_type="metabolomics", metabolite_identification_description="described"),
            MockExperiment(platform_type="lipidomics", metabolite_identification_description=None),
        ])
        errors = make_validator().validate(data)
        assert len(errors) == 2
        assert errors[0].row == 1
        assert errors[1].row == 3

    def test_no_errors_when_all_pass(self):
        data = MockData(experiments=[
            MockExperiment(platform_type="bulk_rnaseq", metabolite_identification_description=None),
            MockExperiment(platform_type="metabolomics", metabolite_identification_description="described"),
        ])
        assert make_validator().validate(data) == []


class TestEdgeCases:
    def test_empty_experiments_list(self):
        assert make_validator().validate(MockData(experiments=[])) == []

    def test_missing_entity(self):
        assert make_validator().validate(MockData()) == []
