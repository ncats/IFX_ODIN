import pytest
import pandas as pd
from unittest.mock import patch
from datetime import datetime

from src.input_adapters.excel_sheet_adapter import ExcelsheetParser


def create_parser_with_sheets(sheet_data: dict) -> ExcelsheetParser:
    """Helper to create a parser with mocked sheet data."""
    with patch.object(ExcelsheetParser, '_read_all_sheets', return_value=sheet_data):
        parser = ExcelsheetParser(file_path="dummy.xlsx")
    return parser


def create_key_value_df(data: dict) -> pd.DataFrame:
    """Helper to create a DataFrame with NCATSDPI_Variable_Name and Submitter_Value columns."""
    return pd.DataFrame({
        ExcelsheetParser.KEY_COLUMN: list(data.keys()),
        ExcelsheetParser.VALUE_COLUMN: list(data.values())
    })


def create_mapping_df(data: dict) -> pd.DataFrame:
    """Helper to create a DataFrame with NCATSDPI_Variable_Name and Submitter_Variable_Name columns."""
    return pd.DataFrame({
        ExcelsheetParser.KEY_COLUMN: list(data.keys()),
        ExcelsheetParser.MAPPED_VALUE_COLUMN: list(data.values())
    })


# --- get_one_string tests ---

def test_get_one_string_returns_trimmed_value():
    # Arrange
    sheet_df = create_key_value_df({"project_name": "  My Project  "})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string("TestSheet", "project_name")

    # Assert
    assert result == "My Project"


def test_get_one_string_returns_none_for_empty_string():
    # Arrange
    sheet_df = create_key_value_df({"project_name": ""})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string("TestSheet", "project_name")

    # Assert
    assert result is None


def test_get_one_string_returns_none_for_nan():
    # Arrange
    sheet_df = create_key_value_df({"project_name": float('nan')})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string("TestSheet", "project_name")

    # Assert
    assert result is None


def test_get_one_string_raises_for_missing_key():
    # Arrange
    sheet_df = create_key_value_df({"other_key": "value"})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act & Assert
    with pytest.raises(LookupError):
        parser.get_one_string("TestSheet", "project_name")


def test_get_one_string_raises_for_duplicate_keys():
    # Arrange
    sheet_df = pd.DataFrame({
        ExcelsheetParser.KEY_COLUMN: ["project_name", "project_name"],
        ExcelsheetParser.VALUE_COLUMN: ["Value1", "Value2"]
    })
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act & Assert
    with pytest.raises(LookupError):
        parser.get_one_string("TestSheet", "project_name")


# --- get_one_string_list tests ---

def test_get_one_string_list_splits_by_pipe():
    # Arrange
    sheet_df = create_key_value_df({"keywords": "alpha|beta|gamma"})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string_list("TestSheet", "keywords")

    # Assert
    assert result == ["alpha", "beta", "gamma"]


def test_get_one_string_list_trims_values():
    # Arrange
    sheet_df = create_key_value_df({"keywords": " alpha | beta | gamma "})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string_list("TestSheet", "keywords")

    # Assert
    assert result == ["alpha", "beta", "gamma"]


def test_get_one_string_list_returns_empty_for_none():
    # Arrange
    sheet_df = create_key_value_df({"keywords": ""})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string_list("TestSheet", "keywords")

    # Assert
    assert result == []


def test_get_one_string_list_custom_delimiter():
    # Arrange
    sheet_df = create_key_value_df({"keywords": "alpha,beta,gamma"})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string_list("TestSheet", "keywords", delimiter=",")

    # Assert
    assert result == ["alpha", "beta", "gamma"]


def test_get_one_string_list_single_value():
    # Arrange
    sheet_df = create_key_value_df({"keywords": "single"})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_string_list("TestSheet", "keywords")

    # Assert
    assert result == ["single"]


# --- get_one_date tests ---

def test_get_one_date_parses_yyyymmdd():
    # Arrange
    sheet_df = create_key_value_df({"date": "20240315"})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_date("TestSheet", "date")

    # Assert
    assert result == datetime(2024, 3, 15)


def test_get_one_date_handles_integer_value():
    # Arrange
    sheet_df = create_key_value_df({"date": 20240315})
    parser = create_parser_with_sheets({"TestSheet": sheet_df})

    # Act
    result = parser.get_one_date("TestSheet", "date")

    # Assert
    assert result == datetime(2024, 3, 15)


# --- get_parameter_map tests ---

def test_get_parameter_map_returns_dict():
    # Arrange
    sheet_df = create_mapping_df({
        "biosample_id": "Sample_ID",
        "biosample_type": "Sample_Type"
    })
    parser = create_parser_with_sheets({"MappingSheet": sheet_df})

    # Act
    result = parser.get_parameter_map("MappingSheet")

    # Assert
    assert result == {
        "biosample_id": "Sample_ID",
        "biosample_type": "Sample_Type"
    }


def test_get_parameter_map_caches_result():
    # Arrange
    sheet_df = create_mapping_df({"key": "value"})
    parser = create_parser_with_sheets({"MappingSheet": sheet_df})

    # Act
    result1 = parser.get_parameter_map("MappingSheet")
    result2 = parser.get_parameter_map("MappingSheet")

    # Assert
    assert result1 is result2  # Same object from cache


def test_get_parameter_map_skips_rows_with_missing_values():
    # Arrange
    sheet_df = pd.DataFrame({
        ExcelsheetParser.KEY_COLUMN: ["key1", "key2", "key3"],
        ExcelsheetParser.MAPPED_VALUE_COLUMN: ["value1", None, "value3"]
    })
    parser = create_parser_with_sheets({"MappingSheet": sheet_df})

    # Act
    result = parser.get_parameter_map("MappingSheet")

    # Assert
    assert result == {"key1": "value1", "key3": "value3"}


# --- get_mapped_value tests ---

def test_get_mapped_value_returns_value():
    # Arrange
    sheet_df = create_mapping_df({"biosample_id": "Sample_ID"})
    parser = create_parser_with_sheets({"MappingSheet": sheet_df})

    # Act
    result = parser.get_mapped_value("MappingSheet", "biosample_id")

    # Assert
    assert result == "Sample_ID"


def test_get_mapped_value_raises_for_missing_key():
    # Arrange
    sheet_df = create_mapping_df({"other_key": "value"})
    parser = create_parser_with_sheets({"MappingSheet": sheet_df})

    # Act & Assert
    with pytest.raises(KeyError):
        parser.get_mapped_value("MappingSheet", "biosample_id")
