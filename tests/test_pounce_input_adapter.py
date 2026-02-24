import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

from src.input_adapters.pounce_sheets.parsed_classes import ParsedPerson
from src.input_adapters.pounce_sheets.pounce_input_adapter import PounceInputAdapter
from src.input_adapters.pounce_sheets.pounce_node_builder import PounceNodeBuilder
from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.models.pounce.project import Project
from src.constants import DataSourceName


def create_mock_parser(project_data: dict, biosample_map_data: dict = None, biosample_meta_df: pd.DataFrame = None):
    """Helper to create a PounceInputAdapter with mocked parser data."""
    mock_parser = MagicMock(spec=ExcelsheetParser)

    # Mock get_one_string
    def mock_get_one_string(sheet_name, data_key):
        if sheet_name == "ProjectMeta":
            return project_data.get(data_key)
        return None
    mock_parser.get_one_string = MagicMock(side_effect=mock_get_one_string)

    # Mock get_one_string_list
    def mock_get_one_string_list(sheet_name, data_key):
        if sheet_name == "ProjectMeta":
            val = project_data.get(data_key)
            if val is None:
                return []
            if isinstance(val, list):
                return val
            return [val]
        return []
    mock_parser.get_one_string_list = MagicMock(side_effect=mock_get_one_string_list)

    # Mock get_one_date
    def mock_get_one_date(sheet_name, data_key):
        if sheet_name == "ProjectMeta" and data_key == "date":
            return datetime(2024, 3, 15)
        return None
    mock_parser.get_one_date = MagicMock(side_effect=mock_get_one_date)

    # Mock get_parameter_map
    def mock_get_parameter_map(sheet_name):
        if sheet_name == "BioSampleMap" and biosample_map_data:
            return biosample_map_data
        return {}
    mock_parser.get_parameter_map = MagicMock(side_effect=mock_get_parameter_map)

    # Mock sheet_dfs
    mock_parser.sheet_dfs = {}
    if biosample_meta_df is not None:
        mock_parser.sheet_dfs["BioSampleMeta"] = biosample_meta_df

    return mock_parser


def create_adapter_with_mock(project_data: dict, biosample_map_data: dict = None, biosample_meta_df: pd.DataFrame = None):
    """Helper to create a PounceInputAdapter with mocked parser."""
    mock_parser = create_mock_parser(project_data, biosample_map_data, biosample_meta_df)

    with patch.object(PounceInputAdapter, '__init__', lambda self, **kwargs: None):
        adapter = PounceInputAdapter()
        adapter.project_file = "dummy.xlsx"
        adapter.project_parser = mock_parser
        adapter.experiment_files = []
        adapter.stats_results_files = []

    return adapter


# --- _person_nodes_and_edges tests ---

def test_person_nodes_and_edges_with_names_and_emails():
    proj = Project(id="PROJ001", name="Test")
    people = [
        ParsedPerson(name="Alice", email="alice@example.com", role="Owner"),
        ParsedPerson(name="Bob", email="bob@example.com", role="Owner"),
    ]
    nodes, edges = PounceNodeBuilder._person_nodes_and_edges(proj, people)
    assert len(nodes) == 2
    assert nodes[0].id == "alice@example.com"
    assert nodes[0].email == "alice@example.com"
    assert nodes[1].id == "bob@example.com"
    assert nodes[1].email == "bob@example.com"


def test_person_nodes_and_edges_with_no_email():
    proj = Project(id="PROJ001", name="Test")
    people = [ParsedPerson(name="Alice", role="Owner")]
    nodes, edges = PounceNodeBuilder._person_nodes_and_edges(proj, people)
    assert len(nodes) == 1
    assert nodes[0].id == "alice"
    assert nodes[0].email is None


def test_person_nodes_and_edges_with_empty_people():
    proj = Project(id="PROJ001", name="Test")
    nodes, edges = PounceNodeBuilder._person_nodes_and_edges(proj, [])
    assert nodes == []
    assert edges == []


def test_person_nodes_and_edges_creates_correct_edges():
    proj = Project(id="PROJ001", name="Test")
    people = [
        ParsedPerson(name="Alice", email="alice@example.com", role="Owner"),
        ParsedPerson(name="Bob", email="bob@example.com", role="Collaborator"),
    ]
    nodes, edges = PounceNodeBuilder._person_nodes_and_edges(proj, people)
    assert len(edges) == 2
    assert edges[0].role == "Owner"
    assert edges[1].role == "Collaborator"


# --- get_datasource_name tests ---

def test_get_datasource_name_returns_ncats_pounce():
    # Arrange
    adapter = create_adapter_with_mock({})

    # Act
    result = adapter.get_datasource_name()

    # Assert
    assert result == DataSourceName.NCATSPounce
