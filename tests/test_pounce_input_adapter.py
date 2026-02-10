import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

from src.input_adapters.pounce_sheets.pounce_input_adapter import PounceInputAdapter
from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.models.pounce.project import Project, Person, ProjectPersonEdge, ProjectBiosampleEdge, AccessLevel
from src.models.pounce.biosample import Biosample, BiosampleBiospecimenEdge
from src.models.pounce.biospecimen import BioSpecimen
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
        adapter.experiment_file = None
        adapter.experiment_parser = None
        adapter.stats_results_file = None
        adapter.stats_results_parser = None
        adapter._biosample_by_original_id = {}
        adapter._run_biosample_by_original_id = {}
        adapter._gene_by_raw_id = {}
        adapter._metabolite_by_raw_id = {}

    return adapter


# --- _get_persons_from_lists tests ---

def test_get_persons_from_lists_with_matching_names_and_emails():
    # Arrange
    names = ["Alice", "Bob"]
    emails = ["alice@example.com", "bob@example.com"]

    # Act
    result = PounceInputAdapter._get_persons_from_lists(names, emails)

    # Assert
    assert len(result) == 2
    assert result[0].id == "Alice"
    assert result[0].email == "alice@example.com"
    assert result[1].id == "Bob"
    assert result[1].email == "bob@example.com"


def test_get_persons_from_lists_with_empty_emails():
    # Arrange
    names = ["Alice", "Bob"]
    emails = []

    # Act
    result = PounceInputAdapter._get_persons_from_lists(names, emails)

    # Assert
    assert len(result) == 2
    assert result[0].id == "Alice"
    assert result[0].email is None
    assert result[1].id == "Bob"
    assert result[1].email is None


def test_get_persons_from_lists_with_mismatched_counts_raises():
    # Arrange
    names = ["Alice", "Bob"]
    emails = ["alice@example.com"]  # Only one email for two names

    # Act & Assert
    with pytest.raises(LookupError):
        PounceInputAdapter._get_persons_from_lists(names, emails)


def test_get_persons_from_lists_with_empty_names():
    # Arrange
    names = []
    emails = []

    # Act
    result = PounceInputAdapter._get_persons_from_lists(names, emails)

    # Assert
    assert result == []


# --- get_datasource_name tests ---

def test_get_datasource_name_returns_ncats_pounce():
    # Arrange
    adapter = create_adapter_with_mock({})

    # Act
    result = adapter.get_datasource_name()

    # Assert
    assert result == DataSourceName.NCATSPounce


# --- get_project_* tests ---

def test_get_project_id():
    # Arrange
    adapter = create_adapter_with_mock({"project_id": "PROJ001"})

    # Act
    result = adapter.get_project_id()

    # Assert
    assert result == "PROJ001"


def test_get_project_name():
    # Arrange
    adapter = create_adapter_with_mock({"project_name": "My Test Project"})

    # Act
    result = adapter.get_project_name()

    # Assert
    assert result == "My Test Project"


def test_get_project_rd_tag_true():
    # Arrange
    adapter = create_adapter_with_mock({"RD_tag": "yes"})

    # Act
    result = adapter.get_project_rd_tag()

    # Assert
    assert result is True


def test_get_project_rd_tag_false():
    # Arrange
    adapter = create_adapter_with_mock({"RD_tag": "no"})

    # Act
    result = adapter.get_project_rd_tag()

    # Assert
    assert result is False


def test_get_project_rd_tag_none():
    # Arrange
    adapter = create_adapter_with_mock({})

    # Act
    result = adapter.get_project_rd_tag()

    # Assert
    assert result is False


def test_get_project_privacy_level():
    # Arrange
    adapter = create_adapter_with_mock({"privacy_type": "public"})

    # Act
    result = adapter.get_project_privacy_level()

    # Assert
    assert result == AccessLevel.public


def test_get_project_keywords():
    # Arrange
    adapter = create_adapter_with_mock({"keywords": ["cancer", "genomics", "biomarkers"]})

    # Act
    result = adapter.get_project_keywords()

    # Assert
    assert result == ["cancer", "genomics", "biomarkers"]


# --- _create_project tests ---

def test_create_project_creates_project_with_all_fields():
    # Arrange
    project_data = {
        "project_id": "PROJ001",
        "project_name": "Test Project",
        "description": "A test project",
        "lab_groups": ["Lab A", "Lab B"],
        "privacy_type": "ncats",
        "keywords": ["test", "example"],
        "project_type": ["Metabolomics"],
        "RD_tag": "true",
        "biosample_preparation": "Standard prep"
    }
    adapter = create_adapter_with_mock(project_data)

    # Act
    result = adapter._create_project()

    # Assert
    assert isinstance(result, Project)
    assert result.id == "PROJ001"
    assert result.name == "Test Project"
    assert result.description == "A test project"
    assert result.lab_groups == ["Lab A", "Lab B"]
    assert result.access == AccessLevel.ncats
    assert result.keywords == ["test", "example"]
    assert result.project_type == ["Metabolomics"]
    assert result.rare_disease_focus is True
    assert result.sample_preparation == "Standard prep"


# --- get_all tests ---

def test_get_all_yields_project_and_persons():
    # Arrange
    project_data = {
        "project_id": "PROJ001",
        "project_name": "Test Project",
        "description": "A test",
        "lab_groups": [],
        "privacy_type": "public",
        "keywords": [],
        "project_type": [],
        "RD_tag": "false",
        "biosample_preparation": "",
        "owner_name": ["Alice"],
        "owner_email": ["alice@example.com"],
        "collaborator_name": ["Bob"],
        "collaborator_email": ["bob@example.com"]
    }

    # Empty biosample data
    biosample_meta_df = pd.DataFrame(columns=["sample_id"])

    adapter = create_adapter_with_mock(project_data, {}, biosample_meta_df)

    # Act
    batches = list(adapter.get_all())

    # Assert - first batch should have project, owners, collaborators, and edges
    first_batch = batches[0]

    projects = [obj for obj in first_batch if isinstance(obj, Project)]
    persons = [obj for obj in first_batch if isinstance(obj, Person)]
    person_edges = [obj for obj in first_batch if isinstance(obj, ProjectPersonEdge)]

    assert len(projects) == 1
    assert projects[0].id == "PROJ001"

    assert len(persons) == 2
    assert persons[0].id == "Alice"
    assert persons[1].id == "Bob"

    assert len(person_edges) == 2
    owner_edge = [e for e in person_edges if e.role == "Owner"][0]
    collab_edge = [e for e in person_edges if e.role == "Collaborator"][0]
    assert owner_edge.end_node.id == "Alice"
    assert collab_edge.end_node.id == "Bob"


def test_get_all_yields_biospecimens_and_biosamples():
    # Arrange
    project_data = {
        "project_id": "PROJ001",
        "project_name": "Test Project",
        "description": "A test",
        "lab_groups": [],
        "privacy_type": "public",
        "keywords": [],
        "project_type": [],
        "RD_tag": "false",
        "biosample_preparation": "",
        "owner_name": [],
        "owner_email": [],
        "collaborator_name": [],
        "collaborator_email": []
    }

    biosample_map_data = {
        "biosample_id": "SampleID",
        "biosample_type": "SampleType",
        "biospecimen_id": "SpecimenID",
        "biospecimen_type": "SpecimenType",
        "biospecimen_description": "Description",
        "organism_names": "Organism",
        "disease_names": "Disease"
    }

    biosample_meta_df = pd.DataFrame({
        "SampleID": ["S1", "S2"],
        "SampleType": ["blood", "tissue"],
        "SpecimenID": ["SP1", "SP1"],  # Same specimen for both samples
        "SpecimenType": ["human", "human"],
        "Description": ["Desc 1", "Desc 1"],
        "Organism": ["Homo sapiens", "Homo sapiens"],
        "Disease": ["Cancer", "Cancer"]
    })

    adapter = create_adapter_with_mock(project_data, biosample_map_data, biosample_meta_df)

    # Act
    batches = list(adapter.get_all())

    # Assert
    # Batch 0: project, persons, edges
    # Batch 1: biospecimens
    # Batch 2: biosamples
    # Batch 3: exposures
    # Batch 4: sample_exposure_edges
    # Batch 5: project_biosample_edges
    # Batch 6: biosample_biospecimen_edges

    assert len(batches) >= 2

    # Check biospecimens (should be deduplicated - only 1 unique)
    biospecimens = batches[1]
    assert len(biospecimens) == 1
    assert isinstance(biospecimens[0], BioSpecimen)
    assert biospecimens[0].original_id == "SP1"

    # Check biosamples
    biosamples = batches[2]
    assert len(biosamples) == 2
    assert all(isinstance(b, Biosample) for b in biosamples)


def test_get_all_creates_correct_edges():
    # Arrange
    project_data = {
        "project_id": "PROJ001",
        "project_name": "Test Project",
        "description": "A test",
        "lab_groups": [],
        "privacy_type": "public",
        "keywords": [],
        "project_type": [],
        "RD_tag": "false",
        "biosample_preparation": "",
        "owner_name": [],
        "owner_email": [],
        "collaborator_name": [],
        "collaborator_email": []
    }

    biosample_map_data = {
        "biosample_id": "SampleID",
        "biosample_type": "SampleType",
        "biospecimen_id": "SpecimenID",
        "biospecimen_type": "SpecimenType",
        "biospecimen_description": "Description",
        "organism_names": "Organism",
        "disease_names": "Disease"
    }

    biosample_meta_df = pd.DataFrame({
        "SampleID": ["S1"],
        "SampleType": ["blood"],
        "SpecimenID": ["SP1"],
        "SpecimenType": ["human"],
        "Description": ["Desc"],
        "Organism": ["Homo sapiens"],
        "Disease": ["Cancer"]
    })

    adapter = create_adapter_with_mock(project_data, biosample_map_data, biosample_meta_df)

    # Act
    batches = list(adapter.get_all())

    # Assert - check project->biosample edges
    project_biosample_edges = batches[5]
    assert len(project_biosample_edges) == 1
    assert isinstance(project_biosample_edges[0], ProjectBiosampleEdge)
    assert project_biosample_edges[0].start_node.id == "PROJ001"

    # Check biosample->biospecimen edges
    biosample_biospecimen_edges = batches[6]
    assert len(biosample_biospecimen_edges) == 1
    assert isinstance(biosample_biospecimen_edges[0], BiosampleBiospecimenEdge)
