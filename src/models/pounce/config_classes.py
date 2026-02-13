from abc import abstractmethod, ABC
from typing import List

from src.input_adapters.pounce_sheets.constants import ProjectWorkbook, ExperimentWorkbook
from src.models.pounce.biosample import Biosample
from src.models.pounce.biospecimen import Biospecimen
from src.models.pounce.category_value import CategoryValue
from src.models.pounce.demographics import Demographics
from src.models.pounce.exposure import Exposure
from src.models.pounce.experiment import RunBiosample


def get_column_name(map_name, key, index=None):
    if index is not None:
        key = key.format(index)
    if key in map_name:
        val = map_name[key]
        if val == '' or val == 'NA' or val == 'N/A':
            return None
        return val
    return None

def get_list_of_column_names(map_name, key):
    """Returns a list of column names starting at 1"""
    column_names = []
    index = 1
    while True:
        column_name = get_column_name(map_name, key, index)
        if column_name is None or column_name == '':
            break
        column_names.append(column_name)
        index += 1
    if len(column_names) == 0:
        return None
    return column_names

class ColumnConfig(ABC):
    biosample_map: dict

    def __init__(self, biosample_map: dict):
        super().__init__()
        self.biosample_map = biosample_map

    @abstractmethod
    def get_data(self, row):
        raise NotImplementedError

    def get_row_value(self, row, column_name, required=False, is_list=False):
        if column_name is None or column_name == '':
            if required:
                raise ValueError(f"No column name was given")
            return None
        if column_name not in row and required:
            raise ValueError(f"Column {column_name} is required in sheet {ProjectWorkbook.BiosampleMetaSheet.name}")
        val = row[column_name]
        if val is None or val == '' or val == 'NA' or val == 'N/A':
            return None
        if is_list:
            return [v.strip() for v in val.split('|')]

        return val.strip() if isinstance(val, str) else val

    def get_category_value(self, row, column_name):
        val = self.get_row_value(row, column_name)
        if val is None or val == '':
            return None
        return CategoryValue(
            id=f"{column_name}-{val}",
            name=column_name, value=val
        )

    def __repr__(self):
        items = vars(self).items()
        max_key_len = max(len(k) for k in vars(self).keys()) if items else 0
        attrs = ',\n\t'.join(f"{k:<{max_key_len}} = {v!r}" for k, v in items)
        return f"{self.__class__.__name__}(\n\t{attrs}\n)"

class ExposureConfig(ColumnConfig):
    exposure_names_column: str
    exposure_names_column: str
    exposure_type_column: str
    exposure_category_column: str
    exposure_concentration_column: str
    exposure_unit_column: str
    exposure_time_column: str
    exposure_time_unit_column: str
    exposure_start_column: str
    exposure_end_column: str
    condition_category_column: str
    media_column: str

    def get_data(self, row):
        return Exposure(
            id = 'calculate',
            names = self.get_row_value(row, self.exposure_names_column, True, True),
            type = self.get_row_value(row, self.exposure_type_column, True),
            category = self.get_category_value(row, self.exposure_category_column),
            concentration = self.get_row_value(row, self.exposure_concentration_column),
            concentration_unit= self.get_row_value(row, self.exposure_unit_column),
            duration = self.get_row_value(row, self.exposure_time_column),
            duration_unit = self.get_row_value(row, self.exposure_time_unit_column),
            start_time = self.get_row_value(row, self.exposure_start_column),
            end_time = self.get_row_value(row, self.exposure_end_column),
            condition = self.get_category_value(row, self.condition_category_column),
            growth_media = self.get_row_value(row, self.media_column)
        )

    def __init__(self, biosample_map, exposure_number):
        super().__init__(biosample_map)
        self.exposure_names_column = get_column_name(biosample_map,
                                                     ProjectWorkbook.BiosampleMapSheet.Key.exposure_names,
                                                     exposure_number)
        self.exposure_type_column = get_column_name(biosample_map,
                                                    ProjectWorkbook.BiosampleMapSheet.Key.exposure_type,
                                                    exposure_number)
        self.exposure_category_column = get_column_name(biosample_map,
                                                        ProjectWorkbook.BiosampleMapSheet.Key.exposure_category,
                                                        exposure_number)
        self.exposure_concentration_column = get_column_name(biosample_map,
                                                             ProjectWorkbook.BiosampleMapSheet.Key.exposure_concentration,
                                                             exposure_number)
        self.exposure_unit_column = get_column_name(biosample_map,
                                                    ProjectWorkbook.BiosampleMapSheet.Key.exposure_unit,
                                                    exposure_number)
        self.exposure_time_column = get_column_name(biosample_map,
                                                    ProjectWorkbook.BiosampleMapSheet.Key.exposure_time,
                                                    exposure_number)
        self.exposure_time_unit_column = get_column_name(biosample_map,
                                                         ProjectWorkbook.BiosampleMapSheet.Key.exposure_time_unit,
                                                         exposure_number)
        self.exposure_start_column = get_column_name(biosample_map,
                                                     ProjectWorkbook.BiosampleMapSheet.Key.exposure_start,
                                                     exposure_number)
        self.exposure_end_column = get_column_name(biosample_map,
                                                   ProjectWorkbook.BiosampleMapSheet.Key.exposure_end,
                                                   exposure_number)
        self.condition_category_column = get_column_name(biosample_map,
                                                         ProjectWorkbook.BiosampleMapSheet.Key.condition_category,
                                                         exposure_number)
        self.media_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.growth_media)

    @staticmethod
    def get_valid_configs(biosample_map):
        """Returns a list of ExposureConfig objects starting at 1 until names, type, and category are all missing."""
        configs = []
        exposure_number = 1
        while True:
            config = ExposureConfig(biosample_map, exposure_number)
            if all(col in ('', None) for col in
                   [config.exposure_names_column, config.exposure_type_column, config.exposure_category_column]):
                break
            configs.append(config)
            exposure_number += 1
        return configs

class BiospecimenConfig(ColumnConfig):
    id_column: str
    type_column: str
    description_column: str
    organism_names_column: str
    organism_category_column: str
    disease_names_column: str
    disease_category_column: str
    project_id: str

    def get_data(self, row):
        biospecimen_id = self.get_row_value(row, self.id_column, True)
        return Biospecimen(
                id=f"{self.project_id}-{biospecimen_id}",
                original_id=biospecimen_id,
                type=self.get_row_value(row, self.type_column, True),
                description=self.get_row_value(row, self.description_column),
                organism=self.get_row_value(row, self.organism_names_column, True),
                organism_category=self.get_category_value(row, self.organism_category_column),
                disease_category=self.get_category_value(row, self.disease_category_column),
                diseases=self.get_row_value(row, self.disease_names_column, True, True)
            )

    def __init__(self, biosample_map, project_id):
        super().__init__(biosample_map)
        self.project_id = project_id
        self.id_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.biospecimen_id)
        self.type_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.biospecimen_type)
        self.description_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.biospecimen_description)
        self.organism_names_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.organism_names)
        self.organism_category_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.organism_category)
        self.disease_names_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.disease_names)
        self.disease_category_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.disease_category)

class BiosampleConfig(ColumnConfig):
    id_column: str
    type_column: str
    project_id: str

    def get_data(self, row):
        biospecimen_config = BiospecimenConfig(self.biosample_map, self.project_id)
        biosample_id = self.get_row_value(row, self.id_column, True)
        biospecimen_id = self.get_row_value(row, biospecimen_config.id_column, True)
        biosample_type = self.get_row_value(row, self.type_column, True)
        demographics_config = DemographicsConfig(self.biosample_map)
        full_id = f"{self.project_id}-{biosample_id}-{biospecimen_id}"
        return Biosample(
                id=full_id,
                original_id=biosample_id,
                type=biosample_type,
                demographics=demographics_config.get_data(row, full_id)
            )


    def __init__(self, biosample_map, project_id):
        super().__init__(biosample_map)
        self.project_id = project_id
        self.id_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.biosample_id)
        self.type_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.biosample_type)


class DemographicsConfig(ColumnConfig):
    age_column: str
    race_column: str
    ethnicity_column: str
    sex_column: str
    demographic_category_columns: List[str]
    phenotype_category_columns: List[str]

    def __init__(self, biosample_map):
        super().__init__(biosample_map)
        self.age_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.age)
        self.race_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.race)
        self.ethnicity_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.ethnicity)
        self.sex_column = get_column_name(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.sex)
        self.demographic_category_columns = get_list_of_column_names(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.demographic_category)
        self.phenotype_category_columns = get_list_of_column_names(biosample_map, ProjectWorkbook.BiosampleMapSheet.Key.phenotype_category)

    def get_data(self, row, parent_id: str = None):
        age = self.get_row_value(row, self.age_column)
        race = self.get_row_value(row, self.race_column)
        ethnicity = self.get_row_value(row, self.ethnicity_column)
        sex = self.get_row_value(row, self.sex_column)
        categories = None if self.demographic_category_columns is None else [self.get_category_value(row, col) for col in self.demographic_category_columns]
        phenotype_categories = None if self.phenotype_category_columns is None else [self.get_category_value(row, col) for col in self.phenotype_category_columns]

        if all(v is None for v in [age, race, ethnicity, sex, categories, phenotype_categories]):
            return None
        return Demographics(
            id=f"{parent_id}::demographics" if parent_id else None,
            age=age, race=race, ethnicity=ethnicity, sex=sex, categories=categories,
            phenotype_categories=phenotype_categories
        )

    @staticmethod
    def get_config(biosample_map):
        config = DemographicsConfig(biosample_map)
        if all(col in ('', None) for col in
               [config.age_column, config.race_column, config.ethnicity_column, config.sex_column, config.demographic_category_column]):
            return None
        return config


class RunBiosampleConfig(ColumnConfig):
    run_biosample_id_column: str
    biosample_id_column: str
    biological_replicate_number_column: str
    technical_replicate_number_column: str
    biosample_run_order_column: str
    project_id: str

    def __init__(self, run_sample_map: dict, project_id: str):
        super().__init__(run_sample_map)
        self.project_id = project_id
        self.run_biosample_id_column = get_column_name(run_sample_map, ExperimentWorkbook.RunSampleMapSheet.Key.run_biosample_id)
        self.biosample_id_column = get_column_name(run_sample_map, ExperimentWorkbook.RunSampleMapSheet.Key.biosample_id)
        self.biological_replicate_number_column = get_column_name(run_sample_map, ExperimentWorkbook.RunSampleMapSheet.Key.biological_replicate_number)
        self.technical_replicate_number_column = get_column_name(run_sample_map, ExperimentWorkbook.RunSampleMapSheet.Key.technical_replicate_number)
        self.biosample_run_order_column = get_column_name(run_sample_map, ExperimentWorkbook.RunSampleMapSheet.Key.biosample_run_order)

    def get_data(self, row):
        run_biosample_id = self.get_row_value(row, self.run_biosample_id_column, True)
        return RunBiosample(
            id=f"{self.project_id}-{run_biosample_id}",
            biological_replicate_number=self._parse_int(self.get_row_value(row, self.biological_replicate_number_column)),
            technical_replicate_number=self._parse_int(self.get_row_value(row, self.technical_replicate_number_column)),
            run_order=self._parse_int(self.get_row_value(row, self.biosample_run_order_column))
        )

    def get_biosample_id(self, row) -> str:
        """Get the biosample_id from the row to link RunBiosample to Biosample."""
        return self.get_row_value(row, self.biosample_id_column, True)

    @staticmethod
    def _parse_int(value):
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None