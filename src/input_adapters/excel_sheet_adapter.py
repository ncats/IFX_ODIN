import math
from datetime import datetime

import pandas as pd

from src.models.pounce.investigator import Investigator, InvestigatorRelationship, Role

const_val_column = "Submitter_Value"
const_key_column = "NCATSDPI_Variable_Name"

class ExcelsheetParser:
    file_path: str
    sheet_dfs: dict[str, pd.DataFrame]

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.sheet_dfs = self._read_all_sheets()

    def _read_all_sheets(self) -> dict[str, pd.DataFrame]:
        xls = pd.ExcelFile(self.file_path)
        return {sheet: xls.parse(sheet) for sheet in xls.sheet_names}

    def _get_one_value(self, sheet_df, data_key):
        matches = sheet_df[sheet_df[const_key_column] == data_key][const_val_column]
        if len(matches) != 1:
            raise LookupError(f"Found multiple rows for '{data_key}' There must be one and only one", matches.values)
        value = matches.values[0]
        if pd.isna(value):
            return None
        return value

    def get_one_string(self, sheet_name, data_key):
        sheet_df = self.sheet_dfs[sheet_name]
        return self._get_one_value(sheet_df, data_key).strip()

    def get_one_string_list(self, sheet_name, data_key, delimiter=','):
        sheet_df = self.sheet_dfs[sheet_name]
        value_list = self._get_one_value(sheet_df, data_key)
        if value_list is None:
            return []
        return [val.strip() for val in value_list.split(delimiter)]

    def get_one_date(self, sheet_name, data_key):
        sheet_df = self.sheet_dfs[sheet_name]
        date_string = self._get_one_value(sheet_df, data_key)
        return datetime.strptime(str(date_string), "%Y%m%d")

    def get_other_properties(self, sheet_name, skip_keys):
        sheet_df = self.sheet_dfs[sheet_name]
        return {key: self._get_one_value(sheet_df, key) for key in sheet_df[const_key_column] if key not in skip_keys}

    def get_parameter_map(self, sheet_name):
        key_column: str = 'NCATSDPI_Variable_Name'
        value_column: str = 'Submitter_Value'
        sheet_df = self.sheet_dfs[sheet_name]
        data_frame = sheet_df.dropna(subset=[key_column, value_column])
        series = pd.Series(data_frame[value_column].values, index=data_frame[key_column])
        return series.to_dict()

    def get_mapped_value(self, sheet_name, key):
        map = self.get_parameter_map(sheet_name=sheet_name)
        return map[key]

    #
    #
    # def create_investigator_relationship(self, **kwargs):
    #     return InvestigatorRelationship(**kwargs)
    #
    # def get_investigators(self, project_df, proj_obj, name_key, email_key, role: Role):
    #     names = self.get_one_string_list(project_df, name_key)
    #     emails = self.get_one_string_list(project_df, email_key)
    #     if len(names) != len(emails):
    #         raise LookupError(f"Investigators and emails must have the same length", names, emails)
    #     objs = []
    #     rels = []
    #     for index, name in enumerate(names):
    #         email = emails[index]
    #         investigator_obj = Investigator(id=name, email=email)
    #         objs.append(investigator_obj)
    #         rels.append(
    #             self.create_investigator_relationship(
    #                 start_node=proj_obj,
    #                 end_node=investigator_obj,
    #                 roles=[role])
    #         )
    #     return objs, rels
    #
    #
    # def get_config_dictionaries(self, sheet_name: str):
    #     sheet_df = self.read_sheet(sheet_name, has_header_row=False)
    #
    #     return_dict = {}
    #     current_obj = {}
    #     current_type = None
    #     for index, row in sheet_df.iterrows():
    #         object_type = row[0]
    #         field_name = row[1]
    #
    #         remaining_values = row[2:].dropna()  # Remove NaNs
    #         if len(remaining_values) == 1:
    #             value = remaining_values.iloc[0]  # Extract single value
    #         else:
    #             value = tuple(remaining_values)  # Convert
    #
    #         if not isinstance(object_type, str):
    #             current_type = None
    #             continue
    #
    #         if current_type == None:
    #             current_type = object_type
    #             current_obj = {}
    #             if current_type in return_dict:
    #                 return_dict[current_type].append(current_obj)
    #             else:
    #                 return_dict[current_type] = [current_obj]
    #             # Update the current_obj
    #         if field_name in current_obj:
    #             # If existing value is not a list, convert it to a list
    #             if not isinstance(current_obj[field_name], list):
    #                 current_obj[field_name] = [current_obj[field_name]]
    #             # Append new values
    #             if isinstance(value, list):
    #                 current_obj[field_name].extend(value)
    #             else:
    #                 current_obj[field_name].append(value)
    #         else:
    #             # Assign the value if it doesn't already exist
    #             current_obj[field_name] = value
    #     return return_dict
    #
