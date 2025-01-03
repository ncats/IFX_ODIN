import math
from datetime import datetime

import pandas as pd

from src.models.pounce.investigator import Investigator, InvestigatorRelationship, Role

const_val_column = "Submitter_Value"

const_key_column = "NCATSDPI_Variable_Name"


class ExcelsheetParser:
    file_path: str

    def __init__(self, path_to_sheet: str):
        self.file_path = path_to_sheet

    def read_sheet(self, sheet, has_header_row = True):
        print(f"reading {sheet}: {self.file_path}")
        if has_header_row:
            return pd.read_excel(self.file_path, sheet_name=sheet)
        return pd.read_excel(self.file_path, sheet_name=sheet, header=None)

    @staticmethod
    def get_id_from_name(input_name):
        return str(hash(input_name))

    @staticmethod
    def _get_one_value(data_frame, data_key):
        matches = data_frame[data_frame[const_key_column] == data_key][const_val_column]
        if len(matches) != 1:
            raise LookupError(f"Found multiple rows for '{data_key}' There must be one and only one", matches.values)
        value = matches.values[0]
        if pd.isna(value):
            return None
        return value

    @staticmethod
    def get_one_string(data_frame, data_key):
        return ExcelsheetParser._get_one_value(data_frame, data_key).strip()

    @staticmethod
    def get_one_string_list(data_frame, data_key, delimiter=','):
        value_list = ExcelsheetParser._get_one_value(data_frame, data_key)
        if value_list is None:
            return []
        return [val.strip() for val in value_list.split(delimiter)]

    @staticmethod
    def get_one_date(data_frame, data_key):
        date_string = ExcelsheetParser._get_one_value(data_frame, data_key)
        return datetime.strptime(str(date_string), "%Y%m%d")

    def create_investigator_relationship(self, **kwargs):
        return InvestigatorRelationship(**kwargs)

    def get_investigators(self, project_df, proj_obj, name_key, email_key, role: Role):
        names = self.get_one_string_list(project_df, name_key)
        emails = self.get_one_string_list(project_df, email_key)
        if len(names) != len(emails):
            raise LookupError(f"Investigators and emails must have the same length", names, emails)
        objs = []
        rels = []
        for index, name in enumerate(names):
            email = emails[index]
            investigator_obj = Investigator(id=name, email=email)
            objs.append(investigator_obj)
            rels.append(
                self.create_investigator_relationship(
                    start_node=proj_obj,
                    end_node=investigator_obj,
                    roles=[role])
            )
        return objs, rels

    def get_parameter_map(self, data_frame, key_column: str, value_column: str):
        data_frame = data_frame.dropna(subset=[key_column, value_column])
        series = pd.Series(data_frame[value_column].values, index=data_frame[key_column])
        return series.to_dict()

    def get_config_dictionaries(self, sheet_name: str):
        sheet_df = self.read_sheet(sheet_name, has_header_row=False)

        return_dict = {}
        current_obj = {}
        current_type = None
        for index, row in sheet_df.iterrows():
            object_type = row[0]
            field_name = row[1]

            remaining_values = row[2:].dropna()  # Remove NaNs
            if len(remaining_values) == 1:
                value = remaining_values.iloc[0]  # Extract single value
            else:
                value = tuple(remaining_values)  # Convert

            if not isinstance(object_type, str):
                current_type = None
                continue

            if current_type == None:
                current_type = object_type
                current_obj = {}
                if current_type in return_dict:
                    return_dict[current_type].append(current_obj)
                else:
                    return_dict[current_type] = [current_obj]
                # Update the current_obj
            if field_name in current_obj:
                # If existing value is not a list, convert it to a list
                if not isinstance(current_obj[field_name], list):
                    current_obj[field_name] = [current_obj[field_name]]
                # Append new values
                if isinstance(value, list):
                    current_obj[field_name].extend(value)
                else:
                    current_obj[field_name].append(value)
            else:
                # Assign the value if it doesn't already exist
                current_obj[field_name] = value
        return return_dict

