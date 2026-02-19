from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd


class ExcelsheetParser:
    KEY_COLUMN = "NCATSDPI_Variable_Name"
    VALUE_COLUMN = "Submitter_Value"
    MAPPED_VALUE_COLUMN = "Submitter_Variable_Name"

    file_path: str
    sheet_dfs: Dict[str, pd.DataFrame]
    _parameter_map_cache: Dict[str, Dict[str, str]]

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.sheet_dfs = self._read_all_sheets()
        self._parameter_map_cache = {}

    def _read_all_sheets(self) -> Dict[str, pd.DataFrame]:
        xls = pd.ExcelFile(self.file_path)
        sheets = {}
        for sheet in xls.sheet_names:
            df = xls.parse(sheet, keep_default_na=False)
            df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
            df = df.apply(lambda col: col.map(
                lambda x: x.replace('\xa0', ' ').strip() if isinstance(x, str) else x
            ))
            sheets[sheet] = df
        return sheets

    def _get_one_value(self, sheet_df: pd.DataFrame, data_key: str) -> Any:
        matches = sheet_df[sheet_df[self.KEY_COLUMN] == data_key][self.VALUE_COLUMN]
        if len(matches) == 0:
            raise LookupError(f"Could not find row for '{data_key}'", sheet_df[self.KEY_COLUMN].values)
        if len(matches) != 1:
            raise LookupError(f"Found multiple rows for '{data_key}' There must be one and only one", matches.values)
        value = matches.values[0]
        if pd.isna(value) or value == '':
            return None
        return value

    def get_one_string(self, sheet_name: str, data_key: str) -> Optional[str]:
        sheet_df = self.sheet_dfs[sheet_name]
        value = self._get_one_value(sheet_df, data_key)
        if value is None:
            return None
        return str(value).strip()

    def get_one_string_list(self, sheet_name: str, data_key: str, delimiter: str = '|') -> List[str]:
        sheet_df = self.sheet_dfs[sheet_name]
        value_list = self._get_one_value(sheet_df, data_key)
        if value_list is None:
            return []
        return [val.strip() for val in str(value_list).split(delimiter)]

    def get_one_date(self, sheet_name: str, data_key: str) -> datetime:
        sheet_df = self.sheet_dfs[sheet_name]
        date_string = self._get_one_value(sheet_df, data_key)
        return datetime.strptime(str(date_string), "%Y%m%d")

    def get_parameter_map(self, sheet_name: str) -> Dict[str, str]:
        if sheet_name in self._parameter_map_cache:
            return self._parameter_map_cache[sheet_name]

        sheet_df = self.sheet_dfs[sheet_name]
        data_frame = sheet_df.dropna(subset=[self.KEY_COLUMN, self.MAPPED_VALUE_COLUMN])
        series = pd.Series(data_frame[self.MAPPED_VALUE_COLUMN].values, index=data_frame[self.KEY_COLUMN])
        result = series.to_dict()

        self._parameter_map_cache[sheet_name] = result
        return result

    def safe_get_string(self, sheet_name: str, data_key: str) -> Optional[str]:
        """Like get_one_string, but returns None instead of raising if the key is missing."""
        try:
            return self.get_one_string(sheet_name, data_key)
        except (LookupError, KeyError):
            return None

    def get_mapped_value(self, sheet_name: str, key: str) -> str:
        param_map = self.get_parameter_map(sheet_name=sheet_name)
        return param_map[key]