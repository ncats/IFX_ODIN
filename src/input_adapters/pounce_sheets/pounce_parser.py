"""Parse POUNCE Excel workbooks into typed parsed objects.

``PounceParser`` uses ``sheet_field`` metadata on the parsed dataclasses to
drive Excel extraction.  It is shared by both the validation workflow (metadata
only, no DB) and the ETL pipeline (which additionally processes data matrices).
"""

from typing import List, Optional, Type

from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.input_adapters.pounce_sheets.parsed_classes import (
    ParsedBiosample,
    ParsedBiospecimen,
    ParsedDemographics,
    ParsedExperiment,
    ParsedExposure,
    ParsedPerson,
    ParsedProject,
    ParsedRunBiosample,
    ParsedStatsResultsMeta,
)
from src.input_adapters.pounce_sheets.parsed_pounce_data import ParsedPounceData
from src.input_adapters.pounce_sheets.sheet_field import get_sheet_fields


class PounceParser:
    """Reads POUNCE Excel workbooks into :class:`ParsedPounceData`."""

    # ------------------------------------------------------------------
    # Meta-sheet parsing (single-row key-value sheets like ProjectMeta)
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_meta_field(parser: ExcelsheetParser, sheet: str, key: str, parse_type: str):
        """Read a single value from a meta sheet using the appropriate parse strategy."""
        if parse_type == "string":
            return parser.safe_get_string(sheet, key)
        if parse_type == "string_list":
            return parser.get_one_string_list(sheet, key)
        if parse_type == "date":
            try:
                return parser.get_one_date(sheet, key).date()
            except (LookupError, KeyError, ValueError):
                return None
        if parse_type == "int":
            raw = parser.safe_get_string(sheet, key)
            if raw is None:
                return None
            try:
                return int(float(raw))
            except (ValueError, TypeError):
                return None
        if parse_type == "float":
            raw = parser.safe_get_string(sheet, key)
            if raw is None:
                return None
            try:
                return float(raw)
            except (ValueError, TypeError):
                return None
        if parse_type == "bool":
            raw = parser.safe_get_string(sheet, key)
            if raw is None:
                return None
            return raw.lower() in ("true", "yes", "1")
        if parse_type == "category":
            return parser.safe_get_string(sheet, key)
        return parser.safe_get_string(sheet, key)

    @classmethod
    def parse_meta_sheet(cls, parser: ExcelsheetParser, dataclass_type: Type):
        """Parse a meta sheet into an instance of *dataclass_type*.

        Iterates over every ``sheet_field``-annotated field, reads the value
        from the corresponding sheet/key, and constructs the object.
        """
        kwargs = {}
        for f, meta in get_sheet_fields(dataclass_type):
            sheet = meta["sheet"]
            key = meta["key"]
            parse_type = meta["parse"]
            indexed = meta["indexed"]

            if indexed:
                # Indexed fields like attached_file_{}: collect 1, 2, 3, ...
                values = []
                idx = 1
                while True:
                    actual_key = key.format(idx)
                    val = cls._parse_meta_field(parser, sheet, actual_key, parse_type)
                    if val is None or val == "" or val == []:
                        break
                    values.append(val)
                    idx += 1
                kwargs[f.name] = values
            else:
                kwargs[f.name] = cls._parse_meta_field(parser, sheet, key, parse_type)

        return dataclass_type(**kwargs)

    # ------------------------------------------------------------------
    # Mapped-sheet parsing (multi-row sheets like BioSampleMeta)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_column_name(param_map: dict, key: str, index: int = None) -> Optional[str]:
        """Resolve a column name from the parameter map, optionally with an index."""
        actual_key = key.format(index) if index is not None else key
        col = param_map.get(actual_key)
        if col in ("", "NA", "N/A", None):
            return None
        return col

    @staticmethod
    def _get_row_value(row, column_name: Optional[str], is_list: bool = False):
        """Read a cell value from a DataFrame row."""
        if column_name is None or column_name == "":
            return None
        val = row.get(column_name)
        if val is None or val == "" or val == "NA" or val == "N/A":
            return None
        if is_list:
            return [v.strip() for v in str(val).split("|") if v.strip()]
        return str(val).strip() if isinstance(val, str) else val

    @classmethod
    def _parse_mapped_row(cls, row, param_map: dict, dataclass_type: Type, index: int = None):
        """Parse one data row into an instance of *dataclass_type*.

        For indexed classes (e.g. ParsedExposure), *index* is the slot number.
        """
        kwargs = {}
        for f, meta in get_sheet_fields(dataclass_type):
            key = meta["key"]
            parse_type = meta["parse"]
            indexed = meta["indexed"]

            if indexed:
                if index is not None:
                    col = cls._get_column_name(param_map, key, index)
                else:
                    col = cls._get_column_name(param_map, key)
            else:
                col = cls._get_column_name(param_map, key)

            is_list = parse_type == "string_list"
            raw = cls._get_row_value(row, col, is_list=is_list)

            if parse_type == "int" and raw is not None:
                try:
                    raw = int(float(raw))
                except (ValueError, TypeError):
                    raw = None
            elif parse_type == "float" and raw is not None:
                try:
                    raw = float(raw)
                except (ValueError, TypeError):
                    raw = None

            kwargs[f.name] = raw

        return dataclass_type(**kwargs)

    # ------------------------------------------------------------------
    # High-level workbook parsers
    # ------------------------------------------------------------------

    def parse_project(self, parser: ExcelsheetParser) -> ParsedPounceData:
        """Parse the project workbook (metadata only)."""
        data = ParsedPounceData()

        # --- ProjectMeta (meta sheet) ---
        data.project = self.parse_meta_sheet(parser, ParsedProject)

        # --- People (from project meta sheet) ---
        project = data.project
        owners = self._build_persons(
            project.owner_names or [], project.owner_emails or [], "Owner")
        collaborators = self._build_persons(
            project.collaborator_names or [], project.collaborator_emails or [], "Collaborator")
        data.people = owners + collaborators

        # --- BioSampleMeta via BioSampleMap (mapped sheet) ---
        if "BioSampleMap" in parser.sheet_dfs and "BioSampleMeta" in parser.sheet_dfs:
            biosample_map = parser.get_parameter_map("BioSampleMap")
            sheet_df = parser.sheet_dfs["BioSampleMeta"]

            # Determine how many exposure slots exist
            exposure_indices = self._detect_exposure_indices(biosample_map)

            for _, row in sheet_df.iterrows():
                data.biosamples.append(
                    self._parse_mapped_row(row, biosample_map, ParsedBiosample))
                data.biospecimens.append(
                    self._parse_mapped_row(row, biosample_map, ParsedBiospecimen))
                data.demographics.append(
                    self._parse_demographics_row(row, biosample_map))

                for exp_idx in exposure_indices:
                    parsed_exp = self._parse_mapped_row(
                        row, biosample_map, ParsedExposure, index=exp_idx)
                    parsed_exp.exposure_index = exp_idx
                    data.exposures.append(parsed_exp)

            data.biosample_param_map = biosample_map

        return data

    def parse_experiment(self, parser: ExcelsheetParser) -> ParsedPounceData:
        """Parse an experiment workbook (metadata only, skips data matrices)."""
        data = ParsedPounceData()

        # --- ExperimentMeta (meta sheet) ---
        data.experiments.append(self.parse_meta_sheet(parser, ParsedExperiment))

        # --- RunSampleMeta via RunSampleMap (mapped sheet) ---
        if "RunSampleMap" in parser.sheet_dfs and "RunSampleMeta" in parser.sheet_dfs:
            run_sample_map = parser.get_parameter_map("RunSampleMap")
            for _, row in parser.sheet_dfs["RunSampleMeta"].iterrows():
                data.run_biosamples.append(
                    self._parse_mapped_row(row, run_sample_map, ParsedRunBiosample))

        return data

    def parse_stats_results(self, parser: ExcelsheetParser) -> ParsedPounceData:
        """Parse a stats results workbook (metadata only)."""
        data = ParsedPounceData()

        if "StatsResultsMeta" in parser.sheet_dfs:
            data.stats_results.append(
                self.parse_meta_sheet(parser, ParsedStatsResultsMeta))

        return data

    def parse_all(self, project_file: str,
                  experiment_files: List[str] = None,
                  stats_files: List[str] = None) -> ParsedPounceData:
        """Parse all workbooks into a single :class:`ParsedPounceData`."""
        experiment_files = experiment_files or []
        stats_files = stats_files or []

        project_parser = ExcelsheetParser(file_path=project_file)
        combined = self.parse_project(project_parser)

        for exp_file in experiment_files:
            exp_parser = ExcelsheetParser(file_path=exp_file)
            exp_data = self.parse_experiment(exp_parser)
            combined.experiments.extend(exp_data.experiments)
            combined.run_biosamples.extend(exp_data.run_biosamples)

        for stats_file in stats_files:
            stats_parser = ExcelsheetParser(file_path=stats_file)
            stats_data = self.parse_stats_results(stats_parser)
            combined.stats_results.extend(stats_data.stats_results)

        return combined

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_persons(names: List[str], emails: List[str], role: str) -> List[ParsedPerson]:
        if not names:
            return []
        if emails and len(names) == len(emails):
            return [ParsedPerson(name=n, email=e, role=role) for n, e in zip(names, emails)]
        return [ParsedPerson(name=n, role=role) for n in names]

    @classmethod
    def _detect_exposure_indices(cls, param_map: dict) -> List[int]:
        """Return the list of valid exposure slot numbers (1, 2, ...) from the map."""
        indices = []
        idx = 1
        while True:
            names_col = cls._get_column_name(param_map, "exposure{}_names", idx)
            type_col = cls._get_column_name(param_map, "exposure{}_type", idx)
            cat_col = cls._get_column_name(param_map, "exposure{}_category", idx)
            if all(c is None for c in [names_col, type_col, cat_col]):
                break
            indices.append(idx)
            idx += 1
        return indices

    @classmethod
    def _parse_demographics_row(cls, row, param_map: dict) -> ParsedDemographics:
        """Parse demographics fields from a biosample row.

        Demographics uses indexed fields for category columns, so we handle
        those specially here.
        """
        kwargs = {}
        for f, meta in get_sheet_fields(ParsedDemographics):
            key = meta["key"]
            parse_type = meta["parse"]
            indexed = meta["indexed"]

            if indexed:
                # Collect category1, category2, ... columns
                values = []
                idx = 1
                while True:
                    col = cls._get_column_name(param_map, key, idx)
                    if col is None:
                        break
                    val = cls._get_row_value(row, col)
                    if val is not None:
                        values.append(f"{col}:{val}")
                    idx += 1
                kwargs[f.name] = values if values else []
            else:
                col = cls._get_column_name(param_map, key)
                kwargs[f.name] = cls._get_row_value(row, col)

        return ParsedDemographics(**kwargs)
