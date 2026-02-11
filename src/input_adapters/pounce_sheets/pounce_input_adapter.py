import os
from datetime import datetime, date
from typing import Generator, List, Union, Optional, Type

from src.constants import DataSourceName, Prefix
from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.input_adapters.pounce_sheets.constants import ProjectWorkbook, ExperimentWorkbook, StatsResultsWorkbook
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.biosample import BiosampleBiospecimenEdge
from src.models.pounce.config_classes import ExposureConfig, BiosampleConfig, BiospecimenConfig, RunBiosampleConfig
from src.models.pounce.dataset import Dataset, ExperimentDatasetEdge, DatasetRunBiosampleEdge, DatasetGeneEdge, DatasetMetaboliteEdge
from src.models.pounce.stats_result import StatsResult, ExperimentStatsResultEdge, StatsResultGeneEdge, StatsResultMetaboliteEdge
from src.models.pounce.experiment import Experiment, ProjectExperimentEdge, ExperimentPersonEdge, BiosampleRunBiosampleEdge
from src.models.pounce.exposure import BiosampleExposureEdge
from src.models.pounce.project import Project, AccessLevel, Person, ProjectPersonEdge, ProjectBiosampleEdge


class PounceInputAdapter(InputAdapter):
    project_file: str
    project_parser: ExcelsheetParser
    experiment_files: List[str]
    stats_results_files: List[str]
    _biosample_by_original_id: dict
    _run_biosample_by_original_id: dict
    _gene_by_raw_id: dict
    _metabolite_by_raw_id: dict
    _experiment_counter = 0

    def __init__(self, project_file: str, experiment_files: List[str] = None, stats_results_files: List[str] = None):
        self.project_file = project_file
        self.project_parser = ExcelsheetParser(file_path=project_file)
        self.experiment_files = experiment_files or []
        self.stats_results_files = stats_results_files or []
        self._biosample_by_original_id = {}
        self._run_biosample_by_original_id = {}
        self._gene_by_raw_id = {}
        self._metabolite_by_raw_id = {}

    # --- Interface methods ---

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCATSPounce

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version_date=self.get_project_date(),
            download_date=datetime.fromtimestamp(os.path.getmtime(self.project_file)).date()
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        print(f"reading: {self.project_file}")

        proj_obj = self._create_project()
        owners = self._get_persons_from_lists(self._get_project_owner_names(), self._get_project_owner_emails())
        collaborators = self._get_persons_from_lists(self._get_collaborator_names(), self._get_collaborator_emails())

        proj_owner_edges = [ProjectPersonEdge(start_node=proj_obj, end_node=owner, role="Owner") for owner in owners]
        proj_collab_edges = [ProjectPersonEdge(start_node=proj_obj, end_node=collab, role="Collaborator") for collab in collaborators]

        yield [proj_obj, *owners, *proj_owner_edges, *collaborators, *proj_collab_edges]

        yield from self._get_biosample_data(proj_obj)

        for i, exp_file in enumerate(self.experiment_files):
            exp_parser = ExcelsheetParser(file_path=exp_file)
            stats_parser = None
            if i < len(self.stats_results_files):
                stats_parser = ExcelsheetParser(file_path=self.stats_results_files[i])
            yield from self._get_experiment_data(proj_obj, exp_parser, stats_parser)

    # --- Project workbook ---

    def _create_project(self) -> Project:
        return Project(
            id=self.get_project_id(),
            name=self.get_project_name(),
            description=self.get_project_description(),
            date=self.get_project_date(),
            lab_groups=self.get_project_lab_groups(),
            access=self.get_project_privacy_level(),
            keywords=self.get_project_keywords(),
            project_type=self.get_project_type(),
            rare_disease_focus=self.get_project_rd_tag(),
            sample_preparation=self.get_project_sample_prep()
        )

    def _get_biosample_data(self, proj_obj: Project) -> Generator[List[Union[Node, Relationship]], None, None]:
        biosample_map = self.project_parser.get_parameter_map(ProjectWorkbook.BiosampleMapSheet.name)
        biosample_config = BiosampleConfig(biosample_map, proj_obj.id)
        biospecimen_config = BiospecimenConfig(biosample_map, proj_obj.id)

        sheet_df = self.project_parser.sheet_dfs[ProjectWorkbook.BiosampleMetaSheet.name]

        # Collect unique biospecimens
        biospecimens = {}
        for index, row in sheet_df.iterrows():
            biospecimen_obj = biospecimen_config.get_data(row)
            if biospecimen_obj.id not in biospecimens:
                biospecimens[biospecimen_obj.id] = biospecimen_obj
        yield list(biospecimens.values())

        # Collect biosamples and edges
        biosamples = []
        exposures = {}
        sample_exposure_edges: List[BiosampleExposureEdge] = []
        project_biosample_edges: List[ProjectBiosampleEdge] = []
        biosample_biospecimen_edges: List[BiosampleBiospecimenEdge] = []

        for index, row in sheet_df.iterrows():
            biosample_obj = biosample_config.get_data(row)
            biosamples.append(biosample_obj)
            self._biosample_by_original_id[str(biosample_obj.original_id)] = biosample_obj

            project_biosample_edges.append(ProjectBiosampleEdge(start_node=proj_obj, end_node=biosample_obj))

            biospecimen_obj = biospecimen_config.get_data(row)
            biosample_biospecimen_edges.append(
                BiosampleBiospecimenEdge(start_node=biosample_obj, end_node=biospecimens[biospecimen_obj.id])
            )

            for exposure_config in ExposureConfig.get_valid_configs(biosample_map):
                exposure_obj = exposure_config.get_data(row)
                sample_exposure_edges.append(BiosampleExposureEdge(start_node=biosample_obj, end_node=exposure_obj))
                if exposure_obj.id not in exposures:
                    exposures[exposure_obj.id] = exposure_obj

        yield biosamples
        yield list(exposures.values())
        yield sample_exposure_edges
        yield project_biosample_edges
        yield biosample_biospecimen_edges

    # --- Experiment workbook ---

    def _get_experiment_data(self, proj_obj: Project, exp_parser: ExcelsheetParser,
                             stats_parser: Optional[ExcelsheetParser] = None
                             ) -> Generator[List[Union[Node, Relationship]], None, None]:
        print(f"reading: {exp_parser.file_path}")

        experiment_obj = self._create_experiment(proj_obj.id, exp_parser)

        # Experiment personnel
        data_generator = self._get_experiment_person(
            ExperimentWorkbook.ExperimentSheet.Key.lead_data_generator,
            ExperimentWorkbook.ExperimentSheet.Key.lead_data_generator_email,
            exp_parser
        )
        informatician = self._get_experiment_person(
            ExperimentWorkbook.ExperimentSheet.Key.lead_informatician,
            ExperimentWorkbook.ExperimentSheet.Key.lead_informatician_email,
            exp_parser
        )

        persons = [p for p in [data_generator, informatician] if p is not None]
        person_edges = []
        if data_generator:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=data_generator, role="DataGenerator"))
        if informatician:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=informatician, role="Informatician"))

        project_experiment_edge = ProjectExperimentEdge(start_node=proj_obj, end_node=experiment_obj)

        yield [experiment_obj, *persons, project_experiment_edge, *person_edges]

        yield from self._get_run_biosample_data(proj_obj.id, exp_parser)

        # Parse analytes and data matrices from the experiment workbook
        sheet_names = exp_parser.sheet_dfs.keys()
        if ExperimentWorkbook.GeneMetaSheet.name in sheet_names:
            yield from self._parse_genes(experiment_obj, exp_parser)
        if ExperimentWorkbook.MetabMetaSheet.name in sheet_names:
            yield from self._parse_metabolites(experiment_obj, exp_parser)
        if ExperimentWorkbook.RawDataSheet.name in sheet_names:
            gene_map = exp_parser.get_parameter_map(ExperimentWorkbook.GeneMapSheet.name)
            analyte_id_col = gene_map.get(ExperimentWorkbook.GeneMapSheet.Key.gene_id)
            yield from self._parse_data_matrix(
                experiment_obj,
                meta_sheet=ExperimentWorkbook.RawDataMetaSheet.name,
                data_sheet=ExperimentWorkbook.RawDataSheet.name,
                analyte_id_col=analyte_id_col,
                parser=exp_parser
            )
        if ExperimentWorkbook.PeakDataSheet.name in sheet_names:
            metab_map = exp_parser.get_parameter_map(ExperimentWorkbook.MetabMapSheet.name)
            analyte_id_col = metab_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_id)
            yield from self._parse_data_matrix(
                experiment_obj,
                meta_sheet=ExperimentWorkbook.PeakDataMetaSheet.name,
                data_sheet=ExperimentWorkbook.PeakDataSheet.name,
                analyte_id_col=analyte_id_col,
                parser=exp_parser
            )

        # Parse stats results from the paired stats results file
        if stats_parser:
            yield from self._get_stats_results_data(experiment_obj, stats_parser)

    def _create_experiment(self, project_id: str, parser: ExcelsheetParser) -> Experiment:
        exp_id = self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_id, parser)
        if not exp_id:
            self._experiment_counter += 1
            exp_id = f"{project_id}-exp-{self._experiment_counter}"

        exp_date = None
        date_str = self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.date, parser)
        if date_str:
            try:
                exp_date = datetime.strptime(str(date_str), "%Y%m%d").date()
            except ValueError:
                pass

        return Experiment(
            id=exp_id,
            name=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_name, parser),
            description=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_description, parser),
            design=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_design, parser),
            experiment_type=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_type, parser),
            date=exp_date,
            platform_type=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_type, parser),
            platform_name=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_name, parser),
            platform_provider=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_provider, parser),
            platform_output_type=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_output_type, parser),
            public_repo_id=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.public_repo_id, parser),
            repo_url=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.repo_url, parser),
            raw_file_archive_dir=self._get_experiment_string_list(ExperimentWorkbook.ExperimentSheet.Key.raw_file_archive_dir, parser),
            extraction_protocol=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.extraction_protocol, parser),
            acquisition_method=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.acquisition_method, parser),
            metabolite_identification_description=self._get_experiment_string(
                ExperimentWorkbook.ExperimentSheet.Key.metabolite_identification_description, parser
            ),
            experiment_data_file=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_data_file, parser),
            attached_files=self._get_attached_files(parser)
        )

    def _get_attached_files(self, parser: ExcelsheetParser) -> List[str]:
        """Collect attached_file_1, attached_file_2, etc. until one is empty/missing."""
        attached_files = []
        file_num = 1
        while True:
            key = ExperimentWorkbook.ExperimentSheet.Key.attached_file.format(file_num)
            value = self._get_experiment_string(key, parser)
            if not value:
                break
            attached_files.append(value)
            file_num += 1
        return attached_files

    def _get_run_biosample_data(self, project_id: str, parser: ExcelsheetParser
                                ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse RunSampleMeta sheet and create RunBiosamples with edges to Biosamples."""
        run_sample_map = parser.get_parameter_map(ExperimentWorkbook.RunSampleMapSheet.name)
        run_biosample_config = RunBiosampleConfig(run_sample_map, project_id)

        sheet_df = parser.sheet_dfs[ExperimentWorkbook.RunSampleMetaSheet.name]

        run_biosamples = []
        edges: List[BiosampleRunBiosampleEdge] = []

        for index, row in sheet_df.iterrows():
            run_biosample_obj = run_biosample_config.get_data(row)
            run_biosamples.append(run_biosample_obj)

            raw_run_id = str(run_biosample_config.get_row_value(
                row, run_biosample_config.run_biosample_id_column, True))
            self._run_biosample_by_original_id[raw_run_id] = run_biosample_obj

            biosample_id = str(run_biosample_config.get_biosample_id(row))
            if biosample_id in self._biosample_by_original_id:
                biosample_obj = self._biosample_by_original_id[biosample_id]
                edges.append(BiosampleRunBiosampleEdge(start_node=biosample_obj, end_node=run_biosample_obj))
            else:
                print(f"Warning: RunBiosample references unknown biosample_id: {biosample_id}")

        yield run_biosamples
        yield edges

    # --- Analyte parsing ---

    def _parse_genes(self, experiment_obj: Experiment, parser: ExcelsheetParser
                     ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse gene data from GeneMeta sheet."""
        column_map = parser.get_parameter_map(ExperimentWorkbook.GeneMapSheet.name)

        gene_id_col = column_map.get(ExperimentWorkbook.GeneMapSheet.Key.gene_id)
        symbol_col = column_map.get(ExperimentWorkbook.GeneMapSheet.Key.hgnc_gene_symbol)
        biotype_col = column_map.get(ExperimentWorkbook.GeneMapSheet.Key.gene_biotype)

        sheet_df = parser.sheet_dfs[ExperimentWorkbook.GeneMetaSheet.name]

        genes = []
        for index, row in sheet_df.iterrows():
            gene_id = row.get(gene_id_col) if gene_id_col else None
            if not gene_id:
                continue

            gene_obj = Gene(
                id=gene_id,
                symbol=row.get(symbol_col) if symbol_col else None,
                biotype=row.get(biotype_col) if biotype_col else None
            )
            genes.append(gene_obj)
            self._gene_by_raw_id[str(gene_id)] = gene_obj

        yield genes

    def _parse_metabolites(self, experiment_obj: Experiment, parser: ExcelsheetParser
                           ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse metabolite data from MetabMeta sheet."""
        column_map = parser.get_parameter_map(ExperimentWorkbook.MetabMapSheet.name)

        metab_id_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_id)
        metab_name_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_name)
        metab_type_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_chemclass)
        id_level_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.identification_level)

        sheet_df = parser.sheet_dfs[ExperimentWorkbook.MetabMetaSheet.name]

        metabolites = []
        for index, row in sheet_df.iterrows():
            metab_id = row.get(metab_id_col) if metab_id_col else None
            if not metab_id:
                continue

            metab_obj = Metabolite(
                id=f"{experiment_obj.id}-{metab_id}",
                name=row.get(metab_name_col) if metab_name_col else None,
                type=row.get(metab_type_col) if metab_type_col else None,
                identification_level=self._parse_int(row.get(id_level_col)) if id_level_col else None
            )
            metabolites.append(metab_obj)
            self._metabolite_by_raw_id[str(metab_id)] = metab_obj

        yield metabolites

    # --- Data matrix and stats results parsing ---

    def _prepare_data_frame(self, parser: ExcelsheetParser, data_sheet: str, analyte_id_col: str = None):
        """Clean a data sheet DataFrame: drop empty columns/rows, set analyte ID as index.
        Returns (cleaned_df) or (None) if empty."""
        raw_df = parser.sheet_dfs[data_sheet]

        if raw_df.empty or len(raw_df.columns) == 0:
            print(f"Empty data sheet: {data_sheet}")
            return None

        raw_df = raw_df.dropna(axis=1, how='all')
        raw_df = raw_df.dropna(subset=[analyte_id_col])

        if raw_df.empty:
            print(f"No valid rows in data sheet: {data_sheet}")
            return None

        return raw_df.set_index(analyte_id_col)

    def _build_analyte_edges(self, parent_node, df, gene_edge_cls: Type[Relationship],
                             metab_edge_cls: Type[Relationship]) -> List[Relationship]:
        """Build edges from a parent node (Dataset or StatsResult) to Gene/Metabolite nodes."""
        edges = []
        for analyte_id in df.index:
            raw_id = str(analyte_id)
            if raw_id in self._gene_by_raw_id:
                edges.append(gene_edge_cls(start_node=parent_node, end_node=self._gene_by_raw_id[raw_id]))
            elif raw_id in self._metabolite_by_raw_id:
                edges.append(metab_edge_cls(start_node=parent_node, end_node=self._metabolite_by_raw_id[raw_id]))
        return edges

    def _qualify_data_frame_ids(self, df):
        """Rename DataFrame index and columns to use qualified entity IDs.

        This ensures parquet files store IDs that match the node IDs in the database,
        so foreign keys work correctly in downstream MySQL tables.
        """
        # Rename index: raw analyte IDs → qualified Gene/Metabolite IDs
        index_rename = {}
        for raw_id in df.index:
            raw_str = str(raw_id)
            if raw_str in self._metabolite_by_raw_id:
                index_rename[raw_id] = self._metabolite_by_raw_id[raw_str].id
            # Gene IDs are already globally unique — no rename needed

        # Rename columns: raw sample names → qualified RunBiosample IDs
        col_rename = {}
        for col in df.columns:
            col_str = str(col)
            if col_str in self._run_biosample_by_original_id:
                col_rename[col] = self._run_biosample_by_original_id[col_str].id

        if index_rename:
            df = df.rename(index=index_rename)
        if col_rename:
            df = df.rename(columns=col_rename)
        return df

    def _parse_data_matrix(self, experiment_obj: Experiment, meta_sheet: str,
                           data_sheet: str, analyte_id_col: str = None,
                           default_data_type: str = "raw_counts",
                           parser: ExcelsheetParser = None
                           ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse a data matrix sheet (RawData, PeakData, or StatsReadyData) into a Dataset node."""

        # Read metadata from the companion meta sheet
        pre_processing = None
        peakdata_tag = None
        try:
            pre_processing = parser.get_one_string(meta_sheet, "pre_processing_description")
        except LookupError:
            pass
        try:
            peakdata_tag = parser.get_one_string(meta_sheet, "peakdata_tag")
        except LookupError:
            pass

        data_type = peakdata_tag.lower() if peakdata_tag else default_data_type
        dataset_id = f"{experiment_obj.id}:{data_type}"

        raw_df = self._prepare_data_frame(parser, data_sheet, analyte_id_col)

        if raw_df is None:
            dataset = Dataset(
                id=dataset_id, data_type=data_type, pre_processing_description=pre_processing,
                row_count=0, column_count=0, gene_id_column=analyte_id_col, sample_columns=[]
            )
            yield [dataset, ExperimentDatasetEdge(start_node=experiment_obj, end_node=dataset)]
            return

        # Convert "NA" strings to NaN so pyarrow can write numeric columns
        import pandas as pd
        for col in raw_df.columns:
            raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')

        # Build edges using raw IDs before qualifying the DataFrame
        # (placeholder dataset needed for edge construction)
        placeholder = Dataset(
            id=dataset_id, data_type=data_type, pre_processing_description=pre_processing,
            row_count=len(raw_df), column_count=len(raw_df.columns),
            gene_id_column=analyte_id_col, sample_columns=list(raw_df.columns)
        )

        # Dataset -> RunBiosample edges (using raw column names)
        run_biosample_edges = []
        for col in raw_df.columns:
            if col in self._run_biosample_by_original_id:
                run_biosample_edges.append(
                    DatasetRunBiosampleEdge(start_node=placeholder, end_node=self._run_biosample_by_original_id[col])
                )

        # Dataset -> Gene/Metabolite edges (using raw index IDs)
        analyte_edges = self._build_analyte_edges(placeholder, raw_df, DatasetGeneEdge, DatasetMetaboliteEdge)

        # Qualify DataFrame IDs so parquet stores qualified IDs matching node IDs
        qualified_df = self._qualify_data_frame_ids(raw_df)

        # Create the real Dataset with qualified DataFrame and column names
        dataset = Dataset(
            id=dataset_id, data_type=data_type, pre_processing_description=pre_processing,
            row_count=len(qualified_df), column_count=len(qualified_df.columns),
            gene_id_column=analyte_id_col, sample_columns=list(qualified_df.columns),
            _data_frame=qualified_df
        )

        # Update edge start_nodes to point to the real dataset
        for edge in run_biosample_edges:
            edge.start_node = dataset
        for edge in analyte_edges:
            edge.start_node = dataset

        yield [dataset, ExperimentDatasetEdge(start_node=experiment_obj, end_node=dataset)]

        if run_biosample_edges:
            yield run_biosample_edges

        if analyte_edges:
            yield analyte_edges

    def _get_stats_results_data(self, experiment_obj: Experiment, stats_parser: ExcelsheetParser
                                ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse StatsReadyData and EffectSize from the stats results workbook."""
        stats_sheet_names = stats_parser.sheet_dfs.keys()

        # Resolve analyte ID column from the EffectSize_Map
        analyte_id_col = None
        if StatsResultsWorkbook.EffectSizeMapSheet.name in stats_sheet_names:
            es_map = stats_parser.get_parameter_map(StatsResultsWorkbook.EffectSizeMapSheet.name)
            analyte_id_col = (
                es_map.get(StatsResultsWorkbook.EffectSizeMapSheet.Key.gene_id)
                or es_map.get(StatsResultsWorkbook.EffectSizeMapSheet.Key.metabolite_id)
            )

        if StatsResultsWorkbook.StatsReadyDataSheet.name in stats_sheet_names:
            yield from self._parse_data_matrix(
                experiment_obj,
                meta_sheet=StatsResultsWorkbook.StatsResultsMetaSheet.name,
                data_sheet=StatsResultsWorkbook.StatsReadyDataSheet.name,
                analyte_id_col=analyte_id_col,
                default_data_type="stats_ready",
                parser=stats_parser
            )

        if StatsResultsWorkbook.EffectSizeSheet.name in stats_sheet_names:
            yield from self._parse_effect_size(experiment_obj, analyte_id_col, stats_parser)

    def _parse_effect_size(self, experiment_obj: Experiment,
                           analyte_id_col: str = None,
                           parser: ExcelsheetParser = None
                           ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse EffectSize sheet into a StatsResult node with edges to analytes."""
        stats_result_id = f"{experiment_obj.id}:effect_size"

        raw_df = self._prepare_data_frame(
            parser, StatsResultsWorkbook.EffectSizeSheet.name, analyte_id_col
        )

        if raw_df is None:
            stats_result = StatsResult(
                id=stats_result_id, data_type="effect_size",
                row_count=0, column_count=0,
                analyte_id_column=analyte_id_col, comparison_columns=[]
            )
            yield [stats_result, ExperimentStatsResultEdge(start_node=experiment_obj, end_node=stats_result)]
            return

        # Convert "NA" strings to NaN so pyarrow can write numeric columns
        import pandas as pd
        for col in raw_df.columns:
            raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce')

        # Build analyte edges using raw index IDs before qualifying
        placeholder = StatsResult(
            id=stats_result_id, data_type="effect_size",
            row_count=len(raw_df), column_count=len(raw_df.columns),
            analyte_id_column=analyte_id_col, comparison_columns=list(raw_df.columns)
        )
        analyte_edges = self._build_analyte_edges(placeholder, raw_df, StatsResultGeneEdge, StatsResultMetaboliteEdge)

        # Qualify DataFrame IDs (index only — effect size columns are comparison labels, not sample IDs)
        qualified_df = self._qualify_data_frame_ids(raw_df)

        comparison_columns = list(qualified_df.columns)
        stats_result = StatsResult(
            id=stats_result_id, data_type="effect_size",
            row_count=len(qualified_df), column_count=len(qualified_df.columns),
            analyte_id_column=analyte_id_col, comparison_columns=comparison_columns, _data_frame=qualified_df
        )

        # Update edge start_nodes to point to the real stats_result
        for edge in analyte_edges:
            edge.start_node = stats_result

        yield [stats_result, ExperimentStatsResultEdge(start_node=experiment_obj, end_node=stats_result)]

        if analyte_edges:
            yield analyte_edges

    # --- Experiment field accessors ---

    def _get_experiment_string(self, data_key: str, parser: ExcelsheetParser) -> Optional[str]:
        try:
            return parser.get_one_string(ExperimentWorkbook.ExperimentSheet.name, data_key)
        except LookupError:
            return None

    def _get_experiment_string_list(self, data_key: str, parser: ExcelsheetParser) -> List[str]:
        return parser.get_one_string_list(ExperimentWorkbook.ExperimentSheet.name, data_key)

    def _get_experiment_person(self, name_key: str, email_key: str, parser: ExcelsheetParser) -> Optional[Person]:
        name = self._get_experiment_string(name_key, parser)
        if not name:
            return None
        email = self._get_experiment_string(email_key, parser)
        return Person(id=name, email=email)

    # --- Project field accessors ---

    def _get_project_string(self, data_key: str) -> Optional[str]:
        return self.project_parser.get_one_string(ProjectWorkbook.ProjectSheet.name, data_key)

    def _get_project_string_list(self, data_key: str) -> List[str]:
        return self.project_parser.get_one_string_list(ProjectWorkbook.ProjectSheet.name, data_key)

    def get_project_id(self) -> str:
        return self._get_project_string(ProjectWorkbook.ProjectSheet.Key.project_id)

    def get_project_name(self) -> str:
        return self._get_project_string(ProjectWorkbook.ProjectSheet.Key.project_name)

    def get_project_description(self) -> str:
        return self._get_project_string(ProjectWorkbook.ProjectSheet.Key.description)

    def get_project_date(self) -> date:
        return self.project_parser.get_one_date(
            ProjectWorkbook.ProjectSheet.name, ProjectWorkbook.ProjectSheet.Key.date
        ).date()

    def get_project_lab_groups(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.lab_groups)

    def get_project_privacy_level(self) -> AccessLevel:
        value = self._get_project_string(ProjectWorkbook.ProjectSheet.Key.privacy_type)
        return AccessLevel.parse(value)

    def get_project_keywords(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.keywords)

    def get_project_type(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.project_type)

    def get_project_rd_tag(self) -> bool:
        str_val = self._get_project_string(ProjectWorkbook.ProjectSheet.Key.RD_tag)
        if str_val is None:
            return False
        return str_val.lower() in ("true", "yes", "1")

    def get_project_sample_prep(self) -> str:
        return self._get_project_string(ProjectWorkbook.ProjectSheet.Key.biosample_preparation)

    def _get_project_owner_names(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.owner_name)

    def _get_project_owner_emails(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.owner_email)

    def _get_collaborator_names(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.collaborator_name)

    def _get_collaborator_emails(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.collaborator_email)

    # --- Utilities ---

    @staticmethod
    def _parse_int(value) -> Optional[int]:
        """Safely parse an integer value."""
        if value is None or value == '' or value == 'NA':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _get_persons_from_lists(names: List[str], emails: List[str]) -> List[Person]:
        if len(emails) == 0:
            return [Person(id=name, email=None) for name in names]
        if len(names) != len(emails):
            raise LookupError(f"Names and emails must have the same length: {names} vs {emails}")
        return [Person(id=name, email=email) for name, email in zip(names, emails)]