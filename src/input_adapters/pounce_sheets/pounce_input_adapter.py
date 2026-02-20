import os
import pandas as pd
from datetime import datetime
from typing import Dict, Generator, List, Union, Optional, Tuple, Type

from src.constants import DataSourceName, Prefix
from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.input_adapters.pounce_sheets.constants import ProjectWorkbook, ExperimentWorkbook, StatsResultsWorkbook
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import MeasuredGene, MeasuredGeneEdge, Gene
from src.models.metabolite import MeasuredMetabolite, MeasuredMetaboliteEdge, Metabolite
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.biosample import BiosampleBiospecimenEdge
from src.models.pounce.category_value import CategoryValue
from src.models.pounce.config_classes import ExposureConfig, BiosampleConfig, BiospecimenConfig, RunBiosampleConfig
from src.models.pounce.dataset import Dataset, ExperimentDatasetEdge, DatasetRunBiosampleEdge, DatasetGeneEdge, DatasetMetaboliteEdge
from src.models.pounce.stats_result import StatsResult, ComparisonColumn, ExperimentStatsResultEdge, StatsResultGeneEdge, StatsResultMetaboliteEdge, StatsResultPersonEdge
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
    _experiment_counter: int

    def __init__(self, project_file: str, experiment_files: List[str] = None, stats_results_files: List[str] = None):
        self.project_file = project_file
        self.project_parser = ExcelsheetParser(file_path=project_file)
        self.experiment_files = experiment_files or []
        self.stats_results_files = stats_results_files or []
        self._biosample_by_original_id = {}
        self._run_biosample_by_original_id = {}
        self._gene_by_raw_id = {}
        self._metabolite_by_raw_id = {}
        self._experiment_counter = 0

    # --- Interface methods ---

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCATSPounce

    def get_version(self) -> DatasourceVersionInfo:
        sheet = ProjectWorkbook.ProjectSheet
        return DatasourceVersionInfo(
            version_date=self.project_parser.get_one_date(sheet.name, sheet.Key.date).date(),
            download_date=datetime.fromtimestamp(os.path.getmtime(self.project_file)).date()
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        print(f"reading: {self.project_file}")

        proj_obj = self._create_project()
        p, sheet = self.project_parser, ProjectWorkbook.ProjectSheet

        owners = self._get_persons_from_lists(
            p.get_one_string_list(sheet.name, sheet.Key.owner_name),
            p.get_one_string_list(sheet.name, sheet.Key.owner_email)
        )
        collaborators = self._get_persons_from_lists(
            p.get_one_string_list(sheet.name, sheet.Key.collaborator_name),
            p.get_one_string_list(sheet.name, sheet.Key.collaborator_email)
        )
        proj_owner_edges = [ProjectPersonEdge(start_node=proj_obj, end_node=o, role="Owner") for o in owners]
        proj_collab_edges = [ProjectPersonEdge(start_node=proj_obj, end_node=c, role="Collaborator") for c in collaborators]

        yield [proj_obj, *owners, *proj_owner_edges, *collaborators, *proj_collab_edges]

        yield from self._get_biosample_data(proj_obj)

        for i, exp_file in enumerate(self.experiment_files):
            exp_parser = ExcelsheetParser(file_path=exp_file)
            stats_parser = ExcelsheetParser(file_path=self.stats_results_files[i]) if i < len(self.stats_results_files) else None
            yield from self._get_experiment_data(proj_obj, exp_parser, stats_parser)

    # --- Project workbook ---

    def _create_project(self) -> Project:
        p, sheet = self.project_parser, ProjectWorkbook.ProjectSheet
        rd_tag = p.safe_get_string(sheet.name, sheet.Key.RD_tag)
        return Project(
            id=p.safe_get_string(sheet.name, sheet.Key.project_id),
            name=p.safe_get_string(sheet.name, sheet.Key.project_name),
            description=p.safe_get_string(sheet.name, sheet.Key.description),
            date=p.get_one_date(sheet.name, sheet.Key.date).date(),
            lab_groups=p.get_one_string_list(sheet.name, sheet.Key.lab_groups),
            access=AccessLevel.parse(p.safe_get_string(sheet.name, sheet.Key.privacy_type)),
            keywords=p.get_one_string_list(sheet.name, sheet.Key.keywords),
            project_type=p.get_one_string_list(sheet.name, sheet.Key.project_type),
            rare_disease_focus=rd_tag is not None and rd_tag.lower() in ("true", "yes", "1"),
            sample_preparation=p.safe_get_string(sheet.name, sheet.Key.biosample_preparation)
        )

    def _get_biosample_data(self, proj_obj: Project) -> Generator[List[Union[Node, Relationship]], None, None]:
        biosample_map = self.project_parser.get_parameter_map(ProjectWorkbook.BiosampleMapSheet.name)
        biosample_config = BiosampleConfig(biosample_map, proj_obj.id)
        biospecimen_config = BiospecimenConfig(biosample_map, proj_obj.id)
        exposure_configs = ExposureConfig.get_valid_configs(biosample_map)

        sheet_df = self.project_parser.sheet_dfs[ProjectWorkbook.BiosampleMetaSheet.name]

        biospecimens = {}
        biosamples = []
        exposures = {}
        sample_exposure_edges: List[BiosampleExposureEdge] = []
        project_biosample_edges: List[ProjectBiosampleEdge] = []
        biosample_biospecimen_edges: List[BiosampleBiospecimenEdge] = []

        for _, row in sheet_df.iterrows():
            biospecimen_obj = biospecimen_config.get_data(row)
            if biospecimen_obj.id not in biospecimens:
                biospecimens[biospecimen_obj.id] = biospecimen_obj

            biosample_obj = biosample_config.get_data(row)
            biosamples.append(biosample_obj)
            self._biosample_by_original_id[str(biosample_obj.original_id)] = biosample_obj

            project_biosample_edges.append(ProjectBiosampleEdge(start_node=proj_obj, end_node=biosample_obj))
            biosample_biospecimen_edges.append(
                BiosampleBiospecimenEdge(start_node=biosample_obj, end_node=biospecimens[biospecimen_obj.id])
            )

            for exposure_config in exposure_configs:
                exposure_obj = exposure_config.get_data(row)
                sample_exposure_edges.append(BiosampleExposureEdge(start_node=biosample_obj, end_node=exposure_obj))
                if exposure_obj.id not in exposures:
                    exposures[exposure_obj.id] = exposure_obj

        yield list(biospecimens.values())
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
        sheet = ExperimentWorkbook.ExperimentSheet

        data_generator = self._get_person(
            exp_parser, sheet.name,
            sheet.Key.lead_data_generator, sheet.Key.lead_data_generator_email
        )
        informatician = self._get_person(
            exp_parser, sheet.name,
            sheet.Key.lead_informatician, sheet.Key.lead_informatician_email
        )

        persons = [p for p in [data_generator, informatician] if p is not None]
        person_edges = []
        if data_generator:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=data_generator, role="DataGenerator"))
        if informatician:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=informatician, role="Informatician"))

        yield [experiment_obj, *persons, ProjectExperimentEdge(start_node=proj_obj, end_node=experiment_obj), *person_edges]

        yield from self._get_run_biosample_data(proj_obj.id, exp_parser)

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

        if stats_parser:
            yield from self._get_stats_results_data(experiment_obj, stats_parser)

    def _create_experiment(self, project_id: str, parser: ExcelsheetParser) -> Experiment:
        sheet = ExperimentWorkbook.ExperimentSheet

        exp_id = parser.safe_get_string(sheet.name, sheet.Key.experiment_id)
        if not exp_id:
            self._experiment_counter += 1
            exp_id = f"{project_id}-exp-{self._experiment_counter}"

        exp_date = None
        date_str = parser.safe_get_string(sheet.name, sheet.Key.date)
        if date_str:
            try:
                exp_date = datetime.strptime(str(date_str), "%Y%m%d").date()
            except ValueError:
                pass

        return Experiment(
            id=exp_id,
            name=parser.safe_get_string(sheet.name, sheet.Key.experiment_name),
            description=parser.safe_get_string(sheet.name, sheet.Key.experiment_description),
            design=parser.safe_get_string(sheet.name, sheet.Key.experiment_design),
            experiment_type=parser.safe_get_string(sheet.name, sheet.Key.experiment_type),
            date=exp_date,
            platform_type=parser.safe_get_string(sheet.name, sheet.Key.platform_type),
            platform_name=parser.safe_get_string(sheet.name, sheet.Key.platform_name),
            platform_provider=parser.safe_get_string(sheet.name, sheet.Key.platform_provider),
            platform_output_type=parser.safe_get_string(sheet.name, sheet.Key.platform_output_type),
            public_repo_id=parser.safe_get_string(sheet.name, sheet.Key.public_repo_id),
            repo_url=parser.safe_get_string(sheet.name, sheet.Key.repo_url),
            raw_file_archive_dir=parser.get_one_string_list(sheet.name, sheet.Key.raw_file_archive_dir),
            extraction_protocol=parser.safe_get_string(sheet.name, sheet.Key.extraction_protocol),
            acquisition_method=parser.safe_get_string(sheet.name, sheet.Key.acquisition_method),
            metabolite_identification_description=parser.safe_get_string(sheet.name, sheet.Key.metabolite_identification_description),
            experiment_data_file=parser.safe_get_string(sheet.name, sheet.Key.experiment_data_file),
            attached_files=self._get_attached_files(parser)
        )

    def _get_attached_files(self, parser: ExcelsheetParser) -> List[str]:
        """Collect attached_file_1, attached_file_2, etc. until one is empty/missing."""
        sheet = ExperimentWorkbook.ExperimentSheet
        attached_files = []
        file_num = 1
        while True:
            value = parser.safe_get_string(sheet.name, sheet.Key.attached_file.format(file_num))
            if not value:
                break
            attached_files.append(value)
            file_num += 1
        return attached_files

    def _get_metabolite_categories(self, row, column_map: dict) -> List[CategoryValue]:
        categories = []
        cat_num = 1
        while True:
            key = ExperimentWorkbook.MetabMapSheet.Key.category.format(cat_num)
            column_name = column_map.get(key)
            if not column_name:
                break
            value = row.get(column_name)
            if value is not None and str(value).strip() and str(value).strip().lower() not in ('nan', 'na'):
                categories.append(CategoryValue(
                    id=f"{column_name}-{value}",
                    name=column_name, value=str(value).strip())
                )
            cat_num += 1
        return categories

    def _get_run_biosample_data(self, project_id: str, parser: ExcelsheetParser
                                ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse RunSampleMeta sheet and create RunBiosamples with edges to Biosamples."""
        run_sample_map = parser.get_parameter_map(ExperimentWorkbook.RunSampleMapSheet.name)
        run_biosample_config = RunBiosampleConfig(run_sample_map, project_id)

        run_biosamples = []
        edges: List[BiosampleRunBiosampleEdge] = []

        for _, row in parser.sheet_dfs[ExperimentWorkbook.RunSampleMetaSheet.name].iterrows():
            run_biosample_obj = run_biosample_config.get_data(row)
            run_biosamples.append(run_biosample_obj)

            raw_run_id = str(run_biosample_config.get_row_value(row, run_biosample_config.run_biosample_id_column, True))
            self._run_biosample_by_original_id[raw_run_id] = run_biosample_obj

            biosample_id = str(run_biosample_config.get_biosample_id(row))
            if biosample_id in self._biosample_by_original_id:
                edges.append(BiosampleRunBiosampleEdge(
                    start_node=self._biosample_by_original_id[biosample_id],
                    end_node=run_biosample_obj
                ))
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

        genes = []
        gene_edges = []
        for _, row in parser.sheet_dfs[ExperimentWorkbook.GeneMetaSheet.name].iterrows():
            gene_id = row.get(gene_id_col) if gene_id_col else None
            if not gene_id:
                continue
            gene_ensembl_id = EquivalentId(id=gene_id, type=Prefix.ENSEMBL).id_str()
            gene_obj = MeasuredGene(
                id=gene_ensembl_id,
                symbol=row.get(symbol_col) if symbol_col else None,
                biotype=row.get(biotype_col) if biotype_col else None
            )
            genes.append(gene_obj)
            self._gene_by_raw_id[str(gene_id)] = gene_obj

            gene_edges.append(MeasuredGeneEdge(start_node=gene_obj, end_node=Gene(id=gene_ensembl_id)))

        yield genes
        yield gene_edges

    def _parse_metabolites(self, experiment_obj: Experiment, parser: ExcelsheetParser
                           ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse metabolite data from MetabMeta sheet."""
        column_map = parser.get_parameter_map(ExperimentWorkbook.MetabMapSheet.name)
        metab_id_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_id)
        metab_name_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_name)
        metab_type_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_chemclass)
        id_level_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.identification_level)
        alternate_id_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.alternate_metab_id)
        alternate_symbol_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.alternate_metab_symbol)
        pathway_ids_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.pathway_ids)

        metabolites = []
        metabolite_edges = []

        for _, row in parser.sheet_dfs[ExperimentWorkbook.MetabMetaSheet.name].iterrows():
            metab_id = row.get(metab_id_col) if metab_id_col else None
            if not metab_id:
                continue

            alternate_ids = self._split_pipe(row.get(alternate_id_col) if alternate_id_col else None)
            alternate_names = self._split_pipe(row.get(alternate_symbol_col) if alternate_symbol_col else None)
            pathway_ids = self._split_pipe(row.get(pathway_ids_col) if pathway_ids_col else None)

            metab_obj = MeasuredMetabolite(
                id=f"{experiment_obj.id}-{metab_id}",
                name=row.get(metab_name_col) if metab_name_col else None,
                type=row.get(metab_type_col) if metab_type_col else None,
                identification_level=self._parse_int(row.get(id_level_col)) if id_level_col else None,
                alternate_ids=alternate_ids,
                alternate_names=alternate_names,
                pathway_ids=pathway_ids,
                categories=self._get_metabolite_categories(row, column_map)
            )
            metabolites.append(metab_obj)
            self._metabolite_by_raw_id[str(metab_id)] = metab_obj

            for raw_id in [metab_id] + alternate_ids:
                metabolite_edges.append(MeasuredMetaboliteEdge(start_node=metab_obj, end_node=Metabolite(id=raw_id)))

        yield metabolites
        yield metabolite_edges

    # --- Data matrix and stats results parsing ---

    def _prepare_data_frame(self, parser: ExcelsheetParser, data_sheet: str, analyte_id_col: str = None):
        """Clean a data sheet DataFrame: drop empty columns/rows, set analyte ID as index.
        Returns cleaned_df or None if empty."""
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
        index_rename = {}
        for raw_id in df.index:
            raw_id_str = str(raw_id)
            if raw_id_str in self._metabolite_by_raw_id:
                index_rename[raw_id] = self._metabolite_by_raw_id[raw_id_str].id
            elif raw_id_str in self._gene_by_raw_id:
                index_rename[raw_id] = self._gene_by_raw_id[raw_id_str].id

        col_rename = {
            col: self._run_biosample_by_original_id[str(col)].id
            for col in df.columns if str(col) in self._run_biosample_by_original_id
        }
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
        pre_processing = parser.safe_get_string(meta_sheet, "pre_processing_description")
        peri_processing = parser.safe_get_string(meta_sheet, "peri_processing_description")
        peakdata_tag = parser.safe_get_string(meta_sheet, "peakdata_tag")

        data_type = peakdata_tag.lower() if peakdata_tag else default_data_type
        dataset_id = f"{experiment_obj.id}:{data_type}"

        raw_df = self._prepare_data_frame(parser, data_sheet, analyte_id_col)

        if raw_df is None:
            dataset = Dataset(
                id=dataset_id, data_type=data_type,
                pre_processing_description=pre_processing, peri_processing_description=peri_processing,
                row_count=0, column_count=0, gene_id_column=analyte_id_col, sample_columns=[]
            )
            yield [dataset, ExperimentDatasetEdge(start_node=experiment_obj, end_node=dataset)]
            return

        self._coerce_numeric_columns(raw_df)

        # Build edges using raw IDs before qualifying the DataFrame.
        # (placeholder dataset needed for edge construction)
        placeholder = Dataset(
            id=dataset_id, data_type=data_type,
            pre_processing_description=pre_processing, peri_processing_description=peri_processing,
            row_count=len(raw_df), column_count=len(raw_df.columns),
            gene_id_column=analyte_id_col, sample_columns=list(raw_df.columns)
        )
        run_biosample_edges = [
            DatasetRunBiosampleEdge(start_node=placeholder, end_node=self._run_biosample_by_original_id[str(col)])
            for col in raw_df.columns if str(col) in self._run_biosample_by_original_id
        ]
        analyte_edges = self._build_analyte_edges(placeholder, raw_df, DatasetGeneEdge, DatasetMetaboliteEdge)

        qualified_df = self._qualify_data_frame_ids(raw_df)

        dataset = Dataset(
            id=dataset_id, data_type=data_type,
            pre_processing_description=pre_processing, peri_processing_description=peri_processing,
            row_count=len(qualified_df), column_count=len(qualified_df.columns),
            gene_id_column=analyte_id_col, sample_columns=list(qualified_df.columns),
            _data_frame=qualified_df
        )
        for edge in run_biosample_edges + analyte_edges:
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

        analyte_id_col = None
        es_column_map: Dict[str, ComparisonColumn] = {}
        if StatsResultsWorkbook.EffectSizeMapSheet.name in stats_sheet_names:
            analyte_id_col, es_column_map = self._parse_effect_size_map(stats_parser)

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
            yield from self._parse_effect_size(experiment_obj, analyte_id_col, es_column_map, stats_parser)

    def _parse_effect_size(self, experiment_obj: Experiment,
                           analyte_id_col: str = None,
                           es_column_map: Dict[str, ComparisonColumn] = None,
                           parser: ExcelsheetParser = None
                           ) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse EffectSize sheet into a StatsResult node with edges to analytes."""
        if es_column_map is None:
            es_column_map = {}

        stats_result_id = f"{experiment_obj.id}:effect_size"
        meta_sheet = StatsResultsWorkbook.StatsResultsMetaSheet

        meta_kwargs = dict(
            name=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.statsresults_name),
            description=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.stats_description),
            pre_processing_description=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.pre_processing_description),
            peri_processing_description=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.peri_processing_description),
            effect_size_description=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.effect_size),
            effect_size_pval_description=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.effect_size_pval),
            effect_size_adj_pval_description=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.effect_size_adj_pval),
            effect_size_2_description=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.effect_size_2),
            data_analysis_code_link=parser.safe_get_string(meta_sheet.name, meta_sheet.Key.data_analysis_code_link),
        )
        informatician = self._get_person(
            parser, meta_sheet.name,
            meta_sheet.Key.lead_informatician, meta_sheet.Key.lead_informatician_email
        )

        raw_df = self._prepare_data_frame(parser, StatsResultsWorkbook.EffectSizeSheet.name, analyte_id_col)

        if raw_df is None:
            stats_result = StatsResult(
                id=stats_result_id, data_type="effect_size",
                row_count=0, column_count=0,
                analyte_id_column=analyte_id_col,
                comparison_columns=list(es_column_map.values()), **meta_kwargs
            )
            nodes = [stats_result, ExperimentStatsResultEdge(start_node=experiment_obj, end_node=stats_result)]
            if informatician:
                nodes += [informatician, StatsResultPersonEdge(start_node=stats_result, end_node=informatician, role="Informatician")]
            yield nodes
            return

        self._coerce_numeric_columns(raw_df)

        # Build analyte edges using raw index IDs before qualifying.
        placeholder = StatsResult(id=stats_result_id, data_type="effect_size",
                                  row_count=0, column_count=0, analyte_id_column=analyte_id_col)
        analyte_edges = self._build_analyte_edges(placeholder, raw_df, StatsResultGeneEdge, StatsResultMetaboliteEdge)

        # Qualify DataFrame IDs (index only — effect size columns are comparison labels, not sample IDs)
        qualified_df = self._qualify_data_frame_ids(raw_df)

        # Build enriched ComparisonColumn objects from actual sheet columns.
        # Use map metadata where available; fall back to parsing the column name directly.
        comparison_columns = [
            es_column_map.get(str(col), ComparisonColumn.from_column_name(str(col)))
            for col in qualified_df.columns
        ]

        stats_result = StatsResult(
            id=stats_result_id, data_type="effect_size",
            row_count=len(qualified_df), column_count=len(qualified_df.columns),
            analyte_id_column=analyte_id_col, comparison_columns=comparison_columns,
            _data_frame=qualified_df, **meta_kwargs
        )
        for edge in analyte_edges:
            edge.start_node = stats_result

        nodes = [stats_result, ExperimentStatsResultEdge(start_node=experiment_obj, end_node=stats_result)]
        if informatician:
            nodes += [informatician, StatsResultPersonEdge(start_node=stats_result, end_node=informatician, role="Informatician")]
        yield nodes
        if analyte_edges:
            yield analyte_edges

    # --- Utilities ---

    @staticmethod
    def _parse_effect_size_map(parser: ExcelsheetParser) -> Tuple[Optional[str], Dict[str, ComparisonColumn]]:
        """Parse EffectSize_Map sheet into (analyte_id_col, {col_name: ComparisonColumn}).

        The map sheet has two kinds of rows:
          - Analyte ID row: NCATSDPI key is 'metabolite_id' or 'gene_id'; value is the
            actual column name in the EffectSize sheet that holds analyte identifiers.
          - Comparison rows: NCATSDPI key is 'NA ...' or empty; Submitter_Variable_Name
            is pipe-encoded as 'col_name | biosample_field1 | biosample_field2 | ...';
            Submitter_Notes holds a human-readable description.
        """
        sheet_name = StatsResultsWorkbook.EffectSizeMapSheet.name
        df = parser.sheet_dfs.get(sheet_name)
        if df is None or df.empty:
            return None, {}

        analyte_keys = {
            StatsResultsWorkbook.EffectSizeMapSheet.Key.gene_id,
            StatsResultsWorkbook.EffectSizeMapSheet.Key.metabolite_id,
        }
        notes_col = "Submitter_Notes"
        has_notes = notes_col in df.columns

        analyte_id_col = None
        column_map: Dict[str, ComparisonColumn] = {}

        for _, row in df.iterrows():
            ncats_key = row.get(ExcelsheetParser.KEY_COLUMN, '')
            if not isinstance(ncats_key, str):
                ncats_key = ''
            ncats_key = ncats_key.strip()

            submitter_val = row.get(ExcelsheetParser.MAPPED_VALUE_COLUMN, '')
            if not isinstance(submitter_val, str) or not submitter_val.strip():
                continue
            submitter_val = submitter_val.strip()

            notes = None
            if has_notes:
                raw_notes = row.get(notes_col)
                if isinstance(raw_notes, str) and raw_notes.strip():
                    notes = raw_notes.strip()

            if ncats_key in analyte_keys:
                analyte_id_col = submitter_val
            else:
                parts = [p.strip() for p in submitter_val.split('|') if p.strip()]
                if not parts:
                    continue
                col_name = parts[0]
                properties = parts[1:]
                if len(properties) == 0:
                    properties = None

                col = ComparisonColumn.from_column_name(col_name)
                col.properties = properties
                col.notes = notes
                column_map[col_name] = col

        return analyte_id_col, column_map

    @staticmethod
    def _get_person(parser: ExcelsheetParser, sheet_name: str, name_key: str, email_key: str) -> Optional[Person]:
        name = parser.safe_get_string(sheet_name, name_key)
        if not name:
            return None
        return Person.make(name, parser.safe_get_string(sheet_name, email_key))

    @staticmethod
    def _coerce_numeric_columns(df) -> None:
        """Convert all DataFrame columns to numeric in-place, coercing non-numeric values to NaN."""
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    @staticmethod
    def _split_pipe(value) -> List[str]:
        """Null-safe split of a pipe-delimited cell value. Returns [] for blank/NaN."""
        if not isinstance(value, str) or not value.strip():
            return []
        return [v.strip() for v in value.split('|') if v.strip()]

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
            return [Person.make(name) for name in names]
        if len(names) != len(emails):
            raise LookupError(f"Names and emails must have the same length: {names} vs {emails}")
        return [Person.make(name, email) for name, email in zip(names, emails)]