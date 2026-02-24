import os
import pandas as pd
from datetime import datetime
from typing import Dict, Generator, List, Union, Optional, Tuple, Type

from src.constants import DataSourceName, Prefix
from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.input_adapters.pounce_sheets.constants import ExperimentWorkbook, StatsResultsWorkbook
from src.input_adapters.pounce_sheets.parsed_classes import ParsedPerson, ParsedProject
from src.input_adapters.pounce_sheets.parsed_pounce_data import ParsedPounceData
from src.input_adapters.pounce_sheets.pounce_parser import PounceParser
from src.core.validator_loader import load_validators
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import MeasuredGene, MeasuredGeneEdge, Gene
from src.models.metabolite import MeasuredMetabolite, MeasuredMetaboliteEdge, Metabolite
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.biosample import Biosample, BiosampleBiospecimenEdge
from src.models.pounce.biospecimen import Biospecimen
from src.models.pounce.category_value import CategoryValue
from src.models.pounce.dataset import Dataset, ExperimentDatasetEdge, DatasetRunBiosampleEdge, DatasetGeneEdge, DatasetMetaboliteEdge
from src.models.pounce.demographics import Demographics
from src.models.pounce.exposure import Exposure
from src.models.pounce.stats_result import StatsResult, ComparisonColumn, ExperimentStatsResultEdge, StatsResultGeneEdge, StatsResultMetaboliteEdge, StatsResultPersonEdge
from src.models.pounce.experiment import Experiment, ProjectExperimentEdge, ExperimentPersonEdge, RunBiosample, BiosampleRunBiosampleEdge
from src.models.pounce.exposure import BiosampleExposureEdge
from src.models.pounce.project import Project, AccessLevel, Person, ProjectPersonEdge, ProjectBiosampleEdge


class PounceInputAdapter(InputAdapter):
    project_file: str
    experiment_files: List[str]
    stats_results_files: List[str]
    _cached_project_data: Optional[ParsedPounceData]
    _biosample_by_original_id: dict
    _run_biosample_by_original_id: dict
    _gene_by_raw_id: dict
    _metabolite_by_raw_id: dict
    _experiment_counter: int
    _pounce_parser: PounceParser

    def __init__(self, project_file: str, experiment_files: List[str] = None,
                 stats_results_files: List[str] = None, validators_config: str = None):
        self.project_file = project_file
        self.experiment_files = experiment_files or []
        self.stats_results_files = stats_results_files or []
        self.validators_config = validators_config
        self._cached_project_data = None
        self._biosample_by_original_id = {}
        self._run_biosample_by_original_id = {}
        self._gene_by_raw_id = {}
        self._metabolite_by_raw_id = {}
        self._experiment_counter = 0
        self._pounce_parser = PounceParser()

    # --- Interface methods ---

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCATSPounce

    def get_version(self) -> DatasourceVersionInfo:
        data = self._get_project_data()
        return DatasourceVersionInfo(
            version_date=data.project.date,
            download_date=datetime.fromtimestamp(os.path.getmtime(self.project_file)).date()
        )

    def get_validators(self) -> list:
        if self.validators_config:
            return load_validators(self.validators_config)
        return []

    def get_validation_data(self):
        return self._pounce_parser.parse_all(
            self.project_file, self.experiment_files, self.stats_results_files
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        print(f"reading: {self.project_file}")

        project_data = self._get_project_data()
        proj_obj = self._project_node(project_data.project)
        person_nodes, person_edges = self._person_nodes_and_edges(proj_obj, project_data.people)
        yield [proj_obj, *person_nodes, *person_edges]

        yield from self._biosample_nodes(proj_obj, project_data)

        for i, exp_file in enumerate(self.experiment_files):
            exp_parser = ExcelsheetParser(file_path=exp_file)
            exp_data = self._pounce_parser.parse_experiment(exp_parser)
            stats_parser = ExcelsheetParser(file_path=self.stats_results_files[i]) \
                if i < len(self.stats_results_files) else None
            stats_data = self._pounce_parser.parse_stats_results(stats_parser) if stats_parser else None
            yield from self._get_experiment_data(proj_obj, exp_data, exp_parser, stats_data, stats_parser)

    # --- Lazy project parse cache ---

    def _get_project_data(self) -> ParsedPounceData:
        if self._cached_project_data is None:
            self._cached_project_data = self._pounce_parser.parse_project(
                ExcelsheetParser(file_path=self.project_file)
            )
        return self._cached_project_data

    # --- Project workbook ---

    @staticmethod
    def _project_node(parsed_project: ParsedProject) -> Project:
        return Project(
            id=parsed_project.project_id,
            name=parsed_project.project_name,
            description=parsed_project.description,
            date=parsed_project.date,
            lab_groups=parsed_project.lab_groups or [],
            access=AccessLevel.parse(parsed_project.privacy_type) if parsed_project.privacy_type else None,
            keywords=parsed_project.keywords or [],
            project_type=parsed_project.project_type or [],
            rare_disease_focus=parsed_project.rd_tag if parsed_project.rd_tag is not None else False,
            sample_preparation=parsed_project.biosample_preparation
        )

    @staticmethod
    def _person_nodes_and_edges(
        proj_obj: Project, people: List[ParsedPerson]
    ) -> Tuple[List[Person], List[ProjectPersonEdge]]:
        nodes = []
        edges = []
        for parsed_person in people:
            person_node = Person.make(parsed_person.name, parsed_person.email)
            nodes.append(person_node)
            edges.append(ProjectPersonEdge(start_node=proj_obj, end_node=person_node, role=parsed_person.role))
        return nodes, edges

    def _biosample_nodes(
        self, proj_obj: Project, project_data: ParsedPounceData
    ) -> Generator[List[Union[Node, Relationship]], None, None]:
        biosample_param_map = project_data.biosample_param_map or {}
        num_exposure_slots = len(PounceParser._detect_exposure_indices(biosample_param_map))

        biospecimens = {}
        biosamples = []
        exposures = {}
        sample_exposure_edges: List[BiosampleExposureEdge] = []
        project_biosample_edges: List[ProjectBiosampleEdge] = []
        biosample_biospecimen_edges: List[BiosampleBiospecimenEdge] = []

        for i, parsed_bs in enumerate(project_data.biosamples):
            parsed_spec = project_data.biospecimens[i]

            biospecimen_id = parsed_spec.biospecimen_id
            full_biospecimen_id = f"{proj_obj.id}-{biospecimen_id}"
            if full_biospecimen_id not in biospecimens:
                organism_category = self._make_category_value(
                    parsed_spec.organism_category,
                    biosample_param_map.get("organism_category")) if parsed_spec.organism_category else None
                disease_category = self._make_category_value(
                    parsed_spec.disease_category,
                    biosample_param_map.get("disease_category")) if parsed_spec.disease_category else None

                biospecimens[full_biospecimen_id] = Biospecimen(
                    id=full_biospecimen_id,
                    original_id=biospecimen_id,
                    type=parsed_spec.biospecimen_type,
                    description=parsed_spec.biospecimen_description,
                    organism=parsed_spec.organism_names,
                    organism_category=organism_category,
                    disease_category=disease_category,
                    diseases=parsed_spec.disease_names or []
                )

            full_biosample_id = f"{proj_obj.id}-{parsed_bs.biosample_id}-{biospecimen_id}"

            parsed_demo = project_data.demographics[i] if i < len(project_data.demographics) else None
            demographics = self._convert_demographics(parsed_demo, full_biosample_id) if parsed_demo else None

            biosample_obj = Biosample(
                id=full_biosample_id,
                original_id=parsed_bs.biosample_id,
                type=parsed_bs.biosample_type,
                demographics=demographics
            )
            biosamples.append(biosample_obj)
            self._biosample_by_original_id[str(parsed_bs.biosample_id)] = biosample_obj

            project_biosample_edges.append(ProjectBiosampleEdge(start_node=proj_obj, end_node=biosample_obj))
            biosample_biospecimen_edges.append(
                BiosampleBiospecimenEdge(start_node=biosample_obj, end_node=biospecimens[full_biospecimen_id])
            )

            if num_exposure_slots > 0:
                row_exposures = project_data.exposures[i * num_exposure_slots:(i + 1) * num_exposure_slots]
                for parsed_exp in row_exposures:
                    exposure_obj = self._convert_exposure(parsed_exp)
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

    def _get_experiment_data(
        self,
        proj_obj: Project,
        exp_data: ParsedPounceData,
        exp_parser: ExcelsheetParser,
        stats_data: Optional[ParsedPounceData] = None,
        stats_parser: Optional[ExcelsheetParser] = None,
    ) -> Generator[List[Union[Node, Relationship]], None, None]:
        print(f"reading: {exp_parser.file_path}")

        parsed = exp_data.experiments[0]

        exp_id = parsed.experiment_id
        if not exp_id:
            self._experiment_counter += 1
            exp_id = f"{proj_obj.id}-exp-{self._experiment_counter}"

        exp_date = None
        if parsed.date:
            try:
                exp_date = datetime.strptime(str(parsed.date), "%Y%m%d").date()
            except ValueError:
                pass

        experiment_obj = Experiment(
            id=exp_id,
            name=parsed.experiment_name,
            description=parsed.experiment_description,
            design=parsed.experiment_design,
            experiment_type=parsed.experiment_type,
            date=exp_date,
            platform_type=parsed.platform_type,
            platform_name=parsed.platform_name,
            platform_provider=parsed.platform_provider,
            platform_output_type=parsed.platform_output_type,
            public_repo_id=parsed.public_repo_id,
            repo_url=parsed.repo_url,
            raw_file_archive_dir=parsed.raw_file_archive_dir or [],
            extraction_protocol=parsed.extraction_protocol,
            acquisition_method=parsed.acquisition_method,
            metabolite_identification_description=parsed.metabolite_identification_description,
            experiment_data_file=parsed.experiment_data_file,
            attached_files=parsed.attached_files or []
        )

        data_generator = Person.make(parsed.lead_data_generator, parsed.lead_data_generator_email) \
            if parsed.lead_data_generator else None
        informatician = Person.make(parsed.lead_informatician, parsed.lead_informatician_email) \
            if parsed.lead_informatician else None

        persons = [p for p in [data_generator, informatician] if p is not None]
        person_edges = []
        if data_generator:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=data_generator, role="DataGenerator"))
        if informatician:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=informatician, role="Informatician"))

        yield [experiment_obj, *persons, ProjectExperimentEdge(start_node=proj_obj, end_node=experiment_obj), *person_edges]

        # Run biosamples
        run_biosamples = []
        rb_edges: List[BiosampleRunBiosampleEdge] = []
        for parsed_rb in exp_data.run_biosamples:
            run_biosample_id = parsed_rb.run_biosample_id
            run_biosample_obj = RunBiosample(
                id=f"{proj_obj.id}-{run_biosample_id}",
                biological_replicate_number=self._parse_int(parsed_rb.biological_replicate_number),
                technical_replicate_number=self._parse_int(parsed_rb.technical_replicate_number),
                run_order=self._parse_int(parsed_rb.biosample_run_order)
            )
            run_biosamples.append(run_biosample_obj)
            self._run_biosample_by_original_id[str(run_biosample_id)] = run_biosample_obj

            biosample_id = str(parsed_rb.biosample_id)
            if biosample_id in self._biosample_by_original_id:
                rb_edges.append(BiosampleRunBiosampleEdge(
                    start_node=self._biosample_by_original_id[biosample_id],
                    end_node=run_biosample_obj
                ))
            else:
                print(f"Warning: RunBiosample references unknown biosample_id: {biosample_id}")

        yield run_biosamples
        yield rb_edges

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
            yield from self._get_stats_results_data(experiment_obj, stats_parser, stats_data)

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
        """Rename DataFrame index and columns to use qualified entity IDs."""
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

    def _get_stats_results_data(self, experiment_obj: Experiment, stats_parser: ExcelsheetParser,
                                stats_data: Optional[ParsedPounceData] = None
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
            yield from self._parse_effect_size(experiment_obj, analyte_id_col, es_column_map, stats_parser, stats_data)

    def _parse_effect_size(self, experiment_obj: Experiment,
                           analyte_id_col: str = None,
                           es_column_map: Dict[str, ComparisonColumn] = None,
                           parser: ExcelsheetParser = None,
                           stats_data: Optional[ParsedPounceData] = None
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

        parsed_stats = stats_data.stats_results[0] if stats_data and stats_data.stats_results else None
        informatician = Person.make(parsed_stats.lead_informatician, parsed_stats.lead_informatician_email) \
            if parsed_stats and parsed_stats.lead_informatician else None

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

        placeholder = StatsResult(id=stats_result_id, data_type="effect_size",
                                  row_count=0, column_count=0, analyte_id_column=analyte_id_col)
        analyte_edges = self._build_analyte_edges(placeholder, raw_df, StatsResultGeneEdge, StatsResultMetaboliteEdge)

        qualified_df = self._qualify_data_frame_ids(raw_df)

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

    # --- Conversion helpers (ParsedX -> Node) ---

    @staticmethod
    def _make_category_value(value: str, column_name: str = None) -> Optional[CategoryValue]:
        if value is None:
            return None
        col = column_name or "unknown"
        return CategoryValue(id=f"{col}-{value}", name=col, value=value)

    @staticmethod
    def _convert_demographics(parsed, parent_id: str) -> Optional[Demographics]:
        if parsed is None:
            return None
        age = parsed.age
        race = parsed.race
        ethnicity = parsed.ethnicity
        sex = parsed.sex
        categories_raw = parsed.demographic_categories or []
        phenotype_raw = parsed.phenotype_categories or []

        # Parse "column:value" strings into CategoryValue objects
        categories = []
        for cat_str in categories_raw:
            if ":" in cat_str:
                col, val = cat_str.split(":", 1)
                categories.append(CategoryValue(id=f"{col}-{val}", name=col, value=val))

        phenotype_categories = []
        for cat_str in phenotype_raw:
            if ":" in cat_str:
                col, val = cat_str.split(":", 1)
                phenotype_categories.append(CategoryValue(id=f"{col}-{val}", name=col, value=val))

        if all(v is None for v in [age, race, ethnicity, sex]) and not categories and not phenotype_categories:
            return None

        return Demographics(
            id=f"{parent_id}::demographics" if parent_id else None,
            age=age, race=race, ethnicity=ethnicity, sex=sex,
            categories=categories or None,
            phenotype_categories=phenotype_categories or None
        )

    @staticmethod
    def _convert_exposure(parsed) -> Exposure:
        """Convert a ParsedExposure into an Exposure Node."""
        return Exposure(
            id='calculate',
            names=parsed.names or [],
            type=parsed.type,
            category=CategoryValue(
                id=f"exposure_category-{parsed.category}",
                name="exposure_category", value=parsed.category
            ) if parsed.category else None,
            concentration=parsed.concentration,
            concentration_unit=parsed.concentration_unit,
            duration=parsed.duration,
            duration_unit=parsed.duration_unit,
            start_time=parsed.start_time,
            end_time=parsed.end_time,
            growth_media=parsed.growth_media,
            condition=CategoryValue(
                id=f"condition_category-{parsed.condition_category}",
                name="condition_category", value=parsed.condition_category
            ) if parsed.condition_category else None,
        )

    # --- Utilities ---

    @staticmethod
    def _parse_effect_size_map(parser: ExcelsheetParser) -> Tuple[Optional[str], Dict[str, ComparisonColumn]]:
        """Parse EffectSize_Map sheet into (analyte_id_col, {col_name: ComparisonColumn})."""
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
