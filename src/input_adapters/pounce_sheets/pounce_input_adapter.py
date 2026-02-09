import os
from datetime import datetime, date
from typing import Generator, List, Union, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.input_adapters.pounce_sheets.constants import ProjectWorkbook, ExperimentWorkbook
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.biosample import BiosampleBiospecimenEdge
from src.models.pounce.config_classes import ExposureConfig, BiosampleConfig, BiospecimenConfig, RunBiosampleConfig
from src.models.pounce.experiment import Experiment, ProjectExperimentEdge, ExperimentPersonEdge, BiosampleRunBiosampleEdge
from src.models.pounce.exposure import BiosampleExposureEdge
from src.models.pounce.project import Project, AccessLevel, Person, ProjectPersonEdge, ProjectBiosampleEdge


class PounceInputAdapter(InputAdapter):
    project_file: str
    project_parser: ExcelsheetParser
    experiment_file: Optional[str]
    experiment_parser: Optional[ExcelsheetParser]
    _biosample_by_original_id: dict  # Map of original_id -> Biosample for linking RunBiosamples

    def __init__(self, project_file: str, experiment_file: str = None):
        self.project_file = project_file
        self.project_parser = ExcelsheetParser(file_path=project_file)
        self.experiment_file = experiment_file
        self.experiment_parser = ExcelsheetParser(file_path=experiment_file) if experiment_file else None
        self._biosample_by_original_id = {}

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

        if self.experiment_parser:
            yield from self._get_experiment_data(proj_obj)

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
            # Store for linking RunBiosamples later
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

    def _get_experiment_data(self, proj_obj: Project) -> Generator[List[Union[Node, Relationship]], None, None]:
        print(f"reading: {self.experiment_file}")

        experiment_obj = self._create_experiment(proj_obj.id)

        # Get experiment personnel
        data_generator = self._get_experiment_person(
            ExperimentWorkbook.ExperimentSheet.Key.lead_data_generator,
            ExperimentWorkbook.ExperimentSheet.Key.lead_data_generator_email
        )
        informatician = self._get_experiment_person(
            ExperimentWorkbook.ExperimentSheet.Key.lead_informatician,
            ExperimentWorkbook.ExperimentSheet.Key.lead_informatician_email
        )

        persons = [p for p in [data_generator, informatician] if p is not None]
        person_edges = []
        if data_generator:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=data_generator, role="DataGenerator"))
        if informatician:
            person_edges.append(ExperimentPersonEdge(start_node=experiment_obj, end_node=informatician, role="Informatician"))

        project_experiment_edge = ProjectExperimentEdge(start_node=proj_obj, end_node=experiment_obj)

        yield [experiment_obj, *persons, project_experiment_edge, *person_edges]

        yield from self._get_run_biosample_data(proj_obj.id)

        # Parse analytes based on experiment type (detected by which sheets exist)
        # sheet_names = self.experiment_parser.sheet_dfs.keys()
        # if ExperimentWorkbook.MetabMetaSheet.name in sheet_names:
        #     yield from self._parse_metabolites(experiment_obj)
        # elif ExperimentWorkbook.GeneMetaSheet.name in sheet_names:
        #     yield from self._parse_genes(experiment_obj)

    def _get_run_biosample_data(self, project_id: str) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse RunSampleMeta sheet and create RunBiosamples with edges to Biosamples."""
        run_sample_map = self.experiment_parser.get_parameter_map(ExperimentWorkbook.RunSampleMapSheet.name)
        run_biosample_config = RunBiosampleConfig(run_sample_map, project_id)

        sheet_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.RunSampleMetaSheet.name]

        run_biosamples = []
        edges: List[BiosampleRunBiosampleEdge] = []

        for index, row in sheet_df.iterrows():
            run_biosample_obj = run_biosample_config.get_data(row)
            run_biosamples.append(run_biosample_obj)

            # Link to the parent Biosample
            biosample_id = str(run_biosample_config.get_biosample_id(row))
            if biosample_id in self._biosample_by_original_id:
                biosample_obj = self._biosample_by_original_id[biosample_id]
                edges.append(BiosampleRunBiosampleEdge(start_node=biosample_obj, end_node=run_biosample_obj))
            else:
                print(f"Warning: RunBiosample references unknown biosample_id: {biosample_id}")

        yield run_biosamples
        yield edges

    def _parse_metabolites(self, experiment_obj: Experiment) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse metabolite data from MetabMeta sheet."""
        column_map = self.experiment_parser.get_parameter_map(ExperimentWorkbook.MetabMapSheet.name)

        metab_id_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_id)
        metab_name_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_name)
        metab_type_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.metab_chemclass)
        id_level_col = column_map.get(ExperimentWorkbook.MetabMapSheet.Key.identification_level)

        sheet_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.MetabMetaSheet.name]

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

        yield metabolites

    def _parse_genes(self, experiment_obj: Experiment) -> Generator[List[Union[Node, Relationship]], None, None]:
        """Parse gene data from GeneMeta sheet."""
        column_map = self.experiment_parser.get_parameter_map(ExperimentWorkbook.GeneMapSheet.name)

        gene_id_col = column_map.get(ExperimentWorkbook.GeneMapSheet.Key.gene_id)
        symbol_col = column_map.get(ExperimentWorkbook.GeneMapSheet.Key.hgnc_gene_symbol)
        biotype_col = column_map.get(ExperimentWorkbook.GeneMapSheet.Key.gene_biotype)

        sheet_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.GeneMetaSheet.name]

        genes = []
        for index, row in sheet_df.iterrows():
            gene_id = row.get(gene_id_col) if gene_id_col else None
            if not gene_id:
                continue

            equiv_id = EquivalentId(id=gene_id, type=Prefix.ENSEMBL)
            gene_obj = Gene(
                id=equiv_id.id_str(),
                symbol=row.get(symbol_col) if symbol_col else None,
                biotype=row.get(biotype_col) if biotype_col else None
            )
            genes.append(gene_obj)

        yield genes

    @staticmethod
    def _parse_int(value) -> Optional[int]:
        """Safely parse an integer value."""
        if value is None or value == '' or value == 'NA':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _create_experiment(self, project_id: str) -> Experiment:
        exp_id = self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_id)
        # Generate experiment ID from project ID if not provided
        if not exp_id:
            exp_id = f"{project_id}-exp"

        exp_date = None
        date_str = self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.date)
        if date_str:
            try:
                exp_date = datetime.strptime(str(date_str), "%Y%m%d").date()
            except ValueError:
                pass

        return Experiment(
            id=exp_id,
            name=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_name),
            description=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_description),
            design=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_design),
            experiment_type=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_type),
            date=exp_date,
            platform_type=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_type),
            platform_name=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_name),
            platform_provider=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_provider),
            platform_output_type=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.platform_output_type),
            public_repo_id=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.public_repo_id),
            repo_url=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.repo_url),
            raw_file_archive_dir=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.raw_file_archive_dir),
            extraction_protocol=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.extraction_protocol),
            acquisition_method=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.acquisition_method),
            metabolite_identification_description=self._get_experiment_string(
                ExperimentWorkbook.ExperimentSheet.Key.metabolite_identification_description
            ),
            experiment_data_file=self._get_experiment_string(ExperimentWorkbook.ExperimentSheet.Key.experiment_data_file),
            attached_files=self._get_attached_files()
        )

    def _get_attached_files(self) -> List[str]:
        """Collect attached_file_1, attached_file_2, etc. until one is empty/missing."""
        attached_files = []
        file_num = 1
        while True:
            key = ExperimentWorkbook.ExperimentSheet.Key.attached_file.format(file_num)
            value = self._get_experiment_string(key)
            if not value:
                break
            attached_files.append(value)
            file_num += 1
        return attached_files

    def _get_experiment_person(self, name_key: str, email_key: str) -> Optional[Person]:
        name = self._get_experiment_string(name_key)
        if not name:
            return None
        email = self._get_experiment_string(email_key)
        return Person(id=name, email=email)

    # --- Experiment field accessors ---

    def _get_experiment_string(self, data_key: str) -> Optional[str]:
        try:
            return self.experiment_parser.get_one_string(ExperimentWorkbook.ExperimentSheet.name, data_key)
        except LookupError:
            return None

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

    # --- Person (owner/collaborator) accessors ---

    def _get_project_owner_names(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.owner_name)

    def _get_project_owner_emails(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.owner_email)

    def _get_collaborator_names(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.collaborator_name)

    def _get_collaborator_emails(self) -> List[str]:
        return self._get_project_string_list(ProjectWorkbook.ProjectSheet.Key.collaborator_email)

    @staticmethod
    def _get_persons_from_lists(names: List[str], emails: List[str]) -> List[Person]:
        if len(emails) == 0:
            return [Person(id=name, email=None) for name in names]
        if len(names) != len(emails):
            raise LookupError(f"Names and emails must have the same length: {names} vs {emails}")
        return [Person(id=name, email=email) for name, email in zip(names, emails)]
