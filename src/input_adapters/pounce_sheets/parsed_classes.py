"""Annotated dataclasses for parsed POUNCE Excel data.

Each class maps directly to one (or part of one) Excel sheet.  Every field
carries ``sheet_field`` metadata so that parsers and validators can be
auto-generated from the class definitions alone.

These are *parsed input objects* — NOT Node subclasses.  The
``PounceInputAdapter`` converts them into Nodes/Relationships for the ETL.
"""

from dataclasses import dataclass
from datetime import date
from typing import List, Optional

from src.input_adapters.pounce_sheets.sheet_field import sheet_field


# ---------------------------------------------------------------------------
# Project workbook — ProjectMeta (meta sheet, single-row key-value)
# ---------------------------------------------------------------------------

@dataclass
class ParsedProject:
    project_id: Optional[str] = sheet_field(
        key="project_id", sheet="ProjectMeta")
    project_name: Optional[str] = sheet_field(
        key="project_name", sheet="ProjectMeta")
    description: Optional[str] = sheet_field(
        key="description", sheet="ProjectMeta")
    date: Optional[date] = sheet_field(
        key="date", sheet="ProjectMeta", parse="date")
    owner_names: Optional[List[str]] = sheet_field(
        key="owner_name", sheet="ProjectMeta", parse="string_list",
        default_factory=list)
    owner_emails: Optional[List[str]] = sheet_field(
        key="owner_email", sheet="ProjectMeta", parse="string_list",
        default_factory=list)
    collaborator_names: Optional[List[str]] = sheet_field(
        key="collaborator_name", sheet="ProjectMeta", parse="string_list",
        default_factory=list)
    collaborator_emails: Optional[List[str]] = sheet_field(
        key="collaborator_email", sheet="ProjectMeta", parse="string_list",
        default_factory=list)
    lab_groups: Optional[List[str]] = sheet_field(
        key="lab_groups", sheet="ProjectMeta", parse="string_list",
        default_factory=list)
    keywords: Optional[List[str]] = sheet_field(
        key="keywords", sheet="ProjectMeta", parse="string_list",
        default_factory=list)
    privacy_type: Optional[str] = sheet_field(
        key="privacy_type", sheet="ProjectMeta")
    project_type: Optional[List[str]] = sheet_field(
        key="project_type", sheet="ProjectMeta", parse="string_list",
        default_factory=list)
    rd_tag: Optional[str] = sheet_field(
        key="RD_tag", sheet="ProjectMeta", parse="bool")
    biosample_preparation: Optional[str] = sheet_field(
        key="biosample_preparation", sheet="ProjectMeta")


@dataclass
class ParsedPerson:
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None


# ---------------------------------------------------------------------------
# Project workbook — BioSampleMeta via BioSampleMap (mapped sheet, multi-row)
# ---------------------------------------------------------------------------

@dataclass
class ParsedBiosample:
    biosample_id: Optional[str] = sheet_field(
        key="biosample_id", sheet="BioSampleMeta")
    biosample_type: Optional[str] = sheet_field(
        key="biosample_type", sheet="BioSampleMeta")


@dataclass
class ParsedBiospecimen:
    biospecimen_id: Optional[str] = sheet_field(
        key="biospecimen_id", sheet="BioSampleMeta")
    biospecimen_type: Optional[str] = sheet_field(
        key="biospecimen_type", sheet="BioSampleMeta")
    biospecimen_description: Optional[str] = sheet_field(
        key="biospecimen_description", sheet="BioSampleMeta")
    organism_names: Optional[str] = sheet_field(
        key="organism_names", sheet="BioSampleMeta")
    organism_category: Optional[str] = sheet_field(
        key="organism_category", sheet="BioSampleMeta", parse="category")
    disease_names: Optional[List[str]] = sheet_field(
        key="disease_names", sheet="BioSampleMeta", parse="string_list",
        default_factory=list)
    disease_category: Optional[str] = sheet_field(
        key="disease_category", sheet="BioSampleMeta", parse="category")


@dataclass
class ParsedDemographics:
    age: Optional[str] = sheet_field(
        key="age", sheet="BioSampleMeta")
    race: Optional[str] = sheet_field(
        key="race", sheet="BioSampleMeta")
    ethnicity: Optional[str] = sheet_field(
        key="ethnicity", sheet="BioSampleMeta")
    sex: Optional[str] = sheet_field(
        key="sex", sheet="BioSampleMeta")
    demographic_categories: Optional[List[str]] = sheet_field(
        key="demographic{}_category", sheet="BioSampleMeta", indexed=True,
        parse="category", default_factory=list)
    phenotype_categories: Optional[List[str]] = sheet_field(
        key="phenotype{}_category", sheet="BioSampleMeta", indexed=True,
        parse="category", default_factory=list)


@dataclass
class ParsedExposure:
    names: Optional[List[str]] = sheet_field(
        key="exposure{}_names", sheet="BioSampleMeta", indexed=True,
        parse="string_list", default_factory=list)
    type: Optional[str] = sheet_field(
        key="exposure{}_type", sheet="BioSampleMeta", indexed=True)
    category: Optional[str] = sheet_field(
        key="exposure{}_category", sheet="BioSampleMeta", indexed=True,
        parse="category")
    concentration: Optional[str] = sheet_field(
        key="exposure{}_concentration", sheet="BioSampleMeta", indexed=True)
    concentration_unit: Optional[str] = sheet_field(
        key="exposure{}_unit", sheet="BioSampleMeta", indexed=True)
    duration: Optional[str] = sheet_field(
        key="exposure{}_time", sheet="BioSampleMeta", indexed=True)
    duration_unit: Optional[str] = sheet_field(
        key="exposure{}_time_unit", sheet="BioSampleMeta", indexed=True)
    start_time: Optional[str] = sheet_field(
        key="exposure{}_start", sheet="BioSampleMeta", indexed=True)
    end_time: Optional[str] = sheet_field(
        key="exposure{}_end", sheet="BioSampleMeta", indexed=True)
    condition_category: Optional[str] = sheet_field(
        key="condition{}_category", sheet="BioSampleMeta", indexed=True,
        parse="category")
    growth_media: Optional[str] = sheet_field(
        key="growth_media", sheet="BioSampleMeta")
    exposure_index: Optional[int] = None  # which exposure slot (1, 2, ...)


# ---------------------------------------------------------------------------
# Experiment workbook — ExperimentMeta (meta sheet)
# ---------------------------------------------------------------------------

@dataclass
class ParsedExperiment:
    experiment_id: Optional[str] = sheet_field(
        key="experiment_id", sheet="ExperimentMeta")
    experiment_name: Optional[str] = sheet_field(
        key="experiment_name", sheet="ExperimentMeta")
    experiment_description: Optional[str] = sheet_field(
        key="experiment_description", sheet="ExperimentMeta")
    experiment_design: Optional[str] = sheet_field(
        key="experiment_design", sheet="ExperimentMeta")
    experiment_type: Optional[str] = sheet_field(
        key="experiment_type", sheet="ExperimentMeta")
    date: Optional[str] = sheet_field(
        key="date", sheet="ExperimentMeta")
    lead_data_generator: Optional[str] = sheet_field(
        key="lead_data_generator", sheet="ExperimentMeta")
    lead_data_generator_email: Optional[str] = sheet_field(
        key="lead_data_generator_email", sheet="ExperimentMeta")
    lead_informatician: Optional[str] = sheet_field(
        key="lead_informatician", sheet="ExperimentMeta")
    lead_informatician_email: Optional[str] = sheet_field(
        key="lead_informatician_email", sheet="ExperimentMeta")
    platform_type: Optional[str] = sheet_field(
        key="platform_type", sheet="ExperimentMeta")
    platform_name: Optional[str] = sheet_field(
        key="platform_name", sheet="ExperimentMeta")
    platform_provider: Optional[str] = sheet_field(
        key="platform_provider", sheet="ExperimentMeta")
    platform_output_type: Optional[str] = sheet_field(
        key="platform_output_type", sheet="ExperimentMeta")
    public_repo_id: Optional[str] = sheet_field(
        key="public_repo_id", sheet="ExperimentMeta")
    repo_url: Optional[str] = sheet_field(
        key="repo_url", sheet="ExperimentMeta")
    raw_file_archive_dir: Optional[List[str]] = sheet_field(
        key="raw_file_archive_dir", sheet="ExperimentMeta", parse="string_list",
        default_factory=list)
    extraction_protocol: Optional[str] = sheet_field(
        key="extraction_protocol", sheet="ExperimentMeta")
    acquisition_method: Optional[str] = sheet_field(
        key="acquisition_method", sheet="ExperimentMeta")
    metabolite_identification_description: Optional[str] = sheet_field(
        key="metabolite_identification_description", sheet="ExperimentMeta")
    experiment_data_file: Optional[str] = sheet_field(
        key="experiment_data_file", sheet="ExperimentMeta")
    attached_files: Optional[List[str]] = sheet_field(
        key="attached_file_{}", sheet="ExperimentMeta", indexed=True,
        default_factory=list)


# ---------------------------------------------------------------------------
# Experiment workbook — RunSampleMeta via RunSampleMap (mapped sheet, multi-row)
# ---------------------------------------------------------------------------

@dataclass
class ParsedRunBiosample:
    run_biosample_id: Optional[str] = sheet_field(
        key="run_biosample_id", sheet="RunSampleMeta")
    biosample_id: Optional[str] = sheet_field(
        key="biosample_id", sheet="RunSampleMeta")
    biological_replicate_number: Optional[str] = sheet_field(
        key="biological_replicate_number", sheet="RunSampleMeta", parse="int")
    technical_replicate_number: Optional[str] = sheet_field(
        key="technical_replicate_number", sheet="RunSampleMeta", parse="int")
    biosample_run_order: Optional[str] = sheet_field(
        key="biosample_run_order", sheet="RunSampleMeta", parse="int")


# ---------------------------------------------------------------------------
# StatsResults workbook — StatsResultsMeta (meta sheet)
# ---------------------------------------------------------------------------

@dataclass
class ParsedStatsResultsMeta:
    statsresults_name: Optional[str] = sheet_field(
        key="statsresults_name", sheet="StatsResultsMeta")
    stats_description: Optional[str] = sheet_field(
        key="stats_description", sheet="StatsResultsMeta")
    experiment_name: Optional[str] = sheet_field(
        key="experiment_name", sheet="StatsResultsMeta")
    experiment_id: Optional[str] = sheet_field(
        key="experiment_id", sheet="StatsResultsMeta")
    lead_informatician: Optional[str] = sheet_field(
        key="lead_informatician", sheet="StatsResultsMeta")
    lead_informatician_email: Optional[str] = sheet_field(
        key="lead_informatician_email", sheet="StatsResultsMeta")
    pre_processing_description: Optional[str] = sheet_field(
        key="pre_processing_description", sheet="StatsResultsMeta")
    peri_processing_description: Optional[str] = sheet_field(
        key="peri_processing_description", sheet="StatsResultsMeta")
    effect_size: Optional[str] = sheet_field(
        key="ES", sheet="StatsResultsMeta")
    effect_size_pval: Optional[str] = sheet_field(
        key="ESPval", sheet="StatsResultsMeta")
    effect_size_adj_pval: Optional[str] = sheet_field(
        key="ESadjPval", sheet="StatsResultsMeta")
    effect_size_2: Optional[str] = sheet_field(
        key="ES2", sheet="StatsResultsMeta")
    data_analysis_code_link: Optional[str] = sheet_field(
        key="DataAnalysisCode_Link", sheet="StatsResultsMeta")
