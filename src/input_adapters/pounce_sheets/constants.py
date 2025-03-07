
class ExperimentWorkbook:
    class SampleDataSheet:
        name = "SampleMeta"
    class SampleSheet:
        name = "SampleMap"
        class Key:
            sample_id = "sample_id"
            biospecimen_id = "biospecimen_id"
            biospecimen_comparison_label = "biospecimen_comparison_label"
            biological_replicate_number = "biological_replicate_number"
            sample_type = "sample_type"
    class GeneDataSheet:
        name = "GeneMeta"
    class GeneSheet:
        name = "GeneMap"
        class Key:
            ensembl_gene_id = "ensembl_gene_id"
            hgnc_gene_symbol = "hgnc_gene_symbol"
            gene_biotype = "gene_biotype"
            alternate_gene_id = "alternate_gene_id"
            chromosome_location = "chromosome_location"
            strand = "strand"
            pathway_ids = "pathway_ids"
            category_1 = "category 1"
            category_2 = "category 2"
            category_3 = "category 3"
            category_4 = "category 4"
            category_5 = "category 5"
            category_6 = "category 6"
            category_7 = "category 7"
    class RawDataSheet:
        name = "RawData"
    class StatsReadyDataSheet:
        name = "StatsReadyData"
    class EffectSizeDataSheet:
        name = "EffectSize"
    class DataAnalysisSheet:
        name = "DataAnalysisMeta"
        class Key:
            Pre_processing_Description = "Pre_processing_Description"
            Peri_processing_Description = "Peri_processing_Description"
            Stats_Description = "Stats_Description"
            ES = "ES"
            adjPval = "adjPval"
            DataAnalysisCode_Link = "DataAnalysisCode_Link"



class ProjectWorkbook:
    class ProjectSheet:
        name = "ProjectMeta"
        class Key:
            project_name = "project_name"
            description = "description"
            date = "date"
            owner_name = "owner_name"
            owner_email = "owner_email"
            labgroups = "labgroups"
            custom_keywords = "custom_keywords"
            collaborator_names = "collaborator_names"
            collaborator_email = "collaborator_email"
            privacy_type = "privacy_type"
            project_type = "project_type"
    class ExperimentSheet:
        name = "ExperimentMeta"
        class Key:
            experiment_name = "experiment_name"
            description = "description"
            experiment_design = "experiment_design"
            date = "date"
            point_of_contact = "point_of_contact"
            point_of_contact_email = "point_of_contact_email"
            lead_data_generator = "lead_data_generator"
            lead_data_generator_email = "lead_data_generator_email"
            lead_informatician = "lead_informatician"
            lead_informatician_email = "lead_informatician_email"
            experiment_category = "experiment_category"
            experiment_data_file = "experiment_data_file"
            platform_type = "platform_type"
            platform_output_type = "platform_output_type"
            raw_file_archive_dir = "raw_file_archive_dir"
            public_repo_id = "public_repo_id"
            repo_url = "repo_url"
            platform_name = "platform_name"
            data_provider = "data_provider"
            biospecimen_preparation = "biospecimen_preparation"
            extraction_protocol = "extraction_protocol"
            acquisition_method = "acquisition_method"
            attached_file_1 = "attached_file_1"
            attached_file_2 = "attached_file_2"
    class BiospecimenSheet:
        name = "BiospecimenMap"
        class Key:
            biospecimen_id = "biospecimen_id"
            biospecimen_name = "biospecimen_name"
            biospecimen_organism = "biospecimen_organism"
            biospecimen_type = "biospecimen_type"
            exposure_type = "exposure_type"
            biospecimen_group_label = "biospecimen_group_label"
            cell_line = "cell_line"
            exposure = "exposure"
            exposure_concentration = "exposure_concentration"
            exposure_unit = "exposure_unit"
            exposure_time = "exposure_time"
            exposure_time_unit = "exposure_time_unit"
            exposure_start = "exposure_start"
            exposure_end = "exposure_end"
            treatment_compound = "treatment_compound"
            compound_concentration = "compound_concentration"
            concentration_unit = "concentration_unit"
            treatment_compound_2 = "treatment_compound_2"
            compound_concentration_2 = "compound_concentration_2"
            concentration_unit_2 = "concentration_unit_2"
            exposure_time_2 = "exposure_time_2"
            exposure_time_unit_2 = "exposure_time_unit_2"
            exposure_start_2 = "exposure_start_2"
            exposure_end_2 = "exposure_end_2"
            age = "age"
            race = "race"
            ethinicity = "ethinicity"
            sex = "sex"
            growth_media = "growth_media"
            category_1 = "category 1"
            category_2 = "category 2"
            category_3 = "category 3"
            category_4 = "category 4"
            category_5 = "category 5"
            category_6 = "category 6"
            category_7 = "category 7"
    class BiospecimenDataSheet:
        name = "BiospecimenMeta"




