class ExperimentWorkbook:
    class ExperimentSheet:
        name = "ExperimentMeta"

        class Key:
            experiment_id = "experiment_id"
            experiment_name = "experiment_name"
            experiment_description = "experiment_description"
            experiment_design = "experiment_design"
            experiment_type = "experiment_type"
            date = "date"
            lead_data_generator = "lead_data_generator"
            lead_data_generator_email = "lead_data_generator_email"
            lead_informatician = "lead_informatician"
            lead_informatician_email = "lead_informatician_email"
            platform_type = "platform_type"
            platform_name = "platform_name"
            platform_provider = "platform_provider"
            platform_output_type = "platform_output_type"
            public_repo_id = "public_repo_id"
            repo_url = "repo_url"
            raw_file_archive_dir = "raw_file_archive_dir"
            extraction_protocol = "extraction_protocol"
            acquisition_method = "acquisition_method"
            metabolite_identification_description = "metabolite_identification_description"
            experiment_data_file = "experiment_data_file"
            attached_file = "attached_file_{}"

    class RunSampleMapSheet:
        name = "RunSampleMap"

        class Key:
            run_biosample_id = "run_biosample_id"
            biosample_id = "biosample_id"
            biological_replicate_number = "biological_replicate_number"
            technical_replicate_number = "technical_replicate_number"
            biosample_run_order = "biosample_run_order"

    class RunSampleMetaSheet:
        name = "RunSampleMeta"

    class MetabMapSheet:
        name = "MetabMap"

        class Key:
            metab_id = "metab_id"
            metab_name = "metab_name"
            metab_chemclass = "metab_chemclass"
            identification_level = "identification_level"
            alternate_metab_id = "alternate_metab_id"
            alternate_metab_symbol = "alternate_metab_symbol"
            pathway_ids = "pathway_ids"

    class MetabMetaSheet:
        name = "MetabMeta"

    class PeakDataMetaSheet:
        name = "PeakDataMeta"

        class Key:
            pre_processing_description = "pre_processing_description"
            peri_processing_description = "peri_processing_description"
            peakdata_tag = "peakdata_tag"

    class PeakDataSheet:
        name = "PeakData"

    class GeneMapSheet:
        name = "GeneMap"

        class Key:
            gene_id = "gene_id"
            hgnc_gene_symbol = "hgnc_gene_symbol"
            gene_biotype = "gene_biotype"
            alternate_id = "alternate_id"
            chromosome_location = "chromosome_location"
            strand = "strand"
            pathway_ids = "pathway_ids"

    class GeneMetaSheet:
        name = "GeneMeta"

    class RawDataMetaSheet:
        name = "RawDataMeta"

        class Key:
            pre_processing_description = "pre_processing_description"
            peakdata_tag = "peakdata_tag"

    class RawDataSheet:
        name = "RawData"


class StatsResultsWorkbook:
    class StatsResultsMetaSheet:
        name = "StatsResultsMeta"

        class Key:
            statsresults_name = "statsresults_name"
            experiment_name = "experiment_name"
            experiment_id = "experiment_id"
            lead_informatician = "lead_informatician"

    class StatsReadyDataSheet:
        name = "StatsReadyData"

    class EffectSizeMapSheet:
        name = "EffectSize_Map"

        class Key:
            gene_id = "gene_id"
            metabolite_id = "metabolite_id"

    class EffectSizeSheet:
        name = "EffectSize"


class ProjectWorkbook:
    class ProjectSheet:
        name = "ProjectMeta"
        class Key:
            project_name = "project_name"
            project_id = "project_id"
            description = "description"
            date = "date"
            owner_name = "owner_name"
            owner_email = "owner_email"
            lab_groups = "lab_groups"
            keywords = "keywords"
            collaborator_name = "collaborator_name"
            collaborator_email = "collaborator_email"
            privacy_type = "privacy_type"
            project_type = "project_type"
            RD_tag = "RD_tag"
            biosample_preparation = "biosample_preparation"
    class BiosampleMapSheet:
        name = "BioSampleMap"
        class Key:
            biosample_id = 'biosample_id'
            biosample_type = 'biosample_type'

            biospecimen_id = 'biospecimen_id'
            biospecimen_type = 'biospecimen_type'
            biospecimen_description = 'biospecimen_description'
            organism_names = 'organism_names'
            organism_category = 'organism_category'
            disease_names = 'disease_names'
            disease_category = 'disease_category'

            age = 'age'
            race = 'race'
            ethnicity = 'ethnicity'
            sex = 'sex'
            phenotype_category = 'phenotype{}_category'
            demographic_category = 'demographic{}_category'

            exposure_names = 'exposure{}_names'
            exposure_type = 'exposure{}_type'
            exposure_category = 'exposure{}_category'
            exposure_concentration = 'exposure{}_concentration'
            exposure_unit = 'exposure{}_unit'
            exposure_time = 'exposure{}_time'
            exposure_time_unit = 'exposure{}_time_unit'
            exposure_start = 'exposure{}_start'
            exposure_end = 'exposure{}_end'
            condition_category = 'condition{}_category'

            growth_media = 'growth_media'


    class BiosampleMetaSheet:
        name = "BioSampleMeta"
