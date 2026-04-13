# Dingyin Proteomics POUNCE Ingest

## Discovery

- Old-format source workbooks identified:
  - `input_files/manual/pounce/OLD/originals/POUNCE_Project_Proteo_DingyinDT.xlsx`
  - `input_files/manual/pounce/OLD/originals/POUNCE_Experiment_Proteo_DingyinDT.xlsx`
- New v2 proteomics templates were provided in `/Users/kelleherkj/Downloads/proteomics templates.zip`.
- Copied template targets for this working ingest:
  - `input_files/manual/pounce/dingyin_proteomics/POUNCE_Project_Proteomics_DingyinDT_v2.xlsx`
  - `input_files/manual/pounce/dingyin_proteomics/POUNCE_Experiment_Proteomics_DingyinDT_v2.xlsx`
  - `input_files/manual/pounce/dingyin_proteomics/POUNCE_StatsResults_Proteomics_DingyinDT_v2.xlsx`

## Observed Workbook Structure

Old Dingyin format:
- Project workbook sheets: `ProjectMeta`, `ExperimentMeta`, `BiospecimenMap`, `BiospecimenMeta`
- Experiment workbook sheets: `SampleMeta`, `SampleMap`, `ProteinMeta`, `ProteinMap`, `RawData`, `StatsReadyData`, `EffectSize`, `DataAnalysisMeta`

New v2 proteomics format:
- Project workbook sheets: `ProjectMeta`, `BioSampleMap`, `BioSampleMeta`
- Experiment workbook sheets: `ExperimentMeta`, `RunBioSampleMap`, `RunBioSampleMeta`, `ProteinMap`, `ProteinMeta`, `ProteinDataMeta`, `ProteinData`
- Stats workbook sheets: `StatsResultsMeta`, `StatsReadyData`, `EffectSize_Map`, `EffectSize`

## Initial Mapping Direction

- Treat the old Dingyin workbooks as the source of truth for content.
- Populate the new v2 proteomics templates with Dingyin's data rather than trying to ingest the legacy workbook layout directly.
- Use `src/use_cases/working.yaml` as the first-pass working config with a single `PounceInputAdapter`.
- Carry over the existing POUNCE `Gene` and `Metabolite` resolvers from `src/use_cases/pounce/pounce.yaml`.
- Add a protein resolver in `working.yaml` so proteomics analytes can resolve during validation and ingest.

## Field-Level Mapping

### Project workbook

Old `ProjectMeta` to new `ProjectMeta`:
- `project_name` -> `project_name`
- old project has no `project_id` -> must be assigned in the new workbook
- `description` -> `description`
- `date` -> `date`
- `owner_name` -> `owner_name`
- `owner_email` -> `owner_email`
- `labgroups` -> `lab_groups`
- `custom_keywords` -> `keywords`
- `collaborator_names` -> `collaborator_name`
- `collaborator_email` -> `collaborator_email`
- `privacy_type` -> `privacy_type`
- `project_type` -> `project_type`
- old project has no `RD_tag` -> leave blank unless user wants a value
- old project has no `biosample_preparation` -> leave blank unless recoverable elsewhere

Old `BiospecimenMap` + `BiospecimenMeta` to new `BioSampleMap` + `BioSampleMeta`:
- old `biospecimen_id` -> new `biosample_id` and new `biospecimen_id`
  - observed old sample/biospecimen structure is effectively 1:1
- old `biospecimen_type` -> new `biospecimen_type`
- old `biospecimen_organism` -> new `organism_names`
- old `Treatment_Status` / `Treatment` -> new `exposure1_names`
- old `Treatment_Type` -> new `exposure1_type`
- old `BIOSPECIMEN_Name` or `Treatment_Group_Label` -> likely new submitter-side group label columns, but not a direct NCATS key
- old `cell_type` is important for grouping and stats, but the new template does not expose a dedicated `cell_type` NCATS key
  - most likely destination is one of the `condition{}_category` or phenotype/demographic category slots
  - this needs an explicit mapping decision
- old project workbook has no disease, age, race, ethnicity, sex, growth media, or explicit biospecimen description values
  - those fields will remain blank unless another source provides them

### Experiment workbook

Old `ExperimentMeta` to new `ExperimentMeta`:
- old `experiment_name` -> new `experiment_name`
- old `description` -> new `experiment_description`
- old `experiment_design` -> new `experiment_design`
- old has no explicit `experiment_id` -> must be assigned
- old `experiment_category` -> new `experiment_type`
- old `date` -> new `date`
- old `lead_data_generator` -> new `lead_data_generator`
- old `lead_data_generator_email` -> new `lead_data_generator_email`
- old `lead_informatician` -> new `lead_informatician`
- old `lead_informatician_email` -> new `lead_informatician_email`
- old `platform_type` -> new `platform_type`
- old `platform_name` -> new `platform_name`
- old `platform_provider` -> new `platform_provider`
- old `platform_output_type` -> new `platform_output_type`
- old `public_repo_id` -> new `public_repo_id`
- old `repo_url` -> new `repo_url`
- old `raw_file_archive_dir` -> new `raw_file_archive_dir`
- old `extraction_protocol` -> new `extraction_protocol`
- old `acquisition_method` -> new `acquisition_method`
- old has no proteomics-specific equivalent for `metabolite_identification_description` -> leave blank
- old `experiment_data_file` -> new `experiment_data_file`
- old `point_of_contact` and `point_of_contact_email` do not have matching parsed fields in the new model
  - likely omit unless the model is extended

Old `SampleMap` + `SampleMeta` to new `RunBioSampleMap` + `RunBioSampleMeta`:
- old `sample_id` -> new `run_biosample_id`
- old `biospecimen_id` -> new `biosample_id`
- old sheet has no technical replicate number -> leave blank
- old sheet has no explicit batch -> leave blank
- old sheet has no explicit run order -> leave blank unless derived from column order
- old `Treatment_Group_Label`, `Treatment`, and `cell_type` do not belong in the current new run-biosample parsed model
  - they are better preserved at biosample level in the project workbook

Old `ProteinMap` + `ProteinMeta` to new `ProteinMap` + `ProteinMeta`:
- old `protein_id` -> new `protein_id`
  - observed submitter variable is `Uniprot_Acc`
- old `Protein_Description` -> new `protein_name`
- old `Gene_Symbol` -> new `gene_name`
- old file has no protein class, alternate IDs, or pathway IDs populated
  - leave blank on first pass
- old category slots are present in the map sheet but not populated in observed metadata
  - leave blank on first pass

Old quantitative data to new experiment workbook:
- old `RawData` sheet is empty and should not be used as the source of truth
- old `StatsReadyData` contains the actual protein-by-sample quantitative matrix
- new experiment workbook expects that matrix in `ProteinData`
- old `DataAnalysisMeta.Pre_processing_Description` and `Peri_processing_Description` -> new `ProteinDataMeta`
- old `StatsReadyData.Uniprot_Acc` -> new `ProteinData.protein_id`
- old sample columns such as `3DPolyIC`, `ALV_NT`, `PIC_CS` -> new `ProteinData` run biosample columns

### Stats workbook

Old `DataAnalysisMeta` to new `StatsResultsMeta`:
- old `Stats_Description` -> new `stats_description`
- old `Pre_processing_Description` -> new `pre_processing_description`
- old `Peri_processing_Description` -> new `peri_processing_description`
- old `ES` -> new `ES`
- old `DataAnalysisCode_Link` -> new `DataAnalysisCode_Link`
- new workbook also expects `statsresults_name`, `experiment_name`, `experiment_id`, `lead_informatician`, `lead_informatician_email`, `ESadjPval`, `ESPval`, `ES2`
  - some can be copied from old experiment metadata
  - `ESadjPval`, `ESPval`, and `ES2` appear absent in the old workbook and may remain blank on first pass

Old stats matrices to new stats workbook:
- old `StatsReadyData` -> new `StatsReadyData`
- old `EffectSize` -> new `EffectSize`
- old `Uniprot_Acc` -> new `protein_id`
- old effect-size columns like `ES_3DPolyIC_VS_3DPolyIC_postSMM` align with the new `ES_<comparison_label>` pattern
- new `EffectSize_Map` should map:
  - `protein_id` -> `protein_id`
  - `ES_<comparison_label>` rows for observed effect-size columns
  - if p-value or adjusted p-value columns are absent, leave those rows unbound or blank

## Current Parser / Builder Fit

- The current parser and node builder support project metadata, biosamples, biospecimens, run biosamples, genes, metabolites, raw-data datasets, peak-data datasets, and stats sheets keyed by `gene_id` or `metabolite_id`.
- The new proteomics template uses `ProteinMap`, `ProteinMeta`, `ProteinDataMeta`, `ProteinData`, and stats sheets keyed by `protein_id`.
- Proteomics support is therefore missing in all of the following places:
  - workbook constants
  - parsed dataclasses
  - experiment/stats parser branches
  - node builder analyte parsing
  - dataset/effect-size analyte edge creation keyed by protein IDs

## First-Pass Implementation Scope

- Add proteomics sheet support to the parser and node builder using UniProt-based protein IDs.
- Populate the copied v2 Dingyin templates with a first-pass mapping from the old workbooks.
- Preserve uncertain old fields conservatively rather than forcing lossy mappings.

## Open Questions

- Exact field mapping from old sample/biospecimen sheets into new `BioSample*` and `RunBioSample*` sheets.
- Exact identifier family present in Dingyin's protein tabs and whether the current protein resolver covers all observed IDs.
- Whether the first-pass stats workbook should be fully populated or only minimally scaffolded for adapter development.
