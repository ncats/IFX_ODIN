# src/workflows/targets.Snakefile
configfile: "config/targets_config.yaml"

CONFIG_PATH = "config/targets_config.yaml"
TARGET_CODE_DIR = "src/code/publicdata/target_data"

rule all:
    input:
        # Ensembl
        config['ensembl_data']['output_paths']['final_merged'],
        config['ensembl_data']['output_paths']['comparison'],
        # NCBI
        config['ncbi_data']['parsed_output'],
        # HGNC
        config['hgnc_data']['parsed_output'],
        # RefSeq
        config['refseq_data']['transformed_data_path'],
        # UniProt
        config['uniprot_data']['mapping_output'],
        config['uniprot_data']['reviewed_info_output'],
        config['uniprot_data']['canonical_isoforms_output'],
        config['uniprot_data']['comp_isoforms_output'],
        # NodeNorm
        config['nodenorm_genes']['output_file'],
        config['nodenorm_proteins']['output_file'],
        # Provenance merge tables
        config['gene_merge']['output_file'],
        config['transcript_merge']['transformed_data_path'],
        config['protein_merge']['transformed_data_path'],
        # Gene IDs
        config['gene_data']['gene_ids_path'],
        # Transcript IDs
        config['transcript_ids']['transcript_ids_path'],
        # Protein IDs
        config['protein_data']['protein_ids_path'],
        # Download/version catalog
        config['download_catalog']['out_csv']

# Ensembl
rule ensembl_download:
    output:
        expand("{csv}", csv=config['ensembl_data']['output_paths']['biomart_csvs'])
    shell:
        "python {TARGET_CODE_DIR}/ensembl_download.py --config {CONFIG_PATH}"

rule ensembl_transform:
    input:
        expand("{csv}", csv=config['ensembl_data']['output_paths']['biomart_csvs'])
    output:
        config['ensembl_data']['output_paths']['final_merged']
    shell:
        "python {TARGET_CODE_DIR}/ensembl_transform.py --config {CONFIG_PATH}"

rule ensembl_isoform_compare:
    input:
        config['ensembl_data']['output_paths']['final_merged']
    output:
        config['ensembl_data']['output_paths']['comparison']
    shell:
        "python {TARGET_CODE_DIR}/ensembl_uniprot_isoform_xref.py --config {CONFIG_PATH}"

# NCBI
rule ncbi_download:
    output:
        config['ncbi_data']['output_path']
    shell:
        "python {TARGET_CODE_DIR}/ncbi_download.py --config {CONFIG_PATH}"

rule ncbi_transform:
    input:
        config['ncbi_data']['output_path']
    output:
        config['ncbi_data']['parsed_output']
    shell:
        "python {TARGET_CODE_DIR}/ncbi_transform.py --config {CONFIG_PATH}"

# HGNC
rule hgnc_download:
    output:
        config['hgnc_data']['output_path']
    shell:
        "python {TARGET_CODE_DIR}/hgnc_download.py --config {CONFIG_PATH}"

rule hgnc_transform:
    input:
        config['hgnc_data']['output_path']
    output:
        config['hgnc_data']['parsed_output']
    shell:
        "python {TARGET_CODE_DIR}/hgnc_transform.py --config {CONFIG_PATH}"

# RefSeq
rule refseq_download:
    output:
        config['refseq_data']['refseq']['path']
    shell:
        "python {TARGET_CODE_DIR}/refseq_download.py --config {CONFIG_PATH}"

rule refseq_transform:
    input:
        config['refseq_data']['refseq']['path']
    output:
        config['refseq_data']['transformed_data_path']
    shell:
        "python {TARGET_CODE_DIR}/refseq_transform.py --config {CONFIG_PATH}"

# UniProt
rule uniprot_download:
    output:
        config['uniprot_data']['decompressed_path'],
        config['uniprot_data']['canonical_isoforms_output'],
        config['uniprot_data']['comp_isoforms_output']
    shell:
        "python {TARGET_CODE_DIR}/uniprot_download.py --config {CONFIG_PATH}"

rule uniprot_transform:
    input:
        config['uniprot_data']['decompressed_path']
    output:
        config['uniprot_data']['mapping_output'],
        config['uniprot_data']['reviewed_info_output']
    shell:
        "python {TARGET_CODE_DIR}/uniprot_transform.py --config {CONFIG_PATH}"

# NodeNorm
rule nodenorm_gene_download:
    output:
        config['nodenorm_genes']['raw_file']
    shell:
        "python {TARGET_CODE_DIR}/nodenorm_gene_download.py --config {CONFIG_PATH}"

rule nodenorm_gene_transform:
    input:
        config['nodenorm_genes']['raw_file']
    output:
        config['nodenorm_genes']['output_file']
    shell:
        "python {TARGET_CODE_DIR}/nodenorm_gene_transform.py --config {CONFIG_PATH}"

rule nodenorm_protein_download:
    output:
        config['nodenorm_proteins']['raw_file']
    shell:
        "python {TARGET_CODE_DIR}/nodenorm_protein_download.py --config {CONFIG_PATH}"

rule nodenorm_protein_transform:
    input:
        config['nodenorm_proteins']['raw_file']
    output:
        config['nodenorm_proteins']['output_file']
    shell:
        "python {TARGET_CODE_DIR}/nodenorm_protein_transform.py --config {CONFIG_PATH}"

# Gene provenance merge
rule gene_merge:
    input:
        config['gene_merge']['ensembl_file'],
        config['gene_merge']['ncbi_file'],
        config['gene_merge']['hgnc_file'],
        config['gene_merge']['nodenorm_file']
    output:
        config['gene_merge']['output_file'],
        config['gene_merge']['metadata_file']
    shell:
        "python {TARGET_CODE_DIR}/gene_merge.py --config {CONFIG_PATH}"

# Gene IDs
rule gene_ids:
    input:
        config['gene_data']['source_file'],
        config['gene_data']['ensembl_data'],
        config['gene_data']['ncbi_data'],
        config['gene_data']['hgnc_data']
    output:
        config['gene_data']['gene_ids_path'],
        config['gene_data']['metadata_file']
    shell:
        "python {TARGET_CODE_DIR}/gene_ids.py --config {CONFIG_PATH}"

# Transcript merge & IDs
rule transcript_merge:
    input:
        config['transcript_merge']['isoform_file'],
        config['transcript_merge']['refseq_ensembl_file'],
        config['transcript_merge']['refseq_file']
    output:
        config['transcript_merge']['transformed_data_path'],
        config['transcript_merge']['metadata_file'],
        config['transcript_merge']['metrics_file']
    shell:
        "python {TARGET_CODE_DIR}/transcript_merge.py --config {CONFIG_PATH}"

rule transcript_ids:
    input:
        config['transcript_ids']['source_file']
    output:
        config['transcript_ids']['transcript_ids_path'],
        config['transcript_ids']['metadata_file']
    shell:
        "python {TARGET_CODE_DIR}/transcript_ids.py --config {CONFIG_PATH}"

# Protein merge & IDs
rule protein_merge:
    input:
        ensembl_isoform_csv = config['protein_merge']['ensembl_isoform_csv'],
        refseq_uniprot_csv  = config['protein_merge']['refseq_uniprot_csv'],
        refseq_ensembl_csv  = config['protein_merge']['refseq_ensembl_csv'],
        uniprot_mapping_csv = config['protein_merge']['uniprot_mapping_csv'],
        uniprot_info_csv    = config['protein_merge']['uniprot_info_csv'],
        nodenorm_file       = config['protein_merge']['nodenorm_file']
    output:
        config['protein_merge']['transformed_data_path'],
        config['protein_merge']['metadata_file']
    shell:
        "python {TARGET_CODE_DIR}/protein_merge.py --config {CONFIG_PATH}"

rule protein_ids:
    input:
        config['protein_data']['source_file'],
        config['protein_data']['uniprot_info_file']
    output:
        config['protein_data']['protein_ids_path'],
        config['protein_data']['qc_file'],
        config['protein_data']['metadata_file']
    shell:
        "python {TARGET_CODE_DIR}/protein_ids.py --config {CONFIG_PATH}"

rule target_version:
    input:
        config['ensembl_data']['dl_metadata_file'],
        config['ncbi_data']['dl_metadata_file'],
        config['hgnc_data']['dl_metadata_file'],
        config['refseq_data']['dl_metadata_file'],
        config['uniprot_data']['dl_metadata_file'],
        config['nodenorm_genes']['dl_metadata_file'],
        config['nodenorm_proteins']['dl_metadata_file']
    output:
        config['download_catalog']['out_csv'],
        config['download_catalog']['metadata_file']
    shell:
        "python {TARGET_CODE_DIR}/target_version.py --config {CONFIG_PATH}"
