# src/workflows/targets.Snakefile
configfile: "config/targets/targets_config.yaml"

rule all:
    input:
        # Ensembl
        config['ensembl_data']['output_paths']['final_merged'],
        # NCBI
        config['ncbi_data']['parsed_output'],
        # HGNC
        config['hgnc_data']['parsed_output'],
        # RefSeq
        config['refseq_data']['transformed_data_path'],
        # UniProt
        config['uniprot_data']['mapping_output'],
        config['uniprot_data']['reviewed_info_output'],
        # NodeNorm
        config['nodenorm_genes']['output_file'],
        config['nodenorm_proteins']['output_file'],
        # Gene IDs
        config['gene_data']['gene_ids_path'],
        # Transcript IDs
        config['transcript_ids']['transcript_ids_path'],
        # Protein IDs
        config['protein_data']['protein_ids_path']

# Ensembl
rule ensembl_download:
    output:
        expand("{csv}", csv=config['ensembl_data']['output_paths']['biomart_csvs'])
    shell:
        "python src/publicdata/target_data/ensembl_download.py --config {configfile}"

rule ensembl_transform:
    input:
        expand("{csv}", csv=config['ensembl_data']['output_paths']['biomart_csvs'])
    output:
        config['ensembl_data']['output_paths']['final_merged']
    shell:
        "python src/publicdata/target_data/ensembl_transform.py --config {configfile}"

rule ensembl_isoform_compare:
    input:
        config['ensembl_data']['output_paths']['final_merged']
    output:
        config['ensembl_data']['output_paths']['comparison']
    shell:
        "python src/publicdata/target_data/ensembl_uniprot_isoform_xref.py --config {configfile}"

# NCBI
rule ncbi_download:
    output:
        config['ncbi_data']['output_path']
    shell:
        "python src/publicdata/target_data/ncbi_download.py --config {configfile}"

rule ncbi_transform:
    input:
        config['ncbi_data']['output_path']
    output:
        config['ncbi_data']['parsed_output']
    shell:
        "python src/publicdata/target_data/ncbi_transform.py --config {configfile}"

# HGNC
rule hgnc_download:
    output:
        config['hgnc_data']['output_path']
    shell:
        "python src/publicdata/target_data/hgnc_download.py --config {configfile}"

rule hgnc_transform:
    input:
        config['hgnc_data']['output_path']
    output:
        config['hgnc_data']['parsed_output']
    shell:
        "python src/publicdata/target_data/hgnc_transform.py --config {configfile}"

# RefSeq
rule refseq_download:
    output:
        config['refseq_data']['refseq']['path']
    shell:
        "python src/publicdata/target_data/refseq_download.py --config {configfile}"

rule refseq_transform:
    input:
        config['refseq_data']['refseq']['path']
    output:
        config['refseq_data']['transformed_data_path']
    shell:
        "python src/publicdata/target_data/refseq_transform.py --config {configfile}"

# UniProt
rule uniprot_download:
    output:
        config['uniprot_data']['decompressed_path']
    shell:
        "python src/publicdata/target_data/uniprot_download.py --config {configfile}"

rule uniprot_transform:
    input:
        config['uniprot_data']['decompressed_path']
    output:
        config['uniprot_data']['mapping_output'],
        config['uniprot_data']['reviewed_info_output']
    shell:
        "python src/publicdata/target_data/uniprot_transform.py --config {configfile}"

# NodeNorm
rule nodenorm_gene_download:
    output:
        config['nodenorm_genes']['raw_file']
    shell:
        "python src/publicdata/target_data/nodenorm_gene_download.py --config {configfile}"

rule nodenorm_gene_transform:
    input:
        config['nodenorm_genes']['raw_file']
    output:
        config['nodenorm_genes']['output_file']
    shell:
        "python src/publicdata/target_data/nodenorm_gene_transform.py --config {configfile}"

rule nodenorm_protein_download:
    output:
        config['nodenorm_proteins']['raw_file']
    shell:
        "python src/publicdata/target_data/nodenorm_protein_download.py --config {configfile}"

rule nodenorm_protein_transform:
    input:
        config['nodenorm_proteins']['raw_file']
    output:
        config['nodenorm_proteins']['output_file']
    shell:
        "python src/publicdata/target_data/nodenorm_protein_transform.py --config {configfile}"

# Gene IDs
rule gene_ids:
    input:
        config['gene_data']['source_file']
    output:
        config['gene_data']['gene_ids_path'],
        config['gene_data']['metadata_file']
    shell:
        "python src/publicdata/target_data/gene_ids.py --config {configfile}"

# Transcript merge & IDs
rule transcript_merge:
    input:
        config['transcript_merge']['biomart_output'],
        config['transcript_merge']['isoform_file'],
        config['transcript_merge']['refseq_ensembl_file'],
        config['transcript_merge']['refseq_file']
    output:
        config['transcript_merge']['transformed_data_path'],
        config['transcript_merge']['metadata_file']
    shell:
        "python src/publicdata/target_data/transcript_merge.py --config {configfile}"

rule transcript_ids:
    input:
        config['transcript_ids']['source_file']
    output:
        config['transcript_ids']['transcript_ids_path'],
        config['transcript_ids']['metadata_file']
    shell:
        "python src/publicdata/target_data/transcript_ids.py --config {configfile}"

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
        "python src/publicdata/target_data/protein_merge.py --config {configfile}"

rule protein_ids:
    input:
        config['protein_data']['source_file']
    output:
        config['protein_data']['output_file'],
        config['protein_data']['protein_ids_path'],
        config['protein_data']['isoform_file'],
        config['protein_data']['qc_file'],
        config['protein_data']['metadata_file']
    shell:
        "python src/publicdata/target_data/protein_ids.py --config {configfile}"
