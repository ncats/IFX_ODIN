rule all:
    input:
        "../input_files/manual/ccle/CCLE_RNAseq_genes_counts_20180929.gct.gz",
        "../input_files/manual/ccle/CCLE_RNAseq_rsem_genes_tpm_20180929.txt.gz",
        "../input_files/manual/ccle/Cell_lines_annotations_20181226.txt",
        "../input_files/manual/target_graph/gene_ids.csv",
        "../input_files/auto/cellosaurus/cellosaurus.xml"

rule download_cellosaurus:
    output:
        "../input_files/auto/cellosaurus/cellosaurus.xml"
    shell:
        "curl -o {output} https://ftp.expasy.org/databases/cellosaurus/cellosaurus.xml"
