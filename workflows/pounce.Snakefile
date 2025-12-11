rule all:
    input:
        "../input_files/manual/ccle/CCLE_RNAseq_genes_counts_20180929.gct.gz",
        "../input_files/manual/ccle/CCLE_RNAseq_rsem_genes_tpm_20180929.txt.gz",
        "../input_files/manual/ccle/Cell_lines_annotations_20181226.txt",
        "../input_files/manual/target_graph/gene_ids.csv",
        "../input_files/auto/cellosaurus/cellosaurus.xml",
        "../input_files/auto/uniprot/uniprot-human.json.gz"

rule download_cellosaurus:
    output:
        "../input_files/auto/cellosaurus/cellosaurus.xml"
    shell:
        "curl -o {output} https://ftp.expasy.org/databases/cellosaurus/cellosaurus.xml"

rule download_uniprot_data:
    output:
        "../input_files/auto/uniprot/uniprot-human.json.gz"
    shell:
        "curl -o {output} 'https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=%28*%29+AND+%28reviewed%3Atrue%29+AND+%28model_organism%3A9606%29'"

