rule all:
    input:
        "../input_files/manual/target_graph/gene_ids.csv",
        "../input_files/manual/target_graph/protein_ids.csv",
        "../input_files/manual/target_graph/transcript_ids.csv",
        "../input_files/manual/target_graph/uniprotkb_mapping.csv",
        "../input_files/manual/antibodypedia/antibodypedia_data.csv",
        "../input_files/auto/go/go-basic.json",
        "../input_files/auto/jensenlab/protein_counts.tsv",
        "../input_files/auto/go/goa_human-uniprot.gaf.gz",
        "../input_files/auto/go/goa_human-go.gaf.gz"

rule download_go:
    output:
        "../input_files/auto/go/go-basic.json"
    shell:
        "curl -o {output} https://current.geneontology.org/ontology/go-basic.json"

rule download_pm_scores:
    output:
        "../input_files/auto/jensenlab/protein_counts.tsv"
    shell:
        "curl -o {output} https://download.jensenlab.org/KMC/protein_counts.tsv"

rule download_go_from_uniprot:
    output:
        "../input_files/auto/go/goa_human-uniprot.gaf.gz"
    shell:
        "curl -o {output} https://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz"

rule download_go_from_go:
    output:
        "../input_files/auto/go/goa_human-go.gaf.gz"
    shell:
        "curl -o {output} https://current.geneontology.org/annotations/goa_human.gaf.gz"
