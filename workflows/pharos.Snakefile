rule all:
    input:
        "../input_files/manual/target_graph/gene_ids.csv",
        "../input_files/manual/target_graph/protein_ids.csv",
        "../input_files/manual/target_graph/transcript_ids.csv",
        "../input_files/manual/target_graph/uniprotkb_mapping.csv",
        "../input_files/manual/antibodypedia/genes_php",
        "../input_files/auto/go/go-basic.json",
        "../input_files/auto/jensenlab/protein_counts.tsv",
        "../input_files/auto/go/goa_human-uniprot.gaf.gz",
        "../input_files/auto/go/goa_human-go.gaf.gz",
        "../input_files/auto/uniprot/uniprot-human.json.gz",
        "../input_files/auto/iuphar/ligands.csv",
        "../input_files/auto/iuphar/interactions.csv",

rule download_iuphar:
    output:
        "../input_files/auto/iuphar/ligands.csv",
        "../input_files/auto/iuphar/interactions.csv"
    shell:
        """
        curl -sk -o {output[1]} https://www.guidetopharmacology.org/DATA/ligands.csv
        curl -sk -o {output[2]} https://www.guidetopharmacology.org/DATA/interactions.csv
        """

rule download_uniprot:
    output:
        "../input_files/auto/uniprot/uniprot-human.json.gz"
    shell:
        "curl -o {output} 'https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=(*)+AND+(model_organism:9606)'"


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
