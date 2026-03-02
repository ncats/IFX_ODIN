rule all:
    input:
        "../input_files/manual/target_graph/gene_ids.csv",
        "../input_files/manual/target_graph/protein_ids.csv",
        "../input_files/manual/target_graph/transcript_ids.csv",
        "../input_files/manual/target_graph/uniprotkb_mapping.csv",
        "../input_files/auto/go/go-basic.json",
        "../input_files/auto/jensenlab/protein_counts.tsv",
        "../input_files/auto/go/goa_human-uniprot.gaf.gz",
        "../input_files/auto/go/goa_human-go.gaf.gz",
        "../input_files/auto/uniprot/uniprot-human.json.gz",
        "../input_files/auto/iuphar/ligands.csv",
        "../input_files/auto/iuphar/interactions.csv",
        "../input_files/auto/reactome/ReactomePathways.gmt.zip",
        "../input_files/auto/reactome/ReactomePathwaysRelation.txt",
        "../input_files/auto/reactome/UniProt2Reactome_All_Levels.txt",

rule download_iuphar:
    output:
        "../input_files/auto/iuphar/ligands.csv",
        "../input_files/auto/iuphar/interactions.csv"
    shell:
        """
        curl -sk -o {output[0]} https://www.guidetopharmacology.org/DATA/ligands.csv
        curl -sk -o {output[1]} https://www.guidetopharmacology.org/DATA/interactions.csv
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
        "curl -o {output} https://download.jensenlab.org/protein_counts.tsv"

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

rule download_reactome:
    output:
        "../input_files/auto/reactome/ReactomePathways.gmt.zip",
        "../input_files/auto/reactome/ReactomePathwaysRelation.txt",
        "../input_files/auto/reactome/UniProt2Reactome_All_Levels.txt"
    shell:
        """
        curl -o {output[0]} https://reactome.org/download/current/ReactomePathways.gmt.zip
        curl -o {output[1]} https://reactome.org/download/current/ReactomePathwaysRelation.txt
        curl -o {output[2]} https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt
        """
