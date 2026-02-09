rule all:
    input:
        "../input_files/manual/ccle/CCLE_RNAseq_genes_counts_20180929.gct.gz",
        "../input_files/manual/ccle/CCLE_RNAseq_rsem_genes_tpm_20180929.txt.gz",
        "../input_files/manual/ccle/Cell_lines_annotations_20181226.txt",
        "../input_files/manual/target_graph/gene_ids.csv",
        "../input_files/auto/cellosaurus/cellosaurus.xml",
        "../input_files/auto/uniprot/uniprot-human.json.gz",
        "../input_files/auto/ensembl/version.csv",
        "../input_files/auto/ensembl/Homo_sapiens.current.gtf.gz"

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

rule download_ensembl_data:
    output:
        version = "../input_files/auto/ensembl/version.csv",
        gtf = "../input_files/auto/ensembl/Homo_sapiens.current.gtf.gz"
    shell:
        """
        base_url="https://ftp.ensembl.org/pub/current/gtf/homo_sapiens"

        listing=$(curl -s "$base_url/")

        # Capture the canonical GTF:
        #   Homo_sapiens.<GRCbuild>.<ensembl_version>.gtf.gz
        filename=$(echo "$listing" \
            | grep -o 'Homo_sapiens\\.GRCh[0-9]\\+\\.[0-9]\\+\\.gtf\\.gz' \
            | head -n 1)

        # Extract GRC (e.g., GRCh38)
        grc_build=$(echo "$filename" | sed -E 's/Homo_sapiens\\.(GRCh[0-9]+)\\..*/\\1/')

        # Extract Ensembl version
        ensembl_version=$(echo "$filename" | sed -E 's/.*GRCh[0-9]+\\.([0-9]+)\\.gtf\\.gz/\\1/')

        # Save info (note the double braces {{ ... }} to escape braces)
        {{
            echo "GRC_BUILD,ENSEMBL_VERSION"
            echo "${{grc_build}},${{ensembl_version}}"
        }} > {output.version}

        # Download the file
        curl -s "$base_url/$filename" -o {output.gtf}
        """
