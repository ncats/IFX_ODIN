rule all:
    input:
        "../input_files/auto/ctd/CTD_curated_genes_diseases.tsv.gz",
        "../input_files/auto/ctd/ctd_version.tsv",
        "../input_files/auto/pathwaycommons/pc-hgnc.gmt.gz",
        "../input_files/auto/pathwaycommons/pathwaycommons_version.tsv",
        "../input_files/manual/target_graph/gene_ids.tsv",
        "../input_files/manual/target_graph/protein_ids.tsv",
        "../input_files/manual/target_graph/transcript_ids.tsv",
        "../input_files/manual/target_graph/uniprotkb_mapping_20260315.csv",
        "../input_files/auto/go/go-basic.json",
        "../input_files/auto/jensenlab/protein_counts.tsv",
        "../input_files/auto/jensenlab/human_disease_knowledge_filtered.tsv",
        "../input_files/auto/jensenlab/human_disease_experiments_filtered.tsv",
        "../input_files/auto/jensenlab/human_disease_textmining_filtered.tsv",
        "../input_files/auto/jensenlab/diseases_version.tsv",
        "../input_files/auto/jensenlab/human_textmining_mentions.tsv",
        "../input_files/auto/jensenlab/disease_textmining_mentions.tsv",
        "../input_files/auto/jensenlab/tinx_version.tsv",
        "../input_files/auto/ncbi/gene2pubmed.gz",
        "../input_files/auto/ncbi/generifs_basic.gz",
        "../input_files/auto/ncbi/ncbi_publications_version.tsv",
        "../input_files/auto/tiga/tiga_gene-trait_stats.tsv",
        "../input_files/auto/tiga/tiga_gene-trait_provenance.tsv",
        "../input_files/auto/tiga/tiga_version.tsv",
        "../input_files/auto/go/goa_human-uniprot.gaf.gz",
        "../input_files/auto/go/goa_human-go.gaf.gz",
        "../input_files/auto/uniprot/uniprot-human.json.gz",
        "../input_files/auto/uniprot/uniprot-human-reviewed.json.gz",
        "../input_files/auto/uniprot/uniprot_version.tsv",
        "../input_files/auto/iuphar/ligands.csv",
        "../input_files/auto/iuphar/interactions.csv",
        "../input_files/auto/reactome/ReactomePathways.gmt.zip",
        "../input_files/auto/reactome/ReactomePathwaysRelation.txt",
        "../input_files/auto/reactome/UniProt2Reactome_All_Levels.txt",
        "../input_files/auto/reactome/reactome.homo_sapiens.interactions.tab-delimited.txt",
        "../input_files/auto/reactome/reactome_version.tsv",
        "../input_files/auto/bioplex/BioPlex_293T_Network_10K_Dec_2019.tsv",
        "../input_files/auto/bioplex/BioPlex_HCT116_Network_5.5K_Dec_2019.tsv",
        "../input_files/auto/bioplex/bioplex_version.tsv",
        "../input_files/auto/string/9606.protein.links.v12.0.txt.gz",
        "../input_files/auto/string/string_version.tsv",
        "../input_files/auto/gtex/GTEx_Analysis_2025_08_22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz",
        "../input_files/auto/gtex/GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt",
        "../input_files/auto/gtex/GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt",
        "../input_files/auto/gtex/gtex_version.tsv",
        "../input_files/auto/mondo/mondo.json",
        "../input_files/auto/uberon/uberon.obo",
        "../input_files/auto/hpa/normal_ihc_data.tsv.zip",
        "../input_files/auto/hpa/rna_tissue_hpa.tsv.zip",
        "../input_files/auto/hpa/hpa_version.tsv",
        "../input_files/auto/jensenlab/human_tissue_integrated_full.tsv",
        "../input_files/auto/jensenlab/tissues_version.tsv",
        "../input_files/auto/wikipathways/wikipathways_human.gmt",
        "../input_files/auto/wikipathways/wikipathways_version.tsv",
        "../input_files/auto/disease_ontology/doid.json",
        "../input_files/auto/panther/Protein_Class_19.0",
        "../input_files/auto/panther/Protein_class_relationship",
        "../input_files/auto/panther/PTHR19.0_human",
        "../input_files/auto/panther/panther_classes_version.tsv"

rule download_ctd:
    output:
        "../input_files/auto/ctd/CTD_curated_genes_diseases.tsv.gz",
        "../input_files/auto/ctd/ctd_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/ctd
        url='https://ctdbase.org/reports/CTD_curated_genes_diseases.tsv.gz'
        curl -fL -o {output[0]} "$url"
        python3 -c "
import gzip
import re
import sys
from datetime import datetime

input_path, output_path = sys.argv[1], sys.argv[2]
report_created = None
with gzip.open(input_path, 'rt', encoding='utf-8', errors='replace') as handle:
    for line in handle:
        if line.startswith('# Report created:'):
            report_created = line.split(':', 1)[1].strip()
            break
        if not line.startswith('#'):
            break

if not report_created:
    raise SystemExit('Could not find CTD report creation date in header')

match = re.search(r'([A-Z][a-z]{{2}} [A-Z][a-z]{{2}} \\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}} [A-Z]{{3,4}} \\d{{4}})$', report_created)
if match is None:
    raise SystemExit(f'Could not parse CTD report creation date: {{report_created}}')

version_date = datetime.strptime(match.group(1), '%a %b %d %H:%M:%S %Z %Y').date().isoformat()
with open(output_path, 'w', encoding='utf-8') as out:
    out.write('version\\tversion_date\\n')
    out.write(version_date + '\\t' + version_date + '\\n')
" {output[0]} {output[1]}
        """

rule download_jensenlab_tissues:
    output:
        "../input_files/auto/jensenlab/human_tissue_integrated_full.tsv",
        "../input_files/auto/jensenlab/tissues_version.tsv"
    shell:
        """
        url='https://download.jensenlab.org/human_tissue_integrated_full.tsv'
        curl -fL -o {output[0]} "$url"
        last_modified=$(curl -fsI "$url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        python3 -c "import email.utils,sys; lm=sys.argv[1]; out=sys.argv[2]; dt=email.utils.parsedate_to_datetime(lm).date().isoformat(); open(out,'w').write('version_date\\n'+dt+'\\n')" "$last_modified" {output[1]}
        """

rule download_jensenlab_diseases:
    output:
        "../input_files/auto/jensenlab/human_disease_knowledge_filtered.tsv",
        "../input_files/auto/jensenlab/human_disease_experiments_filtered.tsv",
        "../input_files/auto/jensenlab/human_disease_textmining_filtered.tsv",
        "../input_files/auto/jensenlab/diseases_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/jensenlab
        knowledge_url='https://download.jensenlab.org/human_disease_knowledge_filtered.tsv'
        experiments_url='https://download.jensenlab.org/human_disease_experiments_filtered.tsv'
        textmining_url='https://download.jensenlab.org/human_disease_textmining_filtered.tsv'

        curl -fL -o {output[0]} "$knowledge_url"
        curl -fL -o {output[1]} "$experiments_url"
        curl -fL -o {output[2]} "$textmining_url"

        knowledge_lm=$(curl -fsI "$knowledge_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        experiments_lm=$(curl -fsI "$experiments_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        textmining_lm=$(curl -fsI "$textmining_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')

        python3 -c "import email.utils,sys; vals=[v for v in sys.argv[1:4] if v.strip()]; dates=[email.utils.parsedate_to_datetime(v).date().isoformat() for v in vals]; version_date=max(dates); open(sys.argv[4],'w').write('version\\tversion_date\\n\\t'+version_date+'\\n')" "$knowledge_lm" "$experiments_lm" "$textmining_lm" {output[3]}
        """

rule download_tinx:
    output:
        "../input_files/auto/jensenlab/human_textmining_mentions.tsv",
        "../input_files/auto/jensenlab/disease_textmining_mentions.tsv",
        "../input_files/auto/jensenlab/tinx_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/jensenlab
        protein_url='https://download.jensenlab.org/human_textmining_mentions.tsv'
        disease_url='https://download.jensenlab.org/disease_textmining_mentions.tsv'

        curl -fL -o {output[0]} "$protein_url"
        curl -fL -o {output[1]} "$disease_url"

        protein_lm=$(curl -fsI "$protein_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        disease_lm=$(curl -fsI "$disease_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')

        python3 -c "import email.utils,sys; vals=[v for v in sys.argv[1:3] if v.strip()]; dates=[email.utils.parsedate_to_datetime(v).date().isoformat() for v in vals]; version_date=max(dates); open(sys.argv[3],'w').write('version\\tversion_date\\n\\t'+version_date+'\\n')" "$protein_lm" "$disease_lm" {output[2]}
        """

rule download_ncbi_publications:
    output:
        "../input_files/auto/ncbi/gene2pubmed.gz",
        "../input_files/auto/ncbi/generifs_basic.gz",
        "../input_files/auto/ncbi/ncbi_publications_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/ncbi
        gene2pubmed_url='https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2pubmed.gz'
        generif_url='https://ftp.ncbi.nlm.nih.gov/gene/GeneRIF/generifs_basic.gz'
        curl -fL -o {output[0]} "$gene2pubmed_url"
        curl -fL -o {output[1]} "$generif_url"

        gene2pubmed_lm=$(curl -fsI "$gene2pubmed_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        generif_lm=$(curl -fsI "$generif_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        download_date=$(date -u +%F)

        python3 -c "import email.utils,sys; vals=[v for v in sys.argv[1:3] if v.strip()]; dates=[email.utils.parsedate_to_datetime(v).date().isoformat() for v in vals]; version_date=max(dates) if dates else ''; open(sys.argv[3],'w').write('version\\tversion_date\\tdownload_date\\n\\t'+version_date+'\\t'+sys.argv[4]+'\\n')" "$gene2pubmed_lm" "$generif_lm" {output[2]} "$download_date"
        """

rule download_tiga:
    output:
        "../input_files/auto/tiga/tiga_gene-trait_stats.tsv",
        "../input_files/auto/tiga/tiga_gene-trait_provenance.tsv",
        "../input_files/auto/tiga/tiga_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/tiga
        base_url='https://unmtid-dbs.net/download/TIGA'
        stats_url="$base_url/latest/tiga_gene-trait_stats.tsv"
        provenance_url="$base_url/latest/tiga_gene-trait_provenance.tsv"

        curl -fL -o {output[0]} "$stats_url"
        curl -fL -o {output[1]} "$provenance_url"

        stats_lm=$(curl -fsI "$stats_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        provenance_lm=$(curl -fsI "$provenance_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        download_date=$(date -u +%F)
        version=$(curl -fsL "$base_url/" | grep -Eo 'href="[0-9]{{8}}/"' | cut -d'"' -f2 | tr -d '/' | sort | tail -n1)

        python3 -c "import email.utils,sys; version,stats_lm,prov_lm,out_path,download_date=sys.argv[1:6]; vals=[v for v in (stats_lm, prov_lm) if v.strip()]; dates=[email.utils.parsedate_to_datetime(v).date().isoformat() for v in vals]; version_date=max(dates) if dates else ''; open(out_path,'w').write('version\\tversion_date\\tdownload_date\\n'+version+'\\t'+version_date+'\\t'+download_date+'\\n')" "$version" "$stats_lm" "$provenance_lm" {output[2]} "$download_date"
        """

rule download_uberon:
    output:
        "../input_files/auto/uberon/uberon.obo"
    shell:
        "curl -L -o {output} http://purl.obolibrary.org/obo/uberon.obo"

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
        "../input_files/auto/uniprot/uniprot-human.json.gz",
        "../input_files/auto/uniprot/uniprot-human-reviewed.json.gz",
        "../input_files/auto/uniprot/uniprot_version.tsv"
    shell:
        """
        curl -o {output[0]} 'https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=(*)+AND+(model_organism:9606)'
        curl -o {output[1]} 'https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=(*)+AND+(reviewed:true)+AND+(model_organism:9606)'
        headers=$(curl -fsSI 'https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=json&size=1&query=accession:P04637')
        release=$(printf '%s' "$headers" | awk -F': ' 'tolower($1)=="x-uniprot-release"{{gsub(/\r/,"",$2); print $2}}')
        release_date=$(printf '%s' "$headers" | awk -F': ' 'tolower($1)=="x-uniprot-release-date"{{gsub(/\r/,"",$2); print $2}}')
        printf 'version\tversion_date\n%s\t%s\n' "$release" "$release_date" > {output[2]}
        """


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
        "../input_files/auto/reactome/UniProt2Reactome_All_Levels.txt",
        "../input_files/auto/reactome/reactome.homo_sapiens.interactions.tab-delimited.txt",
        "../input_files/auto/reactome/reactome_version.tsv"
    shell:
        """
        curl -o {output[0]} https://reactome.org/download/current/ReactomePathways.gmt.zip
        curl -o {output[1]} https://reactome.org/download/current/ReactomePathwaysRelation.txt
        curl -o {output[2]} https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt
        curl -o {output[3]} https://reactome.org/download/current/interactors/reactome.homo_sapiens.interactions.tab-delimited.txt
        last_modified=$(curl -fsI https://reactome.org/download/current/interactors/reactome.homo_sapiens.interactions.tab-delimited.txt | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        version=$(curl -fs https://reactome.org/ContentService/data/database/version)
        python3 -c "import email.utils,sys; lm=sys.argv[1]; v=sys.argv[2].strip(); out=sys.argv[3]; dt=email.utils.parsedate_to_datetime(lm).date().isoformat(); open(out,'w').write('version\\tversion_date\\n'+v+'\\t'+dt+'\\n')" "$last_modified" "$version" {output[4]}
        """

rule download_bioplex:
    output:
        "../input_files/auto/bioplex/BioPlex_293T_Network_10K_Dec_2019.tsv",
        "../input_files/auto/bioplex/BioPlex_HCT116_Network_5.5K_Dec_2019.tsv",
        "../input_files/auto/bioplex/bioplex_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/bioplex
        p293t_url='https://bioplex.hms.harvard.edu/data/BioPlex_293T_Network_10K_Dec_2019.tsv'
        hct116_url='https://bioplex.hms.harvard.edu/data/BioPlex_HCT116_Network_5.5K_Dec_2019.tsv'

        curl -fL -o {output[0]} "$p293t_url"
        curl -fL -o {output[1]} "$hct116_url"

        p293t_lm=$(curl -fsI "$p293t_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        hct116_lm=$(curl -fsI "$hct116_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')

        python3 -c "import email.utils,sys; p293t_lm,hct116_lm,out=sys.argv[1:4]; p293t_dt=email.utils.parsedate_to_datetime(p293t_lm).date().isoformat(); hct116_dt=email.utils.parsedate_to_datetime(hct116_lm).date().isoformat(); open(out,'w').write('dataset\\tfile\\tversion\\tversion_date\\nBioPlex 3.0 293T\\tBioPlex_293T_Network_10K_Dec_2019.tsv\\t3.0\\t'+p293t_dt+'\\nBioPlex 3.0 HCT116\\tBioPlex_HCT116_Network_5.5K_Dec_2019.tsv\\t3.0\\t'+hct116_dt+'\\n')" "$p293t_lm" "$hct116_lm" {output[2]}
        """

rule download_string:
    output:
        "../input_files/auto/string/9606.protein.links.v12.0.txt.gz",
        "../input_files/auto/string/string_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/string
        url='https://stringdb-downloads.org/download/protein.links.v12.0/9606.protein.links.v12.0.txt.gz'
        curl -fL -o {output[0]} "$url"
        last_modified=$(curl -fsI "$url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        python3 -c "import email.utils,sys; lm=sys.argv[1]; out=sys.argv[2]; dt=email.utils.parsedate_to_datetime(lm).date().isoformat(); open(out,'w').write('version\\tversion_date\\n12.0\\t'+dt+'\\n')" "$last_modified" {output[1]}
        """

rule download_gtex:
    output:
        "../input_files/auto/gtex/GTEx_Analysis_2025_08_22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz",
        "../input_files/auto/gtex/GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt",
        "../input_files/auto/gtex/GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt",
        "../input_files/auto/gtex/gtex_version.tsv"
    shell:
        """
        expr_url='https://storage.googleapis.com/adult-gtex/bulk-gex/v11/rna-seq/GTEx_Analysis_2025-08-22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz'
        sample_url='https://storage.googleapis.com/adult-gtex/annotations/v11/metadata-files/GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt'
        subject_url='https://storage.googleapis.com/adult-gtex/annotations/v11/metadata-files/GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt'

        curl -fL -o {output[0]} "$expr_url"
        curl -fL -o {output[1]} "$sample_url"
        curl -fL -o {output[2]} "$subject_url"

        printf 'version\tversion_date\nGTEx Analysis Version 11\t2025-08-22\n' > {output[3]}
        """

rule download_hpa:
    output:
        "../input_files/auto/hpa/normal_ihc_data.tsv.zip",
        "../input_files/auto/hpa/rna_tissue_hpa.tsv.zip",
        "../input_files/auto/hpa/hpa_version.tsv"
    shell:
        """
        curl -fL -o {output[0]} https://www.proteinatlas.org/download/tsv/normal_ihc_data.tsv.zip
        curl -fL -o {output[1]} https://www.proteinatlas.org/download/tsv/rna_tissue_hpa.tsv.zip
        last_modified=$(curl -fsI https://www.proteinatlas.org/download/tsv/rna_tissue_hpa.tsv.zip | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        version=$(curl -fs https://www.proteinatlas.org/about/download | python3 -c "import sys,re; m=re.search(r'version ([\d.]+)', sys.stdin.read()); print(m.group(1) if m else '')")
        python3 -c "import email.utils,sys; lm=sys.argv[1]; v=sys.argv[2]; out=sys.argv[3]; dt=email.utils.parsedate_to_datetime(lm).date().isoformat(); open(out,'w').write('version\\tversion_date\\n'+v+'\\t'+dt+'\\n')" "$last_modified" "$version" {output[2]}
        """

rule download_mondo:
    output:
        "../input_files/auto/mondo/mondo.json"
    shell:
        "curl -L -o {output} https://purl.obolibrary.org/obo/mondo.json"

rule download_disease_ontology:
    output:
        "../input_files/auto/disease_ontology/doid.json"
    shell:
        """
        mkdir -p ../input_files/auto/disease_ontology
        curl -L -o {output} https://purl.obolibrary.org/obo/doid.json
        """

rule download_pathwaycommons:
    output:
        "../input_files/auto/pathwaycommons/pc-hgnc.gmt.gz",
        "../input_files/auto/pathwaycommons/pathwaycommons_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/pathwaycommons
        curl -fL -o {output[0]} https://download.baderlab.org/PathwayCommons/PC2/v14/pc-hgnc.gmt.gz
        curl -fs https://download.baderlab.org/PathwayCommons/PC2/v14/datasources.txt | python3 -c "import sys,re; from datetime import datetime; data=sys.stdin.read(); m=re.search(r'PC version (\d+) (\d+ \w+ \d+)',data); v=m.group(1); dt=datetime.strptime(m.group(2),'%d %b %Y').date().isoformat(); open('{output[1]}','w').write('version\\tversion_date\\n'+v+'\\t'+dt+'\\n')"
        """

rule download_panther_classes:
    output:
        "../input_files/auto/panther/Protein_Class_19.0",
        "../input_files/auto/panther/Protein_class_relationship",
        "../input_files/auto/panther/PTHR19.0_human",
        "../input_files/auto/panther/panther_classes_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/panther
        class_url='https://data.pantherdb.org/PANTHER19.0/ontology/Protein_Class_19.0'
        rel_url='https://data.pantherdb.org/PANTHER19.0/ontology/Protein_class_relationship'
        seq_url='https://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/PTHR19.0_human'

        curl -fL -o {output[0]} "$class_url"
        curl -fL -o {output[1]} "$rel_url"
        curl -fL -o {output[2]} "$seq_url"

        class_lm=$(curl -fsI "$class_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        rel_lm=$(curl -fsI "$rel_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        seq_lm=$(curl -fsI "$seq_url" | awk -F': ' 'tolower($1)=="last-modified"{{print $2}}')
        download_date=$(date -u +%F)
        python3 -c "import email.utils,sys; vals=[v for v in sys.argv[1:4] if v.strip()]; dates=[email.utils.parsedate_to_datetime(v).date().isoformat() for v in vals]; version_date=max(dates) if dates else ''; version='19.0'; open(sys.argv[4],'w').write('version\\tversion_date\\tdownload_date\\n'+version+'\\t'+version_date+'\\t'+sys.argv[5]+'\\n')" "$class_lm" "$rel_lm" "$seq_lm" {output[3]} "$download_date"
        """

rule download_wikipathways:
    output:
        "../input_files/auto/wikipathways/wikipathways_human.gmt",
        "../input_files/auto/wikipathways/wikipathways_version.tsv"
    shell:
        """
        mkdir -p ../input_files/auto/wikipathways
        url=$(python3 -c "
import re, urllib.request
html = urllib.request.urlopen('https://data.wikipathways.org/current/gmt/').read().decode()
match = re.search(r'(wikipathways-(\d{{8}})-gmt-Homo_sapiens\.gmt)', html)
if not match: raise SystemExit('Could not find WikiPathways GMT file')
print('https://data.wikipathways.org/current/gmt/' + match.group(1))
")
        date_str=$(python3 -c "
import re, urllib.request
html = urllib.request.urlopen('https://data.wikipathways.org/current/gmt/').read().decode()
match = re.search(r'wikipathways-(\d{{4}})(\d{{2}})(\d{{2}})-gmt-Homo_sapiens\.gmt', html)
if not match: raise SystemExit('Could not parse date')
y,m,d = match.groups()
print(f'{{y}}-{{m}}-{{d}}')
")
        curl -fL -o {output[0]} "$url"
        printf 'version\tversion_date\n%s\t%s\n' "$date_str" "$date_str" > {output[1]}
        """
