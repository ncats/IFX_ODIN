
"""
main.py - CLI wrapper to orchestrate all modular source pipelines
"""
import argparse
import yaml
from pathlib import Path

# TARGETS category imports
from publicdata.target_data.ensembl_download import EnsemblDownloader
from publicdata.target_data.ensembl_transform import EnsemblTransformer
from publicdata.target_data.ensembl_uniprot_isoform_xref import EnsemblUniProtIsoformXref
from publicdata.target_data.ncbi_download import NCBIDownloader
from publicdata.target_data.ncbi_transform import NCBITransformer
from publicdata.target_data.hgnc_download import HGNCDownloader
from publicdata.target_data.hgnc_transform import HGNCTransformer
from publicdata.target_data.refseq_download import RefSeqDownloader
from publicdata.target_data.refseq_transform import RefSeqTransformer
from publicdata.target_data.uniprot_download import UniprotDownloader
from publicdata.target_data.uniprot_transform import UniProtTransformer
from publicdata.target_data.nodenorm_gene_download import NodeNormGeneDownloader
from publicdata.target_data.nodenorm_gene_transform import NodeNormGeneTransformer
from publicdata.target_data.nodenorm_protein_download import NodeNormProteinDownloader
from publicdata.target_data.nodenorm_protein_transform import NodeNormProteinTransformer
from publicdata.target_data.gene_merge import GENEDataMerger
from publicdata.target_data.gene_ids import GeneDataProcessor
from publicdata.target_data.transcript_merge import TranscriptResolver
from publicdata.target_data.transcript_ids import TranscriptDataProcessor
from publicdata.target_data.protein_merge import ProteinResolver
from publicdata.target_data.protein_ids import ProteinDataProcessor
from publicdata.target_data.antibodypedia_scraper import AntibodypediaScraper

# DISEASES category imports
from publicdata.disease_data.mondo_download import MondoDownloader
from publicdata.disease_data.mondo_transform import MondoTransformer
from publicdata.disease_data.doid_download import DOIDDownloader
from publicdata.disease_data.doid_transform import DOIDTransformer
from publicdata.disease_data.medgen_download import MedGenDownloader
from publicdata.disease_data.medgen_transform import MedGenTransformer
from publicdata.disease_data.orphanet_download import OrphanetDownloader
from publicdata.disease_data.orphanet_transform import OrphanetTransformer
from publicdata.disease_data.omim_download import OMIMDownloader
from publicdata.disease_data.omim_transform import OMIMTransformer
from publicdata.disease_data.umls_download import UMLSDownloader  
#from publicdata.disease_data.umls_transform import UMLSTransformer 
from publicdata.disease_data.nodenorm_disease_download import NodeNormDiseaseDownloader
from publicdata.disease_data.nodenorm_disease_transform import NodeNormDiseaseTransformer
from publicdata.disease_data.disease_name_cluster import DiseaseNameClusterer
#from publicdata.disease_data.disgenet_download import DisgenetDownloader
#from publicdata.disease_data.disgenet_transform import DisgenetTransformer
from publicdata.disease_data.disease_merge import DiseaseDataMerger
from publicdata.disease_data.jensen_download import JensenDiseaseDownloader
from publicdata.disease_data.jensen_transform import JensenDiseaseTransformer

#DRUGS category imports
from publicdata.drug_data.gsrs_download import GSRSDownloader
from publicdata.drug_data.gsrs_transform import GSRSTransformer

#GO category imports
from publicdata.GO_data.GO_download import GODownloader
from publicdata.GO_data.GO_transform import GOTransformer

#PPI category imports
from publicdata.PPI_data.string_download import StringPPIDownloader
from publicdata.PPI_data.string_transform import StringPPITransformer

#PHENOTYPES category imports
from publicdata.phenotype_data.hpo_download import HPOPhenotypeDownloader
from publicdata.phenotype_data.hpo_transform import HPOPhenotypeTransformer 

#PATHWAYS category imports
from publicdata.pathway_data.pathwaycommons_download import PathwayCommonsDownloader
from publicdata.pathway_data.pathwaycommons_transform import PathwayCommonsTransformer
from publicdata.pathway_data.panther_download import PantherDownloader
from publicdata.pathway_data.panther_transform import PantherTransformer
from publicdata.pathway_data.reactome_download import ReactomeDownloader
from publicdata.pathway_data.reactome_transform import ReactomeTransformer
from publicdata.pathway_data.wikipathway_download import WikiPathwaysDownloader
from publicdata.pathway_data.wikipathway_transform import WikiPathwaysTransformer
from publicdata.pathway_data.pathways_merge import PathwayMergerTransformer
from publicdata.pathway_data.pathway_ids import PathwayIDGenerator




# Default config paths by category
DEFAULT_CONFIGS = {
    "TARGETS": "config/targets_config.yaml",
    "DISEASES": "config/diseases_config.yaml",
    "DRUGS": "config/drugs_config.yaml",
    "GO": "config/GO_config.yaml",
    "PPI": "config/ppi_config.yaml",
    "PHENOTYPES": "config/phenotypes_config.yaml",
    "PATHWAYS": "config/pathways_config.yaml"
}

def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

#
# Always pass the full config dict to each processor.
#
PROCESSOR_MAP = {
    # TARGETS
    "ensembl_download":        (EnsemblDownloader,        None),
    "ensembl_transform":       (EnsemblTransformer,       None),
    "ensembl_isoform":         (EnsemblUniProtIsoformXref, None),
    "ncbi_download":           (NCBIDownloader,           None),
    "ncbi_transform":          (NCBITransformer,          None),
    "hgnc_download":           (HGNCDownloader,           None),
    "hgnc_transform":          (HGNCTransformer,          None),
    "refseq_download":         (RefSeqDownloader,         None),
    "refseq_transform":        (RefSeqTransformer,        None),
    "uniprot_download":        (UniprotDownloader,        None),
    "uniprot_transform":       (UniProtTransformer,       None),
    "nodenorm_gene_download":     (NodeNormGeneDownloader,    None),
    "nodenorm_gene_transform":    (NodeNormGeneTransformer,   None),
    "nodenorm_protein_download":  (NodeNormProteinDownloader, None),
    "nodenorm_protein_transform": (NodeNormProteinTransformer,None),
    "gene_merge":              (GENEDataMerger,           None),
    "gene_ids":                (GeneDataProcessor,        None),
    "transcript_merge":        (TranscriptResolver,       None),
    "transcript_ids":          (TranscriptDataProcessor,  None),
    "protein_merge":           (ProteinResolver,          None),
    "protein_ids":             (ProteinDataProcessor,     None),
    "antibodypedia":           (AntibodypediaScraper, None),

    # DISEASES
    "mondo_download":          (MondoDownloader,          None),
    "mondo_transform":         (MondoTransformer,         None),
    "doid_download":           (DOIDDownloader,          None),
    "doid_transform":          (DOIDTransformer,         None), 
    "medgen_download":         (MedGenDownloader,         None),
    "medgen_transform":        (MedGenTransformer,        None),
    "orphanet_download":       (OrphanetDownloader,       None),
    "orphanet_transform":      (OrphanetTransformer,      None),
    "omim_download":           (OMIMDownloader,           None),
    "omim_transform":          (OMIMTransformer,          None),
    "umls_download":           (UMLSDownloader,           None),  
    #"umls_transform":          (UMLSTransformer,          None),  
    "nodenorm_disease_download":  (NodeNormDiseaseDownloader, None),
    "nodenorm_disease_transform": (NodeNormDiseaseTransformer, None),
    "disease_name_cluster":      (DiseaseNameClusterer, None),  
   # "disgenet_download":       (DisgenetDownloader,       None), Need license to use DisGeNET data
   # "disgenet_transform":      (DisgenetTransformer,      None),
    "disease_merge":           (DiseaseDataMerger,        None),
    "jensen_download":         (JensenDiseaseDownloader,         None),
    "jensen_transform":        (JensenDiseaseTransformer,        None),

    # DRUGS
    "gsrs_download": (GSRSDownloader, None),
    "gsrs_transform": (GSRSTransformer, None),

    #GO
    "go_download": (GODownloader, None),
    "go_transform": (GOTransformer, None), 

    #PPI
    "string_download": (StringPPIDownloader, None),
    "string_transform": (StringPPITransformer, None),

    #PHENOTYPES
    "hpo_download": (HPOPhenotypeDownloader, None),
    "hpo_transform": (HPOPhenotypeTransformer, None),

    #PATHWAYS
    "pathwaycommons_download": (PathwayCommonsDownloader, None),  
    "pathwaycommons_transform": (PathwayCommonsTransformer, None),
    "panther_download": (PantherDownloader, None),  
    "panther_transform": (PantherTransformer, None),
    "reactome_download": (ReactomeDownloader, None),
    "reactome_transform": (ReactomeTransformer, None),
    "wikipathway_download": (WikiPathwaysDownloader, None),
    "wikipathway_transform": (WikiPathwaysTransformer, None),
    "pathways_merge": (PathwayMergerTransformer, None),
    "pathway_ids": (PathwayIDGenerator, None),
} 

def run_selected_processors(selected, config):
    for key in selected:
        if key not in PROCESSOR_MAP:
            print(f"⚠️  Unknown module: {key}")
            continue

        cls_or_fn, section = PROCESSOR_MAP[key]
        cfg_to_pass = config if section is None else config.get(section, {})

        print(f"➤ Running {key}")
        instance = cls_or_fn(config) if section is None else cls_or_fn(cfg_to_pass)

        if hasattr(instance, "run"):
            instance.run()
        elif hasattr(instance, "process"):
            instance.process()
        elif callable(instance):
            instance()

def main():
    parser = argparse.ArgumentParser(description="Run modular ETL processors from config")
    parser.add_argument("category",
                        choices=["TARGETS", "DISEASES", "DRUGS", "GO", "PPI", "PHENOTYPES", "PATHWAYS"],
                        help="Data category to process")
    parser.add_argument("--config", type=str,
                        help="Path to YAML config file. Defaults per category.")
    parser.add_argument("--all", action="store_true",
                        help="Run all modules in the specified category")
    parser.add_argument("--modules", nargs="+",
                        help="Run specific modules by name")

    # Auto-generate --{processor_name} flags
    for name in PROCESSOR_MAP:
        parser.add_argument(f"--{name}", action="store_true", help=f"Run only `{name}`")

    args = parser.parse_args()

    # Choose config path
    cfg_path = Path(args.config) if args.config else Path(DEFAULT_CONFIGS[args.category])
    if not cfg_path.exists():
        raise FileNotFoundError(f"❌ Config file not found: {cfg_path}")
    config = load_config(cfg_path)

    # Map category to known prefixes
    CATEGORY_MODULE_PREFIXES = {
        "TARGETS": [
            "ensembl_", "ncbi_", "hgnc_", "refseq_", "uniprot_",
            "nodenorm_gene_", "nodenorm_protein_", "gene_", "transcript_", "protein_"],
        "DISEASES": [
            "mondo_", "doid_", "medgen_", "orphanet_", "omim_", "umls",
            "nodenorm_disease_", "disease_", "jensen_"],
        "DRUGS": ["gsrs_"],
        "GO": ["go_"],
        "PPI": ["string_"],
        "PHENOTYPES": ["hpo_"],
        "PATHWAYS": ["pathwaycommons_", "panther", "reactome_", "wikipathway_"],  
    }

    # Determine which processors to run
    if args.all:
        prefixes = CATEGORY_MODULE_PREFIXES.get(args.category.upper(), [])
        to_run = [k for k in PROCESSOR_MAP if any(k.startswith(p) for p in prefixes)]
    elif args.modules:
        to_run = args.modules
    else:
        to_run = [name for name in PROCESSOR_MAP if getattr(args, name)]
        if not to_run:
            parser.error("Must specify --all, --modules, or one of the step flags")

    run_selected_processors(to_run, config)

if __name__ == "__main__":
    main()
