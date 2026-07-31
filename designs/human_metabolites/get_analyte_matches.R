if (!require("BiocManager", quietly = TRUE))
  install.packages("BiocManager")
BiocManager::install(version = "3.23")

#library 
library(devtools)
library(roxygen2)
library(RaMP)
library(tidyverse)
library(BiocManager)
library(rols)

create_package("C:/Users/kunkeelt/OneDrive - National Institutes of Health/Documents/project_4/R_pack_pathway_guided_targeted_msms/R_pack_pathway_guided_targeted_msms")

#documentation for individual functions use Roxygen2

#analyte matches ---------------------------
#' @description
#' Finds a list of analytes that are associated through pathway analysis from
#' characterized metabolites across both positive and negative ionization modes,
#' and whose theoretical masses match those within the provided MS1 data.
#'
#' @details
#' Characterized metabolites from both positive and negative mode data frames are
#' combined and used as input for RaMP pathway analysis (`getPathwayFromAnalyte`)
#' and reverse pathway analysis (`getAnalyteFromPathway`) to find pathway-associated
#' analytes not yet characterized in either mode. Chemical classification
#' (`getChemClass`) is used to filter analytes by metabolite type, and chemical
#' properties (`getChemicalProperties`) are retrieved to calculate theoretical m/z
#' values for each analyte-adduct combination. These theoretical masses are then
#' matched independently against uncharacterized features in the positive mode data
#' and the negative mode data, returning two separate hit data frames.
#'
#' @param dataframe_POS A data frame of positive ionization mode MS1 data containing
#'   at least the columns `mz`, `rt`, and `compound_id`. `compound_id` should be an
#'   HMDB or ChEBI identifier for characterized metabolites, or `NA` for uncharacterized
#'   features.
#' @param dataframe_NEG A data frame of negative ionization mode MS1 data containing
#'   at least the columns `mz`, `rt`, and `compound_id`. `compound_id` should be an
#'   HMDB or ChEBI identifier for characterized metabolites, or `NA` for uncharacterized
#'   features.
#' @param met_type Character string specifying the type of metabolites to search for.
#'   Must be either `"polar"` (non-lipid metabolites) or `"lipids"` (lipid metabolites).
#' @param adduct_list_POS Character vector of positive mode adducts to test against
#'   theoretical masses when matching to uncharacterized features in `dataframe_POS`.
#'   Available options: `"[M+H]"`, `"[M+NH4]"`, `"[M+H2O+H]"`, `"[M-H2O+H]"`,
#'   `"[M+Na]"`. `"[2M+Na]"`, `"[3M+Na]"` `"[M+K]"`, `"[M+DMSO+H]"`, `"[M+Li]"`, `"[M+CH3CN+H]"`, 
#'   `"[M+CH3OH+H]"`, `"[2M+H]"`, `"[3M+H]"`, `"[M+2H]"`
#' @param adduct_list_NEG Character vector of negative mode adducts to test against
#'   theoretical masses when matching to uncharacterized features in `dataframe_NEG`.
#'   Available options: `"[M-H]"`, `"[M+Cl]"`, `"[M+H2O-H]"`, `"[M-H2O-H]"`, 
#'   `"[M+FA-H]"`, `"[M+CH3COO]"`, `"[M+Br]"`, `"[M+NO3]"`, `"[M+CH3OH-H]"`, `"[M+CH3CN-H]"`,
#'  `"[2M-H]"`, `"[3M-H]"`, `"[M-2H]"`
#' @param mz_tolerance Numeric. The mass tolerance (in Da) used when matching theoretical
#'   m/z values to observed m/z values in the uncharacterized feature lists.
#'
#' @return A named list with two elements:
#' \describe{
#'   \item{`hits_POS`}{A data frame of uncharacterized positive mode features whose
#'     observed m/z matched a pathway-associated analyte plus adduct. Contains columns
#'     `mz`, `rt`, `compound_id`, and `adduct`.}
#'   \item{`hits_NEG`}{A data frame of uncharacterized negative mode features whose
#'     observed m/z matched a pathway-associated analyte plus adduct. Contains columns
#'     `mz`, `rt`, `compound_id`, and `adduct`.}
#' }
#'
#' @examples
#' \dontrun{
#' results <- GetAnalyteMatches(
#'   dataframe_POS  = ms_list_pos,
#'   dataframe_NEG  = ms_list_neg,
#'   met_type       = "polar",
#'   adduct_list_POS = c("[M+H]", "[M+Na]"),
#'   adduct_list_NEG = c("[M-H]", "[M+Cl]"),
#'   mz_tolerance   = 0.01
#' )
#'
#' results$hits_POS
#' results$hits_NEG
#' }
#'
#' @export
#' @importFrom dplyr %>% mutate filter distinct if_else union
#' @importFrom stringr str_starts str_extract
get_analyte_matches <- function(dataframe_POS, 
                               dataframe_NEG, 
                               met_type, 
                               adduct_list_POS, 
                               adduct_list_NEG, 
                               mz_tolerance) {
  rampDB <- RaMP()
  
  
  required_cols <- c("mz", "rt", "compound_id")
  
  if (!all(required_cols %in% names(dataframe_POS)))
    stop("`dataframe_POS` must contain columns: mz, rt, compound_id")
  if (!all(required_cols %in% names(dataframe_NEG)))
    stop("`dataframe_NEG` must contain columns: mz, rt, compound_id")
  
  ms_list_POS <- dataframe_POS %>% mutate(ionization = "POS")
  ms_list_NEG <- dataframe_NEG %>% mutate(ionization = "NEG")
  
  # split into characterized and uncharacterized 
  char_POS   <- ms_list_POS%>%
    filter(!is.na(compound_id)) %>%
    mutate(compound_id = if_else(
      str_starts(compound_id, "HMDB"),
      paste0("hmdb:", compound_id),
      compound_id))
  char_NEG   <- ms_list_NEG%>%
    filter(!is.na(compound_id)) %>%
    mutate(compound_id = if_else(
      str_starts(compound_id, "HMDB"),
      paste0("hmdb:", compound_id),
      compound_id))
  unchar_POS <- ms_list_POS %>% filter(is.na(compound_id))
  unchar_NEG <- ms_list_NEG %>% filter(is.na(compound_id))
  
  #combine both modes for pathway analysis 
  analyte_list <- union(char_POS$compound_id, char_NEG$compound_id)
  
  
  # get pathways on all characterized metabolites 
  pathway_df <- getPathwayFromAnalyte(
    analytes       = analyte_list,
    namesOrIds     = "ids",
    minPathwaySize = 2,
    maxPathwaySize = 150,
    db             = rampDB
  ) %>%
    filter(pathwaySource != "pfocr") %>%
    #filter(pathwaySource != "smpdb")
  
  #reverse pathway analysis on all found pathways ------------------------------
  analyte_df <- getAnalyteFromPathway(
    pathway = pathway_df$pathwayName,
    analyteType = "metabolite",
    #maybe add a max pathway size (200?)
    db      = rampDB
  ) %>%
    filter(!analyteName %in% c("Water", "Carbon dioxide")) %>%  #try to expand this 
    mutate(
      chebi_id         = str_extract(sourceAnalyteIDs, "chebi:[^,]*"),
      hmdb_id          = str_extract(sourceAnalyteIDs, "hmdb:[^,]*"),
      #use lipidmaps IDS for the lipids 
      sourceAnalyteIDs = if_else(!is.na(chebi_id), chebi_id, hmdb_id)  #debug with lipids  
    ) %>%
    distinct(sourceAnalyteIDs, .keep_all = TRUE) %>%
    filter(!is.na(sourceAnalyteIDs)) %>%
    filter(!sourceAnalyteIDs %in% analyte_list)   # remove already-characterized
  
  new_analyte_list <- analyte_df$sourceAnalyteIDs
  
  #filter by metabolites type --------------------------------------------------
  met_list_classification <- getChemClass(mets = new_analyte_list,
                                          inferIdMapping = TRUE)
  
  lipid_mets <- met_list_classification$met_classes %>%
    filter(
      (
        class_level_name == "ClassyFire_super_class" &
          class_name       == "Lipids and lipid-like molecules"
      ) |
        source == "lipidmaps"
    )
  
  
  if (met_type == "polar") {
    new_analyte_list <- new_analyte_list[!new_analyte_list %in% lipid_mets$sourceId]
  } else if (met_type == "lipids") {
    new_analyte_list <- new_analyte_list[new_analyte_list %in% lipid_mets$sourceId]
  } else {
    stop("`met_type` must be either 'polar' or 'lipids'")
  }
  
  
  #filter out exogenous metabolites by referencing drug database------------------------

  #define the exclusion terms 
  
  #chebi ontology roots that re,late to xiobiotic or exogenous 
  exogenous_root_chebi <- c(
    "CHEBI:35703",   # xenobiotic
    "CHEBI:23888",   # drug
    "CHEBI:35610",   # pharmaceutical
    "CHEBI:33281"    # antimicrobial agent
  )
  
  #defining non-permissive drug databases - mostly phramaceutical drugs and not endogenous 
  drug_reference_db <- c(
    "drugbank", "drugcentral", "rxnorm", "dailymed", "ttd", "pharmgkb" 
  )
  
  exclude_terms <- c(
    "drug", "pharmaceutical", "xenobiotic", "antibiotic", "antimicrobial",
    "pesticide", "herbicide", "fungicide", "pollutant", "contaminant",
    "toxin", "synthetic", "illicit", "narcotic", "psychoactive",
    "food additive", "preservative", "dye", "solvent"
  )
  
  
  
  if (met_type == "polar") {
    
    chebi_ids  <- new_analyte_list[grepl("^chebi:", new_analyte_list, ignore.case = TRUE)]
    hmdb_only  <- new_analyte_list[!grepl("^chebi:", new_analyte_list, ignore.case = TRUE)]
    
    message("Running exogenous filter on ", length(chebi_ids), " ChEBI IDs …")
    ontology_keep <- vapply(chebi_ids, exclusion_term_check, logical(1))
    
    removed <- chebi_ids[!ontology_keep]
    if (length(removed) > 0) {
      message(length(removed), " metabolite(s) removed:\n  ",
              paste(removed, collapse = "\n  "))
    } else {
      message("No metabolites removed.")
    }
    
    new_analyte_list <- c(chebi_ids[ontology_keep], hmdb_only)
  }
  
  
  if (met_type == "lipid") {
    
    lipid_map_ids <- new_analyte_list[grepl("^chebi:", new_analyte_list, ignore.case = TRUE)] #change for lipidmaps 
    other         <- new_analyte_list[!grepl("^chebi:", new_analyte_list, ignore.case = TRUE)]
    
    message("Running exogenous filter on ", length(lipid_map_ids), " ChEBI IDs …")
    ontology_keep <- vapply(lipid_map_ids, exclusion_term_check_lipid, logical(1))
    
    removed <- lipid_map_ids[!ontology_keep]
    if (length(removed) > 0) {
      message(length(removed), " metabolite(s) removed:\n  ",
              paste(removed, collapse = "\n  "))
    } else {
      message("No metabolites removed.")
    }
    
    new_analyte_list <- c(lipid_map_ids[ontology_keep], other)
  }
  
  
  # get theoretical mass -------------------------------------------------------
  met_properties   <- getChemicalProperties(new_analyte_list, propertyList = "all", db = rampDB)
  new_analyte_list <- new_analyte_list[!new_analyte_list %in% met_properties$query_report$missed_query_elements]
  
  #excluded_terms <- met_properties$query_report$missed_query_elements

  new_analyte_df <- data.frame(compound_id = new_analyte_list, stringsAsFactors = FALSE) %>%
    left_join(
      met_properties$chem_props %>% select(chem_source_id, monoisotop_mass),
      by = c("compound_id" = "chem_source_id")
    ) %>%
    rename(monoisotopic_mass = monoisotop_mass)

  
  #look up adducts and mass -----------------------------------------------------
 
  adduct_lookup <- data.frame(
    name                     = c("[M+H]", "[M+NH4]", "[M+H2O+H]", "[M-H2O+H]", "[M+Na]", "[2M+Na]", "[3M+Na]", "[M+K]", "[M+Li]", "[M+CH3CN+H]", "[M+CH3OH+H]", "[M+DMSO+H]", "[2M+H]", "[3M+H]", "[M+2H]",
                                 "[M-H]", "[M+Cl]", "[M+H2O-H]", "[M-H2O-H]", "[M+FA-H]", "[M+CH3COO]", "[M+Br]", "[M+NO3]", "[M+CH3OH-H]", "[M+CH3CN-H]", "[2M-H]", "[3M-H]", "[M-2H]"),
    adduct_monoisotopic_mass = c(
      #  [M+H]        [M+NH4]      [M+H2O+H]    [M-H2O+H]    [M+Na]
      1.0078,       18.0344,      19.0184,     -17.0027,    22.9897,
      #  [2M+Na]       [3M+Na]       [M+K]         [M+Li]      [M+CH3CN+H]
      22.9897,      22.9897,      38.9632,       7.0160,    42.0338,
      #  [M+CH3OH+H]  [M+DMSO+H]   [2M+H]        [3M+H]       [M+2H]
      33.0340,      79.0211,      1.0078,        1.0078,     1.0078,
      #  [M-H]         [M+Cl]      [M+H2O-H]    [M-H2O-H]    [M+FA-H]
      -1.0078,       34.9694,     17.0027,     -19.0183,    44.9977,
      #  [M+CH3COO]    [M+Br]      [M+NO3]      [M+CH3OH-H]  [M+CH3CN-H]
      59.0133,      78.9183,     61.9880,      31.0184,    40.0187,
      #  [2M-H]        [3M-H]      [M-2H]
      -1.0078,       -1.0078,    -1.0078
    ),
    charge                   = c(
      #  [M+H]  [M+NH4]  [M+H2O+H]  [M-H2O+H]  [M+Na]  [2M+Na]  [3M+Na]  [M+K]  [M+Li]  [M+CH3CN+H]  [M+CH3OH+H]  [M+DMSO+H]  [2M+H]  [3M+H]  [M+2H]
      1,      1,        1,          1,         1,      1,       1,       1,     1,       1,           1,           1,          1,      1,      2,
      #  [M-H]  [M+Cl]  [M+H2O-H]  [M-H2O-H]  [M+FA-H]  [M+CH3COO]  [M+Br]  [M+NO3]  [M+CH3OH-H]  [M+CH3CN-H]  [2M-H]  [3M-H]  [M-2H]
       1,     1,       1,         1,         1,         1,          1,      1,        1,            1,          1,     1,     2
    ),
    ionization               = c(
      #  [M+H]   [M+NH4]  [M+H2O+H]  [M-H2O+H]  [M+Na]  [2M+Na]  [3M+Na]  [M+K]   [M+Li]  [M+CH3CN+H]  [M+CH3OH+H]  [M+DMSO+H]  [2M+H]  [3M+H]  [M+2H]
      "POS",  "POS",   "POS",     "POS",     "POS",  "POS",   "POS",   "POS",  "POS",  "POS",       "POS",       "POS",      "POS",  "POS",  "POS",
      #  [M-H]   [M+Cl]   [M+H2O-H]  [M-H2O-H]  [M+FA-H]  [M+CH3COO]  [M+Br]   [M+NO3]  [M+CH3OH-H]  [M+CH3CN-H]  [2M-H]   [3M-H]   [M-2H]
      "NEG",  "NEG",   "NEG",     "NEG",     "NEG",    "NEG",      "NEG",   "NEG",   "NEG",       "NEG",       "NEG",   "NEG",   "NEG"
    ),
    stringsAsFactors = FALSE
  )
  
  adduct_df_POS <- get_adduct_df(adduct_list_POS, "POS")
  adduct_df_NEG <- get_adduct_df(adduct_list_NEG, "NEG")
  
  # compute theoretical mass + adduct and match ---------------------------------
  
  #match seperatly for POS and NEG 
  hits_df_POS <- match_analytes(unchar_POS, new_analyte_df, adduct_df_POS)
  hits_df_NEG <- match_analytes(unchar_NEG, new_analyte_df, adduct_df_NEG)
  
  # return both results --------------------------------------------------------
  list(
    hits_POS = hits_df_POS,
    hits_NEG = hits_df_NEG
  )
}



####test---------------------------------------------------------------------------
step_1_results <- get_analyte_matches(usher_polar_pos, usher_polar_neg,
                                      met_type = "polar",
                                      adduct_list_POS =  c("[M+H]", "[M+NH4]", "[M+H2O+H]", "[M-H2O+H]","[M+Na]"),
                                      adduct_list_NEG =  c("[M-H]", "[M+Cl]", "[M+H2O-H]","[M-H2O-H]"),
                                      mz_tolerance = 0.01)
