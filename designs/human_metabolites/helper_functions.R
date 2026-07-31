#exclusion_term_check function for get_analyte_matches()------------------------------------------

  #exclusion hierarchy 
  #   Tier 1 :drug xref found - exclude  
  #   Tier 2 :ontology ancestor match - exclude 
  #   Tier 3 : no xref hits; fall through to label matching - keep 
  
  exclusion_term_check_polar <- function(chebi_id_str) {
    
    
    chebi_num     <- sub("chebi:", "", chebi_id_str, ignore.case = TRUE)
    chebi_term_id <- paste0("CHEBI:", chebi_num)
    
    term <- tryCatch(rols::olsTerm("chebi", chebi_term_id), error = function(e) NULL)
    
    if (is.null(term)) {
      message("OLS returned NULL for ", chebi_term_id, " — keeping.")
      return(TRUE)
    }
    
    # check the xref databse
    xrefs        <- tryCatch(term@annotation[["database_cross_reference"]], error = function(e) NULL)
    xref_strings <- tolower(unlist(xrefs))
    has_drug_xref <- any(sapply(drug_reference_db, function(p) any(startsWith(xref_strings, p))))
    
    if (has_drug_xref) {
      message("EXCLUDE (xref): ", chebi_term_id)
      return(FALSE)
    }
    
    # check ancestors 
    anc_ids <- tryCatch({
      term      <- rols::olsTerm("chebi", chebi_term_id)
      anc_terms <- rols::ancestors(term)
      if (is.null(anc_terms) || length(anc_terms) == 0) return(character(0))
      sapply(anc_terms@x, function(t) t@obo_id)
    }, error = function(e) character(0))
    has_exo_ancestor <- any(exogenous_root_chebi %in% anc_ids)
    
    if (has_exo_ancestor) {
      message("EXCLUDE (ancestor): ", chebi_term_id)
      return(FALSE)
    }
    
    # check lables
    all_labels <- tryCatch({
      unique(c(
        term@label,
        unlist(term@synonyms),
        unlist(term@annotation[["has_related_synonym"]]),
        unlist(term@annotation[["has_exact_synonym"]]),
        unlist(term@description)
      ))
    }, error = function(e) character(0))
    
    label_match <- length(all_labels) > 0 &&
      any(sapply(exclude_terms, function(trm) any(grepl(trm, all_labels, ignore.case = TRUE))))
    
    if (label_match) {
      message("EXCLUDE (label): ", chebi_term_id)
      return(FALSE)
    }
    
    TRUE  # passed all checks — keep
    
  }
  
  #need one for lipids 
  exclusion_term_check_lipid <- function(chebi_id_str) {
    
    #need to change and optimize for lipidmaps names and drugs - might need an entirely seperate exclusion term list 
    
    
    chebi_num     <- sub("chebi:", "", chebi_id_str, ignore.case = TRUE)
    chebi_term_id <- paste0("CHEBI:", chebi_num)
    
    term <- tryCatch(rols::olsTerm("chebi", chebi_term_id), error = function(e) NULL)
    
    if (is.null(term)) {
      message("OLS returned NULL for ", chebi_term_id, " — keeping.")
      return(TRUE)
    }
    
    # check the xref databse
    xrefs        <- tryCatch(term@annotation[["database_cross_reference"]], error = function(e) NULL)
    xref_strings <- tolower(unlist(xrefs))
    has_drug_xref <- any(sapply(drug_reference_db, function(p) any(startsWith(xref_strings, p))))
    
    if (has_drug_xref) {
      message("EXCLUDE (xref): ", chebi_term_id)
      return(FALSE)
    }
    
    # check ancestors 
    anc_ids <- tryCatch({
      term      <- rols::olsTerm("chebi", chebi_term_id)
      anc_terms <- rols::ancestors(term)
      if (is.null(anc_terms) || length(anc_terms) == 0) return(character(0))
      sapply(anc_terms@x, function(t) t@obo_id)
    }, error = function(e) character(0))
    has_exo_ancestor <- any(exogenous_root_chebi %in% anc_ids)
    
    if (has_exo_ancestor) {
      message("EXCLUDE (ancestor): ", chebi_term_id)
      return(FALSE)
    }
    
    # check lables
    all_labels <- tryCatch({
      unique(c(
        term@label,
        unlist(term@synonyms),
        unlist(term@annotation[["has_related_synonym"]]),
        unlist(term@annotation[["has_exact_synonym"]]),
        unlist(term@description)
      ))
    }, error = function(e) character(0))
    
    label_match <- length(all_labels) > 0 &&
      any(sapply(exclude_terms, function(trm) any(grepl(trm, all_labels, ignore.case = TRUE))))
    
    if (label_match) {
      message("EXCLUDE (label): ", chebi_term_id)
      return(FALSE)
    }
    
    TRUE  # passed all checks — keep
    
  }
  
#get_adduct_df function for get_analyte_matches() ---------------------------------------------------------------------------
  get_adduct_df <- function(adduct_list, ionization) {
    mode_adducts  <- adduct_lookup %>% filter(ionization == !!ionization)
    wrong_mode    <- adduct_list[adduct_list %in% adduct_lookup$name & !adduct_list %in% mode_adducts$name]
    unknown       <- adduct_list[!adduct_list %in% adduct_lookup$name]
    
    if (length(wrong_mode) > 0)
      stop(paste0("Adducts not valid for ", ionization, " mode: ", paste(wrong_mode, collapse = ", "),
                  "\nAvailable: ", paste(mode_adducts$name, collapse = ", ")))
    if (length(unknown) > 0)
      stop(paste0("Unknown adducts: ", paste(unknown, collapse = ", "),
                  "\nAvailable ", ionization, " adducts: ", paste(mode_adducts$name, collapse = ", ")))
    
    mode_adducts %>% filter(name %in% adduct_list)
  }
  
  
#match_analyts function for get_analyte_matches() -------------------------------------------------------------------------
  
  
  match_analytes <- function(unchar_df, analyte_df, adduct_df) {
    
    combo_df <- merge(analyte_df, adduct_df, by = NULL) %>%
      mutate(theor_mz = (adduct_monoisotopic_mass / charge) + monoisotopic_mass)
    
    hits_df <- data.frame(
      mz          = numeric(0),
      rt          = numeric(0),
      compound_id = character(0),
      adduct      = character(0),
      stringsAsFactors = FALSE
    )
    
    for (a in unique(combo_df$name)) {
      combo_a <- combo_df[combo_df$name == a, ]
      
      for (h in seq_len(nrow(combo_a))) {
        x      <- combo_a$theor_mz[h]
        hits_h <- which(abs(unchar_df$mz - x) <= mz_tolerance)
        if (length(hits_h) == 0) next
        
        hits_df <- rbind(hits_df, data.frame(
          mz          = unchar_df$mz[hits_h],
          rt          = unchar_df$rt[hits_h],
          compound_id = combo_a$compound_id[h],
          adduct      = a,
          stringsAsFactors = FALSE
        ))
      }
    }
    hits_df
  }
  
  
  
#order_and_format function for get_targeted_lists() -------------------------------------------------------------------------
  
  order_and_format <- function(df, rt_tolerance, mz_tolerance,
                               file_offset = 0L, prefix = "high") {
    
    if (nrow(df) == 0) return(list())
    
    df <- df[order(df$rt), , drop = FALSE]
    
    n     <- nrow(df)
    x     <- df$rt
    y     <- df$mz
    group <- rep(1L, n)
    
    if (n > 1) {
      changed <- TRUE
      while (changed) {
        changed <- FALSE
        for (i in seq(2, n)) {
          prev      <- which(seq_len(n) < i & group == group[i])
          if (length(prev) == 0) next
          too_close <- abs(x[i] - x[prev]) < rt_tolerance &
            abs(y[i] - y[prev]) < mz_tolerance
          if (any(too_close)) {
            group[i] <- group[i] + 1L
            changed  <- TRUE
          }
        }
      }
    }
    
    df$group      <- group
    uniq_groups   <- sort(unique(df$group))
    next_group_id <- max(uniq_groups) + 1L
    
    for (g in uniq_groups) {
      idx <- which(df$group == g)
      k   <- length(idx)
      if (k > 30) {
        idx_shuffled  <- sample(idx, k, replace = FALSE)
        new_groups    <- integer(k)
        current_label <- g
        counter       <- 0L
        for (j in seq_len(k)) {
          if (counter == 30L) {
            current_label <- next_group_id
            next_group_id <- next_group_id + 1L
            counter       <- 0L
          }
          new_groups[j] <- current_label
          counter       <- counter + 1L
        }
        df$group[idx_shuffled] <- new_groups
      }
    }
    
    groups      <- unique(df$group)
    result_list <- vector("list", length(groups))
    
    for (i in seq_along(groups)) {
      g          <- groups[i]
      subset_df  <- df %>% dplyr::filter(group == g)
      out_df     <- data.frame(
        "On"                         = TRUE,
        "Prec. m/z"                  = df$mz,
        "Z"                          = 1,
        "Ret. Time (min)"            = subset_df$rt,
        "Delta Ret. Time (min)"      = 0.1,
        "Iso. Width"                 = "Narrow (~1.3 m/z)",
        "Collision Energy"           = NA_character_,
        "Acquisition Time (ms/spec)" = NA_character_,
        "End m/z"                    = 250,
        "Window Width"               = 50,
        check.names = FALSE
      )
      list_name  <- paste0(prefix, "_", i)
      file       <- paste0("targeted_msms_list_", prefix, "_", i + file_offset, ".csv")
      
      cat(
        "TargetedMSMSTable",
        paste(rep("", ncol(out_df) - 1), collapse = ","),
        "\n",
        file = file, sep = ","
      )
      write.table(
        out_df,
        file      = file,
        sep       = ",",
        row.names = FALSE,
        col.names = TRUE,
        append    = TRUE,
        na        = ""
      )
      
      result_list[[i]] <- out_df
      names(result_list)[i] <- list_name
    }
    
    
    
    result_list
  }