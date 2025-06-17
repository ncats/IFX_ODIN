#!/bin/bash

# Define main directories
main_dirs=("publicdata" "NCATSdata")

# Define subdirectories under publicdata
subdirs=("target_data" "disease_data" "drug_data" "GO_data" "PPI_data" "phenotype_data" "pathway_data")

# Define common subdirectories for each main subdirectory
base_dirs=("raw" "cleaned" "qc" "metadata")
cleaned_dirs=("sources" "resolved_node_ids" "resolved_edges")

# Create main directories
for main in "${main_dirs[@]}"; do
    mkdir -p "src/code/$main"
done

# Create subdirectories under publicdata
for sub in "${subdirs[@]}"; do
    mkdir -p "src/code/publicdata/$sub"
    mkdir -p "src/data/publicdata/$sub"

    # Create base directories under each subdirectory
    for base in "${base_dirs[@]}"; do
        mkdir -p "src/data/publicdata/$sub/$base"
        
        # If it's the 'cleaned' directory, create additional cleaned subdirectories
        if [[ $base == "cleaned" ]]; then
            for clean in "${cleaned_dirs[@]}"; do
                mkdir -p "src/data/publicdata/$sub/$base/$clean"
            done
        fi
    done
done
