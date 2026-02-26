#!/bin/bash

# Base URL for the files
base_url="https://stars.renci.org/var/babel_outputs/2025jan23/compendia/Protein.txt."

# Output file for filtered data
filtered_file="human_proteins.txt"

# Loop over file numbers 00 to 25
for i in $(seq -f "%02g" 0 25); do
    # Full file URL
    file_url="${base_url}${i}"

    # Download the file
    echo "Downloading ${file_url}..."
    curl -O "${file_url}"

    # Filter lines containing "NCBITaxon:9606" and append to the output file
    grep "NCBITaxon:9606" "Protein.txt.${i}" >> "${filtered_file}"

    # Delete the file after processing
    rm "Protein.txt.${i}"

    echo "Processed and deleted Protein.txt.${i}"
done

echo "Filtered human data has been saved to ${filtered_file}"
