#!/bin/bash

# Base URL for the files
base_url="https://stars.renci.org/var/babel_outputs/2025jan23/compendia/Gene.txt."

# Output file for filtered data
filtered_file="human_genes.txt"

# Loop over file numbers 00 to 6
for i in $(seq -f "%02g" 0 6); do
    # Full file URL
    file_url="${base_url}${i}"

    # Download the file
    echo "Downloading ${file_url}..."
    curl -O "${file_url}"

    # Filter lines containing "NCBITaxon:9606" and append to the output file
    grep "NCBITaxon:9606" "Gene.txt.${i}" >> "${filtered_file}"

    # Delete the file after processing
    rm "Gene.txt.${i}"

    echo "Processed and deleted Gene.txt.${i}"
done

echo "Filtered human data has been saved to ${filtered_file}"
