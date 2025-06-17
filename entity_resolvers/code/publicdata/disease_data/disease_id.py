import pandas as pd
import json
import secrets
from datetime import datetime

class MondoProcessor:
    def __init__(self):
        print('Starting with MONDO ids from BioPortal, https://bioportal.bioontology.org/ontologies/MONDO note: need bioportal API_key for access/download. version: releases/2024-05-09')
        self.metadata = {
            "timestamp": str(pd.Timestamp.now()),
            "processing_steps": [],
            "outputs": []
        }

    def load_data(self, file_path):
        print(f"Loading data from {file_path}...")
        try:
            df = pd.read_csv(file_path, dtype='string')
            self.append_metadata_step("Loading Data", f"Loaded data from {file_path}.")
            return df
        except Exception as e:
            print(f"Error loading data from {file_path}: {e}")
            return None

    def process_mondo_data(self, df):
        print("Processing MONDO dataframe...")
        df = self.clean_mondo_dataframe(df)
        if df is None:
            return
        
        df_with_obsolete = df
        df = df[df['obsolete'] == 'false']
        
        mondo_parents = df[['mondo_id', 'parents']].assign(parents=df['parents'].str.split('|')).explode('parents')
        mondo_parents = mondo_parents.dropna(subset=['parents'])
        mondo_parents = mondo_parents[mondo_parents['parents'].str.startswith('MONDO')]
        
        # Load disease_mappings.csv
        disease_mappings = pd.read_csv('src/data/publicdata/disease_data/cleaned/mappings/disease_mappings.csv')
        disease_mappings = disease_mappings[['mondo_id', 'ncats_disease_id']].drop_duplicates()
        
        # Create a mapping dictionary for mondo_id to ncats_disease_id
        mondo_to_ncats = disease_mappings.set_index('mondo_id')['ncats_disease_id'].to_dict()
        
        # Map mondo_id and parents to their ncats_disease_id values
        mondo_parents['mondo_id'] = mondo_parents['mondo_id'].map(mondo_to_ncats)
        mondo_parents['parents'] = mondo_parents['parents'].map(mondo_to_ncats)
        
        # Remove rows where mapping failed (resulted in NaN)
        mondo_parents = mondo_parents.dropna(subset=['mondo_id', 'parents'])
        
        self.save_dataframe(mondo_parents, "src/data/publicdata/disease_data/cleaned/edges/parent_mondo.csv")
        
        cleaned_mondo_df = self.process_and_clean_mondo_dataframe(df)
        
        mf4 = df[['mondo_id', 'preferred_label', 'synonyms', 'definition']]
        final_mondo_df = pd.merge(mf4, cleaned_mondo_df, how='right', on='mondo_id')
        
        self.save_dataframe(final_mondo_df, "src/data/publicdata/disease_data/semi/final_mondo_map.csv")
        self.append_metadata_step("Processing MONDO Data", "Processed MONDO data.")
        
    def clean_mondo_dataframe(self, df):
        print("Cleaning MONDO dataframe...")
        selected_columns = ['Class ID', 'Preferred Label', 'Synonyms', 'Definitions', 'Obsolete', 'Parents', 'database_cross_reference']
        renamed_columns = {
            "Class ID": "mondo_id",
            "Preferred Label": "preferred_label",
            "Synonyms": "synonyms",
            "Definitions": "definition",
            "Obsolete": "obsolete",
            "Parents": "parents",
            "database_cross_reference": "database_cross_reference"
        }

        missing_columns = [col for col in selected_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing columns in MONDO dataframe: {missing_columns}")
            return None

        df_cleaned = df[selected_columns].rename(columns=renamed_columns)
        replacements = {'http://purl.obolibrary.org/obo/': '', 'MONDO_': 'MONDO:'}
        for col in ['mondo_id', 'parents']:
            for old_str, new_str in replacements.items():
                df_cleaned[col] = df_cleaned[col].str.replace(old_str, new_str, regex=False)
        
        self.append_metadata_step("Cleaning DataFrame", "Cleaned MONDO dataframe.")
        return df_cleaned

    def process_and_clean_mondo_dataframe(self, df):
        mondo_mapped = df[['mondo_id', 'preferred_label', 'synonyms', 'definition', 'database_cross_reference']].copy()
        mondo_mapped['database_cross_reference'] = mondo_mapped['database_cross_reference'].str.split('|')
        exploded_data_mondo = mondo_mapped.explode('database_cross_reference')
        exploded_data_mondo['source'] = exploded_data_mondo['database_cross_reference'].str.split(':').str[0]

        pivot_data = exploded_data_mondo.pivot_table(index='mondo_id', columns='source', values='database_cross_reference', aggfunc=lambda x: '|'.join(x))
        pivot_data.reset_index(inplace=True)
        pivot_data.rename(columns={"NCIT": "NCI"}, inplace=True)
        pivot_data['NCI'] = pivot_data['NCI'].str.replace('NCIT:', 'NCI:')

        return pivot_data

    def create_disease_nodes(self, df):
        print("Creating disease nodes...")
        df2 = df[['mondo_id', 'preferred_label', 'definition', 'synonyms', 'CSP', 'DECIPHER', 'DOID', 'EFO', 'GARD', 'GTR', 'HGNC', 'HP', 'ICD10CM', 'ICD10EXP', 'ICD10WHO', 'ICD9', 'ICD9CM', 'ICDO', 'IDO', 'MedDRA', 'MEDGEN', 'MESH', 'MFOMD', 'MPATH', 'MTH', 'NCI', 'NDFRT', 'NIFSTD', 'OBI', 'OGMS', 'OMIM', 'OMIMPS', 'ONCOTREE', 'Orphanet', 'PMID', 'SCDO', 'SCTID', 'UMLS','Wikipedia']]
        
        df3 = df2.copy()
        # Generate 'ncats_disease_id', 'createdAt', and 'updatedAt' columns
        current_time = str(datetime.now())
        df3['ncats_disease_id'] = ['IFXDisease:' + ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(7)) for _ in range(len(df3))]
        df3['createdAt'] = current_time
        df3['updatedAt'] = current_time
        self.append_metadata_step("NCATS Disease ID Generation", "Generated NCATS Disease IDs and added timestamps.")

        cols = ['ncats_disease_id'] + [col for col in df3 if col != 'ncats_disease_id']
        Disease_df = df3[cols].sort_values('ncats_disease_id')
        
        self.save_dataframe(Disease_df, "src/data/publicdata/disease_data/cleaned/nodes/disease_ids.csv")
        
        dtg = Disease_df['ncats_disease_id'].nunique()
        print('unique disease_ids', dtg)
        self.append_metadata_step("Creating Disease Nodes", "Created disease nodes for Neo4j.")

    def save_dataframe(self, df, file_path):
        try:
            df.to_csv(file_path, index=False)
            print(f"Data saved to {file_path}")
            self.append_metadata_step("Saving DataFrame", f"Saved dataframe to {file_path}.")
        except Exception as e:
            print(f"Error saving dataframe to {file_path}: {e}")

    def append_metadata_step(self, step_name, description):
        self.metadata["processing_steps"].append({
            "step_name": step_name,
            "description": description,
            "performed_at": str(pd.Timestamp.now())
        })

    def save_metadata(self, file_path):
        with open(file_path, 'w') as metafile:
            json.dump(self.metadata, metafile, indent=4)
        print(f"Metadata saved to {file_path}")

def process_disease_ids():
    processor = MondoProcessor()
    
    mondo_csv_path = 'src/data/publicdata/disease_data/semi/final_mondo_map.csv'
    
    mondo_df = processor.load_data(mondo_csv_path)
    processor.process_mondo_data(mondo_df)
    
    processor.create_disease_nodes(mondo_df)
    
    processor.save_metadata('src/data/publicdata/disease_data/metadata/mondo_metadata.json')
    print("MONDO data processing completed.")

if __name__ == "__main__":
    process_disease_ids()
