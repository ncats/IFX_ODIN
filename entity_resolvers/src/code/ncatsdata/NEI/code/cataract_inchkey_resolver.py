#!/usr/bin/env python3
"""
InChIKey Resolver - Two-Tier Approach
Resolves InChIKeys using:
1. NCATS Resolver (primary)
2. MyChem.info (secondary)
3. NCI CACTUS (tertiary fallback)

Provides comprehensive chemical identifier mapping.
"""

import os
import json
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

class InChIKeyResolver:
    """Three-tier InChIKey resolver"""
    
    def __init__(self, config=None):
        self.config = config or {}
        
        # API endpoints
        self.ncats_url = "https://resolver.ncats.nih.gov/resolver"
        self.ncats_api_key = self.config.get("ncats_api_key", "5fd5bb2a05eb6195")
        self.mychem_url = "http://mychem.info/v1/chem"
        self.cactus_url = "https://cactus.nci.nih.gov/chemical/structure"
        
        # NCATS properties to request
        self.ncats_properties = [
            "smiles", "tpsa", "logp", "logd", "hbd", "hba", 
            "drug", "cns", "description", "cid", "unii", "chembl", 
            "chebi", "cas", "names", "devphase", "molWeight", "molForm"
        ]
        
        # Settings
        self.delay = self.config.get("delay", 0.2)
        
        # Statistics
        self.stats = {
            "total": 0,
            "ncats_resolved": 0,
            "mychem_resolved": 0,
            "cactus_resolved": 0,
            "unresolved": 0
        }
    
    def setup_logging(self, log_file="inchikey_resolver.log"):
        """Setup logging"""
        os.makedirs(Path(log_file).parent, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ],
            force=True
        )
    
    def flatten_value(self, val):
        """Convert various value types to strings"""
        if val is None:
            return None
        elif isinstance(val, (list, tuple)):
            return "|".join(str(v) for v in val if v is not None)
        elif isinstance(val, dict):
            if "value" in val:
                return str(val["value"])
            elif "name" in val:
                return str(val["name"])
            else:
                return json.dumps(val, separators=(",", ":"))
        else:
            return str(val)
    
    def resolve_with_ncats(self, inchikey):
        """
        Tier 1: NCATS Resolver - Full annotation set
        Returns: dict with resolution info or None
        """
        try:
            properties_path = "/".join(self.ncats_properties)
            url = f"{self.ncats_url}/{properties_path}/"
            
            params = {
                "structure": inchikey,
                "standardize": "CHARGE_NORMALIZE",
                "force": "false",
                "apikey": self.ncats_api_key,
                "useApproxMatch": "false",
                "useContains": "false"
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data:
                # Handle list or dict response
                result = data[0] if isinstance(data, list) and len(data) > 0 else data if isinstance(data, dict) else None
                
                if result:
                    resolved_data = {
                        "method": "ncats",
                        "name": self.flatten_value(result.get("names")),
                        "description": self.flatten_value(result.get("description")),
                        "cas": self.flatten_value(result.get("cas")),
                        "chebi": self.flatten_value(result.get("chebi")),
                        "chembl": self.flatten_value(result.get("chembl")),
                        "cid": self.flatten_value(result.get("cid")),
                        "unii": self.flatten_value(result.get("unii")),
                        "smiles": self.flatten_value(result.get("smiles")),
                        "formula": self.flatten_value(result.get("molForm")),
                        "mol_weight": self.flatten_value(result.get("molWeight")),
                        "tpsa": self.flatten_value(result.get("tpsa")),
                        "logp": self.flatten_value(result.get("logp")),
                        "logd": self.flatten_value(result.get("logd")),
                        "hbd": self.flatten_value(result.get("hbd")),
                        "hba": self.flatten_value(result.get("hba")),
                        "drug": self.flatten_value(result.get("drug")),
                        "cns": self.flatten_value(result.get("cns")),
                        "dev_phase": self.flatten_value(result.get("devphase"))
                    }
                    
                    # Check if we got any useful data
                    if any(v for k, v in resolved_data.items() if k != "method" and v):
                        return resolved_data
            
            return None
            
        except Exception as e:
            logging.debug(f"NCATS lookup failed for '{inchikey}': {e}")
            return None
    
    def resolve_with_mychem(self, inchikey):
        """
        Tier 2: MyChem.info
        Returns: dict with resolution info or None
        """
        try:
            url = f"{self.mychem_url}/{inchikey}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data:
                resolved_data = {
                    "method": "mychem",
                    "name": None,
                    "cas": None,
                    "chebi": None,
                    "chembl": None,
                    "cid": None,
                    "unii": None,
                    "smiles": None,
                    "formula": None
                }
                
                # Extract name
                if "chebi" in data and isinstance(data["chebi"], dict):
                    resolved_data["name"] = data["chebi"].get("chebi_name")
                elif "drugbank" in data and isinstance(data["drugbank"], dict):
                    resolved_data["name"] = data["drugbank"].get("name")
                elif "chembl" in data and isinstance(data["chembl"], dict):
                    resolved_data["name"] = data["chembl"].get("pref_name")
                
                # UNII data
                if "unii" in data and isinstance(data["unii"], dict):
                    resolved_data["unii"] = data["unii"].get("unii")
                    resolved_data["cas"] = data["unii"].get("registry_number")
                    resolved_data["formula"] = data["unii"].get("molecular_formula")
                    resolved_data["smiles"] = data["unii"].get("smiles")
                    if not resolved_data["name"]:
                        resolved_data["name"] = data["unii"].get("preferred_term")
                
                # ChEBI
                if "chebi" in data and isinstance(data["chebi"], dict):
                    resolved_data["chebi"] = data["chebi"].get("chebi_id")
                    if not resolved_data["smiles"]:
                        resolved_data["smiles"] = data["chebi"].get("smiles")
                    if not resolved_data["formula"]:
                        resolved_data["formula"] = data["chebi"].get("formula")
                
                # ChEMBL
                if "chembl" in data and isinstance(data["chembl"], dict):
                    resolved_data["chembl"] = data["chembl"].get("molecule_chembl_id")
                    if not resolved_data["smiles"]:
                        resolved_data["smiles"] = data["chembl"].get("smiles")
                
                # PubChem
                if "pubchem" in data and isinstance(data["pubchem"], dict):
                    resolved_data["cid"] = str(data["pubchem"].get("cid", ""))
                
                # DrugBank
                if "drugbank" in data and isinstance(data["drugbank"], dict):
                    if not resolved_data["cas"]:
                        resolved_data["cas"] = data["drugbank"].get("cas_number")
                
                # Check if we got any useful data
                if any(v for k, v in resolved_data.items() if k != "method" and v):
                    return resolved_data
            
            return None
            
        except Exception as e:
            logging.debug(f"MyChem lookup failed for '{inchikey}': {e}")
            return None
    
    def resolve_with_cactus(self, inchikey):
        """
        Tier 3: NCI CACTUS
        Returns: dict with resolution info or None
        """
        try:
            representations = {
                "smiles": "smiles",
                "inchi": "stdinchi",
                "formula": "formula",
                "names": "names"
            }
            
            resolved_data = {
                "method": "cactus",
                "name": None,
                "smiles": None,
                "formula": None
            }
            
            for key, rep in representations.items():
                try:
                    url = f"{self.cactus_url}/{inchikey}/{rep}"
                    response = requests.get(url, timeout=30)
                    if response.status_code == 200:
                        content = response.text.strip()
                        if content:
                            if key == "names":
                                # Get first name from newline-separated list
                                names = content.split("\n")
                                resolved_data["name"] = names[0] if names else None
                            else:
                                resolved_data[key] = content
                    time.sleep(0.1)  # Be polite to CACTUS
                except Exception:
                    continue
            
            # Check if we got any useful data
            if any(v for k, v in resolved_data.items() if k != "method" and v):
                return resolved_data
            
            return None
            
        except Exception as e:
            logging.debug(f"CACTUS lookup failed for '{inchikey}': {e}")
            return None
    
    def resolve_single(self, inchikey):
        """
        Resolve a single InChIKey through all resolvers
        Tries each tier until success
        Returns: dict with resolution info
        """
        if not inchikey or str(inchikey).strip() == '' or str(inchikey) == 'nan':
            return {
                "input_inchikey": inchikey,
                "resolved": False,
                "resolved_by": None
            }
        
        inchikey = str(inchikey).strip()
        result = {"input_inchikey": inchikey}
        
        # Initialize all fields
        all_fields = {
            "name": None,
            "description": None,
            "cas": None,
            "chebi": None,
            "chembl": None,
            "cid": None,
            "unii": None,
            "smiles": None,
            "formula": None,
            "mol_weight": None,
            "tpsa": None,
            "logp": None,
            "logd": None,
            "hbd": None,
            "hba": None,
            "drug": None,
            "cns": None,
            "dev_phase": None
        }
        result.update(all_fields)
        
        resolved_by = None
        
        # Try NCATS first
        ncats_result = self.resolve_with_ncats(inchikey)
        if ncats_result:
            result.update({k: v for k, v in ncats_result.items() if k != "method"})
            resolved_by = "ncats"
            self.stats["ncats_resolved"] += 1
        
        # Try MyChem if NCATS failed or didn't get key identifiers
        if not resolved_by or not result.get("cas"):
            mychem_result = self.resolve_with_mychem(inchikey)
            if mychem_result:
                # Merge results (don't overwrite existing non-null values)
                for k, v in mychem_result.items():
                    if k != "method" and v and not result.get(k):
                        result[k] = v
                
                if not resolved_by:
                    resolved_by = "mychem"
                    self.stats["mychem_resolved"] += 1
        
        # Try CACTUS if still no good data
        if not resolved_by or not result.get("smiles"):
            cactus_result = self.resolve_with_cactus(inchikey)
            if cactus_result:
                # Merge results (don't overwrite existing non-null values)
                for k, v in cactus_result.items():
                    if k != "method" and v and not result.get(k):
                        result[k] = v
                
                if not resolved_by:
                    resolved_by = "cactus"
                    self.stats["cactus_resolved"] += 1
        
        # Overall resolution status
        if resolved_by:
            result["resolved"] = True
            result["resolved_by"] = resolved_by
        else:
            result["resolved"] = False
            result["resolved_by"] = None
            self.stats["unresolved"] += 1
        
        return result
    
    def resolve_batch(self, df, inchikey_column="INCHIKEY", output_file=None, save_every=25):
        """
        Resolve batch of InChIKeys
        
        Args:
            df: DataFrame with InChIKeys
            inchikey_column: Column containing InChIKeys
            output_file: Path to save incremental progress (optional)
            save_every: Save progress every N InChIKeys (default 25)
            
        Returns:
            DataFrame with resolution results
        """
        if inchikey_column not in df.columns:
            raise ValueError(f"Column '{inchikey_column}' not found in DataFrame")
        
        results = []
        total = len(df)
        
        logging.info(f"Starting resolution of {total} InChIKeys...")
        logging.info(f"Strategy: NCATS → MyChem → CACTUS (cascading fallback)")
        if output_file:
            logging.info(f"Saving progress every {save_every} InChIKeys to {output_file}")
        
        for idx, row in df.iterrows():
            inchikey = row[inchikey_column]
            self.stats["total"] += 1
            
            if (idx + 1) % 25 == 0:
                logging.info(f"Progress: {idx + 1}/{total}")
            
            # Resolve
            resolution = self.resolve_single(inchikey)
            
            # Log result
            if resolution["resolved"]:
                method = resolution["resolved_by"]
                name = resolution.get("name", "")
                name_str = f" ({name[:40]}...)" if name and len(name) > 40 else f" ({name})" if name else ""
                logging.info(f"✓ [{idx + 1}/{total}] '{inchikey}' → {method}{name_str}")
            else:
                logging.warning(f"✗ [{idx + 1}/{total}] Could not resolve '{inchikey}'")
            
            # Combine with original data
            result = {inchikey_column: inchikey}
            
            # Add all original columns
            for col in df.columns:
                if col != inchikey_column:
                    result[col] = row[col]
            
            # Add resolution data
            result.update(resolution)
            results.append(result)
            
            # Incremental save
            if output_file and (idx + 1) % save_every == 0:
                temp_df = pd.DataFrame(results)
                os.makedirs(Path(output_file).parent, exist_ok=True)
                temp_df.to_csv(output_file, sep="\t", index=False)
                logging.info(f"💾 Saved progress: {idx + 1}/{total} InChIKeys")
            
            # Rate limiting
            time.sleep(self.delay)
        
        results_df = pd.DataFrame(results)
        
        # Final save
        if output_file:
            os.makedirs(Path(output_file).parent, exist_ok=True)
            results_df.to_csv(output_file, sep="\t", index=False)
            logging.info(f"💾 Final save: {total}/{total} InChIKeys")
        
        # Print statistics
        self.print_stats()
        
        return results_df
    
    def print_stats(self):
        """Print resolution statistics"""
        total = self.stats["total"]
        
        logging.info("=" * 60)
        logging.info("RESOLUTION STATISTICS")
        logging.info("=" * 60)
        logging.info(f"Total InChIKeys: {total}")
        logging.info(f"Resolved: {total - self.stats['unresolved']} ({100*(total - self.stats['unresolved'])/total:.1f}%)")
        logging.info("")
        logging.info(f"  NCATS:   {self.stats['ncats_resolved']} ({100*self.stats['ncats_resolved']/total:.1f}%)")
        logging.info(f"  MyChem:  {self.stats['mychem_resolved']} ({100*self.stats['mychem_resolved']/total:.1f}%)")
        logging.info(f"  CACTUS:  {self.stats['cactus_resolved']} ({100*self.stats['cactus_resolved']/total:.1f}%)")
        logging.info("")
        logging.info(f"Unresolved: {self.stats['unresolved']} ({100*self.stats['unresolved']/total:.1f}%)")
        logging.info("=" * 60)
    
    def save_unresolved(self, results_df, output_file):
        """
        Extract unresolved InChIKeys for review
        
        Args:
            results_df: Full results DataFrame
            output_file: Path to save unresolved InChIKeys
        """
        unresolved = results_df[results_df['resolved'] == False].copy()
        
        if len(unresolved) > 0:
            # Keep only essential columns
            cols_to_keep = ['INCHIKEY'] if 'INCHIKEY' in unresolved.columns else [unresolved.columns[0]]
            unresolved_subset = unresolved[cols_to_keep].copy()
            
            os.makedirs(Path(output_file).parent, exist_ok=True)
            unresolved_subset.to_csv(output_file, sep="\t", index=False)
            
            logging.info(f"\n✓ Saved {len(unresolved)} unresolved InChIKeys to {output_file}")
            logging.info(f"  Review these for potential data quality issues")
        else:
            logging.info("\n✓ All InChIKeys resolved!")


def main():
    """Main execution"""
    # Paths
    INPUT_FILE = "cataract_sig_mets.csv"
    OUTPUT_FILE = "cataract_sig_mets_resolved.tsv"
    UNRESOLVED_FILE = "unresolved_inchikeys.tsv"
    LOG_FILE = "inchikey_resolver.log"
    
    logging.basicConfig(level=logging.INFO)
    logging.info("=" * 60)
    logging.info("INCHIKEY RESOLVER")
    logging.info("=" * 60)
    
    # Load data
    logging.info(f"\nStep 1: Loading data from {INPUT_FILE}...")
    
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")
    
    df = pd.read_csv(INPUT_FILE, dtype=str).fillna("")
    
    logging.info(f"✓ Loaded {len(df)} records")
    logging.info(f"  Columns: {list(df.columns)}")
    
    # Check for INCHIKEY column
    if "INCHIKEY" not in df.columns:
        raise ValueError(f"Column 'INCHIKEY' not found. Available: {list(df.columns)}")
    
    # Initialize resolver
    logging.info("\nStep 2: Initializing InChIKey resolver...")
    
    config = {
        "delay": 0.2,  # Seconds between API calls
        "ncats_api_key": "5fd5bb2a05eb6195"
    }
    
    resolver = InChIKeyResolver(config)
    resolver.setup_logging(LOG_FILE)
    
    # Resolve InChIKeys
    logging.info("\nStep 3: Resolving InChIKeys...")
    logging.info("  This may take a while - coffee recommended ☕")
    results_df = resolver.resolve_batch(
        df, 
        inchikey_column="INCHIKEY", 
        output_file=OUTPUT_FILE, 
        save_every=25
    )
    
    # Finalize
    logging.info(f"\nStep 4: Finalizing results...")
    logging.info(f"✓ All results in {OUTPUT_FILE}")
    
    # Save unresolved
    resolver.save_unresolved(results_df, UNRESOLVED_FILE)
    
    logging.info(f"\n" + "=" * 60)
    logging.info("✓ COMPLETE!")
    logging.info("=" * 60)
    logging.info(f"\nOutputs:")
    logging.info(f"  1. All results:          {OUTPUT_FILE}")
    logging.info(f"  2. Unresolved InChIKeys: {UNRESOLVED_FILE}")
    logging.info(f"  3. Log file:             {LOG_FILE}")


if __name__ == "__main__":
    main()