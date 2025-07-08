import os
import time
import json
import yaml
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class AntibodypediaScraper:
    def __init__(self, full_config):
        self.cfg = full_config["antibodypedia"]
        self.input_file = self.cfg["protein_ids_file"]
        self.output_dir = self.cfg["output_path"]
        self.metadata_file = self.cfg.get("metadata_file", os.path.join(self.output_dir, "antibodypedia_metadata.json"))
        self.checkpoint_file = os.path.join(self.output_dir, "antibodypedia_checkpoint.csv")

        os.makedirs(self.output_dir, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "input_file": self.input_file,
            "outputs": [],
            "scraped_ids": [],
            "errors": [],
        }

    def run(self):
        # Read UniProt IDs
        df_ids = pd.read_csv(self.input_file, usecols=["uniprot_id"], dtype=str)
        uniprot_ids = df_ids["uniprot_id"].dropna().unique().tolist()

        # Resume from checkpoint
        if os.path.exists(self.checkpoint_file):
            df_existing = pd.read_csv(self.checkpoint_file)
            scraped_ids = set(df_existing["uniprot_id"].unique())
            logging.info(f"🔁 Resuming from checkpoint. {len(scraped_ids)} entries already scraped.")
            results = df_existing.to_dict(orient="records")
        else:
            scraped_ids = set()
            results = []

        # Configure headless Chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)

        counter = 0
        for i, uid in enumerate(uniprot_ids):
            if uid in scraped_ids:
                continue

            url = f"https://www-new.antibodypedia.com/explore/{uid}"
            logging.info(f"🔎 [{i+1}/{len(uniprot_ids)}] Loading: {url}")
            driver.get(url)

            data_found = False
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "search_results_table"))
                )
                rows = driver.find_elements(By.CSS_SELECTOR, "#search_results_table tbody tr")
                for row in rows:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if len(cols) < 6:
                        continue
                    results.append({
                        "uniprot_id": uid,
                        "gene_product": cols[1].text.strip().split('\n')[0],
                        "description": cols[2].text.strip(),
                        "tissue_specificity": cols[3].text.strip(),
                        "predicted_location": cols[4].text.strip(),
                        "antibodies": cols[5].text.strip(),
                        "data_found": True
                    })
                    data_found = True
            except Exception as e:
                logging.warning(f"❌ Error for {uid}: {e}")
                self.metadata["errors"].append({"uniprot_id": uid, "error": str(e)})

            if not data_found:
                results.append({
                    "uniprot_id": uid,
                    "gene_product": None,
                    "description": None,
                    "tissue_specificity": None,
                    "predicted_location": None,
                    "antibodies": None,
                    "data_found": False
                })

            self.metadata["scraped_ids"].append(uid)
            counter += 1
            time.sleep(1)

            if counter % 100 == 0:
                pd.DataFrame(results).to_csv(self.checkpoint_file, index=False)
                logging.info(f"💾 Checkpoint saved after {counter} entries")

        # Final save
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        output_path = os.path.join(self.output_dir, f"antibodypedia_scraped_results_{timestamp}.csv")
        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False)
        df_out.to_csv(self.checkpoint_file, index=False)
        logging.info(f"✅ Scraping complete. Saved to {output_path}")

        self.metadata["timestamp"]["end"] = datetime.now().isoformat()
        self.metadata["output_file"] = output_path

        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📄 Metadata written to {self.metadata_file}")

        driver.quit()

# Standalone CLI runner
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape Antibodypedia for UniProt IDs")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    AntibodypediaScraper(config).run()
