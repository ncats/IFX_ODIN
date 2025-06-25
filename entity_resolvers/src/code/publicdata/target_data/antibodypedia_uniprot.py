import pandas as pd
import time
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Load input
input_csv = "src/data/publicdata/target_data/cleaned/resolved_node_ids/protein_ids2.csv"
df_ids = pd.read_csv(input_csv, usecols=["uniprot_id"], dtype=str)
uniprot_ids = df_ids["uniprot_id"].dropna().unique().tolist()

# Create timestamped output path
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
output_path = f"antibodypedia_scraped_results_{timestamp}.csv"

# Checkpoint intermediate (non-timestamped) file
checkpoint_file = "antibodypedia_checkpoint.csv"

# Resume mode — load previously scraped IDs if checkpoint exists
if os.path.exists(checkpoint_file):
    df_existing = pd.read_csv(checkpoint_file)
    scraped_ids = set(df_existing["uniprot_id"].unique())
    print(f"🔁 Resuming from checkpoint. {len(scraped_ids)} entries already scraped.")
    results = df_existing.to_dict(orient="records")
else:
    scraped_ids = set()
    results = []

# Configure headless Chrome
chrome_options = Options()
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(options=chrome_options)

# Begin scraping
counter = 0
for i, uid in enumerate(uniprot_ids):
    if uid in scraped_ids:
        print(f"⏭️ Skipping already scraped: {uid}")
        continue

    url = f"https://www-new.antibodypedia.com/explore/{uid}"
    print(f"🔎 [{i+1}/{len(uniprot_ids)}] Loading: {url}")
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
        print(f"❌ Error for {uid}: {e}")

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

    time.sleep(1)
    counter += 1

    # Save checkpoint every 10
    if counter % 10 == 0:
        pd.DataFrame(results).to_csv(checkpoint_file, index=False)
        print(f"💾 Checkpoint saved after {counter} entries → {checkpoint_file}")

# Final save
df_out = pd.DataFrame(results)
df_out.to_csv(output_path, index=False)
print(f"✅ Done. All results saved to '{output_path}'")

# Final checkpoint overwrite
df_out.to_csv(checkpoint_file, index=False)
print(f"💾 Final checkpoint updated: {checkpoint_file}")

driver.quit()
