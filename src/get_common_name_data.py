import requests
import os
from tqdm import tqdm
from zipfile import ZipFile
import polars as pl

ONE_KB = 1024 
CHUNK_SIZE = ONE_KB * 2000
save_dir = "data"
zip_path = os.path.join(save_dir, "backbone.zip")
tsv_file_name = "VernacularName.tsv"
tsv_path = os.path.join(save_dir, tsv_file_name)
backbone_data_url = "https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip"

os.makedirs(save_dir, exist_ok=True)
try:
    print(f"Downloading GBIF backbone from {backbone_data_url}...")
    with requests.get(backbone_data_url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        with tqdm(total=total_size, unit='iB', unit_scale=True) as t:
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
                    t.update(len(chunk))
except KeyboardInterrupt:
    print("\nDownload interrupted by user. Cleaning up...")
    if os.path.exists(zip_path):
        os.remove(zip_path)
    exit()
except Exception as e:
    print(f"Error during download: {e}")
    exit()

print(f"Extracting {tsv_file_name}...")
try:
    with ZipFile(zip_path, 'r') as zObject:
        zObject.extract(tsv_file_name, path=save_dir)
    print("Extraction complete.")
except Exception as e:
    print(f"Error during extraction: {e}")
    exit()
finally:
    if os.path.exists(zip_path):
        os.remove(zip_path)

print("Converting TSV to Parquet for performance...")
try:
    (
        pl.scan_csv(
            tsv_path, 
            separator="\t", 
            quote_char=None, 
            ignore_errors=True,
            infer_schema_length=10000
        )
        .filter(pl.col("countryCode") == "US")
        .select([
            pl.col("taxonID").cast(pl.Int64).alias("taxonkey"),
            pl.col("vernacularName")
        ])
        .sink_parquet(os.path.join(save_dir, "vernacular_names.parquet"))
    )
    print("Parquet created successfully: data/vernacular_names.parquet")
    
    if os.path.exists(tsv_path):
        os.remove(tsv_path)
        print("Raw TSV removed to save space.")

except Exception as e:
    print(f"Error during Parquet conversion: {e}")