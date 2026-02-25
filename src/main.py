import boto3
from botocore import UNSIGNED
from botocore.config import Config
import polars as pl
import os
from prefect import flow, task
import requests
from tqdm import tqdm
from zipfile import ZipFile

ONE_KB = 1024
CHUNK_SIZE_ZIP = ONE_KB * 2000
ROW_GROUP_SIZE = 122880

SAVE_DIR = "data"
ZIP_PATH = os.path.join(SAVE_DIR, "backbone.zip")
TSV_FILE_NAME = "VernacularName.tsv"
TSV_PATH = os.path.join(SAVE_DIR, TSV_FILE_NAME)
PARQUET_CNAME_PATH = os.path.join(SAVE_DIR, "vernacular_names.parquet")
BACKBONE_DATA_URL = "https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip"

@task()
def get_common_name_tsv():
    """Downloads the GBIF backbone zip and extracts the VernacularName TSV."""
    os.makedirs(SAVE_DIR, exist_ok=True)
    
    try:
        response = requests.get(BACKBONE_DATA_URL, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        with tqdm(total=total_size, unit='iB', unit_scale=True, desc="Downloading GBIF Backbone") as t:
            with open(ZIP_PATH, 'wb') as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE_ZIP):
                    if chunk:
                        f.write(chunk)
                        t.update(len(chunk))
                        
        with ZipFile(ZIP_PATH, 'r') as z_object:
            z_object.extract(TSV_FILE_NAME, path=SAVE_DIR)
            
    finally:
        if os.path.exists(ZIP_PATH):
            os.remove(ZIP_PATH)
    
    return TSV_PATH

@task
def push_common_names_parquet(tsv_file_path):
    """Converts the raw TSV to a filtered, typed Parquet file locally."""
    (
        pl.scan_csv(
            tsv_file_path,
            separator="\t",
            quote_char=None,
            ignore_errors=True,
            infer_schema_length=10000
        )
        .filter(pl.col("countryCode") == "US")
        .select([
            pl.col("taxonID").cast(pl.Int64),
            pl.col("vernacularName")
        ])
        .sink_parquet(PARQUET_CNAME_PATH)
    )
    
    if os.path.exists(tsv_file_path):
        os.remove(tsv_file_path)
    return PARQUET_CNAME_PATH

@task
def get_s3_uris():
    """Identifies the latest GBIF snapshot files from the Public S3 bucket."""
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    bucket = "gbif-open-data-us-east-1"
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix="occurrence/", Delimiter="/")
    folders = [prefix.get("Prefix") for prefix in response.get("CommonPrefixes", [])]
    if not folders:
        raise Exception("No GBIF occurrence folders found.")
    
    latest_prefix = sorted(folders)[-1]
    target_prefix = f"{latest_prefix}occurrence.parquet/"
    
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=target_prefix)
    
    s3_uris = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                s3_uris.append(f"s3://{bucket}/{key}")
                
    print(f"Found {len(s3_uris)} data files in snapshot {latest_prefix}")
    return s3_uris

@task
def process_aves_data(s3_uris):
    """Creates a LazyFrame for Aves (Birds) filtered by quality and location."""
    return pl.scan_parquet(
        s3_uris,
        storage_options={
            "aws_region": "us-east-1",
            "skip_signature": "true",
        },
    ).filter(
        (pl.col("class") == "Aves") &
        (pl.col("countrycode") == "US") &
        (pl.col("occurrencestatus") == "PRESENT") &
        (pl.col("decimallatitude").is_not_null()) &
        (pl.col("decimallongitude").is_not_null()) &
        (pl.col("coordinateuncertaintyinmeters") < 5000) &
        (pl.col("year").is_not_null()) &
        (pl.col("individualcount") > 0) &
        (pl.col("stateprovince").is_not_null()) &
        (pl.col("license").is_in(["CC0_1_0", "CC_BY_4_0"])) &
        (pl.col("rightsholder").is_not_null() | pl.col("license").is_not_null())
    ).with_columns(
        pl.col("taxonkey").cast(pl.Int64)
    )

@task
def join_and_enrich(lf_aves, cname_parquet_path):
    """Joins bird observations with vernacular names and adds metadata."""
    lf_common_names = pl.scan_parquet(cname_parquet_path)
    
    return (
        lf_aves.join(
            lf_common_names,
            left_on="taxonkey",
            right_on="taxonID",
            how="left"
        )
        .with_columns(
            gbif_link = pl.lit("https://www.gbif.org/occurrence/") + pl.col("gbifid").cast(pl.String)
        )
        .select([
            "gbifid", "decimallatitude", "decimallongitude", "eventdate", 
            "year", "month", "scientificname", "vernacularName", "stateprovince",
            "basisofrecord", "individualcount", "gbif_link"
        ])
    )

@task
def sink_to_r2(lf_final: pl.LazyFrame):
    """Sinks the processed LazyFrame to a remote R2/S3 bucket."""
    storage_options = {
        "aws_access_key_id": os.getenv("R2_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("R2_SECRET_ACCESS_KEY"),
        "aws_endpoint": os.getenv("R2_ENDPOINT_URL"),
        "aws_region": "auto",
    }
    
    bucket_name = "gbif-data-bucket"
    remote_path = f"s3://{bucket_name}/gbif_data/processed_aves.parquet"

    lf_final.sink_parquet(
        remote_path,
        compression="zstd",
        row_group_size=ROW_GROUP_SIZE,
        storage_options=storage_options
    )

@flow(log_prints=True)
def avesnap_etl():
    tsv_path = get_common_name_tsv()
    cname_path = push_common_names_parquet(tsv_path)
    s3_uris = get_s3_uris()
    lf_aves = process_aves_data(s3_uris)
    lf_enriched = join_and_enrich(lf_aves, cname_path)
    sink_to_r2(lf_enriched)

if __name__ == "__main__":
    avesnap_etl()
    avesnap_etl.serve(
        name="avesnap_data_pipeline",
        cron="0 0 1 * *"
    )