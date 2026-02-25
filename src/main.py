import boto3
from botocore import UNSIGNED
from botocore.config import Config
import polars as pl
import os
from prefect import flow, task

CHUNK_SIZE = 250 * (1024**2)
ROW_GROUP_SIZE = 122880

@task
def get_s3_uris():
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    bucket = "gbif-open-data-us-east-1"
    response = s3.list_objects_v2(Bucket=bucket, Prefix="occurrence/", Delimiter="/")
    folders = [prefix.get("Prefix") for prefix in response.get("CommonPrefixes", [])]
    folders.sort()
    latest_prefix = folders[-1]
    target_prefix = f"{latest_prefix}occurrence.parquet/"
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=target_prefix)
    s3_uris = []
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                if "occurrence.parquet/" in key and not key.endswith("/"):
                    s3_uris.append(f"s3://{bucket}/{key}")
    print(f"Found {len(s3_uris)} data files to process.")
    return s3_uris

@task
def process_aves_data(s3_uris):
    lf_aves_data = pl.scan_parquet(
        s3_uris,
        storage_options={
            "aws_region": "us-east-1",
            "skip_signature": "true",
        },
    )
    lf_aves_data = (
        lf_aves_data.select(
            "gbifid",
            "class",
            "countrycode",
            "license",
            "rightsholder",
            "occurrencestatus",
            "taxonkey",
            "stateprovince",
            "decimallatitude",
            "decimallongitude",
            "eventdate",
            "year",
            "month",
            "coordinateuncertaintyinmeters",
            "scientificname",
            "genus",
            "species",
            "basisofrecord",
            "issue",
            "individualcount",
            "establishmentmeans",
            "mediatype",
        )
        .filter(pl.col("class") == "Aves")
        .filter(pl.col("countrycode") == "US")
        .filter(pl.col("occurrencestatus") == "PRESENT")
        .filter(pl.col("decimallatitude").is_not_null())
        .filter(pl.col("decimallongitude").is_not_null())
        .filter(pl.col("coordinateuncertaintyinmeters") < 5000)
        .filter(pl.col("year").is_not_null())
        .filter(pl.col("individualcount") > 0)
        .filter(pl.col("stateprovince").is_not_null())
        .with_columns(pl.col("taxonkey").cast(pl.Int64))
        .filter(
            (pl.col("license").is_in(["CC0_1_0", "CC_BY_4_0"]))
            & (pl.col("rightsholder").is_not_null() | pl.col("license").is_not_null())
        )
    )
    return lf_aves_data

@task
def join_common_names(lf_aves_data):
    save_path_for_tsv = "data/vernacular_names.parquet"
    full_path = os.path.abspath(save_path_for_tsv)
    lf_common_name = pl.scan_parquet(source=full_path)
    ave_data_w_cnames = lf_aves_data.join(
        lf_common_name, 
        left_on="taxonkey",
        right_on="taxonID"
    )
    ave_data_w_cnames = ave_data_w_cnames.select(
        "gbifid",
        "decimallatitude",
        "decimallongitude",
        "eventdate",
        "year",
        "month",
        "coordinateuncertaintyinmeters",
        "scientificname",
        "license",
        "rightsholder",
        "stateprovince",
        "genus",
        "species",
        "basisofrecord",
        "issue",
        "vernacularName",
        "individualcount",
        "establishmentmeans",
        "mediatype",
    ).with_columns(
        (
            pl.lit("https://www.gbif.org/occurrence/") + pl.col("gbifid").cast(pl.String)
        ).alias("gbif_link")
    )
    return ave_data_w_cnames

@task
def sink_parquet(ave_data_w_cnames: pl.LazyFrame):
    storage_options = {
        "aws_access_key_id": os.getenv("R2_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("R2_SECRET_ACCESS_KEY"),
        "aws_endpoint": os.getenv("R2_ENDPOINT_URL"), 
        "aws_region": "auto",
    }
    
    bucket_name = "gbif-data-bucket"
    remote_path = f"s3://{bucket_name}/gbif_data/"

    ave_data_w_cnames.sink_parquet(
        pl.PartitionBy(
            remote_path,
            key="stateprovince",
            approximate_bytes_per_file=CHUNK_SIZE,
        ),
        compression="zstd",
        row_group_size=ROW_GROUP_SIZE,
        storage_options=storage_options
    )

@flow
def avesnap_etl():
    s3_uris = get_s3_uris()
    lf_aves_data = process_aves_data(s3_uris)
    ave_data_w_cnames = join_common_names(lf_aves_data)
    sink_parquet(ave_data_w_cnames)

if __name__ == "__main__":
    avesnap_etl.serve(
        name="avesnap_data_pipeline",
        cron="0 0 1 * *")

