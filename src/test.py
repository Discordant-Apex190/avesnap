import boto3
from botocore import UNSIGNED
from botocore.config import Config
import polars as pl
import os
from timeit import default_timer as timer

start = timer()

CHUNK_SIZE = 250 * (1024**2)
ROW_GROUP_SIZE = 122880

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

save_path_for_tsv = "data/VernacularName.tsv"
full_path = os.path.abspath(save_path_for_tsv)
lf_common_name = pl.scan_csv(source=full_path, separator="\t", quote_char=None)

lf_common_name = (
    lf_common_name.select("taxonID", "vernacularName", "countryCode")
    .filter(pl.col("countryCode") == "US")
    .select("taxonID", "vernacularName")
    .with_columns(pl.col("taxonID").cast(pl.Int64))
    .rename({"taxonID": "taxonkey"})
)

ave_data_w_cnames = lf_aves_data.join(lf_common_name, on="taxonkey")

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

ave_data_w_cnames.sink_parquet(
    pl.PartitionBy(
        "gbif_data/",
        key="stateprovince",
        approximate_bytes_per_file=CHUNK_SIZE,
    ),
    compression="zstd",
    row_group_size=ROW_GROUP_SIZE
)

end = timer()
print(f"Full script execution time: {end - start:.4f} seconds")
