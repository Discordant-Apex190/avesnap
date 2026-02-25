# GBIF Bird Data ETL & Visualization Pipeline

Currently, a pipeline runs on my Hetzner server to pull GBIF data from the open S3 bucket they host for occurrences.

## ETL Process

**Extraction:** Pulls occurrence data from GBIF's open S3 bucket.

**Transformation:** Data is filtered using Polars (utilizing lazy frames for efficiency).

**Loading:** The resulting data is sunk into a Parquet file and stored in Cloudflare R2.

**Partitioning:** I considered partitioning the Parquet by state or year, but because the file compresses into such a small size, partitioning is currently inefficient and was skipped.

## Orchestration & Scheduling

**Tooling:** Prefect wraps the single script to monitor the flow and provide visibility.

**Deployment:** The process runs on my server.

**Schedule:** It runs every month via a cron job using the `prefect serve` command.

## Future Roadmap

### Frontend & Content

**Astro Site:** I will be creating an Astro site to generate dynamic routes for each bird found in the filtered GBIF data.

**Data Enrichment:** Eventually, I'll pull in bird description and history data to populate these routes.

### Client-Side Analytics

**DuckDB-Wasm:** I will use DuckDB-Wasm in the browser to allow client-side querying.

**Mapping:** This data will feed into an interactive map for users to filter. I am currently deciding between:

- Leaflet
- deck.gl
- H3 (or similar spatial indexing)