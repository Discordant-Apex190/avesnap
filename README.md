# GBIF Bird Data ETL & Visualization Pipeline

The core pipeline is hosted on Hetzner and managed via Coolify, utilizing Prefect for orchestration.

## ETL Process

**Extraction:** Pulls occurrence data from GBIF's open S3 bucket.

**Transformation:** Data is filtered using Polars (utilizing lazy frames for efficiency).

**Loading:** Sinks transformed data into Parquet gile.
             Deployed to Cloudflare R2 behind a custom DNS.

**Partitioning:** I considered partitioning the Parquet by state or year, but because the file compresses into such a small size, partitioning is currently inefficient and was skipped.

## Orchestration & Scheduling

**Tooling:** Prefect wraps the single script to monitor the flow and provide visibility.

**Deployment:** A prefect serve instance runs within a Coolify managed container.

**Schedule:** Monthly cron job, aligned with GBIF’s data release cycles.

## Future Roadmap

### Frontend & Content

**Astro Site:** Automatically generate a unique, SEO-friendly route for every bird species found in the filtered dataset.

**Data Enrichment:** Integrating Wikipedia/GBIF API hooks to pull in species descriptions, conservation status, and historical metadata.

### Client-Side Analytics

**Mapping:** This data will feed into an interactive map for users to filter using leaflet and javascript
