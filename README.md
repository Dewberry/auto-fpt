# auto-fpt

Automated Flood Prediction Tools — a collection of AWS Lambda containers that ingest real-time hydrometeorological data from several sources, validate it against shared schemas, and consolidate it into cloud-native formats for downstream flood prediction workflows.

## Architecture

Each data source is packaged as an independent Docker container deployed as an AWS Lambda function. A separate **consolidate** container reads from staging S3 prefixes and writes to long-term storage in [Icechunk](https://icechunk.io/) (gridded data) or [Apache Iceberg](https://iceberg.apache.org/) (point data).

```
┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
│  mrms/  │  │  nwp/   │  │  rtma/  │  │  usgs/  │
│ (MRMS)  │  │ (HRRR)  │  │ (RTMA)  │  │  (NWIS) │
└────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
     │             │             │             │
     └─────────────┴──────┬──────┴─────────────┘
                           ▼
                    S3 staging (NetCDF / Parquet)
                           │
                    ┌──────┴──────┐
                    │    etl/     │
                    │ consolidate │
                    └──────┬──────┘
                           ▼
              Icechunk (gridded) / Iceberg (point)
```

## Containers

### `mrms/` — MRMS QPE
Fetches [MRMS](https://www.nssl.noaa.gov/projects/mrms/) quantitative precipitation estimates and saves them to S3 as NetCDF.

| Variable | Description | Units |
|---|---|---|
| `qpe_1hr` | MultiSensor QPE 1-hour accumulation (Pass 2) | mm |
| `qpe_15min` | RadarOnly QPE 15-minute accumulation | mm |

### `nwp/` — HRRR Forecast
Fetches [HRRR](https://rapidrefresh.noaa.gov/hrrr/) NWP forecasts and saves them to S3 as NetCDF. Default configuration fetches 18 hours of hourly precipitation.

| Variable | Description | Units |
|---|---|---|
| `qpf_1hr` | Hourly quantitative precipitation forecast (`tp`) | kg m⁻² |
| `temp_2m` | 2-metre air temperature | K |

### `rtma/` — RTMA Analysis
Fetches [RTMA](https://www.nco.ncep.noaa.gov/pmb/products/rtma/) real-time mesoscale analysis fields and saves them to S3 as NetCDF.

| Variable | Description | Units |
|---|---|---|
| `temp_2m` | 2-metre air temperature analysis | K |

### `usgs/` — USGS NWIS Streamgages
Fetches real-time observations from [USGS NWIS](https://waterdata.usgs.gov/nwis) for a list of gage IDs supplied via a Parquet payload file, and saves records to S3 as Parquet.

| Parameter | Variable name |
|---|---|
| `00060` | `discharge` |
| `00065` | `gage_height` |
| `00045` | `precipitation` |
| `62614–62618` | lake elevation / stage variants |

### `etl/` — Consolidate & HMS Forcing
Two Lambda containers built from this package:

- **`Dockerfile.consolidate`** — reads staged NetCDF/Parquet files from S3, validates each record against the appropriate schema in `schemas/`, and appends to Icechunk (gridded) or Iceberg (point) stores. Behaviour is driven by a config Parquet file (see `etl/configs/consolidate_config.yaml` for the canonical source layout).
- **`Dockerfile.forcing`** — assembles an HMS-ready forcing dataset by combining MRMS QPE and HRRR/RTMA fields from Icechunk into a single xarray Dataset and writing it back to S3.

## Schemas

JSON schemas in `schemas/` define the expected structure of each data source after transformation. They are used by the consolidate ETL to validate data before writing to long-term storage.

| File | Applies to |
|---|---|
| `schemas/gridded-analysis.json` | MRMS (NetCDF → Icechunk) |
| `schemas/gridded-forecast.json` | HRRR, RTMA (NetCDF → Icechunk) |
| `schemas/point.json` | USGS NWIS (Parquet → Iceberg) |


## Building & Deploying

Use `build_and_push.sh` to build and push any container to ECR:

```bash
./build_and_push.sh <path_to_dockerfile> <ecr_image_uri>
# Example:
./build_and_push.sh mrms/Dockerfile.mrms 123456789.dkr.ecr.us-east-1.amazonaws.com/mrms-reaper:v0.1
```

All images target `linux/amd64` for Lambda compatibility.

## Local Testing

Lambda containers can be tested locally using the Lambda Runtime Interface Emulator. Start the container on port 9000, then invoke it:

```bash
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"source": "hrrr"}'
```

