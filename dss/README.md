# DSS Converter

Convert between HEC-DSS and Parquet file formats.

## Features

- **DSS → Parquet**: Extract time series data from DSS files with full path information (A-F parts)
- **Parquet → DSS**: Write Parquet data back to DSS format
- **Optional model name**: Tag exported data with a model identifier
- **Docker support**: Containerized for consistent environments

## Quick Start

### Build
```bash
docker build -t dss-converter .
```

### Convert DSS to Parquet
```bash
docker run --rm -v /data:/data dss-converter \
  dss-to-parquet /data/input.dss -o /data/output -m mymodel
```

### Convert Parquet to DSS
```bash
docker run --rm -v /data:/data dss-converter \
  parquet-to-dss /data/output.parquet -o /data/output.dss
```

## Commands

### dss-to-parquet
```
dss-to-parquet INPUT [-o OUTPUT] [-m MODEL_NAME] [-d] [-q]
```
- `INPUT`: Path to DSS file
- `-o, --output`: Output directory (default: same as input)
- `-m, --model-name`: Optional model name to include in output
- `-d, --debug`: Enable debug logging
- `-q, --quiet`: Suppress DSS library output

### parquet-to-dss
```
parquet-to-dss INPUT [-o OUTPUT] [-d] [-q]
```
- `INPUT`: Path to Parquet file
- `-o, --output`: Output DSS file path (default: input_basename.dss)
- `-d, --debug`: Enable debug logging
- `-q, --quiet`: Suppress output

## Parquet Schema

Output parquet includes:
- `datetime`: Timestamp with timezone
- `value`: Data value
- `A-F`: DSS path parts
- `model_name`: (optional) Model identifier

## Development

Install dependencies:
```bash
uv sync
```

Run converter locally:
```bash
uv run python -m converter.main dss-to-parquet /path/to/file.dss
```
