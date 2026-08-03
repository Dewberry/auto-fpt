from pathlib import Path
from typing import Union
import s3fs

from etl.point.point import process_point_data_source
from etl.gridded.gridded import process_gridded_source
from etl.shared.utils import load_config_parquet
from etl.shared._logging import logger, configure_logger


configure_logger(level="INFO")
SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
DEFAULT_CONFIG_PATH = 's3://flood-warning/dev/consolidate_config.pq'
HMS_SOURCES = ["hms-forecast","hms-stats"]

def process_sources(source: Union[str, list[str]], config_path: str):
    """Process one or more sources as specified in the config file."""

    config = load_config_parquet(sources=source, config_path=config_path)
    fs = s3fs.S3FileSystem(anon=False)

    successful_sources = []
    failed_sources = []

    for source_name, source_config in config.items():
        # Remove HMS sources from the list if 'all' is specified
        if source_name in HMS_SOURCES and source == 'all':
            logger.info(f"Skipping source: {source_name} (not applicable for 'all')")
            continue

        logger.info(f"Processing source: {source_name}")
        try:
            dest_type= source_config['dest_type']
            schema = source_config.get('schema')
            if schema:
                schema_path = str(SCHEMA_DIR / schema)
            else:
                schema_path = None

            input_prefix= source_config['input_prefix']
            bucket= source_config['bucket']
            region= source_config['region']
            prefix= source_config['prefix']
            append_param= source_config['append_param']

            if dest_type == "icechunk":
                group = source_config.get('group')
                
                process_gridded_source(
                    fs=fs,
                    input_prefix=input_prefix,
                    schema_path=schema_path,
                    bucket=bucket,
                    icechunk_prefix=prefix,
                    concat_dim=append_param,
                    region=region,
                    group=group
                )

            elif dest_type == "iceberg":
                table = source_config['table']
                namespace = source_config['namespace']
                warehouse_path = f"s3://{bucket}/{prefix}"
                file_pattern = source_config.get('file_pattern')

                if source_name in HMS_SOURCES:
                    run_time_col = append_param
                    delete_files = False
                    find_latest = True
                else:
                    run_time_col = None
                    delete_files = True
                    find_latest = False

                process_point_data_source(
                    fs=fs,
                    input_prefix=input_prefix,
                    schema_path=schema_path,
                    warehouse_path=warehouse_path,
                    namespace=namespace,
                    table_name=table,
                    time_col=append_param,
                    region=region,
                    file_pattern=file_pattern,
                    run_time_col=run_time_col,
                    delete_files=delete_files,
                    find_latest=find_latest
                )
            else:
                raise ValueError(f"Unsupported dest_type '{dest_type}' for source '{source_name}'")
            
            logger.info(f"Finished processing source: {source_name}")
            successful_sources.append(source_name)

        except Exception as e:
            logger.error(f"Error processing source {source_name}: {e}")
            failed_sources.append(source_name)

    return {"successful_sources": successful_sources, "failed_sources": failed_sources}


def handler(event=None, context=None):
    if event is None:
        event = {}
        
    source = event['source']
    config_path = event.get('config_path', DEFAULT_CONFIG_PATH)
    
    result = process_sources(
        source=source,
        config_path=config_path
    )

    failed_sources = result["failed_sources"]
    if failed_sources:
        raise RuntimeError(f"Task failed for sources: {', '.join(failed_sources)}")

    return {
        "statusCode": 200,
        "body": {
            "message": "Task completed successfully",
            "source": source,
            "result": result
        }
    }