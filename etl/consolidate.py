from pathlib import Path
from typing import Union
import s3fs

from etl.point.point import process_point_data_source
from etl.gridded.gridded import process_gridded_source
from etl.shared.utils import load_config_parquet
from etl.shared._logging import logger, configure_logger


configure_logger(level="INFO")
SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
DEFAULT_CONFIG_PATH = 's3://flood-warning/dev/config.pq'


def process_sources(source: Union[str, list[str]], config_path: str):
    """Process one or more sources as specified in the config file."""

    config = load_config_parquet(sources=source, config_path=config_path)
    fs = s3fs.S3FileSystem(anon=False)

    successful_sources = []
    failed_sources = []

    for source, source_config in config.items():
        logger.info(f"Processing source: {source}")
        try:
            dest_type= source_config['dest_type']
            schema = str(SCHEMA_DIR / source_config['schema'])
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
                    schema_path=schema,
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

                process_point_data_source(
                    fs=fs,
                    input_prefix=input_prefix,
                    schema_path=schema,
                    warehouse_path=warehouse_path,
                    namespace=namespace,
                    table_name=table,
                    time_col=append_param,
                    region=region
                )
            else:
                raise ValueError(f"Unsupported dest_type '{dest_type}' for source '{source}'")
            
            logger.info(f"Finished processing source: {source}")
            successful_sources.append(source)

        except Exception as e:
            logger.error(f"Error processing source {source}: {e}")
            failed_sources.append(source)

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