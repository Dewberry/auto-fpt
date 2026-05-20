from pathlib import Path
from typing import Union
from datetime import datetime, UTC

from etl.gridded.gridded import process_gridded_forcing_ds
from etl.shared.utils import load_config_parquet, upload_ds_to_s3, parse_tz_aware_time
from etl.shared._logging import logger, configure_logger
from etl.gridded.icechunk_utils import IcechunkManager

configure_logger(level="INFO")

DEFAULT_CONFIG_PATH = 's3://flood-warning/staging/payloads/hms-forcing-config.pq'
DEFAULT_CONSOLIDATE_CONFIG_PATH = 's3://flood-warning/dev/config.pq'
DEFAULT_OUTPUT_PREFIX = 's3://flood-warning/staging/temporary/forcing'


def process_forcing_sources(source: Union[str, list[str]], forcing_config_path: str, consolidate_config_path: str , output_prefix: str, end_date: datetime | None = None ):
    """Process one or more forcing sources as specified in the forcing config file."""

    forcing_config = load_config_parquet(sources=source, config_path=forcing_config_path)
    consolidate_config = load_config_parquet(sources="all", config_path=consolidate_config_path)

    successful_sources = []
    failed_sources = []

    for source_name, source_config in forcing_config.items():
        try:
            logger.info(f"Processing source: {source_name}")
            consolidate_source_name = source_config.get('consolidate_source_name', source_name)
            cfg = consolidate_config[consolidate_source_name]

            dest_type, bucket, region, prefix, append_param = cfg['dest_type'], cfg['bucket'], cfg['region'], cfg['prefix'], cfg['append_param']
            group = cfg.get('group')
            filename, lookback_hours, variable = source_config['filename'], source_config.get('lookback_hours'), source_config.get('variable')

            dt_now = end_date or datetime.now(UTC)
            output_path = f"{output_prefix}/{dt_now.year}/{dt_now.month:02d}/{dt_now.day:02d}/{dt_now.hour:02d}/{filename}"

            if dest_type == "icechunk":
                manager = IcechunkManager(bucket=bucket, prefix=prefix, region=region)
                ds = manager.read_icechunk(group=group)

                processed_ds = process_gridded_forcing_ds(ds, variable=variable, append_param=append_param, lookback_hours=lookback_hours, end_date=dt_now)
                upload_ds_to_s3(processed_ds, output_path)
            else:
                raise NotImplementedError(f"Destination type '{dest_type}' not yet supported.")
            
            successful_sources.append(source_name)
            logger.info(f"Successfully processed source: {source_name}. Output path: {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing source {source_name}: {e}")
            failed_sources.append(source_name)

    return {
        "successful_sources": successful_sources,
        "failed_sources": failed_sources
    }

def handler(event=None, context=None):
    if event is None:
        event = {}
        
    source = event.get('source', "all")
    forcing_config_path = event.get('forcing_config_path', DEFAULT_CONFIG_PATH)
    consolidate_config_path = event.get('consolidate_config_path', DEFAULT_CONSOLIDATE_CONFIG_PATH)
    output_prefix = event.get('output_prefix', DEFAULT_OUTPUT_PREFIX)
    end_date_str = event.get('end_date')
    end_date = parse_tz_aware_time(end_date_str) if end_date_str else None

    
    result = process_forcing_sources(
        source=source,
        forcing_config_path=forcing_config_path,
        consolidate_config_path=consolidate_config_path,
        output_prefix=output_prefix,
        end_date=end_date
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
