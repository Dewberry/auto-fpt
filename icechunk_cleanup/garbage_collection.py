"""Script to run garbage collection on Icechunk stores based on a configuration file."""
import logging
import icechunk
import pandas as pd
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_CONFIG_PATH = "s3://flood-warning/dev/consolidate_config.pq"
DEFAULT_EXPIRY_DAYS = 21


def garbage_collect_store(bucket: str, prefix: str, region: str, expiry_days: int = DEFAULT_EXPIRY_DAYS):
    """Run snapshot expiration and garbage collection on a single Icechunk store."""
    store_path = f"s3://{bucket}/{prefix}"
    logger.info(f"Opening Icechunk store: {store_path}")

    storage = icechunk.s3_storage(bucket=bucket, prefix=prefix, region=region)
    repo = icechunk.Repository.open(storage)

    ancestry = list(repo.ancestry(branch="main"))
    logger.info(f"Store {store_path}: {len(ancestry)} snapshots on main branch")

    expiry_time = datetime.now(tz=timezone.utc) - timedelta(days=expiry_days)
    expired = repo.expire_snapshots(older_than=expiry_time)
    logger.info(f"Store {store_path}: expired {len(expired)} snapshots older than {expiry_time.isoformat()}")

    results = repo.garbage_collect(delete_object_older_than = expiry_time)
    logger.info(f"Store {store_path}: garbage collection results: {results}")

    return {
        "store": store_path,
        "total_snapshots": len(ancestry),
        "expired_snapshots": len(expired),
        "gc_results": str(results),
    }


def run_garbage_collection(config_path: str, expiry_days: int = DEFAULT_EXPIRY_DAYS):
    """Run garbage collection on all unique Icechunk stores from the config."""
    df = pd.read_parquet(config_path)
    icechunk_df = df[df["dest_type"] == "icechunk"]

    # Deduplicate by bucket/prefix (e.g. mrms has two groups under the same store)
    unique_stores = icechunk_df.drop_duplicates(subset=["bucket", "prefix"])

    successful = []
    failed = []

    for _, row in unique_stores.iterrows():
        source = row["source"]
        bucket = row["bucket"]
        prefix = row["prefix"]
        region = row.get("region", "us-east-1")

        try:
            result = garbage_collect_store(
                bucket=bucket, prefix=prefix, region=region, expiry_days=expiry_days
            )
            result["source"] = source
            successful.append(result)
        except Exception as e:
            logger.error(f"Error running garbage collection for {source} (s3://{bucket}/{prefix}): {e}")
            failed.append({"source": source, "store": f"s3://{bucket}/{prefix}", "error": str(e)})

    return {"successful": successful, "failed": failed}


def handler(event=None, context=None):
    if event is None:
        event = {}

    config_path = event.get("config_path", DEFAULT_CONFIG_PATH)
    expiry_days = event.get("expiry_days", DEFAULT_EXPIRY_DAYS)

    result = run_garbage_collection(config_path=config_path, expiry_days=expiry_days)

    if result["failed"]:
        failed_names = [f["source"] for f in result["failed"]]
        raise RuntimeError(f"Garbage collection failed for stores: {', '.join(failed_names)}")

    return {
        "statusCode": 200,
        "body": {
            "message": "Garbage collection completed successfully",
            "result": result,
        },
    }