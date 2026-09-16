"""Flood system status checker. Determines blue (clear) or gray (rainy) skies."""

import json
import logging
from datetime import datetime, timezone

import s3fs

from sources import MRMSChecker, NBMChecker, USGSChecker, WPCEROChecker

logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)

CHECKERS = [NBMChecker, WPCEROChecker, USGSChecker, MRMSChecker]

LIVE_STATUS_PREFIX = 's3://flood-warning/status/live/'
ARCHIVED_STATUS_PREFIX = 's3://flood-warning/status/archive/'

def archive_existing_status(fs: s3fs.S3FileSystem, live_prefix: str, archive_prefix: str):
    """Move existing blue.json or gray.json to archive with a timestamp suffix."""
    live_path = live_prefix.replace("s3://", "")

    try:
        existing = fs.ls(live_path, detail=False)
    except FileNotFoundError:
        logger.info("No existing status files to archive")
        return

    now_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

    for filepath in existing:
        filename = filepath.split("/")[-1]
        if filename not in ("blue.json", "gray.json"):
            continue

        base = filename.replace(".json", "")
        archive_name = f"{base}_{now_str}.json"
        archive_dest = f"{archive_prefix}{archive_name}"

        fs.copy(f"s3://{filepath}", archive_dest)
        fs.rm(f"s3://{filepath}")
        logger.info(f"Archived {filename} → {archive_name}")


def run_checks() -> list[dict]:
    """Instantiate and run all source checkers."""
    results = []
    for checker_cls in CHECKERS:
        try:
            result = checker_cls().check()
            results.append(result)
        except Exception as e:
            logger.error(f"Error running {checker_cls.__name__}: {e}", exc_info=True)
            results.append(
                {
                    "source": checker_cls.__name__,
                    "threshold_met": False,
                    "details": {"error": str(e)},
                }
            )
    return results


def determine_status(results: list[dict]) -> str:
    """Return 'gray' if any threshold is met, otherwise 'blue'."""
    return "gray" if any(r["threshold_met"] for r in results) else "blue"


def write_status(fs: s3fs.S3FileSystem, status: str, results: list[dict], live_prefix: str) -> dict:
    """Write the status JSON to the live prefix."""
    payload = {
        "status": status,
        "timestamp": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sources": results,
    }

    path = f"{live_prefix}{status}.json"
    with fs.open(path, "w") as f:
        json.dump(payload, f, indent=2)

    logger.info(f"Wrote {status}.json to {path}")
    return payload


def handler(event=None, context=None):
    """AWS Lambda handler."""
    if event is None:
        event = {}

    fs = s3fs.S3FileSystem()

    # Step 1: Archive any existing status file
    archive_existing_status(fs, LIVE_STATUS_PREFIX, ARCHIVED_STATUS_PREFIX)

    # Step 2: Run source checks
    results = run_checks()

    # Step 3: Determine overall status
    status = determine_status(results)

    # Step 4: Write new status JSON
    payload = write_status(fs, status, results, LIVE_STATUS_PREFIX)

    return {
        "statusCode": 200,
        "body": payload,
    }
