import argparse
import json
import logging
import sys
from pathlib import Path

from converter.parquet import run
from converter.convert import parquet_to_dss

logger = logging.getLogger("auto-fpt-dss")


def setup_logging(debug: bool = False, quiet: bool = False):
    """Configure logging."""
    level = logging.DEBUG if debug else (logging.ERROR if quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
    )
    # Suppress HecDSS library logs
    logging.getLogger("hecdss").setLevel(logging.ERROR)


def dss_to_parquet_cmd(args) -> None:
    """Convert DSS file to Parquet format."""
    setup_logging(args.debug, args.quiet)

    # Validate input file exists
    input_path = Path(args.input_dss)
    if not input_path.exists():
        logger.error(f"Input file not found: {args.input_dss}")
        sys.exit(1)

    # Determine output directory
    output_dir = args.output or str(input_path.parent)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting DSS to Parquet conversion")
    logger.info(f"Input: {args.input_dss}")
    logger.info(f"Output: {output_dir}")

    try:
        manifest = run(
            input_dss_path=str(input_path),
            output_dir=output_dir,
            groupby="F",
            strip_suffix=True,
            include_parts=["B", "C"],
            group_workers=4,
            dss_workers=1,
            event_id=None,
            simulation_name="",
            model_name=args.model_name or "",
            suppress_dss_output=args.quiet,
        )

        print(json.dumps(manifest, indent=2))
        logger.info("Conversion completed successfully")
    except Exception as e:
        logger.error(f"Conversion failed: {e}", exc_info=True)
        sys.exit(1)


def parquet_to_dss_cmd(args) -> None:
    """Convert Parquet file to DSS format."""
    setup_logging(args.debug, args.quiet)

    # Validate input file exists
    input_path = Path(args.input_parquet)
    if not input_path.exists():
        logger.error(f"Input file not found: {args.input_parquet}")
        sys.exit(1)

    # Determine output path
    output_path = args.output or str(input_path.parent / f"{input_path.stem}.dss")

    logger.info(f"Starting Parquet to DSS conversion")
    logger.info(f"Input: {args.input_parquet}")
    logger.info(f"Output: {output_path}")

    try:
        manifest = parquet_to_dss(
            parquet_path=str(input_path),
            output_dss_path=output_path,
            path_f_part=None,
        )

        print(json.dumps(manifest, indent=2))
        logger.info("Conversion completed successfully")
    except Exception as e:
        logger.error(f"Conversion failed: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AUTO FPT DSS utilities - Convert between DSS and Parquet formats"
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Conversion direction")

    # DSS to Parquet command
    dss_to_pq = subparsers.add_parser("dss-to-parquet", help="Convert DSS file to Parquet format")
    dss_to_pq.add_argument("input_dss", help="Path to input DSS file")
    dss_to_pq.add_argument(
        "-o", "--output",
        default=None,
        help="Output directory for Parquet files (default: same directory as input)"
    )
    dss_to_pq.add_argument(
        "-m", "--model-name",
        default=None,
        help="Optional model name to include in parquet output"
    )
    dss_to_pq.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    dss_to_pq.add_argument("-q", "--quiet", action="store_true", help="Suppress DSS library output")
    dss_to_pq.set_defaults(func=dss_to_parquet_cmd)

    # Parquet to DSS command
    pq_to_dss = subparsers.add_parser("parquet-to-dss", help="Convert Parquet file to DSS format")
    pq_to_dss.add_argument("input_parquet", help="Path to input Parquet file")
    pq_to_dss.add_argument(
        "-o", "--output",
        default=None,
        help="Output DSS file path (default: input_basename.dss in same directory)"
    )
    pq_to_dss.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    pq_to_dss.add_argument("-q", "--quiet", action="store_true", help="Suppress output")
    pq_to_dss.set_defaults(func=parquet_to_dss_cmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
