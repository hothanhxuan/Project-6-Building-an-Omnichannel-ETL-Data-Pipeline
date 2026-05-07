"""
TechStore Vietnam - E-commerce Analytics ETL Pipeline
Main entry point for running the ETL pipeline.

Usage:
    python main.py --full          # Run full pipeline (extract + transform + load)
    python main.py --extract       # Extract only
    python main.py --transform     # Extract + transform only
    python main.py --info          # Show BigQuery table info
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from orchestration.pipeline_orchestrator import PipelineOrchestrator
from loaders.bigquery_loader import BigQueryLoader
from utils.logger import setup_logger


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description="TechStore Vietnam E-commerce Analytics ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --full          Run the complete ETL pipeline
  python main.py --extract       Extract data from GCS only
  python main.py --transform     Extract and transform (no BigQuery load)
  python main.py --info          Show BigQuery table information
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full", action="store_true",
                       help="Run full pipeline: extract → transform → load → views")
    group.add_argument("--extract", action="store_true",
                       help="Extract data from GCS only")
    group.add_argument("--transform", action="store_true",
                       help="Extract and transform (no BigQuery load)")
    group.add_argument("--info", action="store_true",
                       help="Show BigQuery table information")

    args = parser.parse_args()
    logger = setup_logger("Main")

    try:
        if args.full:
            logger.info("Mode: FULL PIPELINE")
            orchestrator = PipelineOrchestrator()
            orchestrator.run_full_pipeline()

        elif args.extract:
            logger.info("Mode: EXTRACT ONLY")
            orchestrator = PipelineOrchestrator()
            raw_data = orchestrator.run_extract_only()
            logger.info("Extracted data summary:")
            for key, data in raw_data.items():
                count = len(data) if isinstance(data, list) else "N/A"
                logger.info(f"  {key}: {count} records")

        elif args.transform:
            logger.info("Mode: EXTRACT + TRANSFORM")
            orchestrator = PipelineOrchestrator()
            transformed = orchestrator.run_transform_only()
            logger.info("Transformed data summary:")
            for key, df in transformed.items():
                logger.info(f"  {key}: {len(df)} rows, {list(df.columns)}")

        elif args.info:
            logger.info("Mode: TABLE INFO")
            loader = BigQueryLoader()
            tables = [
                "dim_customers", "dim_products", "dim_locations",
                "dim_staff", "dim_date",
                "fact_orders", "fact_order_items", "fact_payments",
                "fact_cart_events", "fact_bank_transactions"
            ]
            for table in tables:
                info = loader.get_table_info(table)
                logger.info(f"  {info}")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()