"""
BigQuery Loader - Handles loading data into BigQuery and managing schemas.
Creates datasets, tables, and analytical views.
"""

import pandas as pd
from google.cloud import bigquery
from utils.config import get_credentials, get_project_id, get_dataset_id
from utils.logger import setup_logger


class BigQueryLoader:
    """Loads DataFrames into BigQuery and manages dataset/table schemas."""

    def __init__(self, project_id: str = None, dataset_id: str = None):
        """
        Initialize BigQuery loader.

        Args:
            project_id: GCP project ID (default: from .env).
            dataset_id: BigQuery dataset name (default: from .env).
        """
        self.logger = setup_logger(self.__class__.__name__)
        self.project_id = project_id or get_project_id()
        self.dataset_id = dataset_id or get_dataset_id()

        credentials = get_credentials()
        self.client = bigquery.Client(
            credentials=credentials,
            project=self.project_id
        )
        self.dataset_ref = f"{self.project_id}.{self.dataset_id}"

    def create_dataset_if_not_exists(self, location: str = "US"):
        """
        Create the BigQuery dataset if it doesn't already exist.

        Args:
            location: Dataset location (default: US).
        """
        dataset = bigquery.Dataset(self.dataset_ref)
        dataset.location = location
        dataset.description = "TechStore Vietnam Analytics Data Warehouse"

        try:
            self.client.get_dataset(self.dataset_ref)
            self.logger.info(f"Dataset '{self.dataset_ref}' already exists")
        except Exception:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            self.logger.info(f"Created dataset '{self.dataset_ref}' in {location}")

    def load_dataframe(self, table_name: str, df: pd.DataFrame,
                       write_disposition: str = "WRITE_TRUNCATE",
                       partition_field: str = None,
                       clustering_fields: list = None) -> int:
        """
        Load a pandas DataFrame into a BigQuery table.

        Args:
            table_name: Target table name (without dataset prefix).
            df: DataFrame to load.
            write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY.
            partition_field: Optional field for time partitioning.
            clustering_fields: Optional list of fields for clustering.

        Returns:
            Number of rows loaded.
        """
        if df.empty:
            self.logger.warning(f"DataFrame for '{table_name}' is empty. Skipping load.")
            return 0

        table_ref = f"{self.dataset_ref}.{table_name}"
        self.logger.info(
            f"Loading {len(df)} rows into {table_ref} "
            f"(disposition: {write_disposition})..."
        )

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,
        )

        # Add time partitioning if specified
        if partition_field and partition_field in df.columns:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
            self.logger.info(f"  Partitioning on: {partition_field}")

        # Add clustering if specified
        if clustering_fields:
            valid_fields = [f for f in clustering_fields if f in df.columns]
            if valid_fields:
                job_config.clustering_fields = valid_fields
                self.logger.info(f"  Clustering on: {valid_fields}")

        try:
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()  # Wait for completion

            table = self.client.get_table(table_ref)
            self.logger.info(
                f"Successfully loaded {table.num_rows} rows into {table_ref}"
            )
            return table.num_rows

        except Exception as e:
            error_msg = str(e)
            # Handle incompatible partitioning: drop table and retry
            if "Incompatible table partitioning specification" in error_msg:
                self.logger.warning(
                    f"Table {table_ref} has incompatible partitioning. "
                    f"Dropping and recreating..."
                )
                try:
                    self.client.delete_table(table_ref)
                    self.logger.info(f"Deleted incompatible table: {table_ref}")

                    # Retry the load — table will be created with correct partitioning
                    job = self.client.load_table_from_dataframe(
                        df, table_ref, job_config=job_config
                    )
                    job.result()

                    table = self.client.get_table(table_ref)
                    self.logger.info(
                        f"Successfully loaded {table.num_rows} rows into {table_ref} "
                        f"(recreated with correct partitioning)"
                    )
                    return table.num_rows
                except Exception as retry_err:
                    self.logger.error(f"Retry failed for {table_ref}: {retry_err}")
                    raise

            self.logger.error(f"Failed to load data into {table_ref}: {e}")
            raise

    def execute_query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query on BigQuery.

        Args:
            sql: SQL query string.

        Returns:
            Query results as a DataFrame.
        """
        self.logger.info(f"Executing query: {sql[:100]}...")
        try:
            result = self.client.query(sql).result()
            df = result.to_dataframe()
            self.logger.info(f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise

    def create_views(self):
        """
        Create analytical views in BigQuery:
        - vw_customer_journey
        - vw_cashflow_daily
        - vw_payment_status
        """
        self.logger.info("Creating analytical views...")

        views = {
            "vw_customer_journey": self._get_customer_journey_view_sql(),
            "vw_cashflow_daily": self._get_cashflow_daily_view_sql(),
            "vw_payment_status": self._get_payment_status_view_sql(),
        }

        for view_name, sql in views.items():
            try:
                self.client.query(sql).result()
                self.logger.info(f"Created view: {self.dataset_ref}.{view_name}")
            except Exception as e:
                self.logger.error(f"Failed to create view '{view_name}': {e}")

    def _get_customer_journey_view_sql(self) -> str:
        """SQL for vw_customer_journey view."""
        return f"""
        CREATE OR REPLACE VIEW `{self.dataset_ref}.vw_customer_journey` AS
        WITH customer_events AS (
            SELECT
                ce.customer_id,
                ce.session_id,
                ce.event_type,
                ce.event_timestamp,
                ce.product_id,
                ce.source AS traffic_source,
                ce.device,
                ce.utm_source,
                ce.utm_campaign,
                ROW_NUMBER() OVER (
                    PARTITION BY ce.customer_id
                    ORDER BY ce.event_timestamp
                ) AS touchpoint_sequence
            FROM `{self.dataset_ref}.fact_cart_events` ce
            WHERE ce.customer_id IS NOT NULL
        ),
        customer_purchases AS (
            SELECT
                customer_id,
                MIN(order_date) AS first_purchase_date,
                COUNT(*) AS total_purchases,
                SUM(total_vnd) AS total_revenue_vnd
            FROM `{self.dataset_ref}.fact_orders`
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        )
        SELECT
            e.customer_id,
            c.full_name,
            c.customer_segment,
            e.session_id,
            e.event_type,
            e.event_timestamp,
            e.touchpoint_sequence,
            e.product_id,
            e.traffic_source,
            e.device,
            e.utm_source,
            e.utm_campaign,
            p.first_purchase_date,
            p.total_purchases,
            p.total_revenue_vnd,
            TIMESTAMP_DIFF(p.first_purchase_date, MIN(e.event_timestamp) OVER (PARTITION BY e.customer_id), DAY) AS days_to_first_purchase
        FROM customer_events e
        LEFT JOIN `{self.dataset_ref}.dim_customers` c
            ON e.customer_id = c.customer_id
        LEFT JOIN customer_purchases p
            ON e.customer_id = p.customer_id
        """

    def _get_cashflow_daily_view_sql(self) -> str:
        """SQL for vw_cashflow_daily view."""
        return f"""
        CREATE OR REPLACE VIEW `{self.dataset_ref}.vw_cashflow_daily` AS
        WITH daily_revenue AS (
            SELECT
                DATE(order_date) AS report_date,
                SUM(total_vnd) AS sales_revenue_vnd,
                COUNT(*) AS order_count
            FROM `{self.dataset_ref}.fact_orders`
            WHERE status NOT IN ('cancelled', 'refunded')
            GROUP BY DATE(order_date)
        ),
        daily_payments AS (
            SELECT
                DATE(payment_date) AS report_date,
                SUM(CASE WHEN payment_status = 'success' THEN amount_vnd ELSE 0 END) AS payments_received_vnd,
                COUNT(CASE WHEN payment_status = 'success' THEN 1 END) AS successful_payments,
                COUNT(CASE WHEN payment_status != 'success' THEN 1 END) AS failed_payments
            FROM `{self.dataset_ref}.fact_payments`
            GROUP BY DATE(payment_date)
        ),
        daily_bank AS (
            SELECT
                DATE(transaction_date) AS report_date,
                SUM(CASE WHEN amount_vnd > 0 THEN amount_vnd ELSE 0 END) AS bank_inflow_vnd,
                SUM(CASE WHEN amount_vnd < 0 THEN ABS(amount_vnd) ELSE 0 END) AS bank_outflow_vnd
            FROM `{self.dataset_ref}.fact_bank_transactions`
            WHERE status IN ('posted', 'pending')
            GROUP BY DATE(transaction_date)
        )
        SELECT
            COALESCE(r.report_date, p.report_date, b.report_date) AS report_date,
            COALESCE(r.sales_revenue_vnd, 0) AS sales_revenue_vnd,
            COALESCE(r.order_count, 0) AS order_count,
            COALESCE(p.payments_received_vnd, 0) AS payments_received_vnd,
            COALESCE(p.successful_payments, 0) AS successful_payments,
            COALESCE(p.failed_payments, 0) AS failed_payments,
            COALESCE(b.bank_inflow_vnd, 0) AS bank_inflow_vnd,
            COALESCE(b.bank_outflow_vnd, 0) AS bank_outflow_vnd,
            COALESCE(r.sales_revenue_vnd, 0) - COALESCE(b.bank_outflow_vnd, 0) AS net_cashflow_vnd
        FROM daily_revenue r
        FULL OUTER JOIN daily_payments p ON r.report_date = p.report_date
        FULL OUTER JOIN daily_bank b ON COALESCE(r.report_date, p.report_date) = b.report_date
        ORDER BY report_date
        """

    def _get_payment_status_view_sql(self) -> str:
        """SQL for vw_payment_status view."""
        return f"""
        CREATE OR REPLACE VIEW `{self.dataset_ref}.vw_payment_status` AS
        SELECT
            o.order_key,
            o.order_id,
            o.order_number,
            o.customer_id,
            c.full_name AS customer_name,
            c.email AS customer_email,
            o.order_date,
            o.channel,
            o.total_vnd,
            o.payment_status AS order_payment_status,
            o.payment_gateway,
            p.payment_key,
            p.payment_date,
            p.payment_status AS gateway_payment_status,
            p.amount_vnd AS paid_amount_vnd,
            CASE
                WHEN p.payment_status = 'success' THEN 'Paid'
                WHEN p.payment_status = 'failed' THEN 'Failed'
                WHEN p.payment_date IS NULL AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), o.order_date, DAY) > 30 THEN 'Overdue'
                WHEN p.payment_date IS NULL THEN 'Pending'
                ELSE 'Unknown'
            END AS payment_status_category,
            CASE
                WHEN p.payment_date IS NOT NULL THEN
                    TIMESTAMP_DIFF(p.payment_date, o.order_date, HOUR)
                ELSE NULL
            END AS payment_delay_hours,
            CASE
                WHEN p.payment_status != 'success' OR p.payment_status IS NULL THEN
                    o.total_vnd - COALESCE(p.amount_vnd, 0)
                ELSE 0
            END AS outstanding_amount_vnd,
            CASE
                WHEN p.payment_date IS NULL AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), o.order_date, DAY) > 30 THEN
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), o.order_date, DAY)
                ELSE 0
            END AS days_overdue
        FROM `{self.dataset_ref}.fact_orders` o
        LEFT JOIN `{self.dataset_ref}.dim_customers` c
            ON o.customer_id = c.customer_id
        LEFT JOIN `{self.dataset_ref}.fact_payments` p
            ON o.transaction_id = p.transaction_id
        """

    def get_table_info(self, table_name: str) -> dict:
        """
        Get information about a BigQuery table.

        Args:
            table_name: Table name.

        Returns:
            Dict with table info (num_rows, size_bytes, etc.)
        """
        table_ref = f"{self.dataset_ref}.{table_name}"
        try:
            table = self.client.get_table(table_ref)
            return {
                "table": table_ref,
                "num_rows": table.num_rows,
                "size_bytes": table.num_bytes,
                "created": str(table.created),
                "modified": str(table.modified),
            }
        except Exception as e:
            self.logger.error(f"Cannot get info for {table_ref}: {e}")
            return {"table": table_ref, "error": str(e)}
