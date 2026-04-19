"""
Dimension Transformer - Transforms raw data into dimension tables.
Creates: dim_customers, dim_products, dim_locations, dim_staff, dim_date.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from transformers.base_transformer import BaseTransformer


class DimensionTransformer(BaseTransformer):
    """Transforms raw extracted data into dimension tables for the star schema."""

    def __init__(self):
        """Initialize dimension transformer."""
        super().__init__()

    def transform_dim_customers(self, raw_customers: list) -> pd.DataFrame:
        """
        Transform raw customer data into dim_customers.

        Columns: customer_id, email, full_name, phone, city, country,
                 created_at, customer_segment, lifetime_value_vnd,
                 total_orders, first_order_date, last_order_date

        Args:
            raw_customers: List of customer dicts from GCS.

        Returns:
            DataFrame for dim_customers.
        """
        self.logger.info("Transforming dim_customers...")
        df = pd.DataFrame(raw_customers)
        df = self.standardize_columns(df)

        # Rename columns to match schema
        column_map = {
            "id": "customer_id",
            "name": "full_name",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Parse datetime
        df = self.parse_datetime(df, ["created_at"])

        # Select and order columns
        dim_columns = [
            "customer_id", "email", "full_name", "phone",
            "city", "country", "created_at"
        ]
        for col in dim_columns:
            if col not in df.columns:
                df[col] = None

        df = df[dim_columns].copy()

        # Initialize aggregation columns (to be updated after fact tables are loaded)
        df["customer_segment"] = "New"
        df["lifetime_value_vnd"] = 0
        df["total_orders"] = 0
        df["first_order_date"] = pd.NaT
        df["last_order_date"] = pd.NaT

        # Data quality checks
        df = self.check_nulls(df, ["customer_id", "email"], "dim_customers")
        df = self.check_duplicates(df, ["customer_id"], "dim_customers")
        df = self.validate_date_range(df, "created_at", table_name="dim_customers")

        self.logger.info(f"dim_customers: {len(df)} rows")
        return df

    def transform_dim_products(self, raw_products: list) -> pd.DataFrame:
        """
        Transform raw product data into dim_products.

        Columns: product_id, product_name, sku, barcode, category, brand,
                 price_vnd, price_usd, stock_quantity, is_active

        Args:
            raw_products: List of product dicts from GCS.

        Returns:
            DataFrame for dim_products.
        """
        self.logger.info("Transforming dim_products...")
        df = pd.DataFrame(raw_products)
        df = self.standardize_columns(df)

        # Rename columns to match schema
        column_map = {
            "id": "product_id",
            "name": "product_name",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Select and order columns
        dim_columns = [
            "product_id", "product_name", "sku", "barcode", "category",
            "brand", "price_vnd", "price_usd", "stock_quantity"
        ]
        for col in dim_columns:
            if col not in df.columns:
                df[col] = None

        df = df[dim_columns].copy()
        df["is_active"] = True

        # Data quality checks
        df = self.check_nulls(df, ["product_id", "product_name", "sku"], "dim_products")
        df = self.check_duplicates(df, ["product_id"], "dim_products")
        df = self.validate_amounts(df, ["price_vnd", "price_usd"], table_name="dim_products")

        self.logger.info(f"dim_products: {len(df)} rows")
        return df

    def transform_dim_locations(self, raw_locations: list) -> pd.DataFrame:
        """
        Transform raw location data into dim_locations.

        Columns: location_id, location_code, location_name, location_type,
                 city, address, phone, is_active

        Args:
            raw_locations: List of location dicts from GCS.

        Returns:
            DataFrame for dim_locations.
        """
        self.logger.info("Transforming dim_locations...")
        df = pd.DataFrame(raw_locations)
        df = self.standardize_columns(df)

        # Rename columns to match schema
        column_map = {
            "id": "location_id",
            "code": "location_code",
            "name": "location_name",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Select and order columns
        dim_columns = [
            "location_id", "location_code", "location_name",
            "city", "address", "phone"
        ]
        for col in dim_columns:
            if col not in df.columns:
                df[col] = None

        df = df[dim_columns].copy()
        df["location_type"] = "store"
        df["is_active"] = True

        # Data quality checks
        df = self.check_nulls(df, ["location_id", "location_name"], "dim_locations")
        df = self.check_duplicates(df, ["location_id"], "dim_locations")

        self.logger.info(f"dim_locations: {len(df)} rows")
        return df

    def transform_dim_staff(self, raw_orders: list = None) -> pd.DataFrame:
        """
        Transform staff data into dim_staff.
        Extracts staff info from Sapo order data if available.

        Columns: staff_id, staff_code, full_name, position, email,
                 phone, location_id, hire_date, is_active

        Args:
            raw_orders: Optional list of order dicts that may contain staff info.

        Returns:
            DataFrame for dim_staff.
        """
        self.logger.info("Transforming dim_staff...")

        if raw_orders:
            # Extract staff info from order records
            staff_records = []
            seen_ids = set()
            for order in raw_orders:
                staff = order.get("staff") or order.get("staff_member")
                if staff and isinstance(staff, dict):
                    staff_id = staff.get("id")
                    if staff_id and staff_id not in seen_ids:
                        seen_ids.add(staff_id)
                        staff_records.append({
                            "staff_id": staff_id,
                            "staff_code": staff.get("code", ""),
                            "full_name": staff.get("name", ""),
                            "position": staff.get("position", "Staff"),
                            "email": staff.get("email", ""),
                            "phone": staff.get("phone", ""),
                            "location_id": order.get("location_id"),
                            "hire_date": None,
                            "is_active": True,
                        })

            if staff_records:
                df = pd.DataFrame(staff_records)
                df = self.check_duplicates(df, ["staff_id"], "dim_staff")
                self.logger.info(f"dim_staff: {len(df)} rows extracted from orders")
                return df

        # If no staff data, return empty DataFrame with correct schema
        self.logger.warning("No staff data available. Creating empty dim_staff.")
        df = pd.DataFrame(columns=[
            "staff_id", "staff_code", "full_name", "position",
            "email", "phone", "location_id", "hire_date", "is_active"
        ])
        return df

    def transform_dim_date(self, start_date: str = "2024-01-01",
                           end_date: str = "2027-12-31") -> pd.DataFrame:
        """
        Generate a date dimension table programmatically.

        Columns: date_key, full_date, year, quarter, month, month_name,
                 week, day_of_month, day_of_week, day_name, is_weekend,
                 is_holiday, fiscal_year, fiscal_quarter

        Args:
            start_date: Start date for the dimension.
            end_date: End date for the dimension.

        Returns:
            DataFrame for dim_date.
        """
        self.logger.info(f"Generating dim_date from {start_date} to {end_date}...")

        dates = pd.date_range(start=start_date, end=end_date, freq="D")

        df = pd.DataFrame({
            "date_key": dates.strftime("%Y%m%d").astype(int),
            "full_date": dates.date,
            "year": dates.year,
            "quarter": dates.quarter,
            "month": dates.month,
            "month_name": dates.strftime("%B"),
            "week": dates.isocalendar().week.astype(int),
            "day_of_month": dates.day,
            "day_of_week": dates.dayofweek,  # 0=Monday
            "day_name": dates.strftime("%A"),
            "is_weekend": dates.dayofweek >= 5,
            "is_holiday": False,  # Can be enhanced with holiday calendar
            "fiscal_year": dates.year,
            "fiscal_quarter": dates.quarter,
        })

        # Mark Vietnamese holidays (approximate)
        vn_holidays = [
            "01-01",  # New Year
            "04-30",  # Reunification Day
            "05-01",  # International Workers' Day
            "09-02",  # National Day
        ]
        for holiday in vn_holidays:
            mask = dates.strftime("%m-%d") == holiday
            df.loc[mask, "is_holiday"] = True

        self.logger.info(f"dim_date: {len(df)} rows generated")
        return df

    def update_customer_aggregates(self, dim_customers: pd.DataFrame,
                                   fact_orders: pd.DataFrame) -> pd.DataFrame:
        """
        Update dim_customers with aggregated order metrics.
        Should be called after fact_orders is built.

        Args:
            dim_customers: Current dim_customers DataFrame.
            fact_orders: Completed fact_orders DataFrame.

        Returns:
            Updated dim_customers with lifetime_value, total_orders, etc.
        """
        self.logger.info("Updating customer aggregates...")

        if fact_orders.empty or "customer_id" not in fact_orders.columns:
            self.logger.warning("No order data to aggregate. Skipping customer updates.")
            return dim_customers

        # Aggregate from orders
        agg = fact_orders.groupby("customer_id").agg(
            lifetime_value_vnd=("total_vnd", "sum"),
            total_orders=("order_key", "count"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
        ).reset_index()

        # Merge aggregates into dim_customers
        dim_customers = dim_customers.drop(
            columns=["lifetime_value_vnd", "total_orders", "first_order_date", "last_order_date"],
            errors="ignore"
        )
        dim_customers = dim_customers.merge(agg, on="customer_id", how="left")

        # Fill nulls for customers with no orders
        dim_customers["lifetime_value_vnd"] = dim_customers["lifetime_value_vnd"].fillna(0).astype(int)
        dim_customers["total_orders"] = dim_customers["total_orders"].fillna(0).astype(int)

        # Segment customers based on RFM
        dim_customers["customer_segment"] = dim_customers.apply(
            self._assign_segment, axis=1
        )

        self.logger.info("Customer aggregates updated successfully")
        return dim_customers

    @staticmethod
    def _assign_segment(row) -> str:
        """
        Assign customer segment based on order history.

        Segments: VIP, Regular, At-risk, New
        """
        total_orders = row.get("total_orders", 0)
        ltv = row.get("lifetime_value_vnd", 0)

        if total_orders == 0:
            return "New"
        elif total_orders >= 10 or ltv >= 50_000_000:
            return "VIP"
        elif total_orders >= 3:
            return "Regular"
        else:
            last_order = row.get("last_order_date")
            if pd.notna(last_order):
                days_since = (datetime.now() - pd.Timestamp(last_order)).days
                if days_since > 90:
                    return "At-risk"
            return "Regular"
