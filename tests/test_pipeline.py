"""
Unit Tests for TechStore Vietnam ETL Pipeline.
Tests extractors, transformers, loader, and data quality checks.
"""

import unittest
import sys
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ──────────────────────────────────────────────
# Test Base Transformer & Data Quality
# ──────────────────────────────────────────────

class TestBaseTransformer(unittest.TestCase):
    """Test data quality checks and transformation utilities."""

    def setUp(self):
        """Set up test fixtures."""
        from transformers.base_transformer import BaseTransformer
        self.transformer = BaseTransformer()

    def test_check_nulls_detects_nulls(self):
        """Test that null check detects and logs null values."""
        df = pd.DataFrame({
            "id": [1, 2, 3, None],
            "name": ["A", None, "C", "D"]
        })
        result = self.transformer.check_nulls(df, ["id", "name"], "test_table")
        # Should not modify the dataframe
        self.assertEqual(len(result), 4)
        self.assertIn("test_table.id.nulls", self.transformer.quality_report)
        self.assertEqual(self.transformer.quality_report["test_table.id.nulls"], 1)
        self.assertEqual(self.transformer.quality_report["test_table.name.nulls"], 1)

    def test_check_duplicates_removes_dupes(self):
        """Test that duplicates are detected and removed."""
        df = pd.DataFrame({
            "id": [1, 2, 2, 3],
            "value": ["a", "b", "b", "c"]
        })
        result = self.transformer.check_duplicates(df, ["id"], "test_table")
        self.assertEqual(len(result), 3)
        self.assertIn("test_table.duplicates", self.transformer.quality_report)

    def test_check_duplicates_no_dupes(self):
        """Test with no duplicates."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"]
        })
        result = self.transformer.check_duplicates(df, ["id"], "test_table")
        self.assertEqual(len(result), 3)

    def test_validate_amounts_detects_negatives(self):
        """Test negative amount detection."""
        df = pd.DataFrame({
            "amount": [100, -50, 200, 300]
        })
        result = self.transformer.validate_amounts(
            df, ["amount"], allow_negative=False, table_name="test"
        )
        self.assertEqual(len(result), 4)  # Should not remove rows

    def test_validate_amounts_allows_negatives(self):
        """Test that negatives are allowed when specified."""
        df = pd.DataFrame({
            "amount": [100, -50, 200, -300]
        })
        # Should not raise any warnings for negatives
        result = self.transformer.validate_amounts(
            df, ["amount"], allow_negative=True, table_name="test"
        )
        self.assertEqual(len(result), 4)

    def test_generate_surrogate_key(self):
        """Test surrogate key generation is deterministic."""
        key1 = self.transformer.generate_surrogate_key("order_1", "shopify")
        key2 = self.transformer.generate_surrogate_key("order_1", "shopify")
        key3 = self.transformer.generate_surrogate_key("order_2", "shopify")
        self.assertEqual(key1, key2)  # Same inputs → same key
        self.assertNotEqual(key1, key3)  # Different inputs → different key
        self.assertEqual(len(key1), 16)  # 16-char hex string

    def test_standardize_columns(self):
        """Test column name standardization."""
        df = pd.DataFrame({"First Name": [1], "last-name": [2], "AGE": [3]})
        result = self.transformer.standardize_columns(df)
        self.assertListEqual(list(result.columns), ["first_name", "last_name", "age"])

    def test_parse_datetime(self):
        """Test datetime parsing."""
        df = pd.DataFrame({
            "date": ["2025-01-15T10:30:00Z", "2025-06-20T15:00:00+07:00"]
        })
        result = self.transformer.parse_datetime(df, ["date"])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["date"]))

    def test_create_date_key(self):
        """Test date key creation (YYYYMMDD format)."""
        df = pd.DataFrame({
            "order_date": pd.to_datetime(["2025-03-15", "2025-12-01"])
        })
        result = self.transformer.create_date_key(df, "order_date", "date_key")
        self.assertEqual(result["date_key"].iloc[0], 20250315)
        self.assertEqual(result["date_key"].iloc[1], 20251201)


# ──────────────────────────────────────────────
# Test Dimension Transformer
# ──────────────────────────────────────────────

class TestDimensionTransformer(unittest.TestCase):
    """Test dimension table transformations."""

    def setUp(self):
        """Set up test fixtures."""
        from transformers.dimension_transformer import DimensionTransformer
        self.transformer = DimensionTransformer()

    def test_transform_dim_customers(self):
        """Test customer dimension transformation."""
        raw = [
            {"id": 1, "email": "a@test.com", "name": "Alice", "phone": "123",
             "city": "HCM", "country": "VN", "created_at": "2025-01-01T00:00:00Z"},
            {"id": 2, "email": "b@test.com", "name": "Bob", "phone": "456",
             "city": "HN", "country": "VN", "created_at": "2025-02-01T00:00:00Z"},
        ]
        result = self.transformer.transform_dim_customers(raw)

        self.assertEqual(len(result), 2)
        self.assertIn("customer_id", result.columns)
        self.assertIn("customer_segment", result.columns)
        self.assertIn("lifetime_value_vnd", result.columns)
        self.assertEqual(result["customer_segment"].iloc[0], "New")

    def test_transform_dim_products(self):
        """Test product dimension transformation."""
        raw = [
            {"id": 1, "name": "iPhone 15", "sku": "IP15", "barcode": "123",
             "category": "Phone", "brand": "Apple", "price_vnd": 25000000,
             "price_usd": 999.0, "stock_quantity": 100},
        ]
        result = self.transformer.transform_dim_products(raw)

        self.assertEqual(len(result), 1)
        self.assertEqual(result["product_name"].iloc[0], "iPhone 15")
        self.assertTrue(result["is_active"].iloc[0])

    def test_transform_dim_date(self):
        """Test date dimension generation."""
        result = self.transformer.transform_dim_date(
            start_date="2025-01-01", end_date="2025-01-31"
        )
        self.assertEqual(len(result), 31)
        self.assertIn("date_key", result.columns)
        self.assertIn("is_weekend", result.columns)
        self.assertIn("month_name", result.columns)
        self.assertEqual(result["date_key"].iloc[0], 20250101)

    def test_transform_dim_locations(self):
        """Test location dimension transformation."""
        raw = [
            {"id": 1, "code": "S001", "name": "Store HCM", "address": "123 Nguyen Hue",
             "city": "HCM", "phone": "028123456"},
        ]
        result = self.transformer.transform_dim_locations(raw)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["location_name"].iloc[0], "Store HCM")

    def test_update_customer_aggregates(self):
        """Test customer aggregate update from orders."""
        dim_customers = pd.DataFrame({
            "customer_id": [1, 2],
            "email": ["a@test.com", "b@test.com"],
            "full_name": ["Alice", "Bob"],
            "phone": ["123", "456"],
            "city": ["HCM", "HN"],
            "country": ["VN", "VN"],
            "created_at": pd.to_datetime(["2025-01-01", "2025-02-01"]),
            "customer_segment": ["New", "New"],
            "lifetime_value_vnd": [0, 0],
            "total_orders": [0, 0],
            "first_order_date": [pd.NaT, pd.NaT],
            "last_order_date": [pd.NaT, pd.NaT],
        })
        fact_orders = pd.DataFrame({
            "order_key": ["k1", "k2", "k3"],
            "customer_id": [1, 1, 2],
            "total_vnd": [1000000, 2000000, 500000],
            "order_date": pd.to_datetime(["2025-03-01", "2025-04-01", "2025-03-15"]),
        })
        result = self.transformer.update_customer_aggregates(dim_customers, fact_orders)
        self.assertEqual(result.loc[result["customer_id"] == 1, "total_orders"].values[0], 2)
        self.assertEqual(result.loc[result["customer_id"] == 1, "lifetime_value_vnd"].values[0], 3000000)


# ──────────────────────────────────────────────
# Test Fact Transformer
# ──────────────────────────────────────────────

class TestFactTransformer(unittest.TestCase):
    """Test fact table transformations."""

    def setUp(self):
        """Set up test fixtures."""
        from transformers.fact_transformer import FactTransformer
        self.transformer = FactTransformer()

    def test_transform_fact_orders_shopify(self):
        """Test Shopify order transformation."""
        shopify = [
            {"id": 1, "order_number": "S001", "transaction_id": "T001",
             "customer_id": 100, "order_date": "2025-03-01T10:00:00Z",
             "payment_gateway": "stripe", "payment_status": "paid",
             "total_vnd": 5000000, "total_usd": 200.0, "source": "shopify",
             "line_items": [{"product_id": 1, "quantity": 2, "price_vnd": 2500000}]},
        ]
        result = self.transformer.transform_fact_orders(shopify, [], [])

        self.assertEqual(len(result), 1)
        self.assertIn("order_key", result.columns)
        self.assertEqual(result["channel"].iloc[0], "shopify")
        self.assertEqual(result["total_vnd"].iloc[0], 5000000)

    def test_transform_fact_orders_combined(self):
        """Test combined orders from multiple channels."""
        shopify = [{"id": 1, "order_date": "2025-01-01", "total_vnd": 1000000,
                     "source": "shopify", "line_items": []}]
        sapo = [{"id": 2, "code": "SAPO001", "total_vnd": 2000000,
                  "source": "sapo_pos", "line_items": []}]
        online = [{"id": 3, "created_at": "2025-01-03", "total": 3000000,
                    "channel": "lazada", "line_items": []}]

        result = self.transformer.transform_fact_orders(shopify, sapo, online)
        self.assertEqual(len(result), 3)
        channels = set(result["channel"].tolist())
        self.assertIn("shopify", channels)

    def test_transform_fact_order_items(self):
        """Test order line item explosion."""
        orders = [
            {"id": 1, "order_date": "2025-03-01", "source": "shopify",
             "line_items": [
                 {"product_id": 10, "name": "iPhone", "quantity": 1, "price_vnd": 25000000},
                 {"product_id": 20, "name": "Case", "quantity": 2, "price_vnd": 500000},
             ]},
        ]
        result = self.transformer.transform_fact_order_items(orders, [], [])
        self.assertEqual(len(result), 2)
        self.assertIn("order_item_key", result.columns)
        self.assertEqual(result["quantity"].iloc[1], 2)

    def test_transform_fact_payments_paypal(self):
        """Test PayPal payment transformation."""
        paypal = [
            {"transaction_id": "PP001", "customer_id": 100,
             "transaction_amount": {"currency_code": "USD", "value": "99.99"},
             "transaction_amount_vnd": 2400000,
             "transaction_status": "COMPLETED",
             "transaction_initiation_date": "2025-03-01T10:00:00Z"},
        ]
        result = self.transformer.transform_fact_payments(paypal, [], [])
        self.assertEqual(len(result), 1)
        self.assertEqual(result["payment_gateway"].iloc[0], "paypal")
        self.assertEqual(result["amount_vnd"].iloc[0], 2400000)

    def test_transform_fact_payments_momo(self):
        """Test MoMo payment transformation."""
        momo = [
            {"transaction_id": "MM001", "orderId": "ORD001", "transId": 12345,
             "amount": 500000, "resultCode": 0, "message": "Success",
             "payType": "qr", "responseTimeISO": "2025-03-01T10:00:00Z"},
        ]
        result = self.transformer.transform_fact_payments([], momo, [])
        self.assertEqual(len(result), 1)
        self.assertEqual(result["payment_status"].iloc[0], "success")

    def test_transform_fact_payments_zalopay(self):
        """Test ZaloPay payment transformation."""
        zalopay = [
            {"transaction_id": "ZP001", "app_trans_id": "APP001",
             "zp_trans_id": 999, "amount": 750000, "return_code": 1,
             "channel": 38, "bank_code": "VCB",
             "server_time_iso": "2025-03-01T10:00:00Z"},
        ]
        result = self.transformer.transform_fact_payments([], [], zalopay)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["payment_status"].iloc[0], "success")

    def test_transform_fact_bank_transactions(self):
        """Test Mercury bank transaction transformation."""
        mercury = [
            {"transaction_id": "TXN001", "accountId": "ACC001",
             "amount": -500.0, "amount_usd": 500.0, "amount_vnd": 12000000,
             "kind": "outgoingAch", "status": "posted",
             "bankDescription": "Payment to vendor",
             "createdAt": "2025-03-01T10:00:00Z",
             "details": {"counterpartyName": "Vendor Corp"},
             "source": "mercury_bank"},
        ]
        result = self.transformer.transform_fact_bank_transactions(mercury)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["counterparty"].iloc[0], "Vendor Corp")

    def test_transform_fact_cart_events(self):
        """Test cart event transformation."""
        events = [
            {"event_id": "E001", "event_type": "add_to_cart", "session_id": "S001",
             "customer_id": 100, "product_id": 10,
             "timestamp": "2025-03-01T10:00:00Z",
             "source": "website", "device": "mobile", "browser": "Chrome",
             "utm_source": "google", "utm_campaign": "spring_sale"},
        ]
        result = self.transformer.transform_fact_cart_events(events)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["event_type"].iloc[0], "add_to_cart")


# ──────────────────────────────────────────────
# Test BigQuery Loader (Mocked)
# ──────────────────────────────────────────────

class TestBigQueryLoaderMocked(unittest.TestCase):
    """Test BigQuery loader with mocked GCP client."""

    @patch("loaders.bigquery_loader.service_account.Credentials.from_service_account_file")
    @patch("loaders.bigquery_loader.bigquery.Client")
    def test_create_dataset(self, mock_client_class, mock_creds):
        """Test dataset creation."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.get_dataset.side_effect = Exception("Not found")

        from loaders.bigquery_loader import BigQueryLoader
        loader = BigQueryLoader()
        loader.create_dataset_if_not_exists()

        mock_client.create_dataset.assert_called_once()

    @patch("loaders.bigquery_loader.service_account.Credentials.from_service_account_file")
    @patch("loaders.bigquery_loader.bigquery.Client")
    def test_load_empty_dataframe(self, mock_client_class, mock_creds):
        """Test that empty DataFrames are skipped."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        from loaders.bigquery_loader import BigQueryLoader
        loader = BigQueryLoader()
        result = loader.load_dataframe("test_table", pd.DataFrame())
        self.assertEqual(result, 0)


# ──────────────────────────────────────────────
# Test Logger
# ──────────────────────────────────────────────

class TestLogger(unittest.TestCase):
    """Test logging utility."""

    def test_setup_logger(self):
        """Test that logger is created with proper handlers."""
        from utils.logger import setup_logger
        logger = setup_logger("test_logger")
        self.assertEqual(logger.name, "test_logger")
        self.assertTrue(len(logger.handlers) > 0)

    def test_logger_no_duplicates(self):
        """Test that calling setup_logger twice doesn't duplicate handlers."""
        from utils.logger import setup_logger
        logger1 = setup_logger("unique_test_logger")
        handler_count = len(logger1.handlers)
        logger2 = setup_logger("unique_test_logger")
        self.assertEqual(len(logger2.handlers), handler_count)


if __name__ == "__main__":
    unittest.main(verbosity=2)
