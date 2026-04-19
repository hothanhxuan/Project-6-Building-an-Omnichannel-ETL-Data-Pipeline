"""
Fact Transformer - Transforms raw data into fact tables.
Creates: fact_orders, fact_order_items, fact_payments, fact_cart_events, fact_bank_transactions.
"""

import pandas as pd
import numpy as np
from transformers.base_transformer import BaseTransformer


class FactTransformer(BaseTransformer):
    """Transforms raw extracted data into fact tables for the star schema."""

    def __init__(self):
        """Initialize fact transformer."""
        super().__init__()

    # ──────────────────────────────────────────────
    # fact_orders
    # ──────────────────────────────────────────────

    def transform_fact_orders(self, shopify_orders: list, sapo_orders: list,
                              online_orders: list) -> pd.DataFrame:
        """
        Unify orders from Shopify, Sapo POS, and online channels into fact_orders.

        Key Columns: order_key, order_id, order_number, transaction_id,
                     customer_id, location_id, order_date, order_date_key,
                     channel, source, status, payment_status, payment_gateway,
                     payment_method, total_vnd, total_usd

        Args:
            shopify_orders: List of Shopify order dicts.
            sapo_orders: List of Sapo order dicts.
            online_orders: List of online order dicts.

        Returns:
            DataFrame for fact_orders.
        """
        self.logger.info("Transforming fact_orders...")
        all_orders = []

        # ── Shopify Orders ──
        if shopify_orders:
            self.logger.info(f"Processing {len(shopify_orders)} Shopify orders...")
            df_shopify = pd.DataFrame(shopify_orders)
            df_shopify = self.standardize_columns(df_shopify)
            df_shopify = df_shopify.rename(columns={
                "id": "order_id",
                "order_date": "order_date",
            })

            # Ensure source column
            if "source" not in df_shopify.columns:
                df_shopify["source"] = "shopify"
            df_shopify["channel"] = "shopify"

            # Map column names
            col_map = {
                "order_id": "order_id",
                "order_number": "order_number",
                "transaction_id": "transaction_id",
                "customer_id": "customer_id",
                "order_date": "order_date",
                "channel": "channel",
                "source": "source",
                "payment_gateway": "payment_gateway",
                "payment_status": "payment_status",
                "total_vnd": "total_vnd",
                "total_usd": "total_usd",
            }
            for target, src in col_map.items():
                if src not in df_shopify.columns:
                    df_shopify[target] = None

            all_orders.append(df_shopify)

        # ── Sapo Orders ──
        if sapo_orders:
            self.logger.info(f"Processing {len(sapo_orders)} Sapo orders...")
            df_sapo = pd.DataFrame(sapo_orders)
            df_sapo = self.standardize_columns(df_sapo)

            # Handle nested customer object
            if "customer" in df_sapo.columns:
                df_sapo["customer_id"] = df_sapo["customer"].apply(
                    lambda x: x.get("id") if isinstance(x, dict) else None
                )

            # Handle nested staff object
            if "staff" in df_sapo.columns:
                df_sapo["staff_id"] = df_sapo["staff"].apply(
                    lambda x: x.get("id") if isinstance(x, dict) else None
                )

            df_sapo = df_sapo.rename(columns={
                "id": "order_id",
                "code": "order_number",
            })

            if "source" not in df_sapo.columns:
                df_sapo["source"] = "sapo_pos"
            df_sapo["channel"] = "sapo_pos"

            # Map payment_method to payment_gateway if needed
            if "payment_method" in df_sapo.columns and "payment_gateway" not in df_sapo.columns:
                df_sapo["payment_gateway"] = df_sapo["payment_method"]

            # Map status field
            if "status" in df_sapo.columns and "payment_status" not in df_sapo.columns:
                df_sapo["payment_status"] = df_sapo["status"]

            all_orders.append(df_sapo)

        # ── Online Orders ──
        if online_orders:
            self.logger.info(f"Processing {len(online_orders)} online orders...")
            df_online = pd.DataFrame(online_orders)
            df_online = self.standardize_columns(df_online)

            df_online = df_online.rename(columns={
                "order_id": "order_id",
                "id": "order_id",
            })

            # Extract customer_id from nested object if present
            if "customer_id" not in df_online.columns and "customer" in df_online.columns:
                df_online["customer_id"] = df_online["customer"].apply(
                    lambda x: x.get("id") if isinstance(x, dict) else None
                )

            if "source" not in df_online.columns:
                df_online["source"] = "online"

            # Use channel field if present
            if "channel" not in df_online.columns:
                df_online["channel"] = "online"

            if "payment_method" in df_online.columns and "payment_gateway" not in df_online.columns:
                df_online["payment_gateway"] = df_online["payment_method"]

            if "status" in df_online.columns and "payment_status" not in df_online.columns:
                df_online["payment_status"] = df_online["status"]

            # Handle created_at as order_date
            if "order_date" not in df_online.columns and "created_at" in df_online.columns:
                df_online["order_date"] = df_online["created_at"]

            # Handle total field mapping
            if "total_vnd" not in df_online.columns and "total" in df_online.columns:
                df_online["total_vnd"] = df_online["total"]

            all_orders.append(df_online)

        if not all_orders:
            self.logger.warning("No order data to transform!")
            return pd.DataFrame()

        # ── Combine all orders ──
        self.logger.info("Combining orders from all channels...")
        final_columns = [
            "order_id", "order_number", "transaction_id", "customer_id",
            "location_id", "order_date", "channel", "source", "status",
            "payment_status", "payment_gateway", "payment_method",
            "total_vnd", "total_usd"
        ]

        processed = []
        for df in all_orders:
            for col in final_columns:
                if col not in df.columns:
                    df[col] = None
            processed.append(df[final_columns])

        result = pd.concat(processed, ignore_index=True)

        # Convert order_id to string for consistent key generation
        result["order_id"] = result["order_id"].astype(str)

        # Parse dates
        result = self.parse_datetime(result, ["order_date"])

        # Generate surrogate key
        result["order_key"] = result.apply(
            lambda row: self.generate_surrogate_key(
                row["order_id"], row["source"], row["channel"]
            ), axis=1
        )

        # Create date key
        result = self.create_date_key(result, "order_date", "order_date_key")

        # Ensure numeric types
        result["total_vnd"] = pd.to_numeric(result["total_vnd"], errors="coerce").fillna(0).astype(int)
        result["total_usd"] = pd.to_numeric(result["total_usd"], errors="coerce").fillna(0.0)
        result["customer_id"] = pd.to_numeric(result["customer_id"], errors="coerce").astype("Int64")

        # Data quality checks
        result = self.check_nulls(
            result, ["order_id", "customer_id", "total_vnd"], "fact_orders"
        )
        result = self.check_duplicates(result, ["order_key"], "fact_orders")
        result = self.validate_date_range(result, "order_date", table_name="fact_orders")
        result = self.validate_amounts(result, ["total_vnd"], table_name="fact_orders")

        # Reorder columns
        result = result[[
            "order_key", "order_id", "order_number", "transaction_id",
            "customer_id", "location_id", "order_date", "order_date_key",
            "channel", "source", "status", "payment_status",
            "payment_gateway", "payment_method", "total_vnd", "total_usd"
        ]]

        self.logger.info(f"fact_orders: {len(result)} rows")
        return result

    # ──────────────────────────────────────────────
    # fact_order_items
    # ──────────────────────────────────────────────

    def transform_fact_order_items(self, shopify_orders: list, sapo_orders: list,
                                   online_orders: list) -> pd.DataFrame:
        """
        Explode line_items from orders into fact_order_items.

        Key Columns: order_item_key, order_key, order_id, transaction_id,
                     product_id, product_name, quantity, unit_price_vnd,
                     line_total_vnd, order_date, order_date_key

        Args:
            shopify_orders: Shopify order dicts.
            sapo_orders: Sapo order dicts.
            online_orders: Online order dicts.

        Returns:
            DataFrame for fact_order_items.
        """
        self.logger.info("Transforming fact_order_items...")
        all_items = []

        all_sources = [
            ("shopify", shopify_orders),
            ("sapo_pos", sapo_orders),
            ("online", online_orders),
        ]

        for source_name, orders in all_sources:
            if not orders:
                continue

            self.logger.info(f"Extracting line items from {source_name} ({len(orders)} orders)...")
            for order in orders:
                order_id = str(order.get("id", order.get("order_id", "")))
                transaction_id = order.get("transaction_id", "")
                order_date = order.get("order_date", order.get("created_at", ""))
                line_items = order.get("line_items", [])

                if not isinstance(line_items, list):
                    continue

                order_key = self.generate_surrogate_key(order_id, source_name, source_name)

                for idx, item in enumerate(line_items):
                    if not isinstance(item, dict):
                        continue

                    product_id = item.get("product_id", item.get("id"))
                    quantity = item.get("quantity", 1)
                    unit_price = item.get("price_vnd", item.get("price", item.get("unit_price", 0)))
                    line_total = item.get("line_total_vnd",
                                         item.get("total", item.get("line_total", 0)))

                    # Calculate line total if not provided
                    if not line_total and unit_price and quantity:
                        line_total = int(unit_price) * int(quantity)

                    all_items.append({
                        "order_key": order_key,
                        "order_id": order_id,
                        "transaction_id": transaction_id,
                        "product_id": product_id,
                        "product_name": item.get("name", item.get("product_name", "")),
                        "quantity": quantity,
                        "unit_price_vnd": unit_price,
                        "line_total_vnd": line_total,
                        "order_date": order_date,
                    })

        if not all_items:
            self.logger.warning("No line items found!")
            return pd.DataFrame()

        result = pd.DataFrame(all_items)

        # Parse dates
        result = self.parse_datetime(result, ["order_date"])
        result = self.create_date_key(result, "order_date", "order_date_key")

        # Generate surrogate key
        result["order_item_key"] = result.apply(
            lambda row: self.generate_surrogate_key(
                row["order_key"], row["product_id"], row.name  # row.name = index
            ), axis=1
        )

        # Ensure numeric types
        result["quantity"] = pd.to_numeric(result["quantity"], errors="coerce").fillna(0).astype(int)
        result["unit_price_vnd"] = pd.to_numeric(result["unit_price_vnd"], errors="coerce").fillna(0).astype(int)
        result["line_total_vnd"] = pd.to_numeric(result["line_total_vnd"], errors="coerce").fillna(0).astype(int)

        # Data quality checks
        result = self.check_nulls(result, ["order_key", "product_id"], "fact_order_items")
        result = self.validate_amounts(result, ["unit_price_vnd", "line_total_vnd"],
                                       table_name="fact_order_items")

        # Reorder columns
        result = result[[
            "order_item_key", "order_key", "order_id", "transaction_id",
            "product_id", "product_name", "quantity", "unit_price_vnd",
            "line_total_vnd", "order_date", "order_date_key"
        ]]

        self.logger.info(f"fact_order_items: {len(result)} rows")
        return result

    # ──────────────────────────────────────────────
    # fact_payments
    # ──────────────────────────────────────────────

    def transform_fact_payments(self, paypal_data: list, momo_data: list,
                                zalopay_data: list) -> pd.DataFrame:
        """
        Unify payment data from PayPal, MoMo, and ZaloPay into fact_payments.

        Key Columns: payment_key, transaction_id, order_id, customer_id,
                     payment_gateway, payment_method, amount_vnd, amount_usd,
                     payment_status, payment_date, payment_date_key, source

        Args:
            paypal_data: PayPal transaction dicts.
            momo_data: MoMo transaction dicts.
            zalopay_data: ZaloPay transaction dicts.

        Returns:
            DataFrame for fact_payments.
        """
        self.logger.info("Transforming fact_payments...")
        all_payments = []

        # ── PayPal ──
        if paypal_data:
            self.logger.info(f"Processing {len(paypal_data)} PayPal transactions...")
            for txn in paypal_data:
                amount_obj = txn.get("transaction_amount", {})
                all_payments.append({
                    "transaction_id": txn.get("transaction_id", ""),
                    "order_id": txn.get("order_id", ""),
                    "customer_id": txn.get("customer_id"),
                    "payment_gateway": "paypal",
                    "payment_method": "paypal",
                    "amount_vnd": txn.get("transaction_amount_vnd", 0),
                    "amount_usd": float(amount_obj.get("value", 0)) if isinstance(amount_obj, dict) else 0,
                    "payment_status": txn.get("transaction_status", ""),
                    "payment_date": txn.get("transaction_initiation_date", ""),
                    "source": "paypal",
                })

        # ── MoMo ──
        if momo_data:
            self.logger.info(f"Processing {len(momo_data)} MoMo transactions...")
            for txn in momo_data:
                result_code = txn.get("resultCode", -1)
                status = "success" if result_code == 0 else "failed"
                all_payments.append({
                    "transaction_id": txn.get("transaction_id", ""),
                    "order_id": txn.get("orderId", ""),
                    "customer_id": None,
                    "payment_gateway": "momo",
                    "payment_method": txn.get("payType", "momo"),
                    "amount_vnd": txn.get("amount", 0),
                    "amount_usd": 0.0,
                    "payment_status": status,
                    "payment_date": txn.get("responseTimeISO", ""),
                    "source": "momo",
                })

        # ── ZaloPay ──
        if zalopay_data:
            self.logger.info(f"Processing {len(zalopay_data)} ZaloPay transactions...")
            for txn in zalopay_data:
                return_code = txn.get("return_code", -1)
                status = "success" if return_code == 1 else "failed"
                all_payments.append({
                    "transaction_id": txn.get("transaction_id", ""),
                    "order_id": txn.get("app_trans_id", ""),
                    "customer_id": None,
                    "payment_gateway": "zalopay",
                    "payment_method": f"zalopay_ch{txn.get('channel', '')}",
                    "amount_vnd": txn.get("amount", 0),
                    "amount_usd": 0.0,
                    "payment_status": status,
                    "payment_date": txn.get("server_time_iso", ""),
                    "source": "zalopay",
                })

        if not all_payments:
            self.logger.warning("No payment data to transform!")
            return pd.DataFrame()

        result = pd.DataFrame(all_payments)

        # Parse dates
        result = self.parse_datetime(result, ["payment_date"])
        result = self.create_date_key(result, "payment_date", "payment_date_key")

        # Generate surrogate key
        result["payment_key"] = result.apply(
            lambda row: self.generate_surrogate_key(
                row["transaction_id"], row["payment_gateway"]
            ), axis=1
        )

        # Ensure numeric types
        result["amount_vnd"] = pd.to_numeric(result["amount_vnd"], errors="coerce").fillna(0).astype(int)
        result["amount_usd"] = pd.to_numeric(result["amount_usd"], errors="coerce").fillna(0.0)
        result["customer_id"] = pd.to_numeric(result["customer_id"], errors="coerce").astype("Int64")

        # Data quality checks
        result = self.check_nulls(
            result, ["transaction_id", "amount_vnd"], "fact_payments"
        )
        result = self.check_duplicates(result, ["payment_key"], "fact_payments")
        result = self.validate_date_range(result, "payment_date", table_name="fact_payments")
        result = self.validate_amounts(result, ["amount_vnd"], table_name="fact_payments")

        # Reorder columns
        result = result[[
            "payment_key", "transaction_id", "order_id", "customer_id",
            "payment_gateway", "payment_method", "amount_vnd", "amount_usd",
            "payment_status", "payment_date", "payment_date_key", "source"
        ]]

        self.logger.info(f"fact_payments: {len(result)} rows")
        return result

    # ──────────────────────────────────────────────
    # fact_cart_events
    # ──────────────────────────────────────────────

    def transform_fact_cart_events(self, cart_events: list) -> pd.DataFrame:
        """
        Transform cart tracking events into fact_cart_events.

        Key Columns: event_key, event_id, session_id, customer_id, event_type,
                     event_timestamp, event_date_key, product_id, source,
                     device, browser, utm_source, utm_campaign

        Args:
            cart_events: List of cart event dicts.

        Returns:
            DataFrame for fact_cart_events.
        """
        self.logger.info("Transforming fact_cart_events...")
        df = pd.DataFrame(cart_events)
        df = self.standardize_columns(df)

        # Rename columns to match schema
        column_map = {
            "timestamp": "event_timestamp",
        }
        df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

        # Ensure all required columns exist
        required_cols = [
            "event_id", "session_id", "customer_id", "event_type",
            "event_timestamp", "product_id", "source", "device",
            "browser", "utm_source", "utm_campaign"
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        # Parse dates
        df = self.parse_datetime(df, ["event_timestamp"])
        df = self.create_date_key(df, "event_timestamp", "event_date_key")

        # Generate surrogate key
        df["event_key"] = df.apply(
            lambda row: self.generate_surrogate_key(
                row.get("event_id", ""), row.get("session_id", ""), row.name
            ), axis=1
        )

        # Ensure numeric types
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
        df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")

        # Data quality checks
        df = self.check_nulls(
            df, ["event_id", "event_type", "session_id"], "fact_cart_events"
        )
        df = self.check_duplicates(df, ["event_key"], "fact_cart_events")
        df = self.validate_date_range(df, "event_timestamp", table_name="fact_cart_events")

        # Select final columns
        df = df[[
            "event_key", "event_id", "session_id", "customer_id",
            "event_type", "event_timestamp", "event_date_key",
            "product_id", "source", "device", "browser",
            "utm_source", "utm_campaign"
        ]]

        self.logger.info(f"fact_cart_events: {len(df)} rows")
        return df

    def transform_fact_cart_events_chunked(self, cart_event_chunks) -> "pd.DataFrame":
        """
        Transform cart events from a chunk generator — memory efficient.
        
        Processes each chunk independently and concatenates at the end.
        This avoids loading all raw cart events into a single Python list
        before creating the DataFrame.

        Args:
            cart_event_chunks: Generator/iterable yielding lists of cart event dicts.

        Returns:
            DataFrame for fact_cart_events.
        """
        import gc
        self.logger.info("Transforming fact_cart_events (chunked mode)...")
        
        all_chunks = []
        total_input = 0
        chunk_idx = 0
        
        for chunk in cart_event_chunks:
            chunk_idx += 1
            total_input += len(chunk)
            self.logger.info(f"  Processing chunk {chunk_idx}: {len(chunk)} records (total input: {total_input})")
            
            df = pd.DataFrame(chunk)
            # Free the raw chunk immediately
            del chunk
            
            df = self.standardize_columns(df)

            # Rename columns to match schema
            column_map = {"timestamp": "event_timestamp"}
            df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

            # Ensure all required columns exist
            required_cols = [
                "event_id", "session_id", "customer_id", "event_type",
                "event_timestamp", "product_id", "source", "device",
                "browser", "utm_source", "utm_campaign"
            ]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = None

            # Parse dates
            df = self.parse_datetime(df, ["event_timestamp"])
            df = self.create_date_key(df, "event_timestamp", "event_date_key")

            # Generate surrogate key
            base_offset = total_input - len(df)
            df["event_key"] = df.apply(
                lambda row: self.generate_surrogate_key(
                    row.get("event_id", ""), row.get("session_id", ""), base_offset + row.name
                ), axis=1
            )

            # Ensure numeric types
            df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
            df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")

            # Select final columns
            df = df[[
                "event_key", "event_id", "session_id", "customer_id",
                "event_type", "event_timestamp", "event_date_key",
                "product_id", "source", "device", "browser",
                "utm_source", "utm_campaign"
            ]]
            
            all_chunks.append(df)
            gc.collect()

        if not all_chunks:
            self.logger.warning("No cart event data to transform!")
            return pd.DataFrame()

        result = pd.concat(all_chunks, ignore_index=True)
        del all_chunks
        gc.collect()

        # Data quality checks on final result
        result = self.check_nulls(
            result, ["event_id", "event_type", "session_id"], "fact_cart_events"
        )
        result = self.check_duplicates(result, ["event_key"], "fact_cart_events")
        result = self.validate_date_range(result, "event_timestamp", table_name="fact_cart_events")

        self.logger.info(f"fact_cart_events (chunked): {len(result)} rows")
        return result

    # ──────────────────────────────────────────────
    # fact_bank_transactions
    # ──────────────────────────────────────────────

    def transform_fact_bank_transactions(self, mercury_transactions: list) -> pd.DataFrame:
        """
        Transform Mercury bank transactions into fact_bank_transactions.

        Key Columns: transaction_key, transaction_id, account_id, transaction_type,
                     amount_usd, amount_vnd, status, description, counterparty,
                     transaction_date, transaction_date_key, source

        Args:
            mercury_transactions: List of Mercury bank transaction dicts.

        Returns:
            DataFrame for fact_bank_transactions.
        """
        self.logger.info("Transforming fact_bank_transactions...")
        records = []

        for txn in mercury_transactions:
            details = txn.get("details", {}) or {}
            records.append({
                "transaction_id": txn.get("transaction_id", ""),
                "account_id": txn.get("accountId", ""),
                "transaction_type": txn.get("kind", ""),
                "amount_usd": txn.get("amount_usd", txn.get("amount", 0.0)),
                "amount_vnd": txn.get("amount_vnd", 0),
                "status": txn.get("status", ""),
                "description": txn.get("bankDescription", ""),
                "counterparty": details.get("counterpartyName", ""),
                "transaction_date": txn.get("createdAt", txn.get("postedAt", "")),
                "source": txn.get("source", "mercury_bank"),
            })

        if not records:
            self.logger.warning("No bank transaction data!")
            return pd.DataFrame()

        result = pd.DataFrame(records)

        # Parse dates
        result = self.parse_datetime(result, ["transaction_date"])
        result = self.create_date_key(result, "transaction_date", "transaction_date_key")

        # Generate surrogate key
        result["transaction_key"] = result.apply(
            lambda row: self.generate_surrogate_key(
                row["transaction_id"], row["account_id"]
            ), axis=1
        )

        # Ensure numeric types
        result["amount_usd"] = pd.to_numeric(result["amount_usd"], errors="coerce").fillna(0.0)
        result["amount_vnd"] = pd.to_numeric(result["amount_vnd"], errors="coerce").fillna(0).astype(int)

        # Data quality checks (allow negative for bank transactions)
        result = self.check_nulls(
            result, ["transaction_id", "account_id", "amount_vnd"], "fact_bank_transactions"
        )
        result = self.check_duplicates(result, ["transaction_key"], "fact_bank_transactions")
        result = self.validate_date_range(
            result, "transaction_date", table_name="fact_bank_transactions"
        )
        result = self.validate_amounts(
            result, ["amount_vnd", "amount_usd"],
            allow_negative=True,  # Bank transactions can be negative (debits)
            table_name="fact_bank_transactions"
        )

        # Reorder columns
        result = result[[
            "transaction_key", "transaction_id", "account_id",
            "transaction_type", "amount_usd", "amount_vnd", "status",
            "description", "counterparty", "transaction_date",
            "transaction_date_key", "source"
        ]]

        self.logger.info(f"fact_bank_transactions: {len(result)} rows")
        return result
