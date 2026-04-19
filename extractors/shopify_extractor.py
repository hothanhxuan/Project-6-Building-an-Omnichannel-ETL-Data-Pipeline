"""
Shopify Extractor - Extracts order data from Shopify online store.
Handles batched order files from gs://minpy/shopify/
"""

from extractors.base_extractor import BaseExtractor


class ShopifyExtractor(BaseExtractor):
    """Extractor for Shopify e-commerce platform data."""

    def __init__(self, bucket_name: str = "minpy"):
        """Initialize Shopify extractor."""
        super().__init__(bucket_name)

    def extract_orders(self) -> list:
        """
        Extract all Shopify order batches.

        Returns:
            Combined list of all Shopify orders from 5 batch files.
        """
        self.logger.info("Starting Shopify orders extraction...")
        orders = self.extract_json_gzip_batches(
            prefix="shopify/",
            pattern="orders_batch_{}.json.gz",
            batch_count=5
        )
        self.logger.info(f"Shopify extraction complete: {len(orders)} total orders")
        return orders
