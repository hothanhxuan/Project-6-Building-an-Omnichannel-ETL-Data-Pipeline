"""
Sapo Extractor - Extracts POS data from Sapo offline stores.
Handles data from gs://minpy/sapo/ and gs://minpy/shared/
Also extracts shared data: customers, products, locations.
"""

from extractors.base_extractor import BaseExtractor


class SapoExtractor(BaseExtractor):
    """Extractor for Sapo POS and shared master data."""

    def __init__(self, bucket_name: str = "minpy"):
        """Initialize Sapo extractor."""
        super().__init__(bucket_name)

    def extract_orders(self) -> list:
        """
        Extract Sapo POS transaction/order data.

        Returns:
            List of Sapo orders/transactions.
        """
        self.logger.info("Starting Sapo orders extraction...")
        orders = self.extract_json_gzip("sapo/transactions.json.gz")
        self.logger.info(f"Sapo extraction complete: {len(orders)} orders")
        return orders

    def extract_customers(self) -> list:
        """
        Extract all customer batches from shared data.

        Returns:
            Combined list of all customers from 10 batch files.
        """
        self.logger.info("Starting customer extraction...")
        customers = self.extract_json_gzip_batches(
            prefix="shared/customers/",
            pattern="customers_batch_{}.json.gz",
            batch_count=10
        )
        self.logger.info(f"Customer extraction complete: {len(customers)} customers")
        return customers

    def extract_products(self) -> list:
        """
        Extract product catalog from shared data.

        Returns:
            List of products.
        """
        self.logger.info("Starting products extraction...")
        products = self.extract_json_gzip("shared/products.json.gz")
        self.logger.info(f"Products extraction complete: {len(products)} products")
        return products

    def extract_locations(self) -> list:
        """
        Extract store/warehouse locations.

        Returns:
            List of locations.
        """
        self.logger.info("Starting locations extraction...")
        locations = self.extract_json_gzip("shared/sapo_locations.json.gz")
        self.logger.info(f"Locations extraction complete: {len(locations)} locations")
        return locations

    def extract_online_orders(self) -> list:
        """
        Extract multi-channel online orders.

        Returns:
            List of online orders.
        """
        self.logger.info("Starting online orders extraction...")
        orders = self.extract_json_gzip("online_orders/online_orders.json.gz")
        self.logger.info(f"Online orders extraction complete: {len(orders)} orders")
        return orders
