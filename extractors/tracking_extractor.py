"""
Tracking Extractor - Extracts cart tracking and analytics events.
Handles data from gs://minpy/cart_tracking/
"""

from extractors.base_extractor import BaseExtractor


class TrackingExtractor(BaseExtractor):
    """Extractor for cart tracking and user behavior events."""

    def __init__(self, bucket_name: str = "minpy"):
        """Initialize Tracking extractor."""
        super().__init__(bucket_name)

    def extract_cart_events(self) -> list:
        """
        Extract cart tracking events (add_to_cart, view_item, purchase, etc.).

        Note: This file is large (~269MB compressed). Processing may take
        significant time and memory.

        Returns:
            List of cart event records.
        """
        self.logger.info("Starting cart events extraction (large file, please wait)...")
        data = self.extract_json_gzip("cart_tracking/cart_events.json.gz")
        self.logger.info(f"Cart events extraction complete: {len(data)} events")
        return data

    def extract_cart_events_chunked(self, chunk_size: int = 50000):
        """
        Extract cart events in memory-efficient chunks.
        
        Yields chunks of cart event records so the full dataset never needs
        to be held in memory at once.

        Args:
            chunk_size: Number of records per chunk (default 50,000).

        Yields:
            List of cart event dicts (each list ≤ chunk_size).
        """
        self.logger.info("Starting chunked cart events extraction (large file)...")
        yield from self.extract_json_gzip_chunked(
            "cart_tracking/cart_events.json.gz",
            chunk_size=chunk_size
        )
        self.logger.info("Chunked cart events extraction complete.")
