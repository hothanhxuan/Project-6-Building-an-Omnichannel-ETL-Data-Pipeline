"""
Payment Extractor - Extracts payment gateway data.
Handles PayPal, MoMo, ZaloPay, Mercury Bank from GCS.
"""

from extractors.base_extractor import BaseExtractor


class PaymentExtractor(BaseExtractor):
    """Extractor for all payment gateway data."""

    def __init__(self, bucket_name: str = "minpy"):
        """Initialize Payment extractor."""
        super().__init__(bucket_name)

    def extract_paypal(self) -> list:
        """
        Extract PayPal transactions.

        Returns:
            List of PayPal transaction records.
        """
        self.logger.info("Starting PayPal extraction...")
        data = self.extract_json_gzip("paypal/transactions.json.gz")
        self.logger.info(f"PayPal extraction complete: {len(data)} transactions")
        return data

    def extract_momo(self) -> list:
        """
        Extract MoMo transactions.

        Returns:
            List of MoMo transaction records.
        """
        self.logger.info("Starting MoMo extraction...")
        data = self.extract_json_gzip("momo/transactions.json.gz")
        self.logger.info(f"MoMo extraction complete: {len(data)} transactions")
        return data

    def extract_zalopay(self) -> list:
        """
        Extract ZaloPay transactions.

        Returns:
            List of ZaloPay transaction records.
        """
        self.logger.info("Starting ZaloPay extraction...")
        data = self.extract_json_gzip("zalopay/transactions.json.gz")
        self.logger.info(f"ZaloPay extraction complete: {len(data)} transactions")
        return data

    def extract_mercury_accounts(self) -> list:
        """
        Extract Mercury bank account information.

        Returns:
            List of Mercury bank accounts.
        """
        self.logger.info("Starting Mercury accounts extraction...")
        data = self.extract_json_gzip("mercury/accounts.json.gz")
        self.logger.info(f"Mercury accounts extraction complete: {len(data)} accounts")
        return data

    def extract_mercury_transactions(self) -> list:
        """
        Extract Mercury bank transactions.

        Returns:
            List of Mercury bank transaction records.
        """
        self.logger.info("Starting Mercury transactions extraction...")
        data = self.extract_json_gzip("mercury/transactions.json.gz")
        self.logger.info(f"Mercury transactions extraction complete: {len(data)} transactions")
        return data
