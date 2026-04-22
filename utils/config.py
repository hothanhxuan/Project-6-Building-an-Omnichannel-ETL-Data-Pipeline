"""
Configuration loader for TechStore Vietnam ETL Pipeline.
Loads environment variables from .env and provides credential paths.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account


# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def get_credentials_path() -> str:
    """
    Get the absolute path to the Google Cloud service account JSON key.

    Reads GOOGLE_CREDENTIALS_PATH from .env, resolves it relative to
    the project root directory.

    Returns:
        Absolute path string to the credentials JSON file.

    Raises:
        FileNotFoundError: If the credentials file does not exist.
    """
    raw_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
    if not raw_path:
        raise EnvironmentError(
            "GOOGLE_CREDENTIALS_PATH is not set in .env file. "
            "Copy .env.example to .env and fill in your values."
        )

    # Resolve relative to project root
    cred_path = _PROJECT_ROOT / raw_path
    if not cred_path.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {cred_path}\n"
            f"Check GOOGLE_CREDENTIALS_PATH in your .env file."
        )

    return str(cred_path)


def get_credentials() -> service_account.Credentials:
    """
    Load and return Google Cloud credentials object.

    Returns:
        google.oauth2.service_account.Credentials with cloud-platform scope.
    """
    SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
    cred_path = get_credentials_path()
    return service_account.Credentials.from_service_account_file(
        cred_path, scopes=SCOPES
    )


def get_project_id() -> str:
    """Get GCP project ID from .env."""
    return os.getenv("GCP_PROJECT_ID", "minpyws")


def get_dataset_id() -> str:
    """Get BigQuery dataset ID from .env."""
    return os.getenv("BQ_DATASET_ID", "techstore_analytics")


def get_bucket_name() -> str:
    """Get GCS bucket name from .env."""
    return os.getenv("GCS_BUCKET_NAME", "minpy")
