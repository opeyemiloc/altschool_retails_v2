"""
Centralized settings and configuration for the retail platform.
Loads variables from .env file and defines default paths and identifiers.
"""
import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

class Settings:
    """Project configuration settings."""
    
    def __init__(self):
        # Project Root and Data Directories
        self.PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
        self.DATA_DIR: Path = self.PROJECT_ROOT / "data"
        self.RAW_DATA_DIR: Path = self.DATA_DIR / "raw"
        self.PROCESSED_DATA_DIR: Path = self.DATA_DIR / "processed"
        self.ARCHIVE_DATA_DIR: Path = self.DATA_DIR / "archive"
        self.QUARANTINE_DATA_DIR: Path = self.DATA_DIR / "quarantine"
        
        # Ensure data directories exist
        for directory in [self.RAW_DATA_DIR, self.PROCESSED_DATA_DIR, self.ARCHIVE_DATA_DIR, self.QUARANTINE_DATA_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
            
        # Kaggle Ingestion Configuration
        self.KAGGLE_DATASET: str = os.getenv("KAGGLE_DATASET", "olistbr/brazilian-ecommerce")
        self.KAGGLE_USERNAME: str = os.getenv("KAGGLE_USERNAME", "")
        self.KAGGLE_KEY: str = os.getenv("KAGGLE_KEY", "")
        
        # PostgreSQL Operational Database Configuration
        self.POSTGRES_USER: str = os.getenv("POSTGRES_USER", "retail_admin")
        self.POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "mock_password")
        self.POSTGRES_DB: str = os.getenv("POSTGRES_DB", "ECOMMERCE")
        self.POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
        self.POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5434")
        
        # BigQuery Configuration
        self.GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
        self.BIGQUERY_DATASET: str = os.getenv("BIGQUERY_DATASET", "retail_analytics_warehouse")
        self.GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        
    def get_postgres_connection_dict(self) -> Dict[str, Any]:
        """Returns connection dictionary for psycopg2/SQLAlchemy."""
        return {
            "dbname": self.POSTGRES_DB,
            "user": self.POSTGRES_USER,
            "password": self.POSTGRES_PASSWORD,
            "host": self.POSTGRES_HOST,
            "port": self.POSTGRES_PORT
        }

    def get_postgres_url(self) -> str:
        """Returns SQLAlchemy connection URL string."""
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

# Singleton instance
_settings = None

def get_settings() -> Settings:
    """Returns the singleton Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
