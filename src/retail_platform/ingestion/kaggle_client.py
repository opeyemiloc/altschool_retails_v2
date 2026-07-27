"""
Kaggle client wrapper using kagglehub for automated dataset downloading.
"""
import os
import logging
import kagglehub
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class KaggleClient:
    """Wrapper around kagglehub for dataset ingestion."""
    
    def __init__(self, username: Optional[str] = None, api_key: Optional[str] = None):
        self.username = username or os.getenv("KAGGLE_USERNAME")
        self.api_key = api_key or os.getenv("KAGGLE_KEY") or os.getenv("KAGGLE_API_TOKEN")
        self._verify_credentials()
        
    def _verify_credentials(self) -> None:
        """Verifies that Kaggle credentials are available in environment."""
        if not self.username or not self.api_key:
            logger.warning(
                "Kaggle API credentials (KAGGLE_USERNAME, KAGGLE_KEY) not explicitly set. "
                "kagglehub will fall back to ~/.kaggle/kaggle.json or local authentication."
            )
        else:
            # kagglehub expects KAGGLE_USERNAME and KAGGLE_KEY or KAGGLE_API_TOKEN in os.environ
            os.environ["KAGGLE_USERNAME"] = self.username
            if self.api_key.startswith("KGAT_"):
                os.environ["KAGGLE_API_TOKEN"] = self.api_key
            else:
                os.environ["KAGGLE_KEY"] = self.api_key
            logger.debug("Kaggle credentials injected into environment.")
            
    def download_dataset(self, dataset_name: str, output_dir: Optional[str] = None, force_download: bool = False) -> str:
        """
        Downloads a dataset using kagglehub.
        
        Args:
            dataset_name: Kaggle dataset identifier (e.g. 'olistbr/brazilian-ecommerce')
            output_dir: Target directory to save/extract files.
            force_download: If True, forces redownload even if cached.
            
        Returns:
            str: Path to the downloaded dataset folder.
        """
        logger.info(f"Initiating download for dataset: '{dataset_name}' via kagglehub...")
        try:
            if output_dir:
                # Ensure output directory exists
                Path(output_dir).mkdir(parents=True, exist_ok=True)

            download_path = kagglehub.dataset_download(
                dataset_name,
                force_download=force_download,
                output_dir=str(output_dir) if output_dir else None,
            )
            logger.info(f"Dataset successfully downloaded/extracted to: {download_path}")
            return str(download_path)
        except Exception as e:
            logger.error(f"Failed to download dataset '{dataset_name}': {str(e)}")
            raise
