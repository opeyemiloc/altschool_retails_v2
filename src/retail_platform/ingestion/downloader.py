"""
Idempotent dataset downloader that verifies local files before fetching.
"""
import os
import shutil
import logging
from pathlib import Path
from typing import List, Dict, Optional
from retail_platform.config import get_settings
from retail_platform.ingestion.kaggle_client import KaggleClient

logger = logging.getLogger(__name__)

# Expected CSV files in the Olist Brazilian E-Commerce dataset
EXPECTED_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
]

class DatasetDownloader:
    """Manages idempotent data extraction from Kaggle into the local raw data directory."""
    
    def __init__(self, settings=None):
        self.settings = settings or get_settings()
        self.client = KaggleClient(
            username=self.settings.KAGGLE_USERNAME,
            api_key=self.settings.KAGGLE_KEY
        )
        
    def check_local_dataset_complete(self) -> bool:
        """
        Checks if all expected dataset CSV files are already present in raw data directory.
        
        Returns:
            bool: True if all expected files exist and are non-empty, False otherwise.
        """
        raw_dir = self.settings.RAW_DATA_DIR
        if not raw_dir.exists():
            return False
            
        for filename in EXPECTED_FILES:
            file_path = raw_dir / filename
            if not file_path.exists() or file_path.stat().st_size == 0:
                logger.debug(f"Missing or empty expected file: {filename}")
                return False
                
        logger.info(f"All {len(EXPECTED_FILES)} expected CSV files found in {raw_dir}.")
        return True

    def _sync_files_to_raw(self, source_dir: Path) -> None:
        """
        Copies CSV files from kagglehub download location to raw data directory if needed.
        """
        raw_dir = self.settings.RAW_DATA_DIR
        source_path = Path(source_dir)
        
        # If kagglehub downloaded directly into raw_dir, no copy needed
        if source_path.resolve() == raw_dir.resolve():
            return
            
        logger.info(f"Syncing dataset files from {source_dir} to {raw_dir}...")
        for file_path in source_path.glob("*.csv"):
            dest_path = raw_dir / file_path.name
            shutil.copy2(file_path, dest_path)
            logger.debug(f"Copied {file_path.name} -> {dest_path}")

    def print_raw_files_status(self) -> None:
        """
        Prints a formatted status check of all expected raw CSV tables in data/raw/.
        """
        raw_dir = self.settings.RAW_DATA_DIR
        print("\n" + "=" * 65)
        print(f"RAW DATASET STATUS ({raw_dir})")
        print("=" * 65)
        
        present_count = 0
        for filename in EXPECTED_FILES:
            file_path = raw_dir / filename
            if file_path.exists() and file_path.stat().st_size > 0:
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"[✓ PRESENT] {filename:<40} ({size_mb:>6.2f} MB)")
                present_count += 1
            else:
                print(f"[✗ MISSING] {filename:<40} (   0.00 MB)")
                
        print("-" * 65)
        print(f"Summary: {present_count}/{len(EXPECTED_FILES)} expected tables available.")
        print("=" * 65 + "\n")

    def list_downloaded_files(self) -> List[str]:
        """
        Lists and prints all downloaded CSV tables currently in the raw directory.
        
        Returns:
            List[str]: List of filenames.
        """
        raw_dir = self.settings.RAW_DATA_DIR
        files = [f.name for f in raw_dir.glob("*.csv")]
        print(f"Available tables in {raw_dir}:")
        if not files:
            print("  (No files found)")
        else:
            for idx, filename in enumerate(sorted(files), 1):
                print(f"  {idx}. {filename}")
        return files
            
    def run(self, force_download: bool = False) -> Dict[str, str]:
        """
        Executes the dataset ingestion pipeline with idempotency enforcement.
        
        Args:
            force_download: If True, bypasses local existence check and forces download.
            
        Returns:
            Dict[str, str]: Map of filename to absolute file path in raw data directory.
        """
        raw_dir = self.settings.RAW_DATA_DIR
        dataset_name = self.settings.KAGGLE_DATASET
        
        # 1. Idempotency Check
        if not force_download and self.check_local_dataset_complete():
            logger.info("Dataset already present in data/raw. Skipping download to preserve idempotency.")
            return {filename: str(raw_dir / filename) for filename in EXPECTED_FILES}
            
        # 2. Execute Download via KaggleClient
        logger.info(f"Starting ingestion for dataset '{dataset_name}'...")
        download_path = self.client.download_dataset(
            dataset_name=dataset_name,
            output_dir=str(raw_dir),
            force_download=True,  # Set True so kagglehub ignores .gitkeep when downloading
        )
        
        # 3. Ensure files are synced to RAW_DATA_DIR
        self._sync_files_to_raw(Path(download_path))
        
        # 4. Verify Final State
        if not self.check_local_dataset_complete():
            logger.warning("Ingestion completed, but some expected files may be missing in data/raw.")
            
        # Return map of existing CSV files
        result_files = {}
        for file_path in raw_dir.glob("*.csv"):
            result_files[file_path.name] = str(file_path)
            
        logger.info(f"Ingestion successful. {len(result_files)} files available in {raw_dir}.")
        return result_files

def download_retail_dataset(force_download: bool = False) -> Dict[str, str]:
    """Helper function to execute dataset downloading."""
    downloader = DatasetDownloader()
    return downloader.run(force_download=force_download)
