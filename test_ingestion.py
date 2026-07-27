"""
Sequential OOP test script for the Retail Analytics Platform Ingestion Module.
Run this script from the project root: python test_ingestion.py
"""
import logging
from retail_platform.ingestion import DatasetDownloader

# Configure logging for clear visual feedback
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

def main():
    print("\n" + "#" * 70)
    print("### RETAIL ANALYTICS PLATFORM - INGESTION MODULE OOP TEST ###")
    print("#" * 70)

    # 1. Initialize DatasetDownloader OOP Class
    print("\n[Step 1] Initializing DatasetDownloader OOP Class...")
    pipeline = DatasetDownloader()
    print("✓ DatasetDownloader successfully initialized.")

    # 2. Check initial status of raw tables in data/raw/
    print("\n[Step 2] Checking initial local dataset status before ingestion...")
    pipeline.print_raw_files_status()

    # 3. Execute idempotent Kaggle ingestion
    print("\n[Step 3] Executing dataset ingestion (idempotent download)...")
    files_map = pipeline.run(force_download=False)
    print(f"✓ Ingestion run complete. {len(files_map)} files processed.")

    # 4. Verify downloaded dataset tables and print summary
    print("\n[Step 4] Verifying downloaded dataset tables...")
    pipeline.print_raw_files_status()
    pipeline.list_downloaded_files()

    # 5. Idempotency verification test
    print("\n[Step 5] Testing idempotency (running ingestion a second time)...")
    pipeline.run(force_download=False)

    print("\n" + "#" * 70)
    print("### SUCCESS: Ingestion OOP pipeline execution complete! ###")
    print("#" * 70 + "\n")

if __name__ == "__main__":
    main()
