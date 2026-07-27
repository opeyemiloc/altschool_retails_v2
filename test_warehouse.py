"""
Sequential OOP test script for the Retail Analytics Platform Warehouse Module.
Run this script from the project root: python test_warehouse.py
"""
import logging
from retail_platform.warehouse import PostgresLoader

# Configure logging for clear visual feedback
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

def main():
    print("\n" + "#" * 70)
    print("### RETAIL ANALYTICS PLATFORM - WAREHOUSE MODULE OOP TEST ###")
    print("#" * 70)

    # 1. Initialize PostgresLoader OOP Class
    print("\n[Step 1] Initializing PostgresLoader OOP Class...")
    loader = PostgresLoader()
    print("[OK] PostgresLoader successfully initialized.")

    # 2. Test database connection
    print("\n[Step 2] Testing PostgreSQL operational database connection...")
    connected = loader.test_connection()
    if not connected:
        print("[ERROR] Database connection failed. Please ensure Docker containers are running (docker compose up -d).")
        return

    # 3. Check initial table row counts
    print("\n[Step 3] Checking initial operational table row counts...")
    loader.print_table_row_counts()

    # 4. Execute chunked data loading into PostgreSQL
    print("\n[Step 4] Executing chunked data loading from data/raw into PostgreSQL...")
    results = loader.load_all(chunksize=10000, truncate_first=True)
    print(f"[OK] Warehouse loading run complete. Loaded {len(results)} tables.")

    # 5. Verify final table row counts
    print("\n[Step 5] Verifying final PostgreSQL table row counts...")
    loader.print_table_row_counts()

    # 6. Idempotency verification test
    print("\n[Step 6] Testing idempotency (running warehouse loader a second time)...")
    loader.load_all(chunksize=10000, truncate_first=True)
    print("\n[Step 7] Final verification after idempotency test...")
    loader.print_table_row_counts()

    print("\n" + "#" * 70)
    print("### SUCCESS: Warehouse OOP pipeline execution complete! ###")
    print("#" * 70 + "\n")

if __name__ == "__main__":
    main()
