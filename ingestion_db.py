import pandas as pd
import os
import logging
import time
from sqlalchemy import create_engine

# ---------------- LOGGING ----------------
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ---------------- DB ENGINE ----------------
engine = create_engine("sqlite:///inventory.db", future=True)

# ---------------- CONFIG ----------------
CHUNK_SIZE = 50_000      # read size
SQLITE_CHUNK = 10_000    # insert size (SQLite-safe)

# ---------------- FUNCTIONS ----------------
def ingest_csv_to_db(csv_path, table_name, engine):
    """Ingest large CSV files into SQLite using chunking."""
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE)):
        chunk.to_sql(
            table_name,
            con=engine,
            if_exists="replace" if i == 0 else "append",
            index=False,
            chunksize=SQLITE_CHUNK
        )
        logging.info(f"{table_name}: inserted {(i + 1) * CHUNK_SIZE:,} rows")


def load_raw_data():
    """Load all CSV files from /data into SQLite."""
    start = time.time()

    for file in os.listdir("data"):
        if file.endswith(".csv"):
            logging.info(f"Processing {file}")
            ingest_csv_to_db(
                csv_path=os.path.join("data", file),
                table_name=file[:-4],
                engine=engine
            )

    total_time = (time.time() - start) / 60
    logging.info(f"Processing complete. Total time: {total_time:.2f} minutes")


if __name__ == "__main__":
    load_raw_data()