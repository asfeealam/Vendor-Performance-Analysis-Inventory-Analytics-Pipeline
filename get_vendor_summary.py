import pandas as pd
import logging
from sqlalchemy import create_engine

# ---------------- LOGGING ----------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ---------------- DB ENGINE ----------------
engine = create_engine("sqlite:///inventory.db", future=True)

# ---------------- FUNCTIONS ----------------
def create_vendor_summary(conn):
    """Create vendor sales summary using SQL."""
    query = """
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber, p.VendorName, p.Brand,
            p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql_query(query, conn)


def clean_data(df):
    """Clean and enrich vendor summary data."""
    df = df.copy()

    # changing datatype to float
    df['Volume'] = df['Volume'].astype('float')
    
    # filling missing value with 0
    df.fillna(0, inplace = True)
    
    # removing spaces from categorical columns
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()

    # creating new columns for better analysis
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]
    df["ProfitMargin"] = (df["GrossProfit"] / df["TotalSalesDollars"]) * 100
    df["StockTurnover"] = df["TotalSalesQuantity"] / df["TotalPurchaseQuantity"]
    df["SalesToPurchaseRatio"] = df["TotalSalesDollars"] / df["TotalPurchaseDollars"]
    
    df.replace([np.inf, -np.inf], 0, inplace=True)
    df.fillna(0, inplace=True)

    return df


def ingest_df_to_db(df, table_name, engine):
    """Ingest final DataFrame into SQLite."""
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=10_000
    )


# ---------------- MAIN ----------------
if __name__ == "__main__":
    with engine.connect() as conn:
        logging.info("Creating vendor summary")
        summary_df = create_vendor_summary(conn)
        logging.info(summary_df.head())

        logging.info("Cleaning data")
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head())
        
        # ✅ EXPORT FOR POWER BI (ADD THIS)
        clean_df.to_csv(
            "data/processed/vendor_sales_summary.csv",
            index=False)

        logging.info("Ingesting vendor_sales_summary table")
        ingest_df_to_db(clean_df, "vendor_sales_summary", engine)

        logging.info("Vendor summary pipeline completed successfully")
