import pandas as pd
import sqlite3
import logging
import os
import numpy as np
from ingestion_db import ingest_db

# Create the 'logs' directory if it doesn't exist
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"  # Use 'w' to overwrite the log each time you run
)

def create_vendor_summary(conn):
    """This function merges the different tables to get the overall vendor summary."""
    # This SQL query is correct and remains unchanged
    query = """
        WITH FreightSummary AS (
            SELECT VendorNumber, SUM(Freight) AS FreightCost
            FROM vendor_invoice GROUP BY VendorNumber
        ),
        PurchaseSummary AS (
            SELECT
                p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice,
                pp.Price AS ActualPrice, pp.Volume,
                SUM(p.Quantity) AS TotalPurchaseQuantity, SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
        ),
        SalesSummary AS (
            SELECT
                VendorNo, Brand, SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(SalesDollars) AS TotalSalesDollars, SUM(SalesPrice) AS TotalSalesPrice,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        )
        SELECT
            ps.VendorNumber, ps.VendorName, ps.Brand, ps.Description, ps.PurchasePrice,
            ps.ActualPrice, ps.Volume, ps.TotalPurchaseQuantity, ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity, ss.TotalSalesDollars, ss.TotalSalesPrice,
            ss.TotalExciseTax, fs.FreightCost
        FROM PurchaseSummary ps
        LEFT JOIN SalesSummary ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber
    """
    return pd.read_sql_query(query, conn)

def clean_and_transform_data(df):
    """This function cleans the data and adds new analytical columns."""
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    df.fillna(0, inplace=True)
    
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # **CORRECTION**: All calculations now correctly use 'df'
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    
    # **CORRECTION**: Handle potential division by zero errors
    df['ProfitMargin'] = ((df['GrossProfit'] / df['TotalSalesDollars']) * 100).replace([np.inf, -np.inf], 0)
    df['StockTurnover'] = (df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']).replace([np.inf, -np.inf], 0)
    df['SalesToPurchaseRatio'] = (df['TotalSalesDollars'] / df['TotalPurchaseDollars']).replace([np.inf, -np.inf], 0)
    
    return df

if __name__ == '__main__':
    conn = None
    try:
        conn = sqlite3.connect('inventory.db')
        logging.info('PROCESS STARTED: Creating Vendor Summary Table.')
        
        summary_df = create_vendor_summary(conn)
        logging.info('Step 1/3: Successfully created summary DataFrame.')

        clean_df = clean_and_transform_data(summary_df)
        logging.info('Step 2/3: Successfully cleaned and transformed DataFrame.')

        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Step 3/3: Successfully ingested final data into the database.')

    except Exception as e:
        logging.error(f"AN ERROR OCCURRED: {e}")
    
    finally:
        if conn:
            conn.close()
        logging.info("PROCESS FINISHED.")