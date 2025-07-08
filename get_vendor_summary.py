import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db


logging.basicConfig(
    filename="logs/get_vender_summary.log",
    level=logging.DEBUG,
    format='%(asctime)s-%(levelname)s-%(message)s',
    filemode='a'
)

def create_vendor_summary(conn):
    """this function will merge the different table to get the overall vender summary and adding new columns in the resultant data"""
    vendor_sales_summary=pd.read_sql_query("""with FreightSummary as(
        select
            VendorNumber,
            sum(Freight) as FreightCost
            from vendor_invoice
            group by VendorNumber
        ),
        PurchaseSummary as(
        select
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice, 
            pp.price as ActualPrice,
            pp.Volume,
            sum(p.Quantity) as TotalPurchaseQuantity,
            sum(p.Dollars) as TotalPurchaseDollars
            from Purchases p
            join purchase_prices pp
            on p.Brand=pp.Brand
            where p.PurchasePrice>0
            group by p.VendorNumber,p.VendorName, p.Brand,p.Description,p.PurchasePrice,pp.Price,pp.Volume
        ),
        SalesSummary as(
        select
            VendorNo,
            Brand,
            sum(s.SalesQuantity) as TotalSalesQuantity,
            sum(s.SalesDollars) as TotalSalesDollars,
            sum(s.SalesPrice) as TotalSalesPrice,
            sum(s.ExciseTax) as TotalExciseTax
            from sales s
            group by VendorNo,Brand
        ) 

        select
            ps.VendorNumber,
            ps.VendorName,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice, 
            ps.ActualPrice,
            ps.Volume,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalExciseTax,
            ps.TotalPurchaseQuantity,
            ps.TotalPurchaseDollars,
            fs.FreightCost
            from PurchaseSummary ps

            left join SalesSummary ss
            on ps.VendorNumber = ss.VendorNo
            and ps.Brand=ss.Brand

            left join FreightSummary fs
            on ps.VendorNumber= fs.VendorNumber
            order by ps.TotalPurchaseDollars desc
        """,conn)
    return vendor_sales_summary


def clean_data(df):
    '''this function will clean the data'''
    df['Volume'] = df['Volume'].astype('float64')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'].replace(0, 1)) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, 1)
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, 1)
    
    return df
    
    
if __name__ == '__main__':
    try:
        conn = sqlite3.connect('inventory.db')
        logging.info('Creating Vendor Summary Table..')
        summary_df = create_vendor_summary(conn)
        logging.info(summary_df.head())

        logging.info('Cleaning data..')
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head())

        logging.info('Ingesting Data....')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Completed')

    except Exception as e:
        logging.error(f"Error in processing: {e}")
    