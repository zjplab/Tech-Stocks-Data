#!/usr/bin/env python3
import os
import yfinance as yf
import pandas as pd
import logging

# Configure logging to display timestamped messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_stock_data(ticker: str, folder: str, period: str = "max", interval: str = "1h"):
    """
    Fetch historical data for the given ticker and save it as a CSV file in the specified folder.

    Parameters:
      ticker (str): The stock ticker symbol.
      folder (str): The folder where the CSV file will be saved.
      period (str): The time period of data to retrieve (default "max" for all available data).
      interval (str): The data interval (default "1h" for hourly data).
    """
    logging.info(f"Fetching data for {ticker} with period={period} and interval={interval}")
    try:
        data = yf.download(ticker, period=period, interval=interval)
        if data.empty:
            logging.warning(f"No data returned for {ticker}")
        else:
            file_path = os.path.join(folder, f"{ticker}.csv")
            data.to_csv(file_path)
            logging.info(f"Data for {ticker} written to {file_path}")
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")

def main():
    # Define the list of NASDAQ 100 stocks
    nasdaq_100 = [
        "AAPL", "ABNB", "ADBE", "ADI", "ADP", "ADSK", "AEP", "AMAT", "AMD", "AMGN", "AMZN", "ANSS", "APP", "ARM", "ASML", "AVGO", "AXON", "AZN", "BIIB", "BKNG", "BKR", "CCEP", "CDNS", "CDW", "CEG", "CHTR", "CMCSA", "COST", "CPRT", "CRWD", "CSCO", "CSGP", "CSX", "CTAS", "CTSH", "DASH", "DDOG", "DXCM", "EA", "EXC", "FANG", "FAST", "FTNT", "GEHC", "GFS", "GILD", "GOOG", "GOOGL", "HON", "IDXX", "INTC", "INTU", "ISRG", "KDP", "KHC", "KLAC", "LIN", "LRCX", "LULU", "MAR", "MCHP", "MDB", "MDLZ", "MELI", "META", "MNST", "MRVL", "MSFT", "MSTR", "MU", "NFLX", "NVDA", "NXPI", "ODFL", "ON", "ORLY", "PANW", "PAYX", "PCAR", "PDD", "PEP", "PLTR", "PYPL", "QCOM", "REGN", "ROP", "ROST", "SBUX", "SNPS", "TEAM", "TMUS", "TSLA", "TTD", "TTWO", "TXN", "VRSK", "VRTX", "WBD", "WDAY", "XEL", "ZS"
    ]
    
    # Define the list of Hang Seng Tech stocks
    hangseng_tech = [
        "1810.HK", "9618.HK", "9988.HK", "0700.HK", "0981.HK", "3690.HK", "1024.HK", "2015.HK", "9999.HK", "9868.HK",
        "9961.HK", "0992.HK", "6690.HK", "9888.HK", "2382.HK", "0020.HK", "9626.HK", "0300.HK", "3888.HK", "6618.HK",
        "0285.HK", "0268.HK", "0780.HK", "0522.HK", "0241.HK", "1347.HK", "0772.HK", "9866.HK", "6060.HK", "1797.HK"
    ]
    
    # Create directories for storing data if they do not exist
    os.makedirs("data/NASDAQ100", exist_ok=True)
    os.makedirs("data/HangSengTech", exist_ok=True)
    
    # Update historical data for NASDAQ 100 stocks
    for ticker in nasdaq_100:
        update_stock_data(ticker, "data/NASDAQ100", interval="1d")
    
    # Update historical data for Hang Seng Tech stocks
    for ticker in hangseng_tech:
        update_stock_data(ticker, "data/HangSengTech", interval="1d")
    
if __name__ == "__main__":
    main()
