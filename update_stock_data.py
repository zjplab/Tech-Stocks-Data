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
    # Define the list of MAG7 stocks (modify these as needed)
    mag7 = ["META", "AAPL", "GOOGL", "AMZN", "NFLX", "MSFT", "TSLA"]
    
    # Define the list of top 20 Hang Seng Tech stocks (tickers may be adjusted)
    hangseng_tech = [
        "0700.HK", "9988.HK", "3690.HK", "1810.HK", "3692.HK",
        "2018.HK", "6862.HK", "0762.HK", "2688.HK", "2388.HK",
        "1109.HK", "1177.HK", "3888.HK", "1299.HK", "3388.HK",
        "2380.HK", "0823.HK", "0763.HK", "2382.HK", "2318.HK"
    ]
    
    # Create directories for storing data if they do not exist
    os.makedirs("data/MAG7", exist_ok=True)
    os.makedirs("data/HangSengTech", exist_ok=True)
    
    # Update historical data for MAG7 stocks
    for ticker in mag7:
        update_stock_data(ticker, "data/MAG7")
    
    # Update historical data for Hang Seng Tech stocks
    for ticker in hangseng_tech:
        update_stock_data(ticker, "data/HangSengTech")
    
if __name__ == "__main__":
    main()
