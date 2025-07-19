
import logging
import os
import pandas as pd
import re
from datetime import datetime
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
static_time = datetime.now().strftime('%Y-%m-%d')

# InfluxDB setup
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")
token = os.getenv("INFLUXDB_TOKEN")
bucket = os.getenv("INFLUXDB_BUCKET")

logging.info(f"Moving Average Run Started:")
# Initialize the InfluxDB client
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()
write_api = client.write_api(write_options=SYNCHRONOUS)

# Define the Flux query
query = '''
from(bucket: "stocks_5m")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "tick" and r._field == "Volume")
  |> group(columns: ["ticker"])
  |> sort(columns: ["_time"]) 
'''
def calculate_and_store_moving_average():
    try:
        logging.info("Starting the calculation of 10-day moving averages.")
        result = query_api.query(org=org, query=query)
        
        data = []
        for table in result:
            for record in table.records:
                data.append((record.get_time(), record.get_value(), record.values['ticker']))
        
        if not data:
            logging.warning("No data retrieved from InfluxDB.")
            return
        
        df = pd.DataFrame(data, columns=['time', 'Volume', 'ticker'])
        
        # Calculate the 10-day moving average for each ticker
        for ticker in df['ticker'].unique():
            ticker_df = df[df['ticker'] == ticker].sort_values(by='time')
            
            # Skip the ticker if the volume field is blank or null
            if ticker_df['Volume'].isnull().all():
                logging.warning(f"No valid volume data for {ticker}. Skipping this ticker.")
                continue
            
            ticker_df['10_day_moving_avg'] = ticker_df['Volume'].rolling(window=10).mean()
            
            # Get the last moving average value
            last_10_day_avg = ticker_df.iloc[-1]['10_day_moving_avg']
            
            # Check if the moving average calculation is valid
            if pd.isna(last_10_day_avg):
                logging.warning(f"Insufficient data to calculate 10-day moving average for {ticker}.")
                continue
            
            # Prepare the data point to write back to InfluxDB
            point = influxdb_client.Point("10mav").tag("ticker", ticker).field("10_day_moving_avg", last_10_day_avg).time(ticker_df.iloc[-1]['time'])
            
            # Write the point to InfluxDB
            write_api.write(bucket=bucket, org=org, record=point)
            logging.info(f"Stored 10-day moving average for {ticker}: {last_10_day_avg}")
        
        logging.info("Moving averages calculated and stored successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

calculate_and_store_moving_average()

