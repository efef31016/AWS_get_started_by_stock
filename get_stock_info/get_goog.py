'''
我們在此實現與 S3 的數據收集及儲存
'''
import requests
import boto3  # boto3 會自動從 AWS CLI 配置中尋找憑證 (aws configure)
from datetime import datetime
import time
import logging

import os
from dotenv import load_dotenv
load_dotenv()
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Setup basic logging
log_file_name = fr'C:\Users\USER\Desktop\Develop\stock_analysis\log\stock_download_log_{datetime.now().strftime("%Y-%m-%d")}.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=log_file_name,
                    filemode='a')  # Append mode

def make_request_with_exponential_backoff(url, max_attempts=5):
    for attempt in range(max_attempts):
        try:
            headers = {"User-Agent": 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"}
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                wait = 2 ** attempt
                logging.warning(f"Rate limit hit, waiting {wait} seconds")
                time.sleep(wait)
            else:
                return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            time.sleep(2 ** attempt)
    logging.error("Failed to get a successful response after maximum attempts")
    return None

def upload_to_s3(data, bucket_name, file_name):
    try:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=data)
        logging.info(f"File uploaded to S3: {file_name}")
    except Exception as e:
        logging.error(f"Failed to upload file to S3: {e}")

def download_from_s3(bucket_name, s3_file_name, local_file_name):
    try:
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, s3_file_name, local_file_name)
        logging.info(f"File downloaded from S3: {local_file_name}")
    except Exception as e:
        logging.error(f"Failed to download file from S3: {e}")

def uplaod_main():
    URL_GOOG="https://query1.finance.yahoo.com/v7/finance/download/GOOG?period1=0&period2=9999999999&interval=1d&events=history"
    url = URL_GOOG
    bucket_name = "s3-stock-analysis"
    
    response = make_request_with_exponential_backoff(url)
    if response:
        file_name = f"stock_prices_{datetime.now().strftime('%Y-%m-%d')}.csv"
        upload_to_s3(response.content, bucket_name, file_name)
    else:
        logging.error("No response received, aborting file upload")

def download_main():
    bucket_name = 's3-stock-analysis'
    s3_file_name = 'stock_prices_2024-04-06.csv'
    local_file_name = s3_file_name

    download_from_s3(bucket_name, s3_file_name, local_file_name)

if __name__ == "__main__":

    # 抓取數據及儲存至 AWS
    uplaod_main()

    # 載入數據至本地
    download_main()