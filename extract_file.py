import os
import logging
from dotenv import load_dotenv
import pandas as pd
from raw_file import process_data, request_data

load_dotenv()
API_KEY = os.getenv('API_KEY')
def main():

    URL = f"https://api.data.gov/ed/collegescorecard/v1/schools?api_key={API_KEY}&sort=latest.student.size:desc"
    for i in range(10):
        logging.info(f"Requesting for page {i+1}")
        res = request_data(URL, {"page": i, "per_page": 100})
        processed_data = process_data(res)
        processed_data.to_csv('processed_data.csv')
        #yield processed_data

if __name__ == "__main__":
    data = main()
    for page in data:
        print(page)