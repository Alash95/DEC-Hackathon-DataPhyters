import os
import logging
from dotenv import load_dotenv
import pandas as pd
from raw_file import process_data, request_data

load_dotenv()
API_KEY = os.getenv('API_KEY')
def main():

    URL = f"https://api.data.gov/ed/collegescorecard/v1/schools?api_key={API_KEY}&sort=latest.student.size:desc"
    raw_data = []
    for i in range(65):
        # page (i + 1) to not make the page number starts from
        # 0 like it is used in the api
        logging.info(f"Requesting for page {i+1}")
        params = {
            'per_page': 100,
            'page': i
        }
        res = request_data(URL, params)
        logging.info(f"Got {len(res)} on page {i+1}")
        raw_data.extend(res)
    logging.info(f"Total data is {len(res)}")
    print()
    processed_data = process_data(raw_data)
    processed_data.to_csv('extracted2_data.csv')
    return processed_data
        #yield processed_data

if __name__ == "__main__":
    data = main()
    for page in data:
        print(page)
        