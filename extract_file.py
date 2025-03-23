import os
import logging
from dotenv import load_dotenv
from raw_file import process_data, request_data

load_dotenv()
API_KEY = os.getenv('api_key')
def main():

    URL = f"https://api.data.gov/ed/collegescorecard/v1/schools?api_key={API_KEY}"
    for i in range(10):
        logging.info(f"Requesting for page {i+1}")
        res = request_data(URL, {"page": i, "per_page": 100, "sort":"latest.student.size:desc"})
        processed_data = process_data(res)
        # print(processed_data)
        yield processed_data

if __name__ == "__main__":
    data = main()
    for page in data:
        print(page)
