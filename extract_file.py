import logging
from raw_file import process_data, request_data

def main():
    URL = "https://api.data.gov/ed/collegescorecard/v1/schools?api_key=m5pGmzbtSNdDyD4ItXDZEDeWTetKjH4ULb80ncmQ"
    for i in range(10):
        logging.info(f"Requesting for page {i+1}")
        res = request_data(URL, {"page": i, "per_page": 100})
        processed_data = process_data(res)
        # print(processed_data)
        yield processed_data

if __name__ == "__main__":
    data = main()
    for page in data:
        print(page)
