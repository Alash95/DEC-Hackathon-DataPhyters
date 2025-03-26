import logging
import requests
import time
import json
import os
from dotenv import load_dotenv
import pandas as pd
from config import URL
import transform
import load
from config import DATABASE_URI
load_dotenv()

API_KEY = os.getenv('API_KEY')

# def request_data(url, params):
#     base_url = "https://api.data.gov/ed/collegescorecard/v1/schools"
#     logging.info(f"Requesting data from {base_url}")
#     response = requests.get(url, params=params)
#     status = response.status_code
#     logging.info(f"status code for the request {status}")
#     if response.ok:
#         data = response.json()
#         return data["results"]
#     logging.info("Request failed but requesting one more time")
#     return request_data(url, params)

def request_data(url):
    total_pages = 64  # Total number of pages to fetch
    results = []
    for pg in range(total_pages):  # Loop through all pages (0 to 65)
        logging.info(f"Requesting data for page {pg}")
        
        # Construct the URL with pagination
        paginated_url = f"{url}&page={pg}&per_page={100}"
        
        # Retry mechanism
        max_retries = 5
        retry_count = 0
        success = False
        
        while retry_count < max_retries:
            try:
                # Fetch data for the current page
                response = requests.get(paginated_url)
                
                if response.ok:
                    # Log the success status code
                    logging.info(f"Successfully fetched data for page {pg}. Status code: {response.status_code}")
                    
                    # Parse and accumulate the data
                    data = response.json()
                    data = data['results']
                    # fetched_data = pd.concat([all_data, results], ignore_index=True)
                    results.extend(data)  
                    logging.info(f"Fetched {len(results)} records for page {pg}.")
                    success = True
                    break  # Exit the retry loop if successful
                
                else:
                    # Log the failure status code and retry
                    logging.warning(f"Failed to fetch data for page {pg}. Status code: {response.status_code}. Retrying ({retry_count + 1}/{max_retries})...")
                    retry_count += 1
                    time.sleep(10)  # Wait 10 seconds before retrying
            
            except requests.exceptions.RequestException as e:
                # Handle connection errors or timeouts
                logging.error(f"Request failed for page {pg}: {e}. Retrying ({retry_count + 1}/{max_retries})...")
                retry_count += 1
                time.sleep(10)  # Wait 10 seconds before retrying
        
        if not success:
            # Log the failure after exhausting all retries
            logging.error(f"Failed to fetch data for page {pg} after {max_retries} attempts.")
        
        # Sleep to avoid hitting API rate limits
        time.sleep(15)

    # Log the total number of records fetched across all pages
    logging.info(f"Fetched {len(results)} schools across {total_pages} pages.")
    return results

# def request_data(url, params, max_retries=5, backoff_factor=2):
#     url = URL
#     for attempt in range(max_retries):
#         try:
#             logging.info(f"Requesting data from {url} (Attempt {attempt + 1})")
#             response = requests.get(url, params=params, timeout=10)
#             response.raise_for_status()  # Raises an HTTPError for bad responses
            
#             data = response.json()
#             return data["results"]
        
#         except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
#             logging.warning(f"Request failed: {e}")
            
#             if attempt < max_retries - 1:
#                 # Exponential backoff
#                 wait_time = backoff_factor ** attempt
#                 logging.info(f"Retrying in {wait_time} seconds...")
#                 time.sleep(wait_time)
#             else:
#                 logging.error("Max retries reached. Request failed.")
#                 raise

# Main execution (optional, can be imported as a module)
if __name__ == "__main__":
    # Example usage - replace with actual data loading mechanism
    example_data = request_data(URL, {'page':80, 'per_page':100})  # Load your actual data here
    # print(example_data[0]['latest']['admissions'])
    processed_data = transform.process_data(example_data)
    if example_data:
        result = transform.process_and_rank_colleges(processed_data)
        # print(result)
        cleaned_tables = transform.clean_college_data(result)
        # print(cleaned_tables)
        load.main(cleaned_tables, DATABASE_URI)
        print("load_tables successfully")

