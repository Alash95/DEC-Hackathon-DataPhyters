import logging
import requests
import time
import pandas as pd
from config import URL

def fetch_all_data(url):
    all_data = []
    total_pages = 66  # Total number of pages to fetch

    for pg in range(total_pages):  # Loop through all pages (0 to 65)
        logging.info(f"Requesting data for page {pg}")
        
        # Construct the URL with pagination
        paginated_url = f"{url}&page={pg}"
        
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
                    results = data.get('results', [])
                    all_data.extend(results)
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
    logging.info(f"Fetched {len(all_data)} schools across {total_pages} pages.")
    college_data = pd.DataFrame(all_data)
    logging.info(f"Data saved to Dataframe")
    return college_data

def rank_and_sort_schools(data):
    logging.info("Processing data for schools")
    processed_data = []

    def safe_max(keys, default=0):
        
        def get_nested_value(d, keys):
            try:
                value = d
                for key in keys:
                    value = value.get(key, {})
                return value if value not in [None, {}] else default
            except Exception:
                return default

        # Filter out values that are not numbers
        valid_values = [
            get_nested_value(d, keys) 
            for d in data 
            if isinstance(get_nested_value(d, keys), (int, float))
        ]

        # Return max of valid values or default if no valid values found
        return max(valid_values) if valid_values else default
    # Precompute max values for normalization
    max_student_size = safe_max(('latest', 'student', 'size'))
    max_completion_rate = safe_max(('latest', 'completion', 'consumer_rate'))
    max_sat_score = safe_max(('latest', 'admissions', 'sat_scores', 'average', 'overall'))
    max_act_score = safe_max(('latest', 'admissions', 'act_scores', 'midpoint', 'cumulative'))
    
    for result in data:
        try:
            latest = result.get('latest', {})
            school = latest.get('school', {})
            student = latest.get('student', {})
            admission = latest.get('admissions', {})
            completion = latest.get('completion', {})
            cost = latest.get('cost', {})
            aid = latest.get('aid', {})

            # Extract scores with defaults
            sat_score = admission.get('sat_scores', {}).get('average', {}).get('overall', 0)
            act_score = admission.get('act_scores', {}).get('midpoint', {}).get('cumulative', 0)

            # Default values for missing fields
            row = {
                'id': result.get('id'),
                'School_Name': school.get('name', 'Unknown School'),
                'Address': school.get('address', 'Unknown Address'),
                'State': school.get('state', 'Unknown State'),
                'City': school.get('city', 'Unknown City'),
                'latitude': result.get('location', {}).get('lat'),
                'longitude': result.get('location', {}).get('lon'),
                'Student_Size': student.get('size', 0),
                'Highest_Degree': school.get('degrees_awarded', {}).get('highest', 'Unknown Degree'),
                'Predominant_Degree': school.get('degrees_awarded', {}).get('predominant', 'Unknown Degree'),
                'Admission_Rate_Overall': admission.get('admission_rate', {}).get('overall', 1),
                'Completion_Rate': completion.get('consumer_rate', 0),
                'In_State_Tuition': cost.get('tuition', {}).get('in_state', 0),
                'Out_of_State_Tuition': cost.get('tuition', {}).get('out_of_state', 0),
                'Loan_Principal': aid.get('loan_principal', 0),
                'SAT_Score': sat_score,
                'ACT_Score': act_score
            }

            # Calculate score using precomputed max values
            row['Score'] = (
                (row['Student_Size'] / max(max_student_size, 1)) * 0.3 +       # Normalize student size
                (1 - row['Admission_Rate_Overall']) * 0.2 +                     # Lower admission rate -> higher score
                (row['Completion_Rate'] / max(max_completion_rate, 1)) * 0.2 +  # Normalize completion rate
                (row['SAT_Score'] / max(max_sat_score, 1)) * 0.2 +              # Higher SAT score -> higher score
                (row['ACT_Score'] / max(max_act_score, 1)) * 0.1                # Higher ACT score -> higher score
            )

            processed_data.append(row)

        except Exception as e:
            logging.error(f"Error processing record: {e}")

    # Sort schools by score in descending order
    sorted_schools = sorted(processed_data, key=lambda x: x['Score'], reverse=True)
    logging.info("Data processing complete.")
    return sorted_schools

