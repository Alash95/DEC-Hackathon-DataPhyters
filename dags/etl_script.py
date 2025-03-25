import logging
import pandas as pd
from extract import fetch_all_data, rank_and_sort_schools
from transform import transform_schools_data
from load import load_data
import time
from config import DATABASE_URI, URL

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_college_data():
    logging.info("Starting data extraction process")

    # Step 1: Fetch all data from the API
    all_data = fetch_all_data(URL)

    # Step 2: Rank and sort all schools globally
    ranked_schools = rank_and_sort_schools(all_data)

    # Step 3: Extract the top 1000 schools
    top_1000_schools = ranked_schools

    # Convert to DataFrame
    college_data = pd.DataFrame(top_1000_schools)
    logging.info(f"Extracted {len(college_data)} top-ranked schools.")

    # Save to CSV with clear column naming and without index
    output_file = "top_1000_colleges.csv"
    college_data.to_csv(output_file, index=False)
    
    # Log CSV creation
    logging.info(f"Saved top 1000 colleges data to {output_file}")
    
    return college_data
    

def transform_college_data(raw_data):
    logging.info("Starting data transformation process")
    
    # Transform the data into separate tables
    df_schools, df_demographics, df_admission, df_costs, df_aid, df_completion = transform_schools_data(raw_data)
    
    # Clean the data
    df_schools, df_demographics, df_admission, df_costs, df_aid, df_completion = clean_college_data(
        df_schools, df_demographics, df_admission, df_costs, df_aid, df_completion
    )
    
    # Package the dataframes into a dictionary
    tables = {
        'schools': df_schools.to_dict(orient='records'),
        'demographics': df_demographics.to_dict(orient='records'),
        'admission': df_admission.to_dict(orient='records'),
        'costs': df_costs.to_dict(orient='records'),
        'financial_aid': df_aid.to_dict(orient='records'),
        'completion': df_completion.to_dict(orient='records')
    }
    
    logging.info("Data transformation complete")
    
    # Proceed to loading after transformation
    load_college_data(tables=tables, connection_string=DATABASE_URI)  # Call load function here
    return tables

def load_college_data(tables, connection_string=None):
    logging.info("Starting data loading process")
    
    if connection_string is None:
        # Load database credentials from environment variables
        connection_string = DATABASE_URI
    
    # Load data to PostgreSQL
    load_data(
        tables['schools'], 
        tables['demographics'], 
        tables['admission'], 
        tables['costs'], 
        tables['financial_aid'], 
        tables['completion'], 
        connection_string
    )
    
    logging.info("Data loading process complete")
    return True

if __name__ == "__main__":
    # Start the ETL process by calling the extract function
    extract_college_data()