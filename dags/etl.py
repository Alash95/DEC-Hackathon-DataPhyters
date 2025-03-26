import logging
import pandas as pd
import extract
import transform
import load
from config import URL, DATABASE_URI    

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_college_data():
    """
    Extract college data from the College Scorecard API
    
    Returns:
        pd.DataFrame: Raw college data
    """
    try:
        logger.info("Starting data extraction from College Scorecard API")
        raw_data = extract.request_data(URL)
        
        if raw_data is None or raw_data.empty:
            logger.error("No data extracted from the API")
            raise ValueError("No data retrieved from College Scorecard API")
        
        logger.info(f"Successfully extracted {len(raw_data)} records")
        return raw_data
    
    except Exception as e:
        logger.error(f"Error during data extraction: {e}")
        raise

def validate_data(raw_data):
    """
    Validate the extracted data
    
    Args:
        raw_data (pd.DataFrame): Raw extracted data
    
    Returns:
        pd.DataFrame: Validated data
    """
    try:
        logger.info("Starting data validation")
        
        # Check basic validation criteria
        if raw_data is None or raw_data.empty:
            logger.error("Empty DataFrame received")
            raise ValueError("Cannot validate empty DataFrame")
        
        # Check for minimum required columns
        required_columns = [
            'id', 'latest.school.name', 'latest.school.state', 
            'latest.student.size', 'latest.admissions.admission_rate.overall'
        ]
        
        missing_columns = [col for col in required_columns if col not in raw_data.columns]
        
        if missing_columns:
            logger.error(f"Missing critical columns: {missing_columns}")
            raise ValueError(f"Missing critical columns: {missing_columns}")
        
        # Additional validation checks
        if raw_data['id'].isnull().sum() > 0:
            logger.warning("Some records have missing school IDs")
        
        if raw_data['latest.school.state'].nunique() < 10:
            logger.warning("Unusually low number of unique states")
        
        logger.info("Data validation completed successfully")
        return raw_data
    
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        raise

def transform_college_data(validated_data):
    """
    Transform the validated college data
    
    Args:
        validated_data (pd.DataFrame): Validated raw data
    
    Returns:
        dict: Transformed data tables
    """
    try:
        logger.info("Starting data transformation")
        processed_data = transform.process_data(validated_data)
        ranked_colleges = transform.process_and_rank_colleges(processed_data)
        cleaned_tables = transform.clean_college_data(ranked_colleges)
        
        comprehensive_transforms = transform.transform_schools_data(cleaned_tables)
        
        logger.info("Data transformation completed successfully")
        return comprehensive_transforms
    
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise

def load_college_data(transformed_data):
    """
    Load transformed data into the database
    
    Args:
        transformed_data (dict): Dictionary of transformed data tables
    
    Returns:
        dict: Number of rows loaded into each table
    """
    try:
        logger.info("Starting data loading process")
        load_results = load.load_college_data(transformed_data, DATABASE_URI)
        
        # Log detailed loading results
        for table, rows in load_results.items():
            logger.info(f"Loaded {rows} rows into {table}")
        
        logger.info("Data loading completed successfully")
        return load_results
    
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise

# Optional: Main block for standalone execution
if __name__ == "__main__":
    try:
        raw_data = extract_college_data()
        validated_data = validate_data(raw_data)
        transformed_data = transform_college_data(validated_data)
        load_college_data(transformed_data)
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")