import os
import sys
import logging
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl import (
    extract_college_data, 
    validate_data, 
    transform_college_data, 
    load_college_data
)
from config import DATABASE_URI, URL

def run_full_pipeline():
    """
    Execute the complete ETL pipeline with logging and error handling
    """
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Extract stage
        logger.info("Starting Extract stage")
        raw_data = extract_college_data()
        
        # Validate stage
        logger.info("Starting Validate stage")
        validated_data = validate_data(raw_data)
        
        # Transform stage
        logger.info("Starting Transform stage")
        transformed_data = transform_college_data(validated_data)
        
        # Load stage
        logger.info("Starting Load stage")
        load_results = load_college_data(transformed_data)
        
        logger.info("ETL Pipeline completed successfully")
        return load_results
    
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise

def generate_pipeline_report(load_results):
    """
    Generate a report of the ETL pipeline run
    """
    report = {
        'timestamp': datetime.now().isoformat(),
        'status': 'success',
        'load_details': load_results
    }
    
    # Optional: Save report to a file
    report_filename = f"etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_filename, 'w') as f:
        import json
        json.dump(report, f, indent=4)
    
    return report

if __name__ == "__main__":
    try:
        results = run_full_pipeline()
        report = generate_pipeline_report(results)
        print("Pipeline completed successfully")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)