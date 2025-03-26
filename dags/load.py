import logging
import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String  # add other components as needed
from sqlalchemy.exc import SQLAlchemyError
import sys
sys.path.append("C:\DEC_Code\DEC-Hackathon-Team-5\config.py")
from config import DATABASE_URI, URL
import transform
import extract
import time

def setup_logging():
    """Configure logging for the data loader"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def create_db_engine(conn):
    """
    Safely create a database engine and test the connection
    
    :param conn: Connection string or SQLAlchemy database URL
    :return: SQLAlchemy engine object
    """
    try:
        # Use create_engine with proper configuration
        engine = create_engine(conn, pool_pre_ping=True)
        
        # Use text() to properly prepare the SQL statement
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        
        return engine
    
    except SQLAlchemyError as e:
        # More detailed error handling
        print(f"Database connection error: {e}")
        raise ConnectionError(f"Failed to establish database connection: {e}")

def load_dataframe(engine, dataframe, table_name, if_exists='replace'):
    """
    Load a DataFrame into a specified database table
    
    :param engine: SQLAlchemy database engine
    :param dataframe: Pandas DataFrame to load
    :param table_name: Name of the target database table
    :param if_exists: Action if table exists ('fail', 'replace', 'append')
    :return: Number of rows inserted
    """
    try:
        # Remove any NaN values to prevent database insertion errors
        dataframe = dataframe.where(pd.notnull(dataframe), None)
        
        # Load DataFrame to SQL
        rows_affected = dataframe.to_sql(
            name=table_name, 
            con=engine, 
            if_exists=if_exists,
            index=False
        )
        
        logging.info(f"Loaded {rows_affected} rows into {table_name}")
        return rows_affected
    except SQLAlchemyError as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise

def load_college_data(raw_data, conn=DATABASE_URI):
    """
    Comprehensive function to transform and load college data
    
    :param raw_data: Input DataFrame with college data
    :param DATABASE_URI: Database connection configuration
    :return: Dictionary of rows loaded per table
    """
    # Setup logging
    logger = setup_logging()
    
    try:
         # Create engine with improved error handling
        engine = create_db_engine(conn)
        # Import transformation function (assuming it's in the same project)
        from transform import transform_schools_data
        
        # Transform data
        transformed_data = transform_schools_data(raw_data)
        
        # Create database engine
        engine = create_db_engine(conn)
        
        # Load each transformed DataFrame
        load_results = {}
        table_mappings = {
            'dim_school': 'Dim_School',
            'dim_demographics': 'Dim_Demographics',
            'dim_admission': 'Dim_Admission',
            'dim_test_scores': 'Dim_TestScores',
            'dim_transfer_rate': 'Dim_TransferRate',
            'fact_college_metrics': 'Fact_CollegeMetrics'
        }
        
        for key, table_name in table_mappings.items():
            load_results[key] = load_dataframe(
                engine, 
                transformed_data[key], 
                table_name
            )
        
        logger.info("Data loading completed successfully")
        return load_results
    
    except ConnectionError as e:
        print(f"Connection setup failed: {e}")

    except Exception as e:
        logger.error(f"Error in data loading process: {e}")
        raise

# Example usage
def main():
    # Example database configuration
    conn = DATABASE_URI
    # Main execution (optional, can be imported as a module)
    # Example usage - replace with actual data loading mechanism
    example_data = extract.request_data(URL)  # Load your actual data here
    # print(example_data[0]['latest']['admissions'])
    processed_data = transform.process_data(example_data)
    # print(f"len({len(processed_data)}) data processed for page {pg}")
    if example_data:
        result = transform.process_and_rank_colleges(processed_data)
        # print(result)
        # Assuming raw_data is your input DataFrame
        cleaned_tables = transform.clean_college_data(result)
        # print(cleaned_tables) 
        load_college_data(cleaned_tables, conn)

# Example usage
if __name__ == "__main__":
    # Define URL
    # Call main function
    main()