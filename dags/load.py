import logging
import psycopg2
from psycopg2 import sql

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(df_schools, df_demographics, df_admission, df_costs, df_aid, df_completion, connection_string):
    """
    Loads transformed data into PostgreSQL tables.
    """
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()
        logging.info("Connected to PostgreSQL database")

        # Load schools table
        load_table(cursor, "schools", df_schools)
        # Load demographics table
        load_table(cursor, "demographics", df_demographics)
        # Load admission table
        load_table(cursor, "admission", df_admission)
        # Load costs table
        load_table(cursor, "costs", df_costs)
        # Load financial aid table
        load_table(cursor, "financial_aid", df_aid)
        # Load completion table
        load_table(cursor, "completion", df_completion)

        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data loading complete.")
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def load_table(cursor, table_name, df):
    """
    Helper function to load a DataFrame into a PostgreSQL table.
    """
    logging.info(f"Loading data into {table_name} table")
    columns = ', '.join(df.columns)
    values_placeholder = ', '.join(['%s'] * len(df.columns))
    insert_query = sql.SQL(
        f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"
    )
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    logging.info(f"Loaded {len(df)} records into {table_name}")