import logging
import pandas as pd
import extract 

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 
def fetch_all_data(URL):
    college_data = []
    for pg in range(2):
        logging.info(f"Requesting for page {pg+1}")
        res = extract.request_data(URL, {"page": pg, "per_page": 100})
        college_data.extend(res)
        return college_data


def process_data(data):
    logging.info(f"Processing data for school")
    processed_data = []
    # print("data")
    # print(data[0:5])
    for result in data:
        
        try:
            # print("result[0:5]")
            # print(result['latest']['school'])
            # print(result.dtypes)
            school = result['latest']['school']
            student = result['latest']['student']
            admission = result['latest']['admissions']
            cost = result['latest']['cost']
            aid = result['latest']['aid']
            completion = result['latest']['completion']
            
            
            row = {
               'Id':result.get('id'),
                'School_Name': school['name'],
                'Address': school['address'],
                'State': school['state'],
                'City': school['city'],
                'Highest_Degree': school['degrees_awarded']['highest'],
                'Predominant_Degree': school['degrees_awarded']['predominant'],
                'Predominant_Recoded': school['degrees_awarded']['predominant_recoded'],
                'Accreditor_Code':school['accreditor_code'],
                'Institution_Level': school['institutional_characteristics']['level'],
                'Religious_affiliation':school['religious_affiliation'],
                'Student_Size': student['size'],
                'Demographics_men':student['demographics']['men'],
                'Demographics_women':student['demographics']['women'],
                'Admission_Rate_Overall': admission['admission_rate'].get('overall'),
                'Admission_Rate_by_OPE_ID': admission['admission_rate'].get('by_ope_id'),
                'Consumer_Admission_Rate': admission['admission_rate'].get('consumer_rate'),
                'In_State_Tuition': cost['tuition'].get('in_state'),
                'Out_of_State_Tuition': cost['tuition'].get('out_of_state'),
                'Loan_Principal': aid.get('loan_principal'),
                'Pell_Grant_Rate': aid.get('pell_grant_rate'),
                'Federal_Loan_Rate': aid.get('federal_loan_rate'),
                'Completion_Rate': completion.get('consumer_rate')
            }
            
            # Extract ACT scores
            for percentile, subjects in admission.get('act_scores', {}).items():
                for subject, score in subjects.items():
                    row[f'ACT_{percentile}_{subject}'] = score
            
            # Extract SAT scores
            for score_type, scores in admission.get('sat_scores', {}).items():
                if score_type == 'average':
                    for category, score in scores.items():
                        row[f'SAT_{score_type}_{category}'] = score
                else:
                    for subject, score in scores.items():
                        row[f'SAT_{score_type}_{subject}'] = score
            
            # Extract transfer rates -- retention rate
            for category, rates in completion.get('transfer_rate', {}).items():
                for rate_type, rate in rates.items():
                    row[f'Transfer_Rate_{category}_{rate_type}'] = rate

            #Extract type of school
            if 'latest' in result and 'programs' in result['latest']:
                cip_programs = result['latest']['programs'].get('cip_4_digit')
                for item in cip_programs:
                    program_type = item.get('school')
                    row['type_of_school'] = program_type.get('type')
            
            processed_data.append(row)
        
        except KeyError as e:
            logging.error(f"Missing key in data: {e}")
    
    
    df = pd.DataFrame(processed_data)
    logging.info("Data processing complete.")
    # print(df.head())
    return df


def rank_colleges_advanced(processed_data):
    """
    Ranks colleges based on a weighted score derived from various metrics.
    """
    import pandas as pd
    
    # Convert processed_data to a DataFrame if it's not already
    ranking_df = pd.DataFrame(processed_data)
    
    # Define weights for different ranking factors
    weights = {
        'admission_weight': 0.25,
        'completion_weight': 0.20,
        'sat_weight': 0.15,
        'act_weight': 0.10,
        'size_weight': 0.10,
        'tuition_weight': 0.10,
        'financial_aid_weight': 0.10
    }
    
    # Normalize a column, optionally reversing the scale
    def normalize_column(series, reverse=False):
        series = pd.to_numeric(series, errors='coerce').fillna(0)
        if reverse:
            return 1 - (series - series.min()) / (series.max() - series.min() + 1e-10)
        return (series - series.min()) / (series.max() - series.min() + 1e-10)
    
    # Ensure required columns exist
    required_columns = [
        'Admission_Rate_Overall', 'Completion_Rate', 'SAT_Score', 'ACT_Score',
        'Student_Size', 'In_State_Tuition', 'Pell_Grant_Rate'
    ]
    
    for col in required_columns:
        if col not in ranking_df.columns:
            ranking_df[col] = 0  # Default missing columns to zero
    
    # Calculate normalized scores for each metric
    ranking_df['admission_score'] = normalize_column(ranking_df['Admission_Rate_Overall'], reverse=True)
    ranking_df['completion_score'] = normalize_column(ranking_df['Completion_Rate'])
    ranking_df['sat_score'] = normalize_column(ranking_df['SAT_Score'])
    ranking_df['act_score'] = normalize_column(ranking_df['ACT_Score'])
    ranking_df['size_score'] = normalize_column(ranking_df['Student_Size'])
    ranking_df['in_state_tuition_score'] = normalize_column(ranking_df['In_State_Tuition'], reverse=True)
    ranking_df['financial_aid_score'] = normalize_column(ranking_df['Pell_Grant_Rate'])
    
    # Compute the comprehensive ranking score
    ranking_df['ranking_score'] = (
        weights['admission_weight'] * ranking_df['admission_score'] +
        weights['completion_weight'] * ranking_df['completion_score'] +
        weights['sat_weight'] * ranking_df['sat_score'] +
        weights['act_weight'] * ranking_df['act_score'] +
        weights['size_weight'] * ranking_df['size_score'] +
        weights['tuition_weight'] * ranking_df['in_state_tuition_score'] +
        weights['financial_aid_weight'] * ranking_df['financial_aid_score']
    )
    
    # Sort by ranking score and assign ranks
    ranked_colleges = ranking_df.sort_values('ranking_score', ascending=False)
    ranked_colleges['Rank'] = range(1, len(ranked_colleges) + 1)
    
    # Select relevant columns for the final output
    # output_columns = [
    #     'Rank', 'School_Name', 'State', 'City', 
    #     'ranking_score', 'Admission_Rate_Overall', 
    #     'Completion_Rate', 'Student_Size', 
    #     'In_State_Tuition', 'Pell_Grant_Rate'
    # ]
    
    return ranked_colleges #[output_columns]


def process_and_rank_colleges(processed_data):
    """
    Processes raw data and ranks colleges based on advanced metrics.
    """
    # Process raw data
    # processed_data = process_data(data)
    # print(processed_data.head())
    
    # Rank colleges
    ranked_colleges = rank_colleges_advanced(processed_data)
    # print(ranked_colleges.head())
    # Save to CSV
    # ranked_colleges.to_csv('ranked_colleges.csv', index=False)
    return ranked_colleges


def clean_college_data(processed_df):
    # Input validation 
    if not isinstance(processed_df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    
    if processed_df.empty:
        logging.warning("Input DataFrame is empty. Returning empty DataFrame.")
        return pd.DataFrame()
    
    logging.info("Starting data cleaning process for College Scorecard data")
    
    # Copy the input DataFrame to avoid modifying the original
    df = processed_df.copy()
    
    # Demographic percentages
    demo_cols = ['Demographics_men', 'Demographics_women']
    for col in demo_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        mask = df[col] > 1
        df.loc[mask, col] = df.loc[mask, col] / 100
        df[col] = df[col].clip(lower=0, upper=1)
    
    # Admission rates
    admission_cols = ['Admission_Rate_Overall', 'Admission_Rate_by_OPE_ID', 'Consumer_Admission_Rate']
    for col in admission_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        mask = df[col] > 1
        df.loc[mask, col] = df.loc[mask, col] / 100
        df[col] = df[col].clip(lower=0, upper=1)
    
    # Rate columns in college metrics
    rate_cols = ['Pell_Grant_Rate', 'Federal_Loan_Rate', 'Completion_Rate']
    for col in rate_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        mask = df[col] > 1
        df.loc[mask, col] = df.loc[mask, col] / 100
        df[col] = df[col].clip(lower=0, upper=1)
    
    # Clean School Name and ID
    df['School_Name'] = df['School_Name'].replace('', None)
    df = df[df['Id'].notna()].drop_duplicates(subset=['Id'])
    
    # Rename 'Id' column to be more explicit
    df.rename(columns={'Id': 'school_id'}, inplace=True)
    
    return df

# clean_college_data({})

def transform_schools_data(raw_data):
    """
    Transforms raw data into separate tables for different aspects of college data.
    Matches the schema defined in the PostgreSQL init.sql file.
    """
    logging.info("Starting comprehensive data transformation")
    
    # Prepare dataframes for each dimension and fact table
    df_school = transform_dim_school(raw_data)
    df_demographics = transform_dim_demographics(raw_data)
    df_admission = transform_dim_admission(raw_data)
    df_test_scores = transform_dim_test_scores(raw_data)
    df_transfer_rate = transform_dim_transfer_rate(raw_data)
    df_college_metrics = transform_fact_college_metrics(raw_data)
    
    # Optional: Save each DataFrame to CSV
    save_to_csv(df_school, 'dim_school.csv')
    save_to_csv(df_demographics, 'dim_demographics.csv')
    save_to_csv(df_admission, 'dim_admission.csv')
    save_to_csv(df_test_scores, 'dim_test_scores.csv')
    save_to_csv(df_transfer_rate, 'dim_transfer_rate.csv')
    save_to_csv(df_college_metrics, 'fact_college_metrics.csv')
    
    return {
        'dim_school': df_school,
        'dim_demographics': df_demographics,
        'dim_admission': df_admission,
        'dim_test_scores': df_test_scores,
        'dim_transfer_rate': df_transfer_rate,
        'fact_college_metrics': df_college_metrics
    }

def safe_extract_columns(df, column_mappings):
    """
    Safely extract columns from DataFrame using multiple possible column names
    
    Args:
        df (pd.DataFrame): Input DataFrame
        column_mappings (dict): Mapping of desired column names to possible source column names
    
    Returns:
        pd.DataFrame: DataFrame with extracted columns
    """
    extracted_data = {}
    
    for desired_col, possible_cols in column_mappings.items():
        # Try each possible column name
        for col in possible_cols:
            if col in df.columns:
                extracted_data[desired_col] = df[col]
                break
        else:
            # If no matching column is found, set to None or a default value
            extracted_data[desired_col] = None
    
    return pd.DataFrame(extracted_data)

def transform_dim_school(raw_data):
    """Transform raw data into Dim_School table"""
    logging.info("Transforming Dim_School data")
    
    # Possible column names for each desired column
    school_column_mappings = {
        'id': ['id', 'school_id', 'School_Id', 'School_ID'],
        'school_name': ['School_Name', 'school_name', 'name'],
        'address': ['Address', 'address', 'school_address'],
        'city': ['City', 'city', 'school_city'],
        'state': ['State', 'state', 'school_state'],
        'highest_degree': ['Highest_Degree', 'highest_degree', 'school_highest_degree'],
        'predominant_degree': ['Predominant_Degree', 'predominant_degree', 'school_predominant_degree'],
        'predominant_recoded': ['Predominant_Recoded', 'predominant_recoded', 'school_predominant_recoded'],
        'accreditor_code': ['Accreditor_Code', 'accreditor_code', 'school_accreditor_code'],
        'institution_level': ['Institution_Level', 'institution_level', 'school_institution_level'],
        'religious_affiliation': ['Religious_affiliation', 'religious_affiliation', 'school_religious_affiliation'],
        'type_of_school': ['type_of_school', 'Type_of_School', 'school_type']
    }
    
    # Use safe extraction method
    df_school = safe_extract_columns(raw_data, school_column_mappings)
    
    return df_school

def transform_dim_demographics(raw_data):
    """Transform raw data into Dim_Demographics table"""
    logging.info("Transforming Dim_Demographics data")
    
    demographics_column_mappings = {
        'school_id': ['id', 'school_id', 'School_Id', 'School_ID'],
        'student_size': ['Student_Size', 'student_size', 'size'],
        'demographics_men_pct': ['Demographics_men', 'demographics_men', 'men_percentage'],
        'demographics_women_pct': ['Demographics_women', 'demographics_women', 'women_percentage']
    }
    
    df_demographics = safe_extract_columns(raw_data, demographics_column_mappings)
    
    return df_demographics

def transform_dim_admission(raw_data):
    """Transform raw data into Dim_Admission table"""
    logging.info("Transforming Dim_Admission data")
    
    admission_column_mappings = {
        'school_id': ['id', 'school_id', 'School_Id', 'School_ID'],
        'admission_rate_overall': ['Admission_Rate_Overall', 'admission_rate_overall', 'overall_admission_rate'],
        'admission_rate_by_ope_id': ['Admission_Rate_by_OPE_ID', 'admission_rate_by_ope_id'],
        'consumer_admission_rate': ['Consumer_Admission_Rate', 'consumer_admission_rate'],
        'admission_score': ['admission_score', 'Admission_Score']
    }
    
    df_admission = safe_extract_columns(raw_data, admission_column_mappings)
    
    return df_admission

def transform_dim_test_scores(raw_data):
    """Transform raw data into Dim_TestScores table"""
    logging.info("Transforming Dim_TestScores data")
    
    test_score_column_mappings = {
        'school_id': ['id', 'school_id', 'School_Id', 'School_ID'],
        # ACT midpoint scores
        'act_midpoint_math': ['ACT_midpoint_math', 'act_midpoint_math'],
        'act_midpoint_english': ['ACT_midpoint_english', 'act_midpoint_english'],
        'act_midpoint_writing': ['ACT_midpoint_writing', 'act_midpoint_writing'],
        'act_midpoint_cumulative': ['ACT_midpoint_cumulative', 'act_midpoint_cumulative'],
        
        # ACT percentile scores
        'act_25th_percentile_math': ['ACT_25th_percentile_math', 'act_25th_percentile_math'],
        'act_25th_percentile_english': ['ACT_25th_percentile_english', 'act_25th_percentile_english'],
        'act_25th_percentile_writing': ['ACT_25th_percentile_writing', 'act_25th_percentile_writing'],
        
        'act_50th_percentile_math': ['ACT_50th_percentile_math', 'act_50th_percentile_math'],
        'act_50th_percentile_english': ['ACT_50th_percentile_english', 'act_50th_percentile_english'],
        
        'act_75th_percentile_math': ['ACT_75th_percentile_math', 'act_75th_percentile_math'],
        'act_75th_percentile_writing': ['ACT_75th_percentile_writing', 'act_75th_percentile_writing'],
        
        # SAT midpoint scores
        'sat_midpoint_math': ['SAT_midpoint_math', 'sat_midpoint_math'],
        'sat_midpoint_writing': ['SAT_midpoint_writing', 'sat_midpoint_writing'],
        'sat_midpoint_critical_reading': ['SAT_midpoint_critical_reading', 'sat_midpoint_critical_reading'],
        
        # SAT percentile scores
        'sat_25th_percentile_math': ['SAT_25th_percentile_math', 'sat_25th_percentile_math'],
        'sat_25th_percentile_writing': ['SAT_25th_percentile_writing', 'sat_25th_percentile_writing'],
        'sat_25th_percentile_critical_reading': ['SAT_25th_percentile_critical_reading', 'sat_25th_percentile_critical_reading'],
        
        'sat_50th_percentile_math': ['SAT_50th_percentile_math', 'sat_50th_percentile_math'],
        'sat_75th_percentile_math': ['SAT_75th_percentile_math', 'sat_75th_percentile_math'],
        'sat_75th_percentile_writing': ['SAT_75th_percentile_writing', 'sat_75th_percentile_writing'],
        
        # Overall scores
        'act_score': ['ACT_Score', 'act_score'],
        'sat_score': ['SAT_Score', 'sat_score']
    }
    
    df_test_scores = safe_extract_columns(raw_data, test_score_column_mappings)
    
    return df_test_scores

def transform_dim_transfer_rate(raw_data):
    """Transform raw data into Dim_TransferRate table"""
    logging.info("Transforming Dim_TransferRate data")
    
    transfer_rate_column_mappings = {
        'school_id': ['id', 'school_id', 'School_Id', 'School_ID'],
        'transfer_rate_4yr_full_time': ['Transfer_Rate_4yr_full_time', 'transfer_rate_4yr_full_time'],
        'transfer_rate_4yr_full_time_pooled': ['Transfer_Rate_4yr_full_time_pooled', 'transfer_rate_4yr_full_time_pooled'],
        'transfer_rate_cohort_4yr_full_time': ['Transfer_Rate_cohort_4yr_full_time', 'transfer_rate_cohort_4yr_full_time'],
        'transfer_rate_less_than_4yr_full_time': ['Transfer_Rate_less_than_4yr_full_time', 'transfer_rate_less_than_4yr_full_time'],
        'transfer_rate_less_than_4yr_full_time_pooled': ['Transfer_Rate_less_than_4yr_full_time_pooled', 'transfer_rate_less_than_4yr_full_time_pooled']
    }
    
    df_transfer_rate = safe_extract_columns(raw_data, transfer_rate_column_mappings)
    
    return df_transfer_rate

def transform_fact_college_metrics(raw_data):
    """Transform raw data into Fact_CollegeMetrics table"""
    logging.info("Transforming Fact_CollegeMetrics data")
    
    metrics_column_mappings = {
        'school_id': ['id', 'school_id', 'School_Id', 'School_ID'],
        'in_state_tuition': ['In_State_Tuition', 'in_state_tuition'],
        'out_of_state_tuition': ['Out_of_State_Tuition', 'out_of_state_tuition'],
        'loan_principal': ['Loan_Principal', 'loan_principal'],
        'pell_grant_rate': ['Pell_Grant_Rate', 'pell_grant_rate'],
        'federal_loan_rate': ['Federal_Loan_Rate', 'federal_loan_rate'],
        'completion_rate': ['Completion_Rate', 'completion_rate'],
        'completion_score': ['Completion_Score', 'completion_score'],
        'size_score': ['Size_Score', 'size_score'],
        'in_state_tuition_score': ['In_State_Tuition_Score', 'in_state_tuition_score'],
        'financial_aid_score': ['Financial_Aid_Score', 'financial_aid_score'],
        'ranking_score': ['Ranking_Score', 'ranking_score'],
        'rank': ['Rank', 'rank']
    }
    
    df_college_metrics = safe_extract_columns(raw_data, metrics_column_mappings)
    
    return df_college_metrics

def save_to_csv(dataframe, filename, directory='output'):
    import os
    import pandas as pd
    
    # Create output directory if it doesn't exist
    os.makedirs(directory, exist_ok=True)
    
    # Full path for the CSV file
    filepath = os.path.join(directory, filename)
    
    try:
        # Save DataFrame to CSV
        dataframe.to_csv(filepath, index=False)
        print(f"Successfully saved {filename} to {filepath}")
    except Exception as e:
        print(f"Error saving {filename}: {e}")



print("All DataFrames have been saved to CSV files in the 'output' directory.")

# Example usage
# if __name__ == "__main__":
#     # Assuming raw_data is your input DataFrame
#     # raw_data = pd.read_csv('your_input_file.csv')
#     logging.basicConfig(level=logging.INFO)
#     transformed_data = transform_schools_data(raw_data)
