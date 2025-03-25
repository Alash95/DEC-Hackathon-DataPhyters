import logging
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def rank_and_sort_schools(data):
    logging.info("Processing data for schools")
    processed_data = []

    # Helper function to safely compute max values with default fallbacks
    def safe_max(key, default=0):
        return max(d.get(key, default) for d in data if key in d)

    # Precompute max values for normalization
    max_student_size = safe_max(('latest', 'student', 'size'))
    max_completion_rate = safe_max(('latest', 'completion', 'consumer_rate'))
    max_sat_score = safe_max(('latest', 'admissions', 'sat_scores', 'average', 'overall'))
    max_act_score = safe_max(('latest', 'admissions', 'act_scores', 'midpoint', 'cumulative'))

    for result in data:
        try:
            # Extract nested dictionaries with defaults
            latest = result.get('latest', {})
            school = latest.get('school', {})
            student = latest.get('student', {})
            admission = latest.get('admissions', {})
            completion = latest.get('completion', {})
            cost = latest.get('cost', {})
            aid = latest.get('aid', {})

            # Extract SAT and ACT scores with defaults
            sat_score = admission.get('sat_scores', {}).get('average', {}).get('overall', 0)
            act_score = admission.get('act_scores', {}).get('midpoint', {}).get('cumulative', 0)

            # Create a row dictionary with relevant fields and default values
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


def transform_schools_data(raw_data):
    logging.info("Starting transformation of raw data into separate tables")

    # Call individual transformation functions
    df_schools = transform_schools(raw_data)
    df_demographics = transform_demographics(raw_data)
    df_admission = transform_admission(raw_data)
    df_costs = transform_costs(raw_data)
    df_aid = transform_financial_aid(raw_data)
    df_completion = transform_completion(raw_data)

    logging.info(f"Completed transformation into {len(df_schools)} school records across 6 tables")
    return df_schools, df_demographics, df_admission, df_costs, df_aid, df_completion


def transform_schools(raw_data):
    logging.info("Transforming schools data")
    schools_data = []
    for row in raw_data:
        school_id = row.get('id')
        schools_data.append({
            'id': school_id,
            'name': row.get('School_Name', 'Unknown School'),
            'address': row.get('Address', 'Unknown Address'),
            'city': row.get('City', 'Unknown City'),
            'state': row.get('State', 'Unknown State'),
            'highest_degree': row.get('Highest_Degree', 'Unknown Degree'),
            'predominant_degree': row.get('Predominant_Degree', 'Unknown Degree'),
            'institution_level': row.get('Institution_Level', 'Unknown Level')
        })
    df_schools = pd.DataFrame(schools_data)
    logging.info(f"Transformed {len(df_schools)} school records")
    return df_schools


def transform_demographics(raw_data):
    logging.info("Transforming demographics data")
    demographics_data = []
    for row in raw_data:
        school_id = row.get('id')
        demographics_data.append({
            'school_id': school_id,
            'student_size': row.get('Student_Size', 0),
            'men_percentage': row.get('Demographics_men', 0),
            'women_percentage': row.get('Demographics_women', 0)
        })
    df_demographics = pd.DataFrame(demographics_data)
    logging.info(f"Transformed {len(df_demographics)} demographic records")
    return df_demographics


def transform_admission(raw_data):
    logging.info("Transforming admission data")
    admission_data = []
    for row in raw_data:
        school_id = row.get('id')
        admission_data.append({
            'school_id': school_id,
            'overall_rate': row.get('Admission_Rate_Overall', 1),
            'by_ope_id': row.get('Admission_Rate_by_OPE_ID', 1),
            'consumer_rate': row.get('Consumer_Admission_Rate', 1)
        })
    df_admission = pd.DataFrame(admission_data)
    logging.info(f"Transformed {len(df_admission)} admission records")
    return df_admission


def transform_costs(raw_data):
    logging.info("Transforming costs data")
    costs_data = []
    for row in raw_data:
        school_id = row.get('id')
        costs_data.append({
            'school_id': school_id,
            'in_state_tuition': row.get('In_State_Tuition', 0),
            'out_of_state_tuition': row.get('Out_of_State_Tuition', 0)
        })
    df_costs = pd.DataFrame(costs_data)
    logging.info(f"Transformed {len(df_costs)} cost records")
    return df_costs


def transform_financial_aid(raw_data):
    logging.info("Transforming financial aid data")
    aid_data = []
    for row in raw_data:
        school_id = row.get('id')
        aid_data.append({
            'school_id': school_id,
            'loan_principal': row.get('Loan_Principal', 0),
            'pell_grant_rate': row.get('Pell_Grant_Rate', 0),
            'federal_loan_rate': row.get('Federal_Loan_Rate', 0)
        })
    df_aid = pd.DataFrame(aid_data)
    logging.info(f"Transformed {len(df_aid)} financial aid records")
    return df_aid


def transform_completion(raw_data):
    logging.info("Transforming completion data")
    completion_data = []
    for row in raw_data:
        school_id = row.get('id')
        completion_data.append({
            'school_id': school_id,
            'consumer_rate': row.get('Completion_Rate', 0)
        })
    df_completion = pd.DataFrame(completion_data)
    logging.info(f"Transformed {len(df_completion)} completion records")
    return df_completion


def clean_college_data(df_schools, df_demographics, df_admission, df_costs, df_aid, df_completion):
    """
    Cleans and validates college data to ensure consistency and accuracy.
    Handles missing values, invalid ranges, duplicates, and mismatched references.
    """
    logging.info("Starting data cleaning process for College Scorecard data")
    
    # ===========================
    # Clean Schools Data
    # ===========================
    logging.info("Cleaning schools data...")
    df_schools['id'] = df_schools['id'].astype('str')  # Ensure IDs are strings
    df_schools['name'] = df_schools['name'].replace('', None).astype('str')  # Handle empty names
    df_schools['address'] = df_schools['address'].replace('', None).astype('str')
    df_schools['city'] = df_schools['city'].replace('', None).astype('str')
    df_schools['state'] = df_schools['state'].replace('', None).astype('str')
    df_schools['highest_degree'] = df_schools['highest_degree'].replace('', None).astype('str')
    df_schools['predominant_degree'] = df_schools['predominant_degree'].replace('', None).astype('str')
    df_schools['institution_level'] = df_schools['institution_level'].replace('', None).astype('str')

    # Drop rows with missing or invalid IDs
    df_schools = df_schools[df_schools['id'].notna()]
    df_schools = df_schools.drop_duplicates(subset=['id'])  # Remove duplicate IDs

    # ===========================
    # Clean Demographics Data
    # ===========================
    logging.info("Cleaning demographics data...")
    df_demographics['school_id'] = df_demographics['school_id'].astype('str')  # Ensure school IDs are strings
    df_demographics['student_size'] = pd.to_numeric(df_demographics['student_size'], errors='coerce')  # Convert to numeric
    df_demographics['student_size'] = df_demographics['student_size'].fillna(0)  # Replace NaN with 0

    # Normalize percentage fields (e.g., men_percentage, women_percentage)
    for col in ['men_percentage', 'women_percentage']:
        df_demographics[col] = pd.to_numeric(df_demographics[col], errors='coerce')  # Convert to numeric
        mask = df_demographics[col] > 1  # If percentage is greater than 1, assume it's in 0-100 range
        df_demographics.loc[mask, col] = df_demographics.loc[mask, col] / 100
        df_demographics[col] = df_demographics[col].clip(lower=0, upper=1)  # Ensure values are between 0 and 1

    # Validate that men_percentage + women_percentage sums to ~1
    total_pct = df_demographics['men_percentage'] + df_demographics['women_percentage']
    invalid_total = (total_pct < 0.99) | (total_pct > 1.01)
    df_demographics.loc[invalid_total, ['men_percentage', 'women_percentage']] = None

    # ===========================
    # Clean Admission Data
    # ===========================
    logging.info("Cleaning admission data...")
    df_admission['school_id'] = df_admission['school_id'].astype('str')  # Ensure school IDs are strings

    # Normalize admission rate fields
    for col in ['overall_rate', 'by_ope_id', 'consumer_rate']:
        df_admission[col] = pd.to_numeric(df_admission[col], errors='coerce')  # Convert to numeric
        mask = df_admission[col] > 1  # If rate is greater than 1, assume it's in 0-100 range
        df_admission.loc[mask, col] = df_admission.loc[mask, col] / 100
        df_admission[col] = df_admission[col].clip(lower=0, upper=1)  # Ensure values are between 0 and 1

    # Drop rows with invalid admission rates
    invalid_rates = (df_admission['overall_rate'] < 0) | (df_admission['overall_rate'] > 1)
    df_admission = df_admission[~invalid_rates]

    # ===========================
    # Clean Costs Data
    # ===========================
    logging.info("Cleaning costs data...")
    df_costs['school_id'] = df_costs['school_id'].astype('str')  # Ensure school IDs are strings

    # Convert tuition costs to numeric
    df_costs['in_state_tuition'] = pd.to_numeric(df_costs['in_state_tuition'], errors='coerce')
    df_costs['out_of_state_tuition'] = pd.to_numeric(df_costs['out_of_state_tuition'], errors='coerce')

    # Replace negative or invalid tuition values with NaN
    df_costs.loc[df_costs['in_state_tuition'] < 0, 'in_state_tuition'] = None
    df_costs.loc[df_costs['out_of_state_tuition'] < 0, 'out_of_state_tuition'] = None

    # ===========================
    # Clean Financial Aid Data
    # ===========================
    logging.info("Cleaning financial aid data...")
    df_aid['school_id'] = df_aid['school_id'].astype('str')  # Ensure school IDs are strings

    # Convert loan principal to numeric
    df_aid['loan_principal'] = pd.to_numeric(df_aid['loan_principal'], errors='coerce')

    # Normalize rate fields (e.g., pell_grant_rate, federal_loan_rate)
    for col in ['pell_grant_rate', 'federal_loan_rate']:
        df_aid[col] = pd.to_numeric(df_aid[col], errors='coerce')  # Convert to numeric
        mask = df_aid[col] > 1  # If rate is greater than 1, assume it's in 0-100 range
        df_aid.loc[mask, col] = df_aid.loc[mask, col] / 100
        df_aid[col] = df_aid[col].clip(lower=0, upper=1)  # Ensure values are between 0 and 1

    # ===========================
    # Clean Completion Data
    # ===========================
    logging.info("Cleaning completion data...")
    df_completion['school_id'] = df_completion['school_id'].astype('str')  # Ensure school IDs are strings

    # Normalize completion rate field
    df_completion['consumer_rate'] = pd.to_numeric(df_completion['consumer_rate'], errors='coerce')  # Convert to numeric
    mask = df_completion['consumer_rate'] > 1  # If rate is greater than 1, assume it's in 0-100 range
    df_completion.loc[mask, 'consumer_rate'] = df_completion.loc[mask, 'consumer_rate'] / 100
    df_completion['consumer_rate'] = df_completion['consumer_rate'].clip(lower=0, upper=1)  # Ensure values are between 0 and 1

    # ===========================
    # Cross-Validation and Final Cleaning
    # ===========================
    logging.info("Performing cross-validation and final cleaning...")

    # Get valid school IDs from the schools table
    valid_ids = set(df_schools['id'])

    # Filter related tables to only include valid school IDs
    df_demographics = df_demographics[df_demographics['school_id'].isin(valid_ids)]
    df_admission = df_admission[df_admission['school_id'].isin(valid_ids)]
    df_costs = df_costs[df_costs['school_id'].isin(valid_ids)]
    df_aid = df_aid[df_aid['school_id'].isin(valid_ids)]
    df_completion = df_completion[df_completion['school_id'].isin(valid_ids)]

    # Drop duplicates in related tables
    df_demographics = df_demographics.drop_duplicates(subset=['school_id'])
    df_admission = df_admission.drop_duplicates(subset=['school_id'])
    df_costs = df_costs.drop_duplicates(subset=['school_id'])
    df_aid = df_aid.drop_duplicates(subset=['school_id'])
    df_completion = df_completion.drop_duplicates(subset=['school_id'])

    # Log final counts
    logging.info(f"Data cleaning complete. Schools: {len(df_schools)}, Demographics: {len(df_demographics)}, "
                 f"Admission: {len(df_admission)}, Costs: {len(df_costs)}, "
                 f"Financial Aid: {len(df_aid)}, Completion: {len(df_completion)}")

    return df_schools, df_demographics, df_admission, df_costs, df_aid, df_completion