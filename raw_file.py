import requests
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def request_data(url, parameter):
    base_url = "https://api.data.gov/ed/collegescorecard/v1/schools"
    logging.info(f"Requesting data from {base_url}")
    response = requests.get(url, params=parameter)
    status = response.status_code
    logging.info(f"status code for the request {status}")
    if response.ok:
        data = response.json()
        return data
    logging.info("Request failed but requesting one more time")
    return request_data(url, parameter)

def process_data(data):
    logging.info(f"Processing data for school")
    processed_data = []
    
    for result in data['results']:
        
        try:
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
    return df
