-- Create Database (replace 'college_metrics' with your desired database name)
CREATE DATABASE college_scorecard;

-- Connect to the database
\c college_scorecard

-- Drop existing tables if they exist
DROP TABLE IF EXISTS Fact_CollegeMetrics;
DROP TABLE IF EXISTS Dim_School;
DROP TABLE IF EXISTS Dim_Demographics;
DROP TABLE IF EXISTS Dim_Admission;
DROP TABLE IF EXISTS Dim_TestScores;
DROP TABLE IF EXISTS Dim_TransferRate;

-- Dim_School Table
CREATE TABLE Dim_School (
    id VARCHAR(50) PRIMARY KEY,
    school_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    highest_degree VARCHAR(100),
    predominant_degree VARCHAR(100),
    predominant_recoded VARCHAR(100),
    accreditor_code VARCHAR(50),
    institution_level VARCHAR(100),
    religious_affiliation VARCHAR(100),
    type_of_school VARCHAR(100)
);

-- Dim_Demographics Table
CREATE TABLE Dim_Demographics (
    school_id VARCHAR(50) PRIMARY KEY,
    student_size INTEGER,
    demographics_men_pct DECIMAL(5,2),
    demographics_women_pct DECIMAL(5,2),
    FOREIGN KEY (school_id) REFERENCES Dim_School(id)
);

-- Dim_Admission Table
CREATE TABLE Dim_Admission (
    school_id VARCHAR(50) PRIMARY KEY,
    admission_rate_overall DECIMAL(5,2),
    admission_rate_by_ope_id DECIMAL(5,2),
    consumer_admission_rate DECIMAL(5,2),
    admission_score DECIMAL(5,2),
    FOREIGN KEY (school_id) REFERENCES Dim_School(id)
);

-- Dim_TestScores Table
CREATE TABLE Dim_TestScores (
    school_id VARCHAR(50) PRIMARY KEY,
    act_midpoint_math DECIMAL(5,2),
    act_midpoint_english DECIMAL(5,2),
    act_midpoint_writing DECIMAL(5,2),
    act_midpoint_cumulative DECIMAL(5,2),
    act_25th_percentile_math DECIMAL(5,2),
    act_25th_percentile_english DECIMAL(5,2),
    act_25th_percentile_writing DECIMAL(5,2),
    act_50th_percentile_math DECIMAL(5,2),
    act_50th_percentile_english DECIMAL(5,2),
    act_75th_percentile_math DECIMAL(5,2),
    act_75th_percentile_writing DECIMAL(5,2),
    sat_midpoint_math DECIMAL(5,2),
    sat_midpoint_writing DECIMAL(5,2),
    sat_midpoint_critical_reading DECIMAL(5,2),
    sat_25th_percentile_math DECIMAL(5,2),
    sat_25th_percentile_writing DECIMAL(5,2),
    sat_25th_percentile_critical_reading DECIMAL(5,2),
    sat_50th_percentile_math DECIMAL(5,2),
    sat_75th_percentile_math DECIMAL(5,2),
    sat_75th_percentile_writing DECIMAL(5,2),
    act_score DECIMAL(5,2),
    sat_score DECIMAL(5,2),
    FOREIGN KEY (school_id) REFERENCES Dim_School(id)
);

-- Dim_TransferRate Table
CREATE TABLE Dim_TransferRate (
    school_id VARCHAR(50) PRIMARY KEY,
    transfer_rate_4yr_full_time DECIMAL(5,2),
    transfer_rate_4yr_full_time_pooled DECIMAL(5,2),
    transfer_rate_cohort_4yr_full_time DECIMAL(5,2),
    transfer_rate_less_than_4yr_full_time DECIMAL(5,2),
    transfer_rate_less_than_4yr_full_time_pooled DECIMAL(5,2),
    FOREIGN KEY (school_id) REFERENCES Dim_School(id)
);

-- Fact_CollegeMetrics Table
CREATE TABLE Fact_CollegeMetrics (
    school_id VARCHAR(50) PRIMARY KEY,
    in_state_tuition DECIMAL(10,2),
    out_of_state_tuition DECIMAL(10,2),
    loan_principal DECIMAL(10,2),
    pell_grant_rate DECIMAL(5,2),
    federal_loan_rate DECIMAL(5,2),
    completion_rate DECIMAL(5,2),
    completion_score DECIMAL(5,2),
    size_score DECIMAL(5,2),
    in_state_tuition_score DECIMAL(5,2),
    financial_aid_score DECIMAL(5,2),
    ranking_score DECIMAL(5,2),
    rank INTEGER,
    FOREIGN KEY (school_id) REFERENCES Dim_School(id)
);



-- Create a database user (replace 'college_user' and 'your_password' with appropriate credentials)
CREATE USER college_user WITH PASSWORD 'airflow';

-- Grant necessary privileges
-- Grants all privileges on the database
GRANT ALL PRIVILEGES ON DATABASE college_metrics TO college_user;

-- Grant privileges on all tables (current and future)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO college_user;

-- Grant usage on the public schema
GRANT USAGE ON SCHEMA public TO college_user;

-- Allow the user to create tables (if needed)
ALTER USER college_user WITH CREATEDB;

-- Optional: Grant SELECT, INSERT, UPDATE, DELETE privileges specifically
GRANT SELECT, INSERT, UPDATE, DELETE ON 
    Dim_School, 
    Dim_Demographics, 
    Dim_Admission, 
    Dim_TestScores, 
    Dim_TransferRate, 
    Fact_CollegeMetrics 
TO college_user;

-- Ensure future tables get the same privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO college_user;