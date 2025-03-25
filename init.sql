-- Ensure the database exists, but do not recreate it if it already exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'college_scorecard') THEN
        CREATE DATABASE college_scorecard;
        RAISE NOTICE 'Database "college_scorecard" created.';
    ELSE
        RAISE NOTICE 'Database "college_scorecard" already exists.';
    END IF;
END $$;

-- Connect to the database
\c college_scorecard;

-- Create a role for read-only access (if it doesn't exist)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly_user') THEN
        CREATE ROLE readonly_user WITH LOGIN PASSWORD 'readonly_password';
        RAISE NOTICE 'Role "readonly_user" created.';
    ELSE
        RAISE NOTICE 'Role "readonly_user" already exists.';
    END IF;
END $$;

-- Create a role for read-write access (if it doesn't exist)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readwrite_user') THEN
        CREATE ROLE readwrite_user WITH LOGIN PASSWORD 'readwrite_password';
        RAISE NOTICE 'Role "readwrite_user" created.';
    ELSE
        RAISE NOTICE 'Role "readwrite_user" already exists.';
    END IF;
END $$;

-- Grant necessary privileges to roles
GRANT CONNECT ON DATABASE college_scorecard TO readonly_user;
GRANT CONNECT ON DATABASE college_scorecard TO readwrite_user;

-- Create schema for the tables (if it doesn't exist)
CREATE SCHEMA IF NOT EXISTS scorecard;

-- Set the search path to the schema
SET search_path TO scorecard;

-- Create schools dimension table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS schools (
    id VARCHAR(50) PRIMARY KEY,
    name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    highest_degree TEXT,
    predominant_degree TEXT,
    predominant_recoded TEXT,
    accreditor_code TEXT,
    institution_level TEXT
);

-- Create demographics dimension table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS demographics (
    school_id VARCHAR(50) REFERENCES schools(id),
    student_size INTEGER,
    men_percentage DECIMAL(5, 4),
    women_percentage DECIMAL(5, 4),
    PRIMARY KEY (school_id)
);

-- Create admission dimension table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS admission (
    school_id VARCHAR(50) REFERENCES schools(id),
    overall_rate DECIMAL(5, 4),
    by_ope_id DECIMAL(5, 4),
    consumer_rate DECIMAL(5, 4),
    PRIMARY KEY (school_id)
);

-- Create costs dimension table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS costs (
    school_id VARCHAR(50) REFERENCES schools(id),
    in_state_tuition DECIMAL(10, 2),
    out_of_state_tuition DECIMAL(10, 2),
    PRIMARY KEY (school_id)
);

-- Create financial aid dimension table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS financial_aid (
    school_id VARCHAR(50) REFERENCES schools(id),
    loan_principal DECIMAL(10, 2),
    pell_grant_rate DECIMAL(5, 4),
    federal_loan_rate DECIMAL(5, 4),
    PRIMARY KEY (school_id)
);

-- Create completion dimension table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS completion (
    school_id VARCHAR(50) REFERENCES schools(id),
    consumer_rate DECIMAL(5, 4),
    PRIMARY KEY (school_id)
);

-- Create school_facts fact table (if it doesn't exist)
CREATE TABLE IF NOT EXISTS school_facts (
    school_id VARCHAR(50) REFERENCES schools(id),
    student_size INTEGER,
    admission_rate DECIMAL(5, 4),
    completion_rate DECIMAL(5, 4),
    tuition_in_state DECIMAL(10, 2),
    tuition_out_of_state DECIMAL(10, 2),
    loan_principal DECIMAL(10, 2),
    pell_grant_rate DECIMAL(5, 4),
    federal_loan_rate DECIMAL(5, 4),
    PRIMARY KEY (school_id)
);

-- Grant privileges to roles
GRANT USAGE ON SCHEMA scorecard TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA scorecard TO readonly_user;

GRANT USAGE ON SCHEMA scorecard TO readwrite_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA scorecard TO readwrite_user;

RAISE NOTICE 'Schema "scorecard" and tables created successfully.';