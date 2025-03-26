# College Scorecard Data Pipeline

## Overview

This project is a robust ETL (Extract, Transform, Load) pipeline designed to retrieve, process, and analyze data from the U.S. Department of Education's College Scorecard API. The pipeline automates the collection of comprehensive college information, transforms the data into a structured format, and loads it into a PostgreSQL database for further analysis and uses power bi for visualization.

## Architecture

![ETL Architecture Diagram](architecture.png)

### Key Components
- **Data Source**: College Scorecard API
- **Extract**: Python-based data retrieval
- **Transform**: Data cleaning and preprocessing
- **Load**: PostgreSQL database integration
- **Orchestration**: Apache Airflow
- **Containerization**: Docker
- **CI/CD**: GitHub Actions

## Features

- 🔍 Comprehensive college data extraction
- 🧹 Advanced data cleaning and normalization
- 📊 Multi-dimensional data transformation
- 🏆 Advanced college ranking algorithm
- 📈 Flexible and scalable pipeline architecture

## Prerequisites

- Python 3.8+
- Docker
- PostgreSQL
- Apache Airflow
- Power BI (for visualization)

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/Alash95/DEC-Hackathon-Team-5.git
cd DEC-Hackathon-Team-5
```

### 2. Set Up Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file with the following:
```
API_KEY=your_college_scorecard_api_key
DATABASE_URI=postgresql://username:password@localhost/database_name
```

### 4. Docker Deployment
```bash
docker-compose up --build
```

## Project Structure

```
college-scorecard-pipeline/
│
├── dags/                   # Airflow DAG definitions
│   └── dag.py              # Main ETL pipeline DAG
│
├── src/                    # Source code
│   ├── extract.py          # Data extraction logic
│   ├── transform.py        # Data transformation functions
│   └── load.py             # Database loading methods
│
├── tests/                  # Unit and integration tests
├── docs/                   # Project documentation
├── config.py               # Configuration management
├── requirements.txt        # Python dependencies
└── docker-compose.yml      # Docker configuration
```

## Data Transformation

The pipeline transforms college data into six key dimensions:
1. Dim_School: Basic school information
2. Dim_Demographics: Student demographics
3. Dim_Admission: Admission rates and scores
4. Dim_TestScores: Standardized test performance
5. Dim_TransferRate: Student transfer statistics
6. Fact_CollegeMetrics: Comprehensive college metrics

## Ranking Methodology

The advanced ranking algorithm considers multiple factors:
- Admission rates
- Completion rates
- Test scores (SAT/ACT)
- Student body size
- Tuition costs
- Financial aid metrics

## Usage

### Running the Pipeline
```bash
# Start Airflow webserver
airflow webserver -D

# Trigger the DAG
airflow dags trigger college_scorecard_pipeline
```

## Monitoring & Logging

- Comprehensive logging implemented
- Airflow web UI for pipeline monitoring
- Detailed error handling and retry mechanisms

## Visualization

The pipeline supports integration with Power BI for:
- Interactive dashboards
- College performance comparisons
- Trend analysis

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License.

## Contact

- Your Name
- Email: your.email@example.com
- Project Link: https://github.com/yourusername/college-scorecard-pipeline

## Acknowledgments

- U.S. Department of Education
- College Scorecard API
- Apache Airflow Community
```

## Recommended Next Steps

1. Replace placeholder text (like username, email)
2. Add actual architecture diagram
3. Create detailed documentation for each component
4. Set up comprehensive test coverage
5. Configure CI/CD pipeline

Would you like me to elaborate on any section or provide additional details?