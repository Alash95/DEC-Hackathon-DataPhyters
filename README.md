# DEC-Hackathon-Team-5
This is the DEC Hackathon Repository For Team 5 - USA College ScoreCard ETL Pipeline


college-scorecard-etl/
├── .github/
│   └── workflows/
│       ├── ci.yml                    # GitHub Actions CI workflow
│       └── deploy.yml                # Deployment workflow
├── airflow/
│   ├── dags/
│   │   └── college_scorecard_etl.py  # Main ETL workflow definition
│   └── plugins/
│       └── operators/                # Custom Airflow operators if needed
├── src/
│   ├── extract/
│   │   └── college_api.py            # Code to retrieve data from College ScoreCard API
│   ├── transform/
│   │   └── data_processing.py        # Python transformations
│   └── load/
│       └── db_loader.py              # PostgreSQL loading logic
├── db/
│   ├── migrations/                   # Database schema migrations
│   └── init.sql                      # Initial database setup
├── analytics/
│   └── power_bi/
│       └── college_scorecard_dashboard.pbix  # Power BI report template
├── docker/
│   ├── airflow/
│   │   └── Dockerfile                # Airflow container setup
│   ├── postgres/
│   │   └── Dockerfile                # PostgreSQL container setup
│   └── python/
│       └── Dockerfile                # Python processing container
├── tests/
│   ├── extract/
│   ├── transform/
│   └── load/
├── docker-compose.yml                # Docker environment configuration
├── requirements.txt                  # Python dependencies
├── README.md                         # Project documentation
└── .env.example                      # Example environment variables