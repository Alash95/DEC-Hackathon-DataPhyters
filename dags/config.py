# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
BASE_URL = os.getenv('BASE_URL')
API_KEY = os.getenv('API_KEY')
URL = os.getenv('URL')

URL="https://api.data.gov/ed/collegescorecard/v1/schools?api_key=66qL0xk0DCVolcmEjUeiHhdcQ1WBE20PzabZ6KUg&page=1&per_page=100&page=64&sort=latest.student.size:desc"

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database connection URI (replace with your credentials)
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT=os.getenv('DB_PORT')
DB_NAME=os.getenv('DB_NAME')

DATABASE_URI = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@postgres:5432/{DB_NAME}"

