from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_PORT     = os.getenv('DB_PORT', 5432)
DB_NAME     = os.getenv('DB_NAME', 'ipl_analytics')
DB_USER     = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

RAW_DATA_PATH       = os.getenv('RAW_DATA_PATH', 'data/raw/matches')
PROCESSED_DATA_PATH = os.getenv('PROCESSED_DATA_PATH', 'data/processed')
