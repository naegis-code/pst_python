import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

db_url_pstdb = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@103.22.182.82:5432/pstdb'
db_url_pstdb2 = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@103.22.182.82:5432/pstdb2'
db_url_pstdb3 = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@103.22.182.82:5432/pstdb3'