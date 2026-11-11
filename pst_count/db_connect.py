import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

db_url_pstdb = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@localhost:5432/pstdb'
db_url_pstdb2 = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@localhost:5432/pstdb2'
db_url_pstdb3 = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@localhost:5432/pstdb3'
db_url_pstdb4 = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@localhost:5432/pstdb4'
db_url_pstdb5 = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@localhost:5432/pstdb5'
db_url_pstdbtest = f'postgresql+asyncpg://{DB_USER}:{DB_PASS}@localhost:5432/pstdbtest'
db_url_pstdbtest_p =f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@localhost:5432/pstdbtest'