import pandas as pd
from sqlalchemy import create_engine,text
import db_connect
import datetime


print("Start : ",datetime.datetime.now())
start = '20250101'
End = '20251231'

query_missrate = text(f"""