import pyautogui as pg
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

user = os.getenv('pwb_user')
password = os.getenv('pwb_pass')
url = os.getenv('pwb_url')

os.system('start "" "C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\Google Chrome.lnk"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(2)
pg.write(url)
time.sleep(1)
pg.press('enter')
pg.sleep(2)
pg.write(user)
pg.press('tab')
pg.write(password)
pg.press('enter')
pg.sleep(2)
pg.press('tab',presses=6)
pg.sleep(1)
pg.press('down',presses=4)
pg.sleep(1)
pg.press('enter')
pg.sleep(2)