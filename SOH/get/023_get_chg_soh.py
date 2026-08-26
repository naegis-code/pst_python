import pyautogui as pg
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())
url = os.getenv('chg_url')
user = os.getenv('chg_user')
passwd = os.getenv('chg_pass')

os.system('start "" "C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\Google Chrome.lnk"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(2)
pg.write(url)
pg.press('enter')
pg.sleep(2)
pg.write(user)
pg.press('tab')
pg.write(passwd)
pg.press('enter')
pg.sleep(2)
pg.press('tab',presses=15)
pg.sleep(1)
pg.press('down')
pg.sleep(1)
pg.press('enter')
pg.sleep(2)