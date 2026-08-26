import pyautogui as pg
import os
import time
from datetime import datetime, timedelta
from user_pass import *
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

user = os.getenv('pwb_user')
passwd = os.getenv('pwb_pass')

os.system('start "" "C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\Google Chrome.lnk"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(2)
pg.write(os.getenv('pwb_url'))
pg.press('enter')
pg.sleep(5)
pg.write(user)
pg.press('tab')
pg.write(passwd)
pg.press('enter')
pg.sleep(2)
pg.press('tab',presses=10)
pg.sleep(1)
pg.press('down')
pg.sleep(1)
pg.press('enter')
pg.sleep(2)