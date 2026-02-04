import pyautogui as pg
import os
import time
from datetime import datetime, timedelta
from user_pass import *

os.system('start "" "C:\\ProgramData\\Microsoft\\Windows\\Start Menu\\Programs\\Google Chrome.lnk"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(2)
pg.write(pwb_url)
pg.press('enter')
pg.sleep(2)
pg.write(pwb_user)
pg.press('tab')
pg.write(pwb_pass)
pg.press('enter')
pg.sleep(2)
pg.press('tab',presses=6)
pg.sleep(1)
pg.press('down',presses=4)
pg.sleep(1)
pg.press('enter')
pg.sleep(2)