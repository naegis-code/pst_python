import pyautogui
import os
import time
from datetime import datetime, timedelta
from user_pass import ofm_user, ofm_pass, ssp_user, ssp_pass ,b2s_user, b2s_pass

os.system('start "" "D:\\Program Files\\B2S\\Apps\\export_master_csv.dtf"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(5)

pyautogui.press('enter')
time.sleep(1)
pyautogui.write(ofm_user)
pyautogui.press('tab')
time.sleep(0.5)
pyautogui.write(ofm_pass)
pyautogui.press('enter')
time.sleep(3)
