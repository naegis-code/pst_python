import pyautogui
import os
import time
from datetime import datetime, timedelta
from user_pass import ofm_user, ofm_pass, ssp_user, ssp_pass ,b2s_user, b2s_pass

os.system('start "" "D:\\Program Files\\B2S\\Apps\\export_master_csv.dtf"')

print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

time.sleep(5)

pyautogui.press('enter')
time.sleep(1)
pyautogui.write(b2s_user)
pyautogui.press('tab')
time.sleep(0.5)
pyautogui.write(b2s_pass)
pyautogui.press('enter')
time.sleep(3)
