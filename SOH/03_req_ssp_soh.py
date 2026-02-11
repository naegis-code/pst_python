import pyautogui
import os
import time
from datetime import datetime, timedelta
import user_pass as up
import pathlib

user_path = pathlib.Path.home()
desktop_path = user_path / 'Desktop'
program_path = desktop_path / 'SSP.WS'

os.system(f'start "" "{program_path}"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(5)

ssp_user = up.ssp_user
ssp_pass = up.ssp_pass

windows = pyautogui.getWindowsWithTitle("Session A - [24 x 80]")  # Replace with the actual window title

if windows:
    windows[0].activate()
    time.sleep(1)  
    pyautogui.write(ssp_user)
    pyautogui.press('tab')
    time.sleep(0.5)
    pyautogui.write(ssp_pass)
    pyautogui.press('enter')
    #time.sleep(10)
    #pyautogui.write(ssp_user)
    #pyautogui.press('tab')
    #time.sleep(0.5)
    #pyautogui.write(ssp_pass)
    #pyautogui.press('enter')
    time.sleep(0.5)
    pyautogui.press('enter',presses=3)
    time.sleep(0.5)
    pyautogui.write('04')
    time.sleep(0.5)
    pyautogui.write('01')
    time.sleep(0.5)
    pyautogui.press('tab')
    # date -1 days
    pyautogui.write(yesterday)
    pyautogui.press('del', presses=2)
    time.sleep(0.5)
    pyautogui.press('enter')
    time.sleep(0.5)
    pyautogui.press('f7')
    time.sleep(2)
    pyautogui.press('enter')
    time.sleep(0.5)
    #pyautogui.press('f1')
    time.sleep(0.5)
    #pyautogui.press('f7')
    time.sleep(0.5)
    #pyautogui.hotkey('alt', 'f4')
else:
    print("Window not found!")
