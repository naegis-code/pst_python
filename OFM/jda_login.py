import pyautogui
import os
import time
from datetime import datetime, timedelta
from user_pass import ofm_user,ofm_pass

os.system('start "" "D:\\Users\\prthanap\\Desktop\\Program\\OFM.WS"')

time.sleep(10)

windows = pyautogui.getWindowsWithTitle("Session A - [24 x 80]")  # Replace with the actual window title

if windows:
    windows[0].activate()
    time.sleep(1)  
    pyautogui.write(ofm_user)
    pyautogui.press('tab')
    time.sleep(0.5)
    pyautogui.write(ofm_pass)
    pyautogui.press('enter')
    time.sleep(10)
    pyautogui.write(ofm_user)
    pyautogui.press('tab')
    time.sleep(0.5)
    pyautogui.write(ofm_pass)
    time.sleep(0.5)
    pyautogui.press('enter')
    time.sleep(0.5)
    pyautogui.press('enter',presses=3)
    time.sleep(0.5)