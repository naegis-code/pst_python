import pyautogui
import os
import time
from datetime import datetime, timedelta
from user_pass import ofm_user,ofm_pass

os.system('start "" "D:\\Users\\prthanap\\Desktop\\Program\\OFM.WS"')

print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

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
    pyautogui.write('12')
    pyautogui.press('tab',presses=11)
    pyautogui.write('010100')
    pyautogui.press('delete',presses=2)
    pyautogui.press('enter')
    pyautogui.press('f7')
    time.sleep(0.5)
    pyautogui.press('enter')
    time.sleep(0.5)
    pyautogui.press('f1')
    time.sleep(0.5)
    pyautogui.press('f7')
    time.sleep(0.5)
    pyautogui.hotkey('alt', 'f4')
else:
    print("Window not found!")