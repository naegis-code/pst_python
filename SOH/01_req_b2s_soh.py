import pyautogui
import os
import time
from datetime import datetime, timedelta
import pathlib
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

user_path = pathlib.Path.home()
desktop_path = user_path / 'Desktop'
program_path = desktop_path / 'B2S&OFM.WS'

user = os.getenv('b2s_user')
password = os.getenv('b2s_pass')

print(f"User: {user}")
print(f"Password: {password}")

os.system(f'start "" "{program_path}"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(10)

windows = pyautogui.getWindowsWithTitle("Session A - [24 x 80]")  # Replace with the actual window title
windows_pc5250 = pyautogui.getWindowsWithTitle("PC5250")  # Alternative title

if windows_pc5250:
    windows_pc5250[0].activate()
    pyautogui.press('enter')
elif windows:
    windows[0].activate()
    time.sleep(1)  
    pyautogui.write(user)
    pyautogui.press('tab')
    time.sleep(0.5)
    pyautogui.write(password)
    pyautogui.press('enter')
    time.sleep(10)
    pyautogui.write(user)
    pyautogui.press('tab')
    time.sleep(0.5)
    pyautogui.write(password)
    time.sleep(0.5)
    pyautogui.press('enter')
    time.sleep(0.5)
    pyautogui.press('enter',presses=3)
    time.sleep(0.5)
    pyautogui.write('07')
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
    '''pyautogui.press('enter')
    time.sleep(0.5)
    pyautogui.press('f1')
    time.sleep(0.5)
    pyautogui.press('f7')
    time.sleep(0.5)
    pyautogui.hotkey('alt', 'f4')'''
else:
    print("Window not found!")
