import pyautogui as py
import os
import time
from datetime import datetime, timedelta
import pathlib
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

user = os.getenv('b2s_user')
password = os.getenv('b2s_pass')

print(f"User: {user}")
print(f"Password: {password}")

userpath = user_path = pathlib.Path.home()
save_path = userpath / 'Downloads' / 'b2s_soh.csv'
path = r"C:\Program Files (x86)\IBM\Client Access\cwbtf.exe"

os.remove(save_path) if save_path.exists() else None

os.system(f'start "" "{path}"')
time.sleep(2)

windows = py.getWindowsWithTitle("Data Transfer from IBM i")  # Replace with the actual window title

if windows:
    windows[0].activate()
    py.write('ODTHAI')
    py.press('tab')
    py.write('MMB2SUSR/MGRSTK(BHOSTRTPP)')
    py.press('tab', presses=3)
    py.write('f')
    py.press('tab')
    py.press('space')
    time.sleep(2)
    py.hotkey('alt', 'c')
    py.press('tab')
    py.write('c')
    py.hotkey('alt', 's')
    py.press('enter')
    py.press('tab')
    py.write(str(save_path))
    py.press('enter')
    time.sleep(1)
    py.write(user)
    py.press('tab')
    time.sleep(0.5)
    py.write(password)
    py.press('enter')
    time.sleep(3)
else:
    print("Window not found!")


