import pyautogui as py
import os
import time
from datetime import datetime, timedelta
import user_pass as up
import pathlib

userpath = user_path = pathlib.Path.home()
save_path = userpath / 'Documents' / 'b2s_soh.csv'
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
    py.write(up.b2s_user)
    py.press('tab')
    time.sleep(0.5)
    py.write(up.b2s_pass)
    py.press('enter')
    time.sleep(3)
else:
    print("Window not found!")


