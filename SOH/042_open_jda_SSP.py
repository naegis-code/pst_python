import pyautogui
import os
import time
from datetime import datetime, timedelta
import user_pass as up
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

program = "SSP"  # Options: "SSP", "B2S", "OFM"

# username and password selection based on program
if program == "SSP":
    user = os.getenv("ssp_user")
    password = os.getenv("ssp_pass")
elif program == "B2S":
    user = os.getenv("b2s_user")
    password = os.getenv("b2s_pass")
elif program == "OFM":
    user = os.getenv("ofm_user")
    password = os.getenv("ofm_pass")
else : None

if program in ["B2S", "OFM"]:
    program = "B2S&OFM"
else :
    program = program


os.system(f'start "" "D:\\Users\\prthanap\\Desktop\\{program}.WS"')

yesterday = (datetime.now() - timedelta(days=1)).strftime('%d%m%y')

time.sleep(5)

windows = pyautogui.getWindowsWithTitle("Session A - [24 x 80]")  # Replace with the actual window title

if windows:
    windows[0].activate()
    time.sleep(1)  
    pyautogui.write(user)
    pyautogui.press('tab')
    time.sleep(0.5)
    pyautogui.write(password)
    pyautogui.press('enter')
    time.sleep(3)  
    pyautogui.write(user)
    pyautogui.press('tab')
    time.sleep(0.5)
    pyautogui.write(password)
    pyautogui.press('enter')
    time.sleep(3)  


else:
    print("Window not found!")
