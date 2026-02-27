import pyautogui as pg
import time

wholenumber = '#,##0;[Red]-#,##0'
decimalnumber = '#,##0.00;[Red]-#,##0.00'

time.sleep(1)  # Wait for 5 seconds to allow user to switch to the target application
pg.hotkey('alt','tab')

def format(custom_type):
    
    time.sleep(0.5)
    pg.click(x=200, y=150)  # Adjust the coordinates to click on the desired cell
    time.sleep(0.5)
    pg.hotkey('alt','n')
    time.sleep(0.5)
    pg.press('tab')
    time.sleep(0.5)
    pg.press('down', presses=11)
    time.sleep(0.5)
    pg.press('tab')
    time.sleep(0.5)
    pg.write(custom_type)
    pg.press('enter')
    time.sleep(0.5)
    pg.press('tab')
    time.sleep(0.5)
    pg.press('enter')
    time.sleep(0.5)
    pg.press('tab')
    time.sleep(0.5)

for i in range(11):
    format(wholenumber)
for i in range(10):
    format(decimalnumber)
