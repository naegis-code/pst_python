import pyautogui as pg
import time 

time.sleep(10)  # Wait for 10 seconds to allow user to switch to the target application
pg.position()
print(pg.position())