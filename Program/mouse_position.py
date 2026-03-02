import pyautogui as pg
import time 

time.sleep(1)  # Wait for 1 second to allow user to switch to the target application
pg.position()
print(pg.position())

