import os
import pyautogui
import time
from user_pass import *
import pathlib

mkdir = pathlib.Path.home() / 'Documents' / 'soh' / 'wds'
if not os.path.exists(mkdir):
    os.makedirs(mkdir)

def wait_for_color_change(initial_color, desired_color, position, sequence):
    """
    Waits for a pixel at a given position to change to the desired color.
    Executes a series of key presses when the desired color is detected.
    """
    while True:
        screenshot = pyautogui.screenshot()
        current_color = screenshot.getpixel(position)
        
        if current_color == desired_color:
            time.sleep(10)
            perform_actions(sequence)
            print(f"Color changed to {desired_color}. Sequence {sequence} executed.")
            break
        elif current_color != initial_color:
            print(f"Color changed from {initial_color}, but not to {desired_color}. Current color: {current_color}")
        else:
            print(f"Waiting... Current color: {current_color}")
        
        time.sleep(5)  # Reduce CPU usage

def perform_actions(sequence):
    """Executes a series of key presses for the given sequence."""
    pyautogui.press('tab', presses=2)
    pyautogui.press('enter')
    pyautogui.press('tab')
    pyautogui.write(str(sequence).zfill(3))
    pyautogui.press('enter')
    pyautogui.hotkey('alt', 'e')        
    time.sleep(1)

    time.sleep(1)
    pyautogui.write(str(sequence).zfill(3))
    pyautogui.press('enter')
    time.sleep(1)

# Define the initial color, desired color, and position
initial_color = (255, 0, 0)
desired_color = (171, 171, 171)
position = (807, 202)

os.system('start "" "C:\\Program Files (x86)\\Upfront\\USSWDS\\USSWDS.exe"')
time.sleep(5)  # Allow window to open

#windows = pyautogui.getWindowsWithTitle("Log In")  # Replace with the actual window title


if not os.path.exists(mkdir):
    os.makedirs(mkdir)

pyautogui.write(sspwds_user)
pyautogui.press('tab')
pyautogui.write(sspwds_pass)
pyautogui.press('enter')
time.sleep(5)
pyautogui.press('alt')
pyautogui.press('5', presses=2)
time.sleep(5)
pyautogui.write('001')
pyautogui.press('enter')
pyautogui.press('tab', presses=7)
pyautogui.press('down', presses=4)
pyautogui.hotkey('alt', 'e')
time.sleep(2)
pyautogui.write(str(mkdir / '001'))
pyautogui.press('enter')
wait_for_color_change(initial_color, desired_color, position, 2)


for sequence in range(3, 40):
    wait_for_color_change(initial_color, desired_color, position, sequence)