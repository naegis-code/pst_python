import os
import pyautogui
import time
from user_pass import *
import pathlib

#start 8. ขนาด

time.sleep(5)  # Allow user to switch to the correct window
pyautogui.hotkey('alt', 'tab')  # Switch to the SSP.WS window


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
    pyautogui.write(str(sequence).zfill(3))
    pyautogui.press('enter')
    time.sleep(1)

# Define the initial color, desired color, and position
initial_color = (255, 0, 0)
desired_color = (171, 171, 171)
position = (807, 202)


for sequence in range(2, 39):
    wait_for_color_change(initial_color, desired_color, position, sequence)