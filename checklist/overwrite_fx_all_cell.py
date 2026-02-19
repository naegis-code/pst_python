import pyautogui as pg
import time



def overwrite_fx_all_cell(rounds):
    print("Starting the overwrite process...")
    pg.hotkey('alt', 'tab')
    time.sleep(1)

    for i in range(rounds):
        pg.press('f2')
        time.sleep(0.5)
        pg.press('tab')

    print(f"Overwrite process completed after {rounds} rounds.")

overwrite_fx_all_cell(61)  # Adjust the number of rounds as needed