import pyautogui as pg
import time

time.sleep(10)  # Wait for 10 seconds to allow user to start excel switch youtubemusic


def add_music_youtube(wait):
    time.sleep(wait)
    pg.hotkey('ctrl', 'c')  # copy the music name from the clipboard
    time.sleep(wait)
    pg.press('down')  # Move down to the next music name
    time.sleep(wait)
    pg.hotkey('alt', 'tab')  # Switch to youtube music
    time.sleep(wait)
    pg.click(1014, 155)  # Click search bar
    time.sleep(wait)
    pg.hotkey('ctrl', 'v')  # Paste the music name
    time.sleep(wait)
    pg.press('enter')  # Press Enter to search
    time.sleep(2) # Wait for search results to load
    pg.click(1322, 363)  # Click save button
    time.sleep(wait) # Wait for the save action to complete
    pg.press('esc')  # Close the search results
    time.sleep(wait)
    #pg.click(831, 356)  # Click save to top 100
    #time.sleep(wait)
    pg.hotkey('alt', 'tab')  # Switch back to excel
    time.sleep(wait)

for i in range(40):
    add_music_youtube(0.5)