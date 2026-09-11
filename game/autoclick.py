import pyautogui as pag
import time

# 1. ดึงขนาดความกว้าง (width) และความสูง (height) ของหน้าจอ
screen_width, screen_height = pag.size()

# 2. คำนวณจุดกลางจอ (หาร 2)
center_x = screen_width // 2
center_y = screen_height // 2

print(f"ขนาดหน้าจอ: {screen_width} x {screen_height}")
print(f"พิกัดกลางจอ (X, Y): ({center_x}, {center_y})")

time.sleep(1)  # Wait for 5 seconds before starting the auto-clicking
pag.hotkey('alt', 'tab')  # Press Alt + Tab to switch windows

def auto_click(range,timesleep):
    for i in range:
        pag.moveTo(center_x, center_y, duration=0.1)
        pag.click()  # Perform a mouse click
        time.sleep(timesleep)  # Wait for the specified time before the next click

auto_click(range(2000), 0.0001)  # Click 100 times with a 0.1-second interval