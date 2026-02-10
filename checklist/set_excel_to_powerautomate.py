import pyautogui as py
import time


print("Starting in 10 seconds. Please switch to the Excel window and select the starting cell.")
time.sleep(10)

def set_excel_to_powerautomate(wait_time):
    # เริ่ม ตั้ง Excel ให้อยู่ที่ cell ต้องการ copy
    # power automate เริ่มช่องจะวาง
    # run รอได้ 10 วิ ให้สลับไปมา ระหว่าง excel กับ power automate
    # เปิดค้างไว้ที่ Excel
    py.hotkey('ctrl', 'c')
    time.sleep(wait_time)
    py.hotkey('alt', 'tab')
    time.sleep(wait_time)
    py.hotkey('ctrl', 'v')
    time.sleep(wait_time)
    py.press('tab', presses=3)
    time.sleep(wait_time)
    py.hotkey('alt', 'tab')
    time.sleep(wait_time)
    py.press('down')
    time.sleep(wait_time)

def set_powerautomate_to_excel(wait_time):
    # เริ่ม ตั้ง Excel ให้อยู่ที่ cell ต้องการ Paste
    # power automate เริ่มช่อง ชื่อคอลัมน์: เช่น PWBV1F01
    # เปิดค้างไวั้ที่ Power Automate
    py.press('tab', presses=2)
    time.sleep(wait_time)
    py.hotkey('ctrl', 'a')
    time.sleep(wait_time)
    py.hotkey('ctrl', 'c')
    time.sleep(wait_time)
    py.hotkey('alt', 'tab')
    time.sleep(wait_time)
    py.hotkey('ctrl', 'v')
    time.sleep(wait_time)
    py.press('down')
    time.sleep(wait_time)
    py.hotkey('alt', 'tab')
    time.sleep(wait_time)
    py.hotkey('ctrl', 'pagedown')
    time.sleep(wait_time)

for _ in range(53):
    #set_excel_to_powerautomate(0.5)
    set_powerautomate_to_excel(0.5)
