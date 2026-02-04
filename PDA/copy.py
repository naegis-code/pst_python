import pyautogui
import time
import subprocess

for i in range(30):  # ตัวอย่าง loop 5 ครั้ง
    answer = input("Do you want to continue working? (y/n): ")  # ถามผู้ใช้
    if answer.lower() == 'y':
        print(f"Start rond {i + 1}")
        
        # เปิด "This PC" โดยใช้ subprocess
        subprocess.run(["explorer", ","])  

        time.sleep(5)  # รอให้ File Explorer โหลด

        pyautogui.press('tab', presses=12, interval=0.2)  # กด Tab 3 ครั้งเพื่อไปที่แถบที่อยู่

        time.sleep(1)  # รอให้สลับหน้าต่างเสร
        # กดปุ่มต่าง ๆ เพื่อจำลองการนำทาง
        pyautogui.press('n')
        pyautogui.press('n')  # กด 'n' เพื่อไปยังไดเรกทอรีที่เริ่มต้นด้วย 'n'
        pyautogui.press('enter')
        time.sleep(1)
        pyautogui.press('i')  # กด 'i' เพื่อไปยังไดเรกทอรีที่เริ่มต้นด้วย 'i'
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(1)
        '''
        # ลบข้อมูลจากไดเรกทอรีนี้ แก้ไขข้อมูลเนื่องจาก copy profile ผิด
        pyautogui.press('a')
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.3)
        pyautogui.press('d')
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.3)
        pyautogui.press('c')
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.3)
        pyautogui.press('f')
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.3)
        pyautogui.press('e')  # เลือกไฟล์ทั้งหมด
        pyautogui.press('delete')  # เข้าสู่ไดเรกทอรีนั้น
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.5)
        pyautogui.press('e')  # เลือกไฟล์ทั้งหมด
        pyautogui.press('delete')  # เข้าสู่ไดเรกทอรีนั้น
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.5)
        pyautogui.press('e')  # เลือกไฟล์ทั้งหมด
        pyautogui.press('delete')  # เข้าสู่ไดเรกทอรีนั้น
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.5)
        pyautogui.press('e')  # เลือกไฟล์ทั้งหมด
        pyautogui.press('delete')  # เข้าสู่ไดเรกทอรีนั้น
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.5)
        pyautogui.press('e')  # เลือกไฟล์ทั้งหมด
        pyautogui.press('delete')  # เข้าสู่ไดเรกทอรีนั้น
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.5)
        pyautogui.press('e')  # เลือกไฟล์ทั้งหมด
        pyautogui.press('delete')  # เข้าสู่ไดเรกทอรีนั้น
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        time.sleep(0.5)
        pyautogui.press('backspace')  # กลับไปที่ไดเรกทอรีก่อนหน้านี้
        pyautogui.press('backspace')
        pyautogui.press('backspace')
        pyautogui.press('backspace')

        # กด 'd' เพื่อไปยังโฟลเดอร์ที่เริ่มต้นด้วย 'd'
        time.sleep(1)
        pyautogui.press('a')
        pyautogui.press('enter')   
        time.sleep(1)
        pyautogui.press('d')
        pyautogui.press('enter')  # เข้าสู่ไดเรกทอรีนั้น
        '''
        
        time.sleep(1)
        pyautogui.hotkey('ctrl', 'v')  # วางข้อมูลจาก clipboard
        time.sleep(1)
        pyautogui.hotkey('alt', 'a')
        pyautogui.press('enter')
        time.sleep(15)
        pyautogui.hotkey('alt','f4')

    elif answer.lower() == 'n':
        print("End process")
        break  # หยุด loop ถ้าตอบ 'n'
    else:
        print("please y or n ")