import os
import pyautogui as pag
import pyperclip
import time

time.sleep(2)  # Wait for 2 seconds before starting the automation

master_file = "D:\\Users\\prthanap\\Documents\\Report3_OFM.xlsx"

def report3_ofm(stcode,cntdate):
    # Open the report3_ofm.py script
    os.system(f'start "" "D:\\Users\\prthanap\\Documents\\Report3_OFM.xlsx"')
    time.sleep(5)  # Wait for the script to load
    '''pag.hotkey('ctrl', 'pagedown')
    time.sleep(0.5)
    pag.hotkey('ctrl', 'pagedown')
    time.sleep(0.5)
    pag.hotkey('ctrl', 'pagedown')
    time.sleep(0.5)
    pag.hotkey('ctrl', 'pagedown')
    time.sleep(2)  # Wait for the script to load'''
    pag.hotkey('ctrl', 'g')
    time.sleep(1)  # Wait for the Go To dialog to appear
    pag.typewrite('b2') 
    pag.press('enter')
    time.sleep(1)  # Wait for the script to load
    pag.typewrite(stcode)
    pag.press('tab')
    pag.typewrite(cntdate)
    pag.hotkey('enter')  
    pag.hotkey('ctrl', 'alt','f5')  #refreash all
    time.sleep(10)  # Wait for the script to load
    pag.hotkey('ctrl', 'pageup')
    time.sleep(0.5)
    pag.hotkey('ctrl', 'pageup')
    time.sleep(0.5)
    pag.hotkey('ctrl', 'pageup')
    time.sleep(0.5)
    pag.hotkey('ctrl', 'pageup')
    time.sleep(0.5)
    pag.hotkey('f12')  # Save as the script
    time.sleep(5)  # Wait for the Save As dialog to appear
    filename = f"D:\\Users\\prthanap\\Central Group\\PST Performance Team - เอกสาร\\Report\\2026\\00 Summary Report Stock\\06OFM\\Report3_OFM_{stcode}_{cntdate}.xlsx"
    # typewrite พิมพ์ภาษาไทยไม่ได้ (ข้ามตัวอักษรเงียบๆ ทำให้ path ผิด) จึงใช้วางจาก clipboard
    if not os.path.isdir(os.path.dirname(filename)):
        raise FileNotFoundError(f"ไม่พบโฟลเดอร์: {os.path.dirname(filename)}")
    pyperclip.copy(filename)
    pag.hotkey('ctrl', 'v')
    time.sleep(1)  # Wait for a moment before pressing Enter
    pag.press('enter')  # Press Enter to open the file
    time.sleep(15)  # Adjust the sleep time as needed based on your system's performance
    pag.hotkey('alt', 'f4')  # Close the script window
    time.sleep(10)  # Wait for a moment before returning to the main application
    print(f"✅ Report3_OFM for stcode: {stcode}, cntdate: {cntdate} has been executed and saved as {filename}.")

if __name__ == "__main__":
    # Example usage
    '''report3_ofm("73", "20260127")
    report3_ofm("79", "20260129")
    report3_ofm("91", "20260203")
    report3_ofm("43", "20260203")
    report3_ofm("89", "20260204")
    report3_ofm("70", "20260206")
    report3_ofm("84", "20260206")
    report3_ofm("53", "20260304")
    report3_ofm("63", "20260320")
    report3_ofm("25", "20260402")
    report3_ofm("52", "20260402")
    report3_ofm("34", "20260403")
    report3_ofm("87", "20260403")
    report3_ofm("21", "20260408")
    report3_ofm("102", "20260521")
    report3_ofm("51", "20260526")
    report3_ofm("90", "20260526")
    report3_ofm("6", "20260609")
    report3_ofm("86", "20260611")
    report3_ofm("17", "20260612")
    report3_ofm("92", "20260618")
    report3_ofm("49", "20260619")
    report3_ofm("94", "20260619")
    report3_ofm("75", "20260623")
    report3_ofm("72", "20260623")
    report3_ofm("93", "20260623")
    report3_ofm("74", "20260626")
    report3_ofm("50", "20260626")
    report3_ofm("65", "20260626")
    report3_ofm("64", "20260707")
    report3_ofm("66", "20260716")
    report3_ofm("60", "20260717")
    report3_ofm("58", "20260724")
    report3_ofm("40", "20260728")
    report3_ofm("62", "20260803")
    report3_ofm("59", "20260805")
    report3_ofm("110", "20260806")
    report3_ofm("16", "20260806")
    report3_ofm("18", "20260820")
    report3_ofm("7", "20260824")
    report3_ofm("111", "20260904")
    report3_ofm("71", "20260905")'''
    report3_ofm("24", "20260916")
    report3_ofm("28", "20260917")
    report3_ofm("83", "20260918")
    report3_ofm("67", "20260918")
    report3_ofm("5", "20260922")
    report3_ofm("1", "20260922")
    report3_ofm("121", "20261002")
    report3_ofm("80", "20261006")
    report3_ofm("76", "20261009")
    report3_ofm("96", "20261023")
    report3_ofm("55", "20261026")
    report3_ofm("23", "20261030")
    report3_ofm("99", "20261101")
    report3_ofm("10", "20261103")
    report3_ofm("61", "20261112")
    report3_ofm("117", "20261118")
    report3_ofm("44", "20261124")
    report3_ofm("13", "20261128")
    report3_ofm("69", "20261129")
    report3_ofm("120", "20261206")

