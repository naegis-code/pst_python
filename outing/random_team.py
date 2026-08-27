import pandas as pd
import random



# 1. โหลดข้อมูล (สมมติว่าอ่านจากไฟล์ Excel หรือ CSV)
df = pd.read_excel("D:\\Users\\prthanap\\Documents\\Book1.xlsx", sheet_name="Sheet1")

# สร้างตัวอย่าง DataFrame ตามโครงสร้างของคุณ
# (ต้องมีคอลัมน์: 'รหัสนักงาน', 'ชื่อ-สกุล', 'Hub', 'Gen')

def divide_into_teams(df, num_teams=9):
    # รวมกลุ่มตาม (Gen, Hub) เพื่อกระจายความหลากหลาย
    # จัดเรียงจากกลุ่มที่มีจำนวนคนน้อยไปมาก
    grouped = df.groupby(['Gen', 'Hub'])
    
    # แปลงข้อมูลเป็น List ของแต่ละกลุ่ม และสุ่มลำดับคนภายในกลุ่มนั้นๆ
    pools = []
    for _, group in grouped:
        members = group.to_dict('records')
        random.shuffle(members)
        pools.append(members)
    
    # สุ่มสลับลำดับของกลุ่ม
    random.shuffle(pools)
    
    # เตรียมลิสต์สำหรับเก็บ 9 ทีม
    teams = [[] for _ in range(num_teams)]
    
    # กระจายคนเข้าทีมแบบ Round-robin (วนลูปเติมทีละทีม)
    current_team = 0
    for pool in pools:
        for member in pool:
            teams[current_team].append(member)
            current_team = (current_team + 1) % num_teams
            
    # แปลงกลับเป็น DataFrame พร้อมใส่หมายเลขทีม (Team 1 - 9)
    result = []
    for team_id, members in enumerate(teams, 1):
        for m in members:
            m['Team'] = f"Team {team_id}"
            result.append(m)
            
    return pd.DataFrame(result)

# ตัวอย่างการใช้งาน:
result_df = divide_into_teams(df)
print(result_df)
result_df.to_excel("D:\\Users\\prthanap\\Documents\\Book2.xlsx")  # บันทึกผลลัพธ์กลับไปยังไฟล์ Excel

# ตรวจสอบความสมดุลของแต่ละทีม:
print(pd.crosstab(result_df['Team'], result_df['Gen'])) # เช็คจำนวน ชาย/หญิง
print(pd.crosstab(result_df['Team'], result_df['Hub'])) # เช็คจำนวน Hub