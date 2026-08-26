import pandas as pd

path = 'D:\\upload_pwb.xlsx'

# 1. Read the Excel file (default reads the first sheet)
df = pd.read_excel(path, sheet_name=0)

# 2. Export to a tab-delimited text file
df.to_csv('D:\\output_file.txt', sep='\t', index=False, header=False)

print("Export completed successfully!")