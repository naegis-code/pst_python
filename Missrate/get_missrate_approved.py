import pandas as pd
import pathlib
import shutil
from datetime import datetime, timedelta

# Set the time threshold in hours
hour = 168

user_path = pathlib.Path.home()

if (user_path / 'Central Group/PST Performance Team - เอกสาร').exists():
    filepath = user_path / 'Central Group/PST Performance Team - เอกสาร'
else:
    filepath = user_path / 'Central Group/PST Performance Team - Documents'

path = filepath / 'Apps' / 'missrate_approved.csv'

df = pd.read_csv(path, dtype=str)

df['file'] = df['file'].str.extract(r'file=(.*?)(?=&amp|$)', expand=False)
df['file'] = df['file'].str.replace('%20', ' ', regex=False)
df['modified'] = pd.to_datetime(df['modified'], utc=True) + pd.Timedelta(hours=7)
df['file_type'] = df['file'].str.extract(r'(\.[^.]+)$', expand=False)
df['destination'] = user_path / 'OneDrive - Central Group' / 'Apps' / 'Microsoft Forms' / 'Upload Report' / 'Summary First & Final Excel  Missrate' / df['file']

df['cntdate'] = pd.to_datetime(df['cntdate'], errors='coerce')
path_target = filepath / 'Shared' / 'Performance' / 'Missrate' / 'Missrate_approved'
df['target'] = df.apply(lambda row: f"{path_target}\\Missrate_{row['bu']}_{row['stcode']}_{row['cntdate'].strftime('%Y%m%d')}{row['file_type']}", axis=1)


time_threshold = pd.Timestamp.now(tz='UTC') + pd.Timedelta(hours=7)
df = df[df['modified'] >= (time_threshold - pd.Timedelta(hours=hour))]

# Copy files from 'file_destination' to 'path_file_target'
for index, row in df.iterrows():
    try:
        # Check if the source file exists
        if pathlib.Path(row['destination']).exists():
            # Ensure the target directory exists
            target_path = pathlib.Path(row['target'])
            target_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy the file
            shutil.copy(row['destination'], target_path)
            print(f"Copied {row['destination']} to {target_path}")
        else:
            print(f"Source file {row['destination']} does not exist.")
    except Exception as e:
        print(f"Error copying file {row['destination']} to {row['target']}: {e}")

# Export to CSV
df['timestamp'] = datetime.now()
csv_filename = filepath / 'Apps' / 'missrate_approved_save_files.csv'  # Replace with the desired output path
df.to_csv(csv_filename, index=False, encoding='utf-8', mode='a', header=not pathlib.Path(csv_filename).exists())