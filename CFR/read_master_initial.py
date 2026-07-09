import polars as pl
import pathlib

user_home = pathlib.Path.home()
filename = 'PDASTOCK_421_0000517464'
extension = '.txt'
path = user_home / 'Downloads' / (filename + extension)

df = pl.read_csv(
    path,infer_schema=None,separator='|',has_header=False,encoding='cp874')

print(df)

df.write_csv(user_home / 'Downloads' / (filename + '_converted.txt'),separator='|',include_header=False)

print(f"Converted file saved to: {user_home / 'Downloads' / (filename + '_converted.txt')}")
