import polars as pl


set = '500.000'

pl.filter(pl.col(len(set)) < 8)

print(len(set))