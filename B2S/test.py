import polars as pl


df_front = pl.DataFrame({
    "zone": [1, 3, 5],
    "gon":  [99, 99, 99],
    "bay":  [150, 150, 150],
})

df_back = pl.DataFrame({
    "zone": 'B',
    "bay":  1000,
})

result = (
    df_front
    .with_columns(
        gon=pl.int_ranges(1, pl.col("gon") + 1),   # [1,2,...,99]
        bay=pl.int_ranges(1, pl.col("bay") + 1),   # [1,2,...,150]
    )
    .explode("gon")      # แตกเป็นแถว
    .explode("bay")
    .with_columns(
        location=pl.concat_str([
            pl.col("zone").cast(pl.Utf8),
            pl.col("gon").cast(pl.Utf8).str.zfill(2),
            pl.col("bay").cast(pl.Utf8).str.zfill(3),
        ])
    )
    .select(["location"])
)

result_back = (
    df_back
    .with_columns(
        bay=pl.int_ranges(1, pl.col("bay") + 1),   # [1,2,...,1000]
    )
    .explode("bay")
    .with_columns(
        location=pl.concat_str([
            pl.col("zone").cast(pl.Utf8),
            pl.col("bay").cast(pl.Utf8).str.zfill(5),
        ])
    )
    .select(["location"])
)

result_all = pl.concat([result, result_back])

print(result)
print(result_back)
print(result_all)
