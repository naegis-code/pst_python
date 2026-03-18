import pandas as pd
import yfinance as yf

ticker = "TISCO.BK"

start = "2021-03-17"
end   = "2026-03-18"  # yfinance end เป็นแบบไม่รวมวันสุดท้าย เลยบวก 1 วัน

df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=False, progress=False)

# ทำให้เป็นคอลัมน์มาตรฐาน OHLCV
df = df.reset_index()  # จะได้คอลัมน์ Date
df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]  # กันกรณี multi-index

out = "TISCO_OHLCV_5Y_2021-03-17_to_2026-03-17.csv"
df.to_csv(out, index=False, encoding="utf-8-sig")

print("saved:", out)
print("columns:", df.columns.tolist())
print("rows:", len(df))
print(df.head())