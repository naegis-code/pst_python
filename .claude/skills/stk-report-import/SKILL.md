---
name: stk-report-import
description: Run, check, or extend stocktake/stk_report.py, which summarises {bu}_stk_this_year and appends new rows to the stk_report table in pstdb3. Use when the user asks to update stk_report, load stocktake results for a BU (CHG, PWB, CFW, SSP, B2S, OFM), or add a new BU to it.
---

# stk_report import

Script: `stocktake/stk_report.py` → `import_stk_report(bu)`.

## What it does
1. Aggregates `{bu}_stk_this_year` (pstdb3) by stcode / cntdate / rpname / skutype into 15 measures:
   `sku, sgain, sloss, psoh, pqty, pgain, ploss, vrsoh, vsoh, vrqty, vqty, vrgain, vgain, vrloss, vloss`
   (p = pieces, v = cost value, vr = retail value, s = SKU count).
2. Anti-joins (`merge(how='left_anti')`, pandas ≥ 3.0) against `stk_report` on `bu, stcode, cntdate, skutype, rpname`.
3. Appends only new rows with `COPY ... FROM STDIN WITH (FORMAT csv)` in one transaction (rollback on error).

Insert-only, so it is safe to rerun: a rerun prints `new rows: 0`. It never updates existing rows.

## Running
```bash
PYTHONIOENCODING=utf-8 python stocktake/stk_report.py          # BU set in __main__
PYTHONIOENCODING=utf-8 python -c "import sys; sys.path.insert(0,'stocktake'); from stk_report import import_stk_report; import_stk_report('SSP')"
```
`PYTHONIOENCODING=utf-8` is needed in non-UTF-8 consoles because `tunnel.py` prints emoji.
The script writes to the live database, so tell the user the BU and the expected new-row count after running.

## Source column mapping per BU group
| output | CHG / PWB / CFW | SSP / B2S / OFM |
|---|---|---|
| stcode | `stmerch` | `store` (not `stcode`: NULL in many b2s rows) |
| qty variance | `varianceqnt` | `qty_var` |
| counted qty | `cntqnt` | `qty_count` |
| retail count / var | `extphycnt_retail` / `extphy_retailvar` | `phycnt_rtl` / `extrtl_var` |
| cost count / var | `extphycnt_cost` / `extphy_costvar` | `phycnt_cst` / `extcst_var` |

## Adding a new BU
1. Check the source columns first (read-only):
   `select column_name, data_type from information_schema.columns where table_name='<bu>_stk_this_year'`
2. Reuse an existing branch if the columns match, or add a new `elif` that outputs the same 19 column names.
3. Check that the `cntdate` and `stcode` formats match what `stk_report` already holds for that BU (`select bu, min(stcode), max(stcode) from stk_report group by bu`). A format mismatch makes the anti-join miss rows and insert duplicates.
4. CFR is not supported yet. `stk_report` already has CFR duplicate rows: **do not delete or dedupe them** unless the user asks.

## Sanity check after a run
If every source row comes back as "new" for a BU that already has data, check for duplicates before trusting the result:
```sql
select bu, stcode, cntdate, rpname, skutype, count(*) from stk_report
group by 1,2,3,4,5 having count(*) > 1;
```
