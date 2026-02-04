import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text
import db_connect
import math

path_master = r'D:\Program Files\B2S\Apps\master.csv'

# กำหนด chunk size (จำนวนแถวต่อรอบ)
chunksize = 1000

# อ่าน CSV เป็น chunks
chunks = []
with open(path_master, 'r', encoding='cp874', errors='replace') as f:
    for chunk in tqdm(pd.read_csv(f, dtype=str, header=None, chunksize=chunksize), desc="Reading CSV"):
        chunks.append(chunk)

# รวมทุก chunk
df = pd.concat(chunks, ignore_index=True)

columns = [
    "vendor", "v_name", "vndsts", "dept", "dept_name", "sub_dpt", "s_dpt_name", "class", "class_name", "sub_cls",
    "s_cls_name", "sku_no", "sku_descr", "derived_description", "short_description", "pos_description", "brand_id", "brand_no", "brand_desc", "buyer",
    "merchandiser", "set_code", "posdwn", "distribution_type", "delivery_type", "pop_grade", "catalogue", "mfg_part_no", "code_color", "color",
    "color_des", "code_size", "size", "size_des", "home_cost", "reg_cost", "reg_retail", "org_price", "buy_um", "sell_um",
    "packsize", "inner_pack", "discount_t", "sku_status", "i_auto", "i_manual", "sku_type", "rpl_code", "minrplqty", "display_minimum",
    "maximum_stock", "expec_first_receive", "distribute", "plan_week", "week_supply", "sea", "gm", "vat_flag", "isbn", "author",
    "translator", "year_edit", "edition_no", "page_no", "cover", "weight", "length", "width", "height",
    "planogram", "cluster", "rtv_by_itm", "rtv_vendor", "special", "expi_date", "ibc", "sbc#1", "sbc#2", "sbc#3",
    "sbc#4", "sbc#5", "creat_date", "create_by", "last_maintain_date", "sku_lead_time", "top_sale_company", "att_nam1", "att_val1", "att_dsc1",
    "att_nam2", "att_val2", "att_dsc2", "att_nam3", "att_val3", "att_dsc3", "att_nam4", "att_val4", "att_dsc4", "att_nam5",
    "att_val5", "att_dsc5", "att_nam6", "att_val6", "att_dsc6", "att_nam7", "att_val7", "att_dsc7", "att_nam8", "att_val8",
    "att_dsc8", "att_nam9", "att_val9", "att_dsc9", "att_nam10", "att_val10", "att_dsc10", "att_nam11", "att_val11", "att_dsc11",
    "att_nam12", "att_val12", "att_dsc12", "att_nam13", "att_val13", "att_dsc13", "att_nam14", "att_val14", "att_dsc14", "att_nam15",
    "att_val15", "att_dsc15", "soh_chain", "trf_on_order", "po_on_order", "avg_8wk_sale", "cur_w_sale", "wks_sale_01", "wks_sale_02", "wks_sale_03",
    "wks_sale_04", "wks_sale_05", "wks_sale_06", "wks_sale_07", "wks_sale_08", "po_comment_1", "po_comment_2", "po_comment_3", "thai_spec_01", "thai_spec_02",
    "thai_spec_03", "thai_spec_04", "thai_spec_05", "thai_spec_06", "thai_spec_07", "thai_spec_08", "thai_spec_09", "thai_spec_10", "thai_spec_11", "thai_spec_12",
    "eng_spec_01", "eng_spec_02", "eng_spec_03", "eng_spec_04", "eng_spec_05", "eng_spec_06", "eng_spec_07", "eng_spec_08", "eng_spec_09", "eng_spec_10",
    "eng_spec_11", "eng_spec_12", "size_range", "size_range_name", "option_code", "option_name", "style_cg", "season_start", "brand_group", "brand_group_name",
    "lifecycle_code", "lifecycle_name", "mupc#1", "mupc#2", "mupc#3", "mupc#4", "mupc#5"
]

df.columns = columns

print(df.shape)
print(df.info())

# Connection ไปยัง DB
db_connection_str = db_connect.db_url_pstdb3
engine = create_engine(db_connection_str)

# คำนวณจำนวนรอบที่จะ insert
total_rows = len(df)
num_chunks = math.ceil(total_rows / chunksize)

with engine.begin() as conn:
    conn.execute(text("DELETE FROM b2s_master"))
    print("✅ Existing data cleared from b2s_master table.")
    for i in tqdm(range(num_chunks), desc="Inserting to DB"):
        start = i * chunksize
        end = min(start + chunksize, total_rows)
        df.iloc[start:end].to_sql(
            'b2s_master',
            conn,
            if_exists='append',
            index=False,
            method='multi'
        )

print("✅ Data imported to database successfully.")
