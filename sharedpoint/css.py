"""
อ่าน SharePoint List -> Polars DataFrame
ใช้ Microsoft Graph + MSAL device code flow (delegated, ไม่ต้องใช้ client secret)

https://login.microsoft.com/device

ติดตั้ง:  pip install msal requests polars
"""
from pathlib import Path
from urllib.parse import unquote, urlparse

import msal
import polars as pl
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LIST_URL = (
    "https://centralgroup.sharepoint.com/sites/PSTPerformanceTeam/Lists/"
    "Customer%20Satisfaction%20Survery/Allitemsg.aspx"
)

CLIENT_ID = "14d82eec-204b-4c2f-b7e8-296a70dab67e"  # Microsoft Graph Command Line Tools
AUTHORITY = "https://login.microsoftonline.com/organizations"
SCOPES = ["Sites.Read.All"]
CACHE_FILE = Path.home() / ".sp_token_cache.json"
GRAPH = "https://graph.microsoft.com/v1.0"

# แยก host / site path / list name จาก URL อัตโนมัติ
_parts = urlparse(LIST_URL)
HOST = _parts.netloc                                   # centralgroup.sharepoint.com
_segs = [unquote(s) for s in _parts.path.split("/") if s]
SITE_PATH = "/" + "/".join(_segs[:2])                  # /sites/PSTPerformanceTeam
LIST_NAME = _segs[_segs.index("Lists") + 1]            # Customer Satisfaction Survery


# ---------------------------------------------------------------------------
# Auth (มี token cache: ล็อกอินครั้งเดียว ครั้งต่อไปใช้ซ้ำได้)
# ---------------------------------------------------------------------------
def get_token() -> str:
    cache = msal.SerializableTokenCache()
    if CACHE_FILE.exists():
        cache.deserialize(CACHE_FILE.read_text())

    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY, token_cache=cache)

    result = None
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if not result:
        flow = app.initiate_device_flow(scopes=SCOPES)
        if "message" not in flow:
            raise RuntimeError(f"เริ่ม device flow ไม่สำเร็จ: {flow}")
        print(flow["message"])
        result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        raise RuntimeError(f"ขอ token ไม่สำเร็จ: {result.get('error')} - {result.get('error_description')}")

    if cache.has_state_changed:
        CACHE_FILE.write_text(cache.serialize())

    return result["access_token"]


# ---------------------------------------------------------------------------
# Graph helpers
# ---------------------------------------------------------------------------
def graph_get(url: str, headers: dict) -> dict:
    r = requests.get(url, headers=headers, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()


def get_all(url: str, headers: dict) -> list:
    """ดึงทุกหน้า (pagination)"""
    items = []
    while url:
        data = graph_get(url, headers)
        items.extend(data["value"])
        url = data.get("@odata.nextLink")
    return items


def find_list(site_id: str, headers: dict) -> dict:
    """หา list จากชื่อใน URL / displayName / webUrl"""
    lists = get_all(f"{GRAPH}/sites/{site_id}/lists?$select=id,name,displayName,webUrl", headers)
    target = LIST_NAME.lower()

    for lst in lists:
        if lst["name"].lower() == target or lst["displayName"].lower() == target:
            return lst
    for lst in lists:
        if target.replace(" ", "%20") in lst["webUrl"].lower() or target in unquote(lst["webUrl"]).lower():
            return lst

    names = "\n".join(f"  - {l['displayName']}  (url name: {l['name']})" for l in lists)
    raise RuntimeError(f"ไม่พบ list '{LIST_NAME}' ใน site นี้ list ที่มี:\n{names}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def load_list() -> pl.DataFrame:
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    # 1) site
    site = graph_get(f"{GRAPH}/sites/{HOST}:{SITE_PATH}", headers)
    site_id = site["id"]

    # 2) list
    lst = find_list(site_id, headers)
    list_id = lst["id"]
    print(f"พบ list: {lst['displayName']}")

    # 3) map internal name -> display name ของ column
    cols = get_all(f"{GRAPH}/sites/{site_id}/lists/{list_id}/columns", headers)
    name_map = {c["name"]: c["displayName"] for c in cols if not c.get("hidden")}

    # 4) items
    url = f"{GRAPH}/sites/{site_id}/lists/{list_id}/items?expand=fields&$top=999"
    items = get_all(url, headers)
    rows = [i["fields"] for i in items]
    if not rows:
        print("list ว่าง")
        return pl.DataFrame()

    df = pl.DataFrame(rows, infer_schema_length=None)

    # 5) ทำความสะอาด: ตัด metadata (@odata...) และเปลี่ยนเป็นชื่อ column ที่เห็นบนหน้าเว็บ
    df = df.select([c for c in df.columns if not c.startswith("@")])

    rename, used = {}, set()
    for c in df.columns:
        new = name_map.get(c, c)
        if new in used:          # กันชื่อซ้ำ
            new = c
        used.add(new)
        rename[c] = new
    df = df.rename(rename)

    return df


if __name__ == "__main__":
    df = load_list()
    print(df.shape)
    print(df.head())
    print(df.schema)

    # บันทึกไว้ใช้ต่อ (เอาคอมเมนต์ออกถ้าต้องการ)
    # df.write_csv("customer_satisfaction_survey.csv")
    # df.write_parquet("customer_satisfaction_survey.parquet")