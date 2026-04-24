import json
import pandas as pd
import psycopg2
import streamlit as st
import streamlit.components.v1 as components
from datetime import timedelta
from plotly.offline import get_plotlyjs

# 設定網頁標題與寬度
st.set_page_config(page_title="ETF 持股趨勢儀表板", layout="wide")

# ==========================================================
# 1. 資料庫連線與撈取 (加入快取機制，1小時內不重複撈DB)
# ==========================================================
@st.cache_data(ttl=3600)
def load_all_data_from_db() -> pd.DataFrame:
    # 透過 st.secrets 安全讀取密碼
    conn = psycopg2.connect(
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"]
    )
    
    query = 'SELECT * FROM "V_ETF_TRACKING_DATA"'
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()

    if not rows:
        raise ValueError("資料庫讀取完成，但視圖為空。")

    df = pd.DataFrame(rows, columns=cols)
    df.columns = [str(col).upper() for col in df.columns]

    rename_mapping = {
        "DATE": "資料日期", "DATA_DATE": "資料日期", "STOCK_CODE": "股票代號",
        "STOCK_NAME": "股票名稱", "STOCK_KEY": "股票鍵", "INDUSTRY_NAME": "產業分類",
        "IND_CATEGORY": "產業分類", "SHARES": "股數", "SHARES_LOT": "股數(張)",
        "SHARES_NUM": "股數(張)", "WEIGHT": "持股權重"
    }
    df = df.rename(columns=rename_mapping)

    df["資料日期"] = pd.to_datetime(df["資料日期"])
    if "檔名日期" not in df.columns:
        df["檔名日期"] = df["資料日期"]
    else:
        df["檔名日期"] = pd.to_datetime(df["檔名日期"])
        
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0)
    df["股數(張)"] = pd.to_numeric(df["股數(張)"], errors="coerce").fillna(0)
    df["持股權重"] = pd.to_numeric(df["持股權重"], errors="coerce").fillna(0.0)

    df = df.sort_values(["資料日期", "股票代號", "股票名稱"]).reset_index(drop=True)
    return df

# ==========================================================
# 2. 資料處理函數 (沿用 V11 邏輯)
# ==========================================================
def get_master_info(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["資料日期", "股票代號", "股票名稱"]).drop_duplicates(subset=["股票鍵"], keep="last")[["股票鍵", "股票代號", "股票名稱", "產業分類"]].copy()

def build_default_top10_keys(df: pd.DataFrame) -> list[str]:
    latest_date = df["資料日期"].max()
    latest_snapshot = df[df["資料日期"] == latest_date].sort_values(["持股權重", "股數"], ascending=[False, False]).drop_duplicates(subset=["股票鍵"], keep="first")
    return latest_snapshot["股票鍵"].head(10).tolist()

def build_delta_table(df: pd.DataFrame):
    unique_dates = sorted(df["資料日期"].dropna().unique())
    if len(unique_dates) < 2: return pd.DataFrame(), None, None
    latest_date = pd.to_datetime(unique_dates[-1])
    prev_date = pd.to_datetime(unique_dates[-2])
    master = get_master_info(df)
    latest_df = df[df["資料日期"] == latest_date][["股票鍵", "股數", "股數(張)", "持股權重"]].rename(columns={"股數": "最新股數", "股數(張)": "最新張數", "持股權重": "最新權重(%)"})
    prev_df = df[df["資料日期"] == prev_date][["股票鍵", "股數", "股數(張)", "持股權重"]].rename(columns={"股數": "前一日股數", "股數(張)": "前一日張數", "持股權重": "前一日權重(%)"})
    delta = master.merge(latest_df, on="股票鍵", how="left").merge(prev_df, on="股票鍵", how="left")
    fill_columns = ["最新股數", "最新張數", "最新權重(%)", "前一日股數", "前一日張數", "前一日權重(%)"]
    delta[fill_columns] = delta[fill_columns].fillna(0)
    delta["張數變化"] = delta["最新張數"] - delta["前一日張數"]
    delta["股數變化"] = delta["最新股數"] - delta["前一日股數"]
    delta["權重變化(百分點)"] = delta["最新權重(%)"] - delta["前一日權重(%)"]
    delta = delta.sort_values(["權重變化(百分點)", "張數變化"], ascending=[False, False]).reset_index(drop=True)
    return delta, latest_date.strftime("%Y-%m-%d"), prev_date.strftime("%Y-%m-%d")

def analyze_action_streak(signs: list[int], changes: list[float]):
    if not signs: return {"current_sign": 0, "current_days": 0, "previous_sign": 0, "previous_days": 0, "latest_change": 0}
    current_sign = signs[-1]
    latest_change = changes[-1] if changes else 0
    if current_sign == 0: return {"current_sign": 0, "current_days": 0, "previous_sign": 0, "previous_days": 0, "latest_change": latest_change}
    idx = len(signs) - 1
    current_days = 0
    while idx >= 0 and signs[idx] == current_sign:
        current_days += 1; idx -= 1
    previous_sign = 0; previous_days = 0
    if idx >= 0 and signs[idx] != 0:
        previous_sign = signs[idx]
        while idx >= 0 and signs[idx] == previous_sign:
            previous_days += 1; idx -= 1
    return {"current_sign": current_sign, "current_days": current_days, "previous_sign": previous_sign, "previous_days": previous_days, "latest_change": latest_change}

def build_state_tables(df: pd.DataFrame):
    unique_dates = sorted(pd.to_datetime(df["資料日期"].dropna().unique()))
    result = {"latest_date": unique_dates[-1].strftime("%Y-%m-%d") if unique_dates else None, "prev_date": unique_dates[-2].strftime("%Y-%m-%d") if len(unique_dates) >= 2 else None, "buy_to_sell": [], "sell_to_buy": [], "consecutive_buy": [], "consecutive_sell": [], "removed": [], "newly_added": []}
    if len(unique_dates) < 2: return result
    master = get_master_info(df)
    latest_date = unique_dates[-1]; prev_date = unique_dates[-2]
    pivot_lots = df.pivot_table(index="股票鍵", columns="資料日期", values="股數(張)", aggfunc="sum").reindex(columns=unique_dates).fillna(0.0)
    pivot_weight = df.pivot_table(index="股票鍵", columns="資料日期", values="持股權重", aggfunc="sum").reindex(columns=unique_dates).fillna(0.0)
    latest_vs_prev = master.merge(pivot_lots[[prev_date, latest_date]].rename(columns={prev_date: "前一日張數", latest_date: "最新張數"}), left_on="股票鍵", right_index=True, how="left").merge(pivot_weight[[prev_date, latest_date]].rename(columns={prev_date: "前一日權重(%)", latest_date: "最新權重(%)"}), left_on="股票鍵", right_index=True, how="left")
    latest_vs_prev[["前一日張數", "最新張數", "前一日權重(%)", "最新權重(%)"]] = latest_vs_prev[["前一日張數", "最新張數", "前一日權重(%)", "最新權重(%)"]].fillna(0)
    latest_vs_prev["張數變化"] = latest_vs_prev["最新張數"] - latest_vs_prev["前一日張數"]
    latest_vs_prev["權重變化(百分點)"] = latest_vs_prev["最新權重(%)"] - latest_vs_prev["前一日權重(%)"]
    
    removed = latest_vs_prev[(latest_vs_prev["前一日張數"] > 0) & (latest_vs_prev["最新張數"] == 0)].sort_values(["前一日張數", "前一日權重(%)"], ascending=[False, False])
    newly_added = latest_vs_prev[(latest_vs_prev["前一日張數"] == 0) & (latest_vs_prev["最新張數"] > 0)].sort_values(["最新張數", "最新權重(%)"], ascending=[False, False])
    result["removed"] = removed[["股票鍵", "股票代號", "股票名稱", "前一日張數", "最新張數", "張數變化", "前一日權重(%)", "最新權重(%)", "權重變化(百分點)"]].to_dict(orient="records")
    result["newly_added"] = newly_added[["股票鍵", "股票代號", "股票名稱", "前一日張數", "最新張數", "張數變化", "前一日權重(%)", "最新權重(%)", "權重變化(百分點)"]].to_dict(orient="records")

    if len(unique_dates) < 3: return result
    changes = pivot_lots.diff(axis=1).iloc[:, 1:].fillna(0.0)
    sign_changes = changes.map(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    action_rows = []
    master_lookup = master.set_index("股票鍵")
    for stock_key in pivot_lots.index.tolist():
        stock_info = master_lookup.loc[stock_key]
        lots_series = pivot_lots.loc[stock_key].tolist()
        change_series = changes.loc[stock_key].tolist()
        sign_series = sign_changes.loc[stock_key].tolist()
        streak = analyze_action_streak(sign_series, change_series)
        action_rows.append({
            "股票鍵": stock_key, "股票代號": stock_info["股票代號"], "股票名稱": stock_info["股票名稱"],
            "前一日張數": lots_series[-2], "最新張數": lots_series[-1], "最新變化": streak["latest_change"],
            "目前動作": "買" if streak["current_sign"] > 0 else ("賣" if streak["current_sign"] < 0 else "無"),
            "目前連續天數": streak["current_days"],
            "轉換前動作": "買" if streak["previous_sign"] > 0 else ("賣" if streak["previous_sign"] < 0 else ""),
            "轉換前連續天數": streak["previous_days"],
        })
    state_df = pd.DataFrame(action_rows)
    result["buy_to_sell"] = state_df[(state_df["目前動作"] == "賣") & (state_df["轉換前動作"] == "買")].sort_values(["轉換前連續天數", "目前連續天數", "最新變化"], ascending=[False, False, True]).to_dict(orient="records")
    result["sell_to_buy"] = state_df[(state_df["目前動作"] == "買") & (state_df["轉換前動作"] == "賣")].sort_values(["轉換前連續天數", "目前連續天數", "最新變化"], ascending=[False, False, False]).to_dict(orient="records")
    result["consecutive_buy"] = state_df[(state_df["目前動作"] == "買") & (state_df["轉換前動作"] != "賣") & (state_df["目前連續天數"] > 0)].sort_values(["目前連續天數", "最新變化"], ascending=[False, False]).to_dict(orient="records")
    result["consecutive_sell"] = state_df[(state_df["目前動作"] == "賣") & (state_df["轉換前動作"] != "買") & (state_df["目前連續天數"] > 0)].sort_values(["目前連續天數", "最新變化"], ascending=[False, True]).to_dict(orient="records")
    return result

def to_json_records(df: pd.DataFrame) -> list[dict]:
    records = []
    for row in df.to_dict(orient="records"):
        item = {}
        for key, value in row.items():
            if isinstance(value, pd.Timestamp): item[key] = value.strftime("%Y-%m-%d")
            elif pd.isna(value): item[key] = None
            else: item[key] = value
        records.append(item)
    return records

# ==========================================================
# 3. 產出 HTML 語法 (直接回傳字串)
# ==========================================================
def build_html(df, delta_df, compare_latest, compare_prev, state_tables, default_stock_keys) -> str:
    plotly_js = get_plotlyjs()
    data_records = to_json_records(df)
    delta_records = to_json_records(delta_df) if not delta_df.empty else []
    industry_df = df.groupby(['資料日期', '產業分類']).agg({'股數(張)': 'sum', '持股權重': 'sum'}).reset_index()
    industry_records = to_json_records(industry_df)

    min_date = pd.to_datetime(df["資料日期"].min())
    max_date = pd.to_datetime(df["資料日期"].max())
    default_start = max(min_date, max_date - pd.Timedelta(days=6))

    summary = {
        "earliest_date": min_date.strftime("%Y-%m-%d"),
        "latest_date": max_date.strftime("%Y-%m-%d"),
        "default_start_date": default_start.strftime("%Y-%m-%d"),
        "default_end_date": max_date.strftime("%Y-%m-%d"),
        "compare_latest_date": compare_latest,
        "compare_prev_date": compare_prev,
        "default_stock_keys": default_stock_keys,
    }

    # 這裡放您 V11 的完整 HTML 模板
    return f'''<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ETF 持股趨勢儀表板 (雲端版)</title>
  <script>{plotly_js}</script>
  <style>
    body {{ font-family: Arial, "Microsoft JhengHei", sans-serif; margin: 0; background: #f5f7fb; color: #1f2937; padding-top: 10px; }}
    .container {{ max-width: 1560px; margin: 0 auto; padding: 24px; }}
    h1, h2, h3 {{ margin: 0 0 12px 0; }}
    .summary-bar {{ display: flex; flex-wrap: wrap; gap: 16px; align-items: center; font-size: 14px; color: #374151; background: #ffffff; border-radius: 12px; padding: 12px 16px; box-shadow: 0 6px 16px rgba(15, 23, 42, 0.08); margin-bottom: 18px; }}
    .section {{ background: #ffffff; border-radius: 14px; padding: 18px; box-shadow: 0 6px 16px rgba(15, 23, 42, 0.08); margin-bottom: 20px; }}
    details {{ margin-bottom: 20px; background: #ffffff; border-radius: 14px; padding: 18px; box-shadow: 0 6px 16px rgba(15, 23, 42, 0.08); }}
    summary {{ cursor: pointer; list-style: none; display: flex; align-items: center; outline: none; }}
    summary::-webkit-details-marker {{ display: none; }}
    summary h2 {{ margin: 0; color: #3b82f6; }}
    summary::before {{ content: '▶'; margin-right: 10px; color: #3b82f6; font-size: 18px; transition: transform 0.2s; }}
    details[open] summary::before {{ transform: rotate(90deg); }}
    details[open] .content {{ margin-top: 16px; border-top: 1px solid #e5e7eb; padding-top: 16px; }}
    .sticky-filter {{ position: sticky; top: 10px; z-index: 1000; background: rgba(255, 255, 255, 0.95); backdrop-filter: blur(8px); border-radius: 12px; padding: 12px 16px; box-shadow: 0 8px 20px rgba(15, 23, 42, 0.12); border: 1px solid #e5e7eb; margin-bottom: 20px; }}
    .sticky-filter details summary {{ font-size: 14px; font-weight: 700; color: #111827; cursor: pointer; outline: none; user-select: none; display: flex; align-items: center; }}
    .sticky-filter details summary::before {{ content: '▶'; margin-right: 8px; color: #4f46e5; transition: transform 0.2s; font-size: 12px; }}
    .filter-content {{ max-height: 25vh; overflow-y: auto; margin-top: 12px; padding-right: 6px; font-size: 12px; }}
    .filter-content::-webkit-scrollbar {{ width: 6px; }}
    .filter-content::-webkit-scrollbar-thumb {{ background: #cbd5e1; border-radius: 4px; }}
    .filter-content::-webkit-scrollbar-thumb:hover {{ background: #9ca3af; }}
    .filter-stack {{ display: grid; gap: 12px; }}
    .filter-block {{ display: grid; gap: 6px; }}
    .select-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }}
    .filter-title {{ font-size: 12px; font-weight: 700; color: #1f2937; }}
    .date-range-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; align-items: end; }}
    .date-item label {{ display: block; font-size: 11px; font-weight: 700; margin-bottom: 4px; color: #4b5563; }}
    .date-item input, .action-buttons button {{ width: 100%; padding: 6px 10px; height: 30px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 12px; background: #fff; box-sizing: border-box; }}
    .action-buttons {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
    .action-buttons button {{ cursor: pointer; font-weight: 600; color: #374151; transition: all 0.1s; }}
    .action-buttons button:hover {{ background: #eef2ff; color: #4f46e5; border-color: #c7d2fe; }}
    .sticky-filter .small-note {{ font-size: 11px; color: #6b7280; margin-top: 0; line-height: 1.4; }}
    .custom-dropdown {{ position: relative; width: 100%; }}
    .dropdown-btn {{ width: 100%; height: 30px; padding: 0 10px; text-align: left; background: #fff; border: 1px solid #d1d5db; border-radius: 6px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; font-size: 12px; color: #1f2937; box-sizing: border-box; }}
    .dropdown-btn:hover {{ border-color: #9ca3af; }}
    .dropdown-btn::after {{ content: "▼"; font-size: 10px; color: #6b7280; transition: transform 0.2s; }}
    .custom-dropdown.active .dropdown-btn::after {{ transform: rotate(180deg); }}
    .dropdown-content {{ display: none; position: fixed; background: #fff; border: 1px solid #d1d5db; box-shadow: 0 8px 20px rgba(0,0,0,0.15); z-index: 9999; max-height: 35vh; overflow-y: auto; border-radius: 6px; padding: 6px; font-size: 12px; }}
    .dropdown-content.show {{ display: block; }}
    .checkbox-label {{ display: flex; align-items: center; padding: 6px 8px; cursor: pointer; border-radius: 4px; transition: background 0.1s; color: #374151; }}
    .checkbox-label:hover {{ background: #f3f4f6; color: #111827; }}
    .checkbox-label input {{ margin-right: 8px; cursor: pointer; }}
    .chart {{ min-height: 620px; width: 100%; }}
    .pie-chart {{ min-height: 650px; width: 100%; margin-bottom: 24px; }}
    .table-wrap {{ overflow-x: auto; width: 100%; }}
    .table-grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .state-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .state-card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 14px; background: #fff; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #e5e7eb; text-align: left; white-space: nowrap; }}
    th {{ background: #f9fafb; position: sticky; top: 0; z-index: 1; }}
    .positive {{ color: #047857; font-weight: 700; }}
    .negative {{ color: #b91c1c; font-weight: 700; }}
    .muted {{ color: #6b7280; }}
    .sub-title {{ font-size: 16px; margin-bottom: 8px; }}
    @media (max-width: 1280px) {{ .table-grid-2, .state-grid {{ grid-template-columns: 1fr; }} .select-grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class="container">
    <h1>📊 ETF 持股趨勢儀表板</h1>

    <div class="summary-bar">
      <div><b>資料最早日：</b><span id="earliestDate"></span></div>
      <div><b>資料最晚日：</b><span id="latestDate"></span></div>
    </div>

    <div class="sticky-filter">
      <details open id="filterDetails">
        <summary><h2 style="margin: 0; display: inline-block; font-size: 16px;">🔍 篩選控制面板</h2></summary>
        <div class="filter-content">
          <div class="filter-stack">
            <div style="display: flex; flex-wrap: wrap; gap: 16px;">
                <div class="filter-block">
                    <div class="filter-title">區間篩選 (影響全部時間圖表)</div>
                    <div class="date-range-grid">
                        <div class="date-item"><label>起始日期</label><input type="date" id="startDate"></div>
                        <div class="date-item"><label>結束日期</label><input type="date" id="endDate"></div>
                    </div>
                </div>
                <div class="filter-block" style="flex-grow: 1;">
                    <div class="filter-title">批次操作</div>
                    <div class="action-buttons" style="align-items: flex-end; height: 100%;">
                        <button type="button" id="selectAllBtn">全選股票與族群</button>
                        <button type="button" id="clearSelectionBtn">清除選取 (顯示全部)</button>
                    </div>
                </div>
            </div>

            <div class="select-grid">
                <div class="filter-block stock-block">
                    <div class="filter-title" style="color: #4f46e5;">🏭 族群篩選 (自動連動右側股票)</div>
                    <div class="custom-dropdown" id="indDropdownWrap">
                        <button type="button" class="dropdown-btn" id="indDropdownBtn">讀取中...</button>
                    </div>
                </div>
                <div class="filter-block stock-block">
                    <div class="filter-title" style="color: #4f46e5;">📈 股票篩選 (單獨調整不影響族群)</div>
                    <div class="custom-dropdown" id="stockDropdownWrap">
                        <button type="button" class="dropdown-btn" id="stockDropdownBtn">讀取中...</button>
                    </div>
                </div>
            </div>
          </div>
        </div>
      </details>
    </div>

    <div class="dropdown-content" id="indDropdownContent"></div>
    <div class="dropdown-content" id="stockDropdownContent"></div>

    <details open>
      <summary><h2>單產業分類維度圓餅圖 (整體資金規模狀態)</h2></summary>
      <div class="content">
        <div id="industryPieChart" class="pie-chart"></div>
        <div class="table-wrap" id="industryWeightTableWrap"></div>
      </div>
    </details>
    
    <div class="section">
      <h2>產業趨勢：買賣輪動 (張數)</h2>
      <div id="industrySharesChart" class="chart"></div>
    </div>

    <div class="section">
      <h2 style="color: #4f46e5;">1. 個股：持股股數趨勢圖 (連動上方篩選)</h2>
      <div id="sharesChart" class="chart"></div>
    </div>

    <details>
      <summary><h2>產業族群占比趨勢圖 (持股權重)</h2></summary>
      <div class="content">
        <div id="industryWeightChart" class="chart"></div>
      </div>
    </details>

    <details>
      <summary><h2 style="color: #4f46e5;">個股持股占比趨勢圖 (連動上方篩選)</h2></summary>
      <div class="content">
        <div id="weightChart" class="chart"></div>
      </div>
    </details>

    <div class="section">
      <h2 style="color: #4f46e5;">2. 個股：最新日 vs 前一日變化表格 (連動上方篩選)</h2>
      <div class="compare-note" id="compareNoteLots"></div>
      <div class="table-grid-2">
        <div><h3 class="sub-title positive">加碼 (張數)</h3><div class="table-wrap" id="sharesIncreaseTableWrap"></div></div>
        <div><h3 class="sub-title negative">減碼 (張數)</h3><div class="table-wrap" id="sharesDecreaseTableWrap"></div></div>
      </div>
    </div>

    <details>
      <summary><h2 style="color: #4f46e5;">最新日 vs 前一日變化表格（持股占比） (連動上方篩選)</h2></summary>
      <div class="content">
        <div class="compare-note" id="compareNoteWeight"></div>
        <div class="table-grid-2">
          <div><h3 class="sub-title positive">加碼 (百分點)</h3><div class="table-wrap" id="weightIncreaseTableWrap"></div></div>
          <div><h3 class="sub-title negative">減碼 (百分點)</h3><div class="table-wrap" id="weightDecreaseTableWrap"></div></div>
        </div>
      </div>
    </details>

    <div class="section">
      <h2 style="color: #4f46e5;">3. 標的異動分類 (連動上方篩選)</h2>
      <div class="small-note" id="stateCompareNote"></div>
      <div class="state-grid">
        <div class="state-card"><h3 class="sub-title">由買轉賣</h3><div class="table-wrap" id="buyToSellWrap"></div></div>
        <div class="state-card"><h3 class="sub-title">由賣轉買</h3><div class="table-wrap" id="sellToBuyWrap"></div></div>
        <div class="state-card"><h3 class="sub-title">連買標的</h3><div class="table-wrap" id="consecutiveBuyWrap"></div></div>
        <div class="state-card"><h3 class="sub-title">連賣標的</h3><div class="table-wrap" id="consecutiveSellWrap"></div></div>
        <div class="state-card"><h3 class="sub-title">被剔除標的</h3><div class="table-wrap" id="removedWrap"></div></div>
        <div class="state-card"><h3 class="sub-title">被新加入標的</h3><div class="table-wrap" id="newlyAddedWrap"></div></div>
      </div>
    </div>
  </div>

  <script>
    const summary = {json.dumps(summary, ensure_ascii=False)};
    const dataRecords = {json.dumps(data_records, ensure_ascii=False)};
    const deltaRecords = {json.dumps(delta_records, ensure_ascii=False)};
    const stateTables = {json.dumps(state_tables, ensure_ascii=False)};
    const industryRecords = {json.dumps(industry_records, ensure_ascii=False)};

    const indToStocks = {{}};
    dataRecords.forEach(r => {{
        if(!indToStocks[r['產業分類']]) indToStocks[r['產業分類']] = new Set();
        indToStocks[r['產業分類']].add(r['股票鍵']);
    }});

    function escapeHtml(text) {{ return String(text ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;'); }}
    function formatLots(value) {{ return Number(value || 0).toLocaleString('zh-TW', {{ minimumFractionDigits: 0, maximumFractionDigits: 2 }}); }}
    function formatPercent(value) {{ return `${{Number(value || 0).toFixed(2)}}%`; }}
    function formatSigned(value, digits = 2) {{ const n = Number(value || 0); return `${{n > 0 ? '+' : ''}}${{n.toFixed(digits)}}`; }}
    function signedClass(value) {{ const n = Number(value || 0); return n > 0 ? 'positive' : (n < 0 ? 'negative' : ''); }}
    function groupBy(records, key) {{ return records.reduce((acc, row) => {{ const groupKey = row[key]; if (!acc[groupKey]) acc[groupKey] = []; acc[groupKey].push(row); return acc; }}, {{}}); }}
    function getAllStockKeys() {{ return [...new Set(dataRecords.map(r => r['股票鍵']))].sort((a, b) => a.localeCompare(b, 'zh-Hant')); }}
    function getAllIndustries() {{ return [...new Set(dataRecords.map(r => r['產業分類']))].sort((a, b) => a.localeCompare(b, 'zh-Hant')); }}
    function getSelectedStocks() {{ return Array.from(document.querySelectorAll('.stock-cb:checked')).map(cb => cb.value); }}
    function getSelectedIndustries() {{ return Array.from(document.querySelectorAll('.industry-cb:checked')).map(cb => cb.value); }}
    function getEffectiveSelectedStocks() {{ const sel = getSelectedStocks(); return sel.length ? sel : getAllStockKeys(); }}
    function getDateRange() {{ return {{ startDate: document.getElementById('startDate').value || summary.default_start_date, endDate: document.getElementById('endDate').value || summary.default_end_date }}; }}

    function filterRecordsByControls() {{
      const selectedStocks = getEffectiveSelectedStocks(); const {{ startDate, endDate }} = getDateRange();
      return dataRecords.filter(row => selectedStocks.includes(row['股票鍵']) && row['資料日期'] >= startDate && row['資料日期'] <= endDate);
    }}
    function filterIndustryRecords() {{
      const {{ startDate, endDate }} = getDateRange();
      return industryRecords.filter(row => row['資料日期'] >= startDate && row['資料日期'] <= endDate && !row['產業分類'].includes('其他'));
    }}

    function populateSummary() {{
      document.getElementById('earliestDate').textContent = summary.earliest_date;
      document.getElementById('latestDate').textContent = summary.latest_date;
      if (summary.compare_prev_date && summary.compare_latest_date) {{
        document.getElementById('compareNoteLots').textContent = `比較區間：${{summary.compare_prev_date}} → ${{summary.compare_latest_date}}`;
        document.getElementById('compareNoteWeight').textContent = `比較區間：${{summary.compare_prev_date}} → ${{summary.compare_latest_date}}`;
      }}
      if (stateTables.prev_date && stateTables.latest_date) {{
        document.getElementById('stateCompareNote').textContent = `連買 / 連賣 / 由買轉賣 / 由賣轉買，依歷史資料計算連續天數並排序；被剔除 / 新加入則依最近 2 個資料日判斷。`;
      }}
    }}

    function updateDropdownBtnText() {{
        const selInds = getSelectedIndustries(); const indBtn = document.getElementById('indDropdownBtn'); const allIndsLen = getAllIndustries().length;
        if(selInds.length === 0) indBtn.innerText = "全部未選 (視為顯示全部)"; else if(selInds.length === allIndsLen) indBtn.innerText = "已全選所有族群"; else indBtn.innerText = `已選擇 ${{selInds.length}} 個族群`;
        const selStocks = getSelectedStocks(); const stockBtn = document.getElementById('stockDropdownBtn'); const allStocksLen = getAllStockKeys().length;
        if(selStocks.length === 0) stockBtn.innerText = "全部未選 (視為顯示全部)"; else if(selStocks.length === allStocksLen) stockBtn.innerText = "已全選所有股票"; else stockBtn.innerText = `已選擇 ${{selStocks.length}} 檔股票`;
    }}

    function populateCustomDropdowns() {{
      const stockContent = document.getElementById('stockDropdownContent');
      const indContent = document.getElementById('indDropdownContent');
      document.body.appendChild(stockContent); document.body.appendChild(indContent);
      const allStocks = getAllStockKeys(); const defaultSet = new Set(summary.default_stock_keys || []);
      let stockHtml = '';
      allStocks.forEach(s => {{ stockHtml += `<label class="checkbox-label"><input type="checkbox" class="stock-cb" value="${{escapeHtml(s)}}" ${{defaultSet.has(s) ? 'checked' : ''}}>${{escapeHtml(s)}}</label>`; }});
      stockContent.innerHTML = stockHtml;
      let indHtml = '';
      getAllIndustries().forEach(ind => {{ indHtml += `<label class="checkbox-label"><input type="checkbox" class="industry-cb" value="${{escapeHtml(ind)}}">${{escapeHtml(ind)}}</label>`; }});
      indContent.innerHTML = indHtml;

      document.querySelectorAll('.industry-cb').forEach(cb => {{
          cb.addEventListener('change', (e) => {{
              const stocksToToggle = indToStocks[e.target.value];
              if(stocksToToggle) {{ document.querySelectorAll('.stock-cb').forEach(scb => {{ if(stocksToToggle.has(scb.value)) scb.checked = e.target.checked; }}); }}
              updateDropdownBtnText(); renderCharts();
          }});
      }});
      document.querySelectorAll('.stock-cb').forEach(cb => {{ cb.addEventListener('change', () => {{ updateDropdownBtnText(); renderCharts(); }}); }});
      updateDropdownBtnText();
    }}

    function setupDropdownInteractions() {{
        const setups = [ {{ wrap: 'indDropdownWrap', btn: 'indDropdownBtn', content: 'indDropdownContent' }}, {{ wrap: 'stockDropdownWrap', btn: 'stockDropdownBtn', content: 'stockDropdownContent' }} ];
        setups.forEach(s => {{
            const btn = document.getElementById(s.btn); const content = document.getElementById(s.content); const wrap = document.getElementById(s.wrap);
            btn.addEventListener('click', (e) => {{
                e.stopPropagation();
                document.querySelectorAll('.dropdown-content').forEach(c => {{ if (c.id !== s.content) c.classList.remove('show'); }});
                document.querySelectorAll('.custom-dropdown').forEach(w => {{ if (w.id !== s.wrap) w.classList.remove('active'); }});
                if (!content.classList.contains('show')) {{
                    const rect = btn.getBoundingClientRect(); content.style.top = (rect.bottom + 4) + 'px'; content.style.left = rect.left + 'px'; content.style.width = rect.width + 'px';
                    content.classList.add('show'); wrap.classList.add('active');
                }} else {{ content.classList.remove('show'); wrap.classList.remove('active'); }}
            }});
            content.addEventListener('click', (e) => e.stopPropagation());
        }});
        const hideDropdowns = () => {{ document.querySelectorAll('.dropdown-content').forEach(c => c.classList.remove('show')); document.querySelectorAll('.custom-dropdown').forEach(w => w.classList.remove('active')); }};
        document.addEventListener('click', hideDropdowns); window.addEventListener('scroll', hideDropdowns, {{ passive: true }}); window.addEventListener('resize', hideDropdowns, {{ passive: true }});
    }}

    function initDateInputs() {{
      const startInput = document.getElementById('startDate'); const endInput = document.getElementById('endDate');
      startInput.min = summary.earliest_date; startInput.max = summary.latest_date; endInput.min = summary.earliest_date; endInput.max = summary.latest_date;
      startInput.value = summary.default_start_date; endInput.value = summary.default_end_date;
    }}

    function buildTableHtml(records, columns, emptyMessage = '沒有符合條件的資料。') {{
      if (!records || !records.length) return `<div class="muted" style="padding: 10px;">${{escapeHtml(emptyMessage)}}</div>`;
      let html = '<table><thead><tr>' + columns.map(c => `<th>${{escapeHtml(c.label)}}</th>`).join('') + '</tr></thead><tbody>';
      records.forEach(row => {{ html += '<tr>' + columns.map(col => {{ const value = row[col.key]; const formatted = col.formatter ? col.formatter(value, row) : escapeHtml(value); const cls = col.classNameFn ? col.classNameFn(value, row) : ''; return `<td class="${{cls}}">${{formatted}}</td>`; }}).join('') + '</tr>'; }});
      return html + '</tbody></table>';
    }}

    function renderDeltaTables() {{
      if (!deltaRecords.length) return;
      const selectedStocks = getEffectiveSelectedStocks();
      const masterDict = {{}}; dataRecords.forEach(r => masterDict[r['股票代號']] = r['產業分類']);
      let filteredDelta = deltaRecords.map(r => {{ r['產業分類'] = masterDict[r['股票代號']] || '其他/未分類'; return r; }}).filter(r => selectedStocks.includes(r['股票鍵']));
      const sharesIncrease = filteredDelta.filter(r => Number(r['張數變化']) > 0).sort((a, b) => Number(b['張數變化']) - Number(a['張數變化']));
      const sharesDecrease = filteredDelta.filter(r => Number(r['張數變化']) < 0).sort((a, b) => Number(a['張數變化']) - Number(b['張數變化']));
      const weightIncrease = filteredDelta.filter(r => Number(r['權重變化(百分點)']) > 0).sort((a, b) => Number(b['權重變化(百分點)']) - Number(a['權重變化(百分點)']));
      const weightDecrease = filteredDelta.filter(r => Number(r['權重變化(百分點)']) < 0).sort((a, b) => Number(a['權重變化(百分點)']) - Number(b['權重變化(百分點)']));
      const sharesColumns = [ {{ key: '股票代號', label: '股票代號' }}, {{ key: '股票名稱', label: '股票名稱' }}, {{ key: '產業分類', label: '產業分類' }}, {{ key: '最新張數', label: '最新張數', formatter: v => formatLots(v) }}, {{ key: '前一日張數', label: '前一日張數', formatter: v => formatLots(v) }}, {{ key: '張數變化', label: '張數變化', formatter: v => formatSigned(v), classNameFn: v => signedClass(v) }} ];
      const weightColumns = [ {{ key: '股票代號', label: '股票代號' }}, {{ key: '股票名稱', label: '股票名稱' }}, {{ key: '產業分類', label: '產業分類' }}, {{ key: '最新權重(%)', label: '最新權重(%)', formatter: v => formatPercent(v) }}, {{ key: '前一日權重(%)', label: '前一日權重(%)', formatter: v => formatPercent(v) }}, {{ key: '權重變化(百分點)', label: '權重變化(百分點)', formatter: v => formatSigned(v), classNameFn: v => signedClass(v) }} ];
      document.getElementById('sharesIncreaseTableWrap').innerHTML = buildTableHtml(sharesIncrease, sharesColumns);
      document.getElementById('sharesDecreaseTableWrap').innerHTML = buildTableHtml(sharesDecrease, sharesColumns);
      document.getElementById('weightIncreaseTableWrap').innerHTML = buildTableHtml(weightIncrease, weightColumns);
      document.getElementById('weightDecreaseTableWrap').innerHTML = buildTableHtml(weightDecrease, weightColumns);
    }}

    function renderStateTables() {{
      const selectedStocks = getEffectiveSelectedStocks();
      const filterByStock = (rows) => rows.filter(r => selectedStocks.includes(r['股票鍵']));
      const switchColumns = [ {{ key: '股票代號', label: '股票代號' }}, {{ key: '股票名稱', label: '股票名稱' }}, {{ key: '轉換前動作', label: '轉換前動作' }}, {{ key: '轉換前連續天數', label: '轉換前連續天數', formatter: v => `${{v}} 天` }}, {{ key: '目前動作', label: '目前動作' }}, {{ key: '目前連續天數', label: '目前連續天數', formatter: v => `${{v}} 天` }}, {{ key: '最新變化', label: '最新變化', formatter: v => formatSigned(v), classNameFn: v => signedClass(v) }} ];
      const streakColumns = [ {{ key: '股票代號', label: '股票代號' }}, {{ key: '股票名稱', label: '股票名稱' }}, {{ key: '目前連續天數', label: '連續天數', formatter: v => `${{v}} 天` }}, {{ key: '前一日張數', label: '前一日張數', formatter: v => formatLots(v) }}, {{ key: '最新張數', label: '最新張數', formatter: v => formatLots(v) }}, {{ key: '最新變化', label: '最新變化', formatter: v => formatSigned(v), classNameFn: v => signedClass(v) }} ];
      const latestPrevColumns = [ {{ key: '股票代號', label: '股票代號' }}, {{ key: '股票名稱', label: '股票名稱' }}, {{ key: '前一日張數', label: '前一日張數', formatter: v => formatLots(v) }}, {{ key: '最新張數', label: '最新張數', formatter: v => formatLots(v) }}, {{ key: '張數變化', label: '張數變化', formatter: v => formatSigned(v), classNameFn: v => signedClass(v) }} ];
      document.getElementById('buyToSellWrap').innerHTML = buildTableHtml(filterByStock(stateTables.buy_to_sell), switchColumns);
      document.getElementById('sellToBuyWrap').innerHTML = buildTableHtml(filterByStock(stateTables.sell_to_buy), switchColumns);
      document.getElementById('consecutiveBuyWrap').innerHTML = buildTableHtml(filterByStock(stateTables.consecutive_buy), streakColumns);
      document.getElementById('consecutiveSellWrap').innerHTML = buildTableHtml(filterByStock(stateTables.consecutive_sell), streakColumns);
      document.getElementById('removedWrap').innerHTML = buildTableHtml(filterByStock(stateTables.removed), latestPrevColumns);
      document.getElementById('newlyAddedWrap').innerHTML = buildTableHtml(filterByStock(stateTables.newly_added), latestPrevColumns);
    }}

    function makeLineChart(records, valueKey, targetId, title, yAxisTitle, hoverFormat, isPercent = false) {{
      const target = document.getElementById(targetId);
      if (!records.length) {{ target.innerHTML = '<div class="muted" style="padding: 20px; text-align: center;">所選條件下沒有可顯示的資料。</div>'; return; }}
      const grouped = groupBy(records, '股票鍵'); const stockKeys = Object.keys(grouped).sort((a, b) => a.localeCompare(b, 'zh-Hant'));
      const traces = stockKeys.map(stockKey => {{
        const rows = grouped[stockKey].slice().sort((a, b) => a['資料日期'].localeCompare(b['資料日期']));
        const midIdx = Math.floor(rows.length / 2); const textArray = rows.map((r, i) => i === midIdx ? r['股票鍵'] : '');
        return {{ x: rows.map(r => r['資料日期']), y: rows.map(r => Number(r[valueKey])), mode: 'lines+markers+text', text: textArray, textposition: 'top center', name: rows[0]['股票鍵'], line: {{ width: 2 }}, marker: {{ size: 7 }}, customdata: rows.map(r => [r['股票代號'], r['股票名稱'], r['來源檔名'], r['產業分類']]), hovertemplate: '日期: %{{x|%Y-%m-%d}}<br>股票代號: %{{customdata[0]}}<br>股票名稱: %{{customdata[1]}}<br>產業分類: %{{customdata[3]}}<br>' + `${{yAxisTitle}}: %{{y:${{hoverFormat}}}}${{isPercent ? '%' : ''}}<br>來源檔名: %{{customdata[2]}}<extra></extra>` }};
      }});
      const {{ startDate, endDate }} = getDateRange();
      const layout = {{ title: {{ text: title, x: 0.5, xanchor: 'center' }}, template: 'plotly_white', hovermode: 'x unified', height: 620, margin: {{ l: 80, r: 40, t: 60, b: 120 }}, legend: {{ orientation: 'h', x: 0.5, xanchor: 'center', y: -0.15, yanchor: 'top', font: {{ size: 12 }} }}, xaxis: {{ title: '日期', type: 'date', range: [startDate, endDate], tickformat: '%Y-%m-%d', hoverformat: '%Y-%m-%d', rangeslider: {{ visible: false }} }}, yaxis: {{ title: yAxisTitle, tickformat: isPercent ? ',.2f' : ',.0f', automargin: true }} }};
      Plotly.react(targetId, traces, layout, {{ responsive: true, displaylogo: false }});
    }}

    function makeIndustryChart(records, valueKey, targetId, title, yAxisTitle, hoverFormat, isPercent = false) {{
      const target = document.getElementById(targetId);
      if (!records.length) {{ target.innerHTML = '<div class="muted" style="padding: 20px; text-align: center;">無產業資料可顯示。</div>'; return; }}
      const grouped = groupBy(records, '產業分類'); const indKeys = Object.keys(grouped).sort();
      const traces = indKeys.map(indKey => {{
        const rows = grouped[indKey].slice().sort((a, b) => a['資料日期'].localeCompare(b['資料日期']));
        const midIdx = Math.floor(rows.length / 2); const textArray = rows.map((r, i) => i === midIdx ? indKey : '');
        return {{ x: rows.map(r => r['資料日期']), y: rows.map(r => Number(r[valueKey])), mode: 'lines+markers+text', text: textArray, textposition: 'top center', name: indKey, line: {{ width: 2 }}, marker: {{ size: 7 }}, hovertemplate: `日期: %{{x|%Y-%m-%d}}<br>產業分類: ${{indKey}}<br>${{yAxisTitle}}: %{{y:${{hoverFormat}}}}${{isPercent ? '%' : ''}}<extra></extra>` }};
      }});
      const {{ startDate, endDate }} = getDateRange();
      const layout = {{ title: {{ text: title, x: 0.5, xanchor: 'center' }}, template: 'plotly_white', hovermode: 'x unified', height: 620, margin: {{ l: 80, r: 40, t: 60, b: 120 }}, legend: {{ orientation: 'h', x: 0.5, xanchor: 'center', y: -0.15, yanchor: 'top', font: {{ size: 12 }} }}, xaxis: {{ title: '日期', type: 'date', range: [startDate, endDate], tickformat: '%Y-%m-%d', hoverformat: '%Y-%m-%d', rangeslider: {{ visible: false }} }}, yaxis: {{ title: yAxisTitle, tickformat: isPercent ? ',.2f' : ',.0f', automargin: true }} }};
      Plotly.react(targetId, traces, layout, {{ responsive: true, displaylogo: false }});
    }}
    
    function renderLatestIndustryPieAndTable() {{
      const absoluteLatestDate = summary.latest_date;
      let latestInd = industryRecords.filter(r => r['資料日期'] === absoluteLatestDate && !r['產業分類'].includes('其他'));
      if (!latestInd.length) {{ document.getElementById('industryPieChart').innerHTML = '<div class="muted" style="padding: 20px; text-align: center;">無有效產業資料可顯示圓餅圖。</div>'; document.getElementById('industryWeightTableWrap').innerHTML = ''; return; }}
      latestInd.sort((a, b) => Number(b['持股權重']) - Number(a['持股權重']));
      const trace = {{ labels: latestInd.map(r => r['產業分類']), values: latestInd.map(r => Number(r['持股權重'])), type: 'pie', textinfo: 'label+percent', hovertemplate: '<b>%{{label}}</b><br>資金佔比: %{{value:.2f}}%<extra></extra>' }};
      const layout = {{ title: {{ text: `單產業分類維度圓餅圖 - 整體資金規模佔比 (資料日：${{absoluteLatestDate}})`, x: 0.5, xanchor: 'center' }}, margin: {{ t: 80, b: 60, l: 40, r: 40 }}, height: 650, showlegend: true, legend: {{ orientation: 'h', x: 0.5, xanchor: 'center', y: -0.1, yanchor: 'top', font: {{ size: 14 }} }} }};
      Plotly.react('industryPieChart', [trace], layout, {{ responsive: true, displaylogo: false }});
      const columns = [ {{ key: '產業分類', label: '產業分類' }}, {{ key: '持股權重', label: '整體持股權重(%)', formatter: v => formatPercent(v) }}, {{ key: '股數(張)', label: '總張數', formatter: v => formatLots(v) }} ];
      document.getElementById('industryWeightTableWrap').innerHTML = buildTableHtml(latestInd, columns);
    }}

    function renderCharts() {{
      const records = filterRecordsByControls();
      makeLineChart(records, '股數(張)', 'sharesChart', '持股股數趨勢圖', '張數（股數 / 1000）', ',.2f', false);
      makeLineChart(records, '持股權重', 'weightChart', '持股占比趨勢圖', '持股占比', '.2f', true);
      renderDeltaTables(); renderStateTables();
      const indRecords = filterIndustryRecords();
      makeIndustryChart(indRecords, '股數(張)', 'industrySharesChart', '產業族群買賣輪動趨勢圖', '總張數', ',.0f', false);
      makeIndustryChart(indRecords, '持股權重', 'industryWeightChart', '產業族群占比趨勢圖', '總持股占比', '.2f', true);
      renderLatestIndustryPieAndTable();
    }}

    function bindEvents() {{
      document.getElementById('startDate').addEventListener('change', () => {{ const start = document.getElementById('startDate'); const end = document.getElementById('endDate'); if (start.value && end.value && start.value > end.value) end.value = start.value; renderCharts(); }});
      document.getElementById('endDate').addEventListener('change', () => {{ const start = document.getElementById('startDate'); const end = document.getElementById('endDate'); if (start.value && end.value && end.value < start.value) start.value = end.value; renderCharts(); }});
      document.getElementById('selectAllBtn').addEventListener('click', () => {{ document.querySelectorAll('.stock-cb').forEach(opt => opt.checked = true); document.querySelectorAll('.industry-cb').forEach(opt => opt.checked = true); updateDropdownBtnText(); renderCharts(); }});
      document.getElementById('clearSelectionBtn').addEventListener('click', () => {{ document.querySelectorAll('.stock-cb').forEach(opt => opt.checked = false); document.querySelectorAll('.industry-cb').forEach(opt => opt.checked = false); updateDropdownBtnText(); renderCharts(); }});
      document.querySelectorAll('details').forEach(detail => {{ detail.addEventListener('toggle', (e) => {{ if (e.target.open) window.dispatchEvent(new Event('resize')); }}); }});
    }}

    populateSummary(); populateCustomDropdowns(); setupDropdownInteractions(); initDateInputs(); renderCharts(); bindEvents();
  </script>
</body>
</html>'''

# ==========================================================
# 4. Streamlit 渲染畫面 (將產生的 HTML 塞入網頁)
# ==========================================================
def main():
    try:
        # 從資料庫撈取資料
        df = load_all_data_from_db()
        
        # 建立相關表格
        delta_df, compare_latest, compare_prev = build_delta_table(df)
        state_tables = build_state_tables(df)
        default_stock_keys = build_default_top10_keys(df)

        # 產生 HTML 字串
        html_string = build_html(
            df=df,
            delta_df=delta_df,
            output_name="Streamlit_Cloud",
            compare_latest=compare_latest,
            compare_prev=compare_prev,
            state_tables=state_tables,
            default_stock_keys=default_stock_keys,
        )
        
        # 讓 Streamlit 把這段 HTML 畫在網頁上 (設定足夠的高度並開啟捲軸)
        components.html(html_string, height=2000, scrolling=True)

    except Exception as e:
        st.error(f"系統發生錯誤，請檢查資料庫連線或資料格式。詳細錯誤：{e}")

if __name__ == "__main__":
    main()