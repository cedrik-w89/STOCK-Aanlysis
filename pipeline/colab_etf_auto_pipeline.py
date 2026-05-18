# -*- coding: utf-8 -*-
"""
Colab / scheduled-friendly merged ETF data pipeline.

合併內容：
1. ETF 持股更新、股價、法人、產業分類、技術指標更新
2. ETF / 股價 / 三大法人訊號評分寫入 STOCK_DAILY_SIGNAL_RESULT

設計重點：
- 不使用 input()，全部改用 argparse 參數，方便 Colab Enterprise / n8n / GitHub Actions / Cloud Run Job 排程。
- DB 帳密從環境變數讀取，避免寫死在程式碼。
"""

import argparse
import os
import time
from datetime import datetime, timedelta
import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()

APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Taipei")
try:
    os.environ["TZ"] = APP_TIMEZONE
    if hasattr(time, "tzset"):
        time.tzset()
except Exception:
    pass

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def validate_db_env():
    missing = [k for k, v in {
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "DB_NAME": DB_NAME,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            "缺少資料庫環境變數：" + ", ".join(missing) +
            "。請在 Colab Secrets / .env / 執行環境變數中設定。"
        )


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


import os
import sys
import json
import time
import re
from datetime import datetime, timedelta

# 爬蟲與數據處理
import requests
from bs4 import BeautifulSoup
import pandas as pd
try:
    import pandas_ta as ta
except ModuleNotFoundError:
    ta = None
try:
    import yfinance as yf
except ModuleNotFoundError:
    yf = None

# 資料庫相關
try:
    import psycopg2
    from psycopg2.extras import execute_values, Json
except ModuleNotFoundError:
    psycopg2 = None
    execute_values = None
    Json = None
try:
    from sqlalchemy import create_engine, text
except ModuleNotFoundError:
    create_engine = None
    text = None
	

# =========================================================
# 0-1. ETF 抓取設定
# =========================================================
# 群益 Capital Fund API 可自動取得 fund_id，這邊填股票代號即可。
CAPITAL_TARGET_ETFS = ["00982A", "00992A"]

# ezmoney 來源：用 fund_id 設定多檔 ETF。
# 注意：ezmoney 頁面本身沒有日期查詢參數，target_date 只代表寫入 ETF_TRACKING_LIST 的日期。
ENABLE_EZMONEY_ETFS = True
EZMONEY_TARGET_ETFS = [
    {"fund_id": "49YTW", "etf_code": "00981A", "etf_name": "主動統一台股增長"},
    {"fund_id": "63YTW", "etf_code": "00403A", "etf_name": "主動統一升級50"},
]

# 若公司環境可正常驗證憑證，可改為 True。
# 你貼上的新程式有使用 verify=False，因此這裡保留成可設定參數。
EZMONEY_VERIFY_SSL = False

# 友善爬蟲延遲秒數
CRAWLER_DELAY_SECONDS = 1.5

def get_db_connection():
    if psycopg2 is None:
        raise ModuleNotFoundError("尚未安裝 psycopg2，請先執行：pip install psycopg2-binary")
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def get_engine():
    if create_engine is None:
        raise ModuleNotFoundError("尚未安裝 SQLAlchemy，請先執行：pip install SQLAlchemy")
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)

# =========================================================
# 1. ETF 持股更新：整合 00981A ezmoney + 群益 Capital Fund API
# =========================================================
def _normalize_number_series(series):
    '''將含逗號、百分比或空值的欄位轉成數值。'''
    return pd.to_numeric(
        series.astype(str)
              .str.replace(',', '', regex=False)
              .str.replace('%', '', regex=False)
              .str.strip()
              .replace({'': None, 'None': None, 'nan': None, 'NaN': None, '--': None}),
        errors='coerce'
    )


def write_etf_tracking_list(df):
    '''
    共用 ETF_TRACKING_LIST 寫入函式。
    寫入前會先依 DATE + ETF_CODE 刪除舊資料，讓每日排程重跑時不會重複或 PK 衝突。
    '''
    if df is None or df.empty:
        print("⚠️ 沒有 ETF 持股資料需要寫入。")
        return False

    final_columns = ["DATE", "ETF_CODE", "ETF_NAME", "STOCK_CODE", "STOCK_NAME", "SHARES", "WEIGHT"]
    df = df[[c for c in final_columns if c in df.columns]].copy()

    # 統一型別，避免 DB 寫入與後續查詢時格式不一致
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    df["ETF_CODE"] = df["ETF_CODE"].astype(str)
    df["STOCK_CODE"] = df["STOCK_CODE"].astype(str).str.strip()
    df = df[df["STOCK_CODE"].str.isdigit()].copy()

    if "SHARES" in df.columns:
        df["SHARES"] = _normalize_number_series(df["SHARES"]).astype("Int64")
    if "WEIGHT" in df.columns:
        df["WEIGHT"] = _normalize_number_series(df["WEIGHT"]).round(4)

    if df.empty:
        print("⚠️ ETF 持股資料清洗後沒有有效股票代號。")
        return False

    try:
        engine = get_engine()
        unique_tasks = df[["DATE", "ETF_CODE"]].drop_duplicates()

        print("\n⏳ 準備將 ETF 持股資料寫入 PostgreSQL...")
        with engine.begin() as conn:
            for _, row in unique_tasks.iterrows():
                conn.execute(
                    text('DELETE FROM public."ETF_TRACKING_LIST" WHERE "DATE" = :date AND "ETF_CODE" = :etf_code'),
                    {"date": row["DATE"], "etf_code": row["ETF_CODE"]}
                )

            df.to_sql(
                name="ETF_TRACKING_LIST",
                con=conn,
                schema="public",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

        print(f"✅ ETF_TRACKING_LIST 成功寫入 {len(df)} 筆，涵蓋 {len(unique_tasks)} 組日期/ETF。")
        return True

    except Exception as e:
        print(f"❌ ETF_TRACKING_LIST 寫入失敗: {e}")
        print("💡 請確認 PostgreSQL 服務、帳密、資料表名稱與欄位大小寫是否正確。")
        return False


def fetch_ezmoney_etf_holdings(target_date=None, target_etfs=None):
    '''抓取 ezmoney 多檔 ETF 當前持股，回傳標準欄位 DataFrame，不直接寫 DB。

    注意：ezmoney Fund/Info 頁面沒有歷史日期查詢參數。
    target_date 僅用於寫入 ETF_TRACKING_LIST 的 DATE 欄位。
    '''
    target_date = target_date or datetime.now().strftime('%Y-%m-%d')
    target_etfs = target_etfs or EZMONEY_TARGET_ETFS

    if not target_etfs:
        print("⚠️ 未設定 EZMONEY_TARGET_ETFS，已略過 ezmoney ETF 抓取。")
        return pd.DataFrame()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    print(f"\n========== ezmoney ETF 持股抓取開始（設定寫入日期：{target_date}）==========")
    all_data_frames = []

    for etf in target_etfs:
        fund_id = etf.get('fund_id')
        etf_code = etf.get('etf_code')
        etf_name = etf.get('etf_name') or etf_code

        if not fund_id or not etf_code:
            print(f"⚠️ ETF 設定不完整，已略過：{etf}")
            continue

        url = f"https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode={fund_id}"
        print(f"\n>>> 正在抓取 {etf_name} ({etf_code})...", end="")

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=30,
                verify=EZMONEY_VERIFY_SSL
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            target_div = soup.find('div', id='DataAsset')
            if not target_div:
                print(" ❌ 找不到網頁中的資料區塊 DataAsset。")
                continue

            raw_content = target_div.get('data-content')
            if not raw_content:
                print(" ❌ DataAsset 沒有 data-content。")
                continue

            data = json.loads(raw_content)
            holdings = next(
                (item.get('Details') for item in data if item.get('AssetCode') == 'ST' and item.get('Details')),
                []
            )

            if not holdings:
                print(" ❌ 查無股票持股明細資料。")
                continue

            df = pd.DataFrame(holdings).rename(columns={
                'DetailCode': 'STOCK_CODE',
                'DetailName': 'STOCK_NAME',
                'Share': 'SHARES',
                'NavRate': 'WEIGHT'
            })

            df['DATE'] = target_date
            df['ETF_CODE'] = etf_code
            df['ETF_NAME'] = etf_name

            for col in ['STOCK_CODE', 'STOCK_NAME', 'SHARES', 'WEIGHT']:
                if col not in df.columns:
                    df[col] = None

            final_columns = ['DATE', 'ETF_CODE', 'ETF_NAME', 'STOCK_CODE', 'STOCK_NAME', 'SHARES', 'WEIGHT']
            df = df[final_columns].copy()
            all_data_frames.append(df)

            print(f" ✅ 成功取得 {len(df)} 筆。")
            time.sleep(CRAWLER_DELAY_SECONDS)

        except Exception as e:
            print(f" ❌ 發生錯誤: {e}")

    print("\n========== ezmoney ETF 持股抓取結束 ==========")

    if not all_data_frames:
        return pd.DataFrame()
    return pd.concat(all_data_frames, ignore_index=True)

def get_auto_etf_mapping():
    '''自動取得群益 ETF 股票代號與 fund_id 對應表。'''
    url = "https://www.capitalfund.com.tw/CFWeb/api/etf/items"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8"
    }
    try:
        response = requests.post(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        auto_mapping = {}
        for item in data.get("data", []):
            stock_code = item.get("stockNo") or item.get("code")
            fund_id = str(item.get("fundNo") or item.get("id"))
            fund_name = item.get("shortName") or item.get("name")
            if stock_code and fund_id:
                auto_mapping[str(stock_code)] = {"fund_id": fund_id, "name": fund_name}
        return auto_mapping

    except Exception as e:
        print(f"❌ 取得群益 ETF 清單失敗: {e}")
        return {}


def fetch_capital_daily_etf_data(etf_code, etf_info, target_date):
    '''抓取群益單日單檔 ETF 持股明細。'''
    api_url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Content-Type": "application/json;charset=UTF-8"
    }
    payload = {"fundId": etf_info["fund_id"], "date": target_date.replace("-", "/")}

    data = None
    for attempt in range(3):
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            break
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                time.sleep(2)
            else:
                print(f"  [錯誤] 取得 {etf_code} {target_date} 失敗: {e}")
                return pd.DataFrame()

    records = []
    search_target = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(search_target, list):
        records = search_target
    elif isinstance(search_target, dict):
        for _, value in search_target.items():
            if isinstance(value, list) and len(value) > 0:
                records = value
                break

    if not records:
        print(f"  [跳過] {etf_code} 於 {target_date} 無資料（可能為非交易日或尚未公告）。")
        return pd.DataFrame()

    df = pd.DataFrame(records).rename(columns={
        "stocNo": "STOCK_CODE",
        "stocName": "STOCK_NAME",
        "share": "SHARES",
        "weight": "WEIGHT"
    })
    df["DATE"] = pd.to_datetime(target_date).date()
    df["ETF_CODE"] = etf_code
    df["ETF_NAME"] = etf_info.get("name") or etf_code

    for col in ["STOCK_CODE", "STOCK_NAME", "SHARES", "WEIGHT"]:
        if col not in df.columns:
            df[col] = None

    final_columns = ["DATE", "ETF_CODE", "ETF_NAME", "STOCK_CODE", "STOCK_NAME", "SHARES", "WEIGHT"]
    df = df[final_columns].copy()

    print(f"  [成功] 取得 {etf_code} 於 {target_date}，共 {len(df)} 筆明細。")
    return df


def fetch_capital_etf_holdings(start_date, end_date, target_etfs=None):
    '''抓取群益 ETF 區間持股，回傳 DataFrame，不直接寫 DB。'''
    target_etfs = target_etfs or CAPITAL_TARGET_ETFS
    date_range = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%Y-%m-%d').tolist()

    print(f"\n📅 群益 ETF 查詢區間: {start_date} ~ {end_date}（共 {len(date_range)} 天）")
    etf_mapping = get_auto_etf_mapping()
    if not etf_mapping:
        return pd.DataFrame()

    all_data_frames = []
    for target_date in date_range:
        for etf_code in target_etfs:
            if etf_code not in etf_mapping:
                print(f"  [跳過] 群益 API 清單中找不到 ETF_CODE={etf_code}。")
                continue

            df_part = fetch_capital_daily_etf_data(etf_code, etf_mapping[etf_code], target_date)
            if not df_part.empty:
                all_data_frames.append(df_part)
            time.sleep(CRAWLER_DELAY_SECONDS)

    if not all_data_frames:
        return pd.DataFrame()
    return pd.concat(all_data_frames, ignore_index=True)


def update_etf_holdings(start_date=None, end_date=None, include_ezmoney=True, include_capital=True):
    '''整合後的 ETF 持股更新入口。
    - include_ezmoney=True：抓取 EZMONEY_TARGET_ETFS，例如 00981A、00403A。
    - include_capital=True：抓取群益 API，支援 CAPITAL_TARGET_ETFS 與日期區間。

    注意：ezmoney 來源目前是頁面當前持股，沒有日期查詢參數。
    因此在區間更新時，僅當區間包含今天才抓一次並寫入今天；避免把今日持股錯寫到歷史日期。
    若你確定要指定寫入日期，可直接呼叫 fetch_ezmoney_etf_holdings(target_date) 後再 write_etf_tracking_list()。
    '''
    today = datetime.now().strftime('%Y-%m-%d')
    start_date = start_date or today
    end_date = end_date or start_date

    print("\n" + "=" * 50)
    print(f"🚀 開始更新 ETF 持股清單：{start_date} ~ {end_date}")
    print("=" * 50)

    all_data_frames = []

    if include_ezmoney and ENABLE_EZMONEY_ETFS:
        # ezmoney 頁面沒有歷史日期參數；區間含今天時，僅寫入今天，避免把今日成分股灌到整段歷史日期。
        if start_date <= today <= end_date:
            df_ezmoney = fetch_ezmoney_etf_holdings(target_date=today, target_etfs=EZMONEY_TARGET_ETFS)
            if not df_ezmoney.empty:
                all_data_frames.append(df_ezmoney)
        else:
            print("⚠️ ezmoney 來源目前只支援當前頁面持股；本次區間不含今天，已略過 ezmoney ETF。")

    if include_capital and CAPITAL_TARGET_ETFS:
        df_capital = fetch_capital_etf_holdings(start_date, end_date, CAPITAL_TARGET_ETFS)
        if not df_capital.empty:
            all_data_frames.append(df_capital)

    if not all_data_frames:
        print("⚠️ 本次沒有取得任何 ETF 持股資料。")
        return False

    df_master = pd.concat(all_data_frames, ignore_index=True)
    return write_etf_tracking_list(df_master)

# =========================================================
# 2. 獲取追蹤清單與清除舊資料
# =========================================================
def get_target_stocks_and_clean_db(start_date, end_date, use_all_tracking_codes=True):
    '''
    取得後續股票/法人/技術指標流程要處理的股票清單。

    use_all_tracking_codes=True：
        使用 ETF_TRACKING_LIST 目前所有出現過的 STOCK_CODE 作為股票池，
        並將同一份股票池套用到 start_date ~ end_date 每一天。
        這樣新加入 ETF 的股票也會補齊過去股價、法人與技術指標，避免報表歷史資料斷層。

    use_all_tracking_codes=False：
        保留舊邏輯，只處理指定區間內每天實際 ETF 持股。
    '''
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        daily_holdings = {}
        all_codes = set()

        if use_all_tracking_codes:
            # 新邏輯：只要 ETF_TRACKING_LIST 曾經出現過，就納入股價/法人/指標補齊股票池
            cur.execute('''
                SELECT DISTINCT "STOCK_CODE"
                FROM "ETF_TRACKING_LIST"
                WHERE "STOCK_CODE" IS NOT NULL
            ''')
            all_codes = {str(row[0]).strip() for row in cur.fetchall() if str(row[0]).strip().isdigit()}

            if not all_codes:
                print('⚠️ ETF_TRACKING_LIST 目前沒有任何可用的 STOCK_CODE。')
                return [], {}

            # 將完整股票池套用到區間每一天；後續股價、法人、指標都會補齊每一檔股票的歷史資料
            date_range = pd.date_range(start=start_date, end=end_date)
            for d in date_range:
                daily_holdings[d.strftime('%Y-%m-%d')] = set(all_codes)

            print(f"📌 股票池模式：使用 ETF_TRACKING_LIST 全部歷史 STOCK_CODE，共 {len(all_codes)} 檔。")

        else:
            # 舊邏輯：只取得指定區間內每天實際 ETF 持股
            cur.execute('SELECT "DATE", "STOCK_CODE" FROM "ETF_TRACKING_LIST" WHERE "DATE" BETWEEN %s AND %s', (start_date, end_date))
            rows = cur.fetchall()

            for r in rows:
                date_str = r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else str(r[0])
                code = str(r[1]).strip()
                if code.isdigit():
                    daily_holdings.setdefault(date_str, set()).add(code)
                    all_codes.add(code)

            if not daily_holdings:
                print(f"⚠️ 資料庫中找不到 {start_date} 至 {end_date} 的 ETF 持股紀錄。")
                return [], {}

        if all_codes:
            codes_list = list(all_codes)
            print(f"🧹 開始清除 {start_date} 至 {end_date} 的舊資料，準備重新補齊股票/法人/指標資料...")
            cur.execute('DELETE FROM "DAILY_STOCK_PRICE" WHERE "date" BETWEEN %s AND %s AND "stock_code" = ANY(%s)', (start_date, end_date, codes_list))
            cur.execute('DELETE FROM stock_institutional_investors WHERE trade_date BETWEEN %s AND %s AND stock_id = ANY(%s)', (start_date, end_date, codes_list))
            cur.execute('DELETE FROM "STOCK_TECH_INDICATORS" WHERE date BETWEEN %s AND %s AND stock_code = ANY(%s)', (start_date, end_date, codes_list))
            conn.commit()
            print("✅ 舊資料清除完畢！")

        return sorted(all_codes), daily_holdings

    except Exception as e:
        conn.rollback()
        print(f"❌ 獲取名單失敗: {e}")
        return [], {}
    finally:
        cur.close()
        conn.close()

# =========================================================
# 3. 更新股價（依 ETF_TRACKING_LIST 股票池補齊歷史資料）
# =========================================================
def backfill_ohlcv(start_date, end_date, codes, daily_holdings):
    if yf is None:
        print("❌ 尚未安裝 yfinance，已跳過股價抓取。請先執行：pip install yfinance")
        return
    fetch_start = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=100)).strftime('%Y-%m-%d')
    yf_end = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"\n🚀 [1/4] 開始獲取股價（自動涵蓋暖身期: {fetch_start} 至 {end_date}）...")
    tickers = [f"{c}.TW" for c in codes] + [f"{c}.TWO" for c in codes]
    
    with open(os.devnull, 'w') as devnull:
        sys.stdout = devnull; sys.stderr = devnull
        try:
            data = yf.download(tickers, start=fetch_start, end=yf_end, progress=False, group_by='ticker')
        except Exception:
            data = pd.DataFrame()
        finally:
            sys.stdout = sys.__stdout__; sys.stderr = sys.__stderr__

    records = []
    is_multi = isinstance(data.columns, pd.MultiIndex)

    for ticker in tickers:
        try:
            code = ticker.split('.')[0]
            s = data[ticker].dropna(subset=['Close']) if is_multi and ticker in data.columns.get_level_values(0) else data.dropna(subset=['Close']) if not is_multi and 'Close' in data.columns else pd.DataFrame()

            for timestamp, row in s.iterrows():
                if row['Close'] > 0:
                    date_str = timestamp.strftime('%Y-%m-%d')
                    
                    # 🎯 補齊邏輯判斷
                    keep = False
                    if date_str < start_date:
                        keep = True # 暖身期必須保留，否則技術指標無法計算
                    elif date_str <= end_date and date_str in daily_holdings and code in daily_holdings[date_str]:
                        keep = True # 股票池模式下，會補齊 ETF_TRACKING_LIST 全部 STOCK_CODE 的區間資料
                        
                    if keep:
                        records.append((
                            date_str, code,
                            round(float(row['Close']), 2), round(float(row['Open']), 2),
                            round(float(row['High']), 2), round(float(row['Low']), 2),
                            int(row['Volume'])
                        ))
        except Exception:
            continue

    if records:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            query = """
                INSERT INTO "DAILY_STOCK_PRICE" ("date", stock_code, close_price, open_price, high_price, low_price, volume)
                VALUES %s ON CONFLICT ("date", stock_code) DO UPDATE SET
                close_price=EXCLUDED.close_price, open_price=EXCLUDED.open_price, high_price=EXCLUDED.high_price, low_price=EXCLUDED.low_price, volume=EXCLUDED.volume;
            """
            execute_values(cur, query, records)
            conn.commit()
            print(f"✅ 成功寫入 {len(records)} 筆嚴格對齊後的股價資料。")
        except Exception as e:
            print(f"❌ 股價寫入失敗: {e}")
        finally:
            cur.close()
            conn.close()

# =========================================================
# 4. 更新三大法人買賣超（依 ETF_TRACKING_LIST 股票池補齊歷史資料）
# =========================================================
def backfill_institutional(start_date, end_date, daily_holdings):
    print(f"\n🚀 [2/4] 開始逐日獲取三大法人數據（依 ETF_TRACKING_LIST 股票池過濾）...")
    date_range = pd.date_range(start=start_date, end=end_date)
    
    for current_date in date_range:
        if current_date.weekday() >= 5: continue
        
        d_db = current_date.strftime("%Y-%m-%d")
        valid_codes_for_today = daily_holdings.get(d_db, set())
        
        if not valid_codes_for_today:
            print(f"⌛ [{d_db}] 今日無 ETF 持股名單，跳過抓取。")
            continue
            
        d_twse = current_date.strftime("%Y%m%d")
        url_twse = f"https://www.twse.com.tw/fund/T86?response=json&date={d_twse}&selectType=ALL"
        d_tpex = f"{current_date.year - 1911}/{current_date.strftime('%m/%d')}"
        url_tpex = f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=json&se=EW&t=D&d={d_tpex}"
        
        records = []
        
        # --- 1. 上市 (過濾出今天有在名單的) ---
        try:
            resp_twse = requests.get(url_twse, timeout=10)
            data_twse = resp_twse.json()
            if data_twse.get('stat') == 'OK':
                df_twse = pd.DataFrame(data_twse['data'], columns=data_twse['fields'])
                # 🎯 嚴格過濾
                df_twse = df_twse[df_twse['證券代號'].isin(valid_codes_for_today)]
                
                def get_twse_val(row, col_name):
                    if col_name in row:
                        val = str(row[col_name]).replace(',', '').strip()
                        if val and val not in ('--', 'None', 'nan', 'NaN'):
                            try: return int(float(val))
                            except ValueError: return 0
                    return 0

                for _, row in df_twse.iterrows():
                    records.append((
                        d_db, row['證券代號'], row.get('證券名稱', ''),
                        get_twse_val(row, '外陸資買進股數(不含外資自營商)'), get_twse_val(row, '外陸資賣出股數(不含外資自營商)'), get_twse_val(row, '外陸資買賣超股數(不含外資自營商)'),
                        get_twse_val(row, '投信買進股數'), get_twse_val(row, '投信賣出股數'), get_twse_val(row, '投信買賣超股數'),
                        get_twse_val(row, '自營商買進股數(自行買賣)'), get_twse_val(row, '自營商賣出股數(自行買賣)'), get_twse_val(row, '自營商買賣超股數'),
                        get_twse_val(row, '自營商買賣超股數(自行買賣)'), get_twse_val(row, '自營商買賣超股數(避險)'), get_twse_val(row, '三大法人買賣超股數')
                    ))
        except Exception: pass

        # --- 2. 上櫃 (過濾出今天有在名單的) ---
        try:
            resp_tpex = requests.get(url_tpex, timeout=10)
            data_tpex = resp_tpex.json()
            if data_tpex.get('aaData'):
                def get_tpex_val(val):
                    try: return int(float(str(val).replace(',', '').strip()))
                    except: return 0
                
                for row in data_tpex['aaData']:
                    code = row[0]
                    # 🎯 嚴格過濾
                    if code in valid_codes_for_today:
                        records.append((
                            d_db, code, row[1],
                            get_tpex_val(row[2]), get_tpex_val(row[3]), get_tpex_val(row[4]),
                            get_tpex_val(row[11]), get_tpex_val(row[12]), get_tpex_val(row[13]),
                            get_tpex_val(row[14]) + get_tpex_val(row[17]), get_tpex_val(row[15]) + get_tpex_val(row[18]), get_tpex_val(row[16]) + get_tpex_val(row[19]),
                            get_tpex_val(row[16]), get_tpex_val(row[19]), 
                            get_tpex_val(row[23]) if len(row) > 23 else (get_tpex_val(row[4]) + get_tpex_val(row[13]) + get_tpex_val(row[16]) + get_tpex_val(row[19]))
                        ))
        except Exception: pass

        if records:
            conn = get_db_connection()
            cur = conn.cursor()
            query = """INSERT INTO stock_institutional_investors 
                       (trade_date, stock_id, stock_name, foreign_buy_qty, foreign_sell_qty, foreign_net_qty, itrust_buy_qty, itrust_sell_qty, itrust_net_qty, dealer_buy_qty, dealer_sell_qty, dealer_net_qty, dealer_prop_net_qty, dealer_hedge_net_qty, total_net_qty)
                       VALUES %s ON CONFLICT (trade_date, stock_id) DO UPDATE SET
                       stock_name=EXCLUDED.stock_name, foreign_buy_qty=EXCLUDED.foreign_buy_qty, foreign_sell_qty=EXCLUDED.foreign_sell_qty, foreign_net_qty=EXCLUDED.foreign_net_qty,
                       itrust_buy_qty=EXCLUDED.itrust_buy_qty, itrust_sell_qty=EXCLUDED.itrust_sell_qty, itrust_net_qty=EXCLUDED.itrust_net_qty,
                       dealer_buy_qty=EXCLUDED.dealer_buy_qty, dealer_sell_qty=EXCLUDED.dealer_sell_qty, dealer_net_qty=EXCLUDED.dealer_net_qty,
                       dealer_prop_net_qty=EXCLUDED.dealer_prop_net_qty, dealer_hedge_net_qty=EXCLUDED.dealer_hedge_net_qty, total_net_qty=EXCLUDED.total_net_qty;"""
            execute_values(cur, query, records)
            conn.commit(); cur.close(); conn.close()
            print(f"✅ [{d_db}] 寫入 {len(records)} 檔法人資料。")
        time.sleep(4)

# =========================================================
# 5. 更新產業分類
# =========================================================
def update_industry_mapping(codes):
    print(f"\n🚀 [3/4] 開始檢查與更新中文產業分類...")
    CUSTOM_MAP = {
        "3017": "散熱模組", "3324": "散熱模組", "6510": "測試介面", "6669": "伺服器", 
        "3583": "半導體設備", "3661": "矽智財 (IP)", "5347": "晶圓代工", "2330": "晶圓代工",
        "1519": "重電", "1514": "重電", "3037": "PCB 載板", "8046": "PCB 載板", "3189": "PCB 載板", "3665": "電子零組件"
    }

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT "STOCK_CODE" FROM "STOCK_IND_MAPPING"')
        existing_codes = set([str(row[0]) for row in cur.fetchall()])
        missing_codes = [c for c in codes if c not in existing_codes]
        
        if not missing_codes:
            print("✅ 所有股票都已有產業分類，無需更新。")
            return
            
        print(f"🔍 發現 {len(missing_codes)} 檔股票缺乏分類，開始建立...")
        records = []
        
        for code in missing_codes:
            if code in CUSTOM_MAP:
                industry = CUSTOM_MAP[code]
            else:
                try:
                    headers = {'User-Agent': 'Mozilla/5.0'}
                    res = requests.get(f"https://tw.stock.yahoo.com/quote/{code}", headers=headers, timeout=5)
                    match = re.search(r'href="/class-quote\?sectorId=[^>]+>([^<]+)</a>', res.text)
                    industry = match.group(1) if match else "未分類"
                except:
                    industry = "未分類"
            
            records.append((code, industry))
            print(f"   - {code} 標記為: {industry}")
            time.sleep(0.5)

        if records:
            query = """
                INSERT INTO "STOCK_IND_MAPPING" ("STOCK_CODE", "INDUSTRY_NAME")
                VALUES %s
                ON CONFLICT ("STOCK_CODE") DO UPDATE SET
                "INDUSTRY_NAME" = EXCLUDED."INDUSTRY_NAME";
            """
            execute_values(cur, query, records)
            conn.commit()
            print(f"✅ 成功寫入 {len(records)} 筆產業分類資料。")
            
    except Exception as e:
        print(f"❌ 產業分類更新失敗: {e}")
    finally:
        cur.close()
        conn.close()

# =========================================================
# 6. 計算與更新技術指標
# =========================================================
def update_technical_indicators(start_date, end_date, codes, daily_holdings):
    print(f"\n🚀 [4/4] 開始計算並寫入技術指標 (嚴格過濾呈現)...")
    if ta is None:
        print("❌ 尚未安裝 pandas_ta，已跳過技術指標計算。請先執行：pip install pandas-ta")
        return
    buffer_start = (datetime.strptime(start_date, '%Y-%m-%d') - timedelta(days=100)).strftime('%Y-%m-%d')
    
    try:
        query = f"SELECT date, stock_code, open_price, high_price, low_price, close_price, volume FROM \"DAILY_STOCK_PRICE\" WHERE date >= '{buffer_start}' AND date <= '{end_date}' ORDER BY date ASC"
        engine = get_engine()
        df_all = pd.read_sql(query, con=engine) 
        if df_all.empty: return

        records = []
        ta_kwargs = {"open": "open_price", "high": "high_price", "low": "low_price", "close": "close_price", "volume": "volume"}
        
        for code in codes:
            df = df_all[df_all['stock_code'] == code].copy()
            if len(df) < 20: continue
            try:
                df.ta.sma(length=5, append=True, close="close_price"); df.ta.sma(length=10, append=True, close="close_price")
                df.ta.sma(length=20, append=True, close="close_price"); df.ta.sma(length=60, append=True, close="close_price")
                df.ta.macd(append=True, close="close_price"); df.ta.stoch(append=True, **ta_kwargs)
                df.ta.rsi(length=14, append=True, close="close_price"); df.ta.bbands(length=20, std=2, append=True, close="close_price")
                df.ta.atr(length=14, append=True, **ta_kwargs)
            except Exception: continue

            rename_map = {'SMA_5': 'ma_5', 'SMA_10': 'ma_10', 'SMA_20': 'ma_20', 'SMA_60': 'ma_60', 'MACD_12_26_9': 'macd_dif', 'MACDs_12_26_9': 'macd_signal', 'MACDh_12_26_9': 'macd_hist', 'STOCHk_14_3_3': 'kd_k', 'STOCHd_14_3_3': 'kd_d', 'RSI_14': 'rsi_14', 'BBL_20_2.0': 'bb_lower', 'BBM_20_2.0': 'bb_middle', 'BBU_20_2.0': 'bb_upper', 'ATRr_14': 'atr_14'}
            df.rename(columns=rename_map, inplace=True)
            df = df[(df['date'].astype(str) >= start_date) & (df['date'].astype(str) <= end_date)]
            df = df.where(pd.notnull(df), None)

            for _, row in df.iterrows():
                date_str = str(row['date'])
                # 🎯 股票池補齊：這一天屬於補齊區間，且股票在 ETF_TRACKING_LIST 股票池內才寫入 DB
                if date_str in daily_holdings and row['stock_code'] in daily_holdings[date_str]:
                    if row.get('ma_5') is not None or row.get('macd_dif') is not None:
                        records.append((
                            date_str, row['stock_code'], row.get('ma_5'), row.get('ma_10'), row.get('ma_20'), row.get('ma_60'),
                            row.get('macd_dif'), row.get('macd_signal'), row.get('macd_hist'), row.get('kd_k'), row.get('kd_d'), row.get('rsi_14'),
                            row.get('bb_upper'), row.get('bb_middle'), row.get('bb_lower'), row.get('atr_14')
                        ))

        if records:
            conn = get_db_connection()
            cur = conn.cursor()
            insert_query = """INSERT INTO "STOCK_TECH_INDICATORS" (date, stock_code, ma_5, ma_10, ma_20, ma_60, macd_dif, macd_signal, macd_hist, kd_k, kd_d, rsi_14, bb_upper, bb_middle, bb_lower, atr_14)
                              VALUES %s ON CONFLICT (date, stock_code) DO UPDATE SET
                              ma_5=EXCLUDED.ma_5, ma_10=EXCLUDED.ma_10, ma_20=EXCLUDED.ma_20, ma_60=EXCLUDED.ma_60, macd_dif=EXCLUDED.macd_dif, macd_signal=EXCLUDED.macd_signal, macd_hist=EXCLUDED.macd_hist, kd_k=EXCLUDED.kd_k, kd_d=EXCLUDED.kd_d, rsi_14=EXCLUDED.rsi_14, bb_upper=EXCLUDED.bb_upper, bb_middle=EXCLUDED.bb_middle, bb_lower=EXCLUDED.bb_lower, atr_14=EXCLUDED.atr_14;"""
            execute_values(cur, insert_query, records)
            conn.commit(); cur.close(); conn.close()
            print(f"✅ 成功計算並寫入 {len(records)} 筆嚴格對齊的指標資料！")
    except Exception as e: print(f"❌ 指標計算失敗: {e}")

# =========================================================
# 執行股票分析流程包裝
# =========================================================
def run_stock_data_pipeline(start_date, end_date, use_all_tracking_codes=True):
    print(f"\n▶️ 鎖定執行區間：{start_date} 至 {end_date}")

    # 預設使用 ETF_TRACKING_LIST 所有 STOCK_CODE 補齊歷史資料，避免新增個股缺少過去股價與技術指標
    target_codes, daily_holdings = get_target_stocks_and_clean_db(
        start_date,
        end_date,
        use_all_tracking_codes=use_all_tracking_codes
    )

    if not target_codes:
        print("❌ 無法取得追蹤股票代號，流程中斷。")
    else:
        if use_all_tracking_codes:
            print(f"📊 本次以 ETF_TRACKING_LIST 全部歷史股票池執行，共涵蓋 {len(target_codes)} 檔股票。")
        else:
            print(f"📊 本次區間共涵蓋 {len(target_codes)} 檔股票，將套用區間內 ETF 持股逐日過濾。")

        backfill_ohlcv(start_date, end_date, target_codes, daily_holdings)
        backfill_institutional(start_date, end_date, daily_holdings)
        update_industry_mapping(target_codes)
        update_technical_indicators(start_date, end_date, target_codes, daily_holdings)
        print("\n✨ 所有指定區間的股票數據已順利更新完畢！")


# =========================================================
# 7. 每日訊號評分邏輯（附件合併）
# =========================================================
RULE_VERSION = "v1.0"
SIGNIFICANT_SHARE_CHANGE_PCT = 0.05


CATEGORY_META = {
    "STRONG_BUY_WATCH": ("強買入關注", 1),
    "PULLBACK_BUY_WATCH": ("回檔承接關注", 2),
    "NEW_ENTRY_WATCH": ("新納入關注", 3),
    "BUY_OBSERVE_NO_CHASE": ("買入觀察，不追價", 4),
    "CHIP_STRENGTH_OBSERVE": ("籌碼轉強觀察", 5),
    "HIGH_LEVEL_DISTRIBUTION_WATCH": ("高檔調節關注", 6),
    "SELL_WATCH": ("賣出關注", 7),
    "NEUTRAL": ("不顯示", 99),
}


@dataclass
class ScoreBucket:
    etf_breadth: float = 0
    etf_action: float = 0
    etf_weight: float = 0
    price_divergence: float = 0
    institutional: float = 0
    same_direction: float = 0
    risk: float = 0

    def total(self) -> float:
        return round(
            self.etf_breadth
            + self.etf_action
            + self.etf_weight
            + self.price_divergence
            + self.institutional
            + self.same_direction
            + self.risk,
            2,
        )



def to_date_str(value: Any) -> str:
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def add_detail(details: List[Dict[str, Any]], group: str, rule: str, score: float):
    if score == 0:
        return
    details.append({"group": group, "rule": rule, "score": round(float(score), 2)})


def load_source_data(start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    """讀取計算所需資料。為了計算 streak 與前一日比較，會多抓一段 buffer。"""
    buffer_start = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=120)).strftime("%Y-%m-%d")

    with get_db_connection() as conn:
        etf_sql = '''
            SELECT
                "DATE"::date AS date,
                "ETF_CODE"::text AS etf_code,
                COALESCE("ETF_NAME", '')::text AS etf_name,
                "STOCK_CODE"::text AS stock_code,
                COALESCE("STOCK_NAME", "STOCK_CODE"::text)::text AS stock_name,
                COALESCE("SHARES", 0)::numeric AS shares,
                COALESCE("WEIGHT", 0)::numeric AS weight
            FROM public."ETF_TRACKING_LIST"
            WHERE "DATE"::date <= %(end_date)s::date
              AND "DATE"::date >= %(buffer_start)s::date
        '''
        df_etf = pd.read_sql(etf_sql, conn, params={"end_date": end_date, "buffer_start": buffer_start})

        price_sql = '''
            SELECT
                "date"::date AS date,
                stock_code::text AS stock_code,
                close_price::numeric AS close_price,
                open_price::numeric AS open_price,
                high_price::numeric AS high_price,
                low_price::numeric AS low_price,
                volume::numeric AS volume
            FROM public."DAILY_STOCK_PRICE"
            WHERE "date"::date <= %(end_date)s::date
              AND "date"::date >= %(buffer_start)s::date
        '''
        df_price = pd.read_sql(price_sql, conn, params={"end_date": end_date, "buffer_start": buffer_start})

        inst_sql = '''
            SELECT
                trade_date::date AS date,
                stock_id::text AS stock_code,
                COALESCE(stock_name, '')::text AS stock_name,
                COALESCE(foreign_net_qty, 0)::bigint AS foreign_net_qty,
                COALESCE(itrust_net_qty, 0)::bigint AS itrust_net_qty,
                COALESCE(dealer_net_qty, 0)::bigint AS dealer_net_qty,
                COALESCE(total_net_qty, 0)::bigint AS total_net_qty
            FROM public.stock_institutional_investors
            WHERE trade_date::date <= %(end_date)s::date
              AND trade_date::date >= %(buffer_start)s::date
        '''
        df_inst = pd.read_sql(inst_sql, conn, params={"end_date": end_date, "buffer_start": buffer_start})

        tech_sql = '''
            SELECT
                date::date AS date,
                stock_code::text AS stock_code,
                ma_5::numeric AS ma_5,
                ma_10::numeric AS ma_10,
                ma_20::numeric AS ma_20,
                ma_60::numeric AS ma_60,
                macd_dif::numeric AS macd_dif,
                macd_signal::numeric AS macd_signal,
                rsi_14::numeric AS rsi_14
            FROM public."STOCK_TECH_INDICATORS"
            WHERE date::date <= %(end_date)s::date
              AND date::date >= %(buffer_start)s::date
        '''
        df_tech = pd.read_sql(tech_sql, conn, params={"end_date": end_date, "buffer_start": buffer_start})

        try:
            ind_sql = '''
                SELECT "STOCK_CODE"::text AS stock_code, "INDUSTRY_NAME"::text AS industry_name
                FROM public."STOCK_IND_MAPPING"
            '''
            df_ind = pd.read_sql(ind_sql, conn)
        except Exception:
            df_ind = pd.DataFrame(columns=["stock_code", "industry_name"])

    for df in [df_etf, df_price, df_inst, df_tech]:
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["stock_code"] = df["stock_code"].astype(str).str.strip()

    if not df_etf.empty:
        df_etf["etf_code"] = df_etf["etf_code"].astype(str).str.strip()
        df_etf["shares"] = pd.to_numeric(df_etf["shares"], errors="coerce").fillna(0)
        df_etf["weight"] = pd.to_numeric(df_etf["weight"], errors="coerce").fillna(0)

    if not df_ind.empty:
        df_ind["stock_code"] = df_ind["stock_code"].astype(str).str.strip()

    return {
        "etf": df_etf,
        "price": df_price,
        "inst": df_inst,
        "tech": df_tech,
        "industry": df_ind,
    }


def get_price_snapshot(price_df: pd.DataFrame, stock_code: str, target_date: pd.Timestamp) -> Dict[str, Any]:
    rows = price_df[(price_df["stock_code"] == stock_code) & (pd.to_datetime(price_df["date"]) <= target_date)].sort_values("date")
    if rows.empty:
        return {
            "close_price": None,
            "price_return_1d": None,
            "price_return_3d": None,
            "price_return_5d": None,
        }

    def calc_return(days_back: int) -> Optional[float]:
        if len(rows) <= days_back:
            return None
        current = safe_float(rows.iloc[-1]["close_price"], 0)
        previous = safe_float(rows.iloc[-1 - days_back]["close_price"], 0)
        if previous <= 0:
            return None
        return round((current / previous - 1) * 100, 4)

    return {
        "close_price": safe_float(rows.iloc[-1]["close_price"], None),
        "price_return_1d": calc_return(1),
        "price_return_3d": calc_return(3),
        "price_return_5d": calc_return(5),
    }


def get_tech_snapshot(tech_df: pd.DataFrame, stock_code: str, target_date: pd.Timestamp, close_price: Optional[float]) -> Dict[str, bool]:
    rows = tech_df[(tech_df["stock_code"] == stock_code) & (pd.to_datetime(tech_df["date"]) <= target_date)].sort_values("date")
    if rows.empty or close_price is None:
        return {"is_break_ma20": False, "is_above_ma20": False}
    ma20 = safe_float(rows.iloc[-1].get("ma_20"), 0)
    return {
        "is_break_ma20": bool(ma20 > 0 and close_price < ma20),
        "is_above_ma20": bool(ma20 > 0 and close_price >= ma20),
    }


def calc_streak(inst_rows: pd.DataFrame, column: str) -> Tuple[str, int]:
    if inst_rows.empty or column not in inst_rows.columns:
        return "flat", 0
    values = inst_rows.sort_values("date")[column].fillna(0).tolist()
    if not values:
        return "flat", 0
    last = safe_float(values[-1], 0)
    if last > 0:
        direction = "buy"
    elif last < 0:
        direction = "sell"
    else:
        return "flat", 0

    days = 0
    for value in reversed(values):
        v = safe_float(value, 0)
        if direction == "buy" and v > 0:
            days += 1
        elif direction == "sell" and v < 0:
            days += 1
        else:
            break
    return direction, days


def get_institution_snapshot(inst_df: pd.DataFrame, stock_code: str, target_date: pd.Timestamp) -> Dict[str, Any]:
    rows = inst_df[(inst_df["stock_code"] == stock_code) & (pd.to_datetime(inst_df["date"]) <= target_date)].sort_values("date")
    if rows.empty:
        return {
            "foreign_net_qty": 0,
            "itrust_net_qty": 0,
            "dealer_net_qty": 0,
            "total_net_qty": 0,
            "foreign_streak_type": "flat",
            "foreign_streak_days": 0,
            "itrust_streak_type": "flat",
            "itrust_streak_days": 0,
            "total_inst_streak_type": "flat",
            "total_inst_streak_days": 0,
        }

    last = rows.iloc[-1]
    foreign_type, foreign_days = calc_streak(rows, "foreign_net_qty")
    itrust_type, itrust_days = calc_streak(rows, "itrust_net_qty")
    total_type, total_days = calc_streak(rows, "total_net_qty")

    return {
        "foreign_net_qty": safe_int(last.get("foreign_net_qty"), 0),
        "itrust_net_qty": safe_int(last.get("itrust_net_qty"), 0),
        "dealer_net_qty": safe_int(last.get("dealer_net_qty"), 0),
        "total_net_qty": safe_int(last.get("total_net_qty"), 0),
        "foreign_streak_type": foreign_type,
        "foreign_streak_days": foreign_days,
        "itrust_streak_type": itrust_type,
        "itrust_streak_days": itrust_days,
        "total_inst_streak_type": total_type,
        "total_inst_streak_days": total_days,
    }


def weight_bonus(weight: float, sign: int) -> float:
    """ETF 權重調分。高權重加碼/減碼影響更大。"""
    weight = abs(safe_float(weight, 0))
    if weight >= 5:
        return sign * 8
    if weight >= 2:
        return sign * 5
    if weight >= 0.5:
        return sign * 3
    return 0


def score_institutional(inst: Dict[str, Any], etf_net_direction: int, details: List[Dict[str, Any]]) -> Tuple[float, float]:
    institutional_score = 0.0
    same_direction_score = 0.0

    def add_inst(score: float, rule: str):
        nonlocal institutional_score
        institutional_score += score
        add_detail(details, "三大法人", rule, score)

    def add_same(score: float, rule: str):
        nonlocal same_direction_score
        same_direction_score += score
        add_detail(details, "ETF/法人同向", rule, score)

    # 外資 streak
    if inst["foreign_streak_type"] == "buy":
        if inst["foreign_streak_days"] >= 5:
            add_inst(8, "外資連 5 買")
        elif inst["foreign_streak_days"] >= 3:
            add_inst(5, "外資連 3 買")
    elif inst["foreign_streak_type"] == "sell":
        if inst["foreign_streak_days"] >= 5:
            add_inst(-8, "外資連 5 賣")
        elif inst["foreign_streak_days"] >= 3:
            add_inst(-5, "外資連 3 賣")

    # 投信 streak
    if inst["itrust_streak_type"] == "buy":
        if inst["itrust_streak_days"] >= 5:
            add_inst(12, "投信連 5 買")
        elif inst["itrust_streak_days"] >= 3:
            add_inst(8, "投信連 3 買")
    elif inst["itrust_streak_type"] == "sell":
        if inst["itrust_streak_days"] >= 5:
            add_inst(-12, "投信連 5 賣")
        elif inst["itrust_streak_days"] >= 3:
            add_inst(-8, "投信連 3 賣")

    # 三大法人合計 streak
    if inst["total_inst_streak_type"] == "buy":
        if inst["total_inst_streak_days"] >= 5:
            add_inst(10, "三大法人合計連 5 買")
        elif inst["total_inst_streak_days"] >= 3:
            add_inst(6, "三大法人合計連 3 買")
    elif inst["total_inst_streak_type"] == "sell":
        if inst["total_inst_streak_days"] >= 5:
            add_inst(-10, "三大法人合計連 5 賣")
        elif inst["total_inst_streak_days"] >= 3:
            add_inst(-6, "三大法人合計連 3 賣")

    # ETF 與法人當日方向同向/背離
    if etf_net_direction > 0:
        if inst["itrust_net_qty"] > 0:
            add_same(10, "ETF 增持且投信同日買超")
        if inst["foreign_net_qty"] > 0:
            add_same(5, "ETF 增持且外資同日買超")
        if inst["total_net_qty"] > 0:
            add_same(6, "ETF 增持且三大法人合計買超")
        if inst["foreign_net_qty"] < 0 and inst["itrust_net_qty"] < 0 and inst["dealer_net_qty"] < 0:
            add_same(-10, "ETF 增持但三大法人全數賣超")
        if inst["itrust_streak_type"] == "sell" and inst["itrust_streak_days"] >= 3:
            add_same(-8, "ETF 增持但投信連賣")

    elif etf_net_direction < 0:
        if inst["itrust_net_qty"] < 0:
            add_same(-10, "ETF 減持且投信同日賣超")
        if inst["foreign_net_qty"] < 0:
            add_same(-5, "ETF 減持且外資同日賣超")
        if inst["total_net_qty"] < 0:
            add_same(-6, "ETF 減持且三大法人合計賣超")
        if inst["total_net_qty"] > 0:
            add_same(5, "ETF 減持但三大法人合計買超，訊號分歧")

    return institutional_score, same_direction_score


def categorize(
    bucket: ScoreBucket,
    held_count: int,
    increased_count: int,
    decreased_count: int,
    new_count: int,
    removed_count: int,
    etf_net_direction: int,
    price_return_5d: Optional[float],
    is_break_ma20: bool,
) -> Tuple[str, str, str, int]:
    ret5 = price_return_5d if price_return_5d is not None else 0

    if held_count >= 2 and increased_count >= 2 and bucket.etf_action >= 25 and bucket.price_divergence >= 10 and ret5 > -8:
        code = "STRONG_BUY_WATCH"
        return code, CATEGORY_META[code][0], "多檔 ETF 同步加碼且股價回檔", CATEGORY_META[code][1]

    if etf_net_direction > 0 and -8 <= ret5 <= -3:
        code = "PULLBACK_BUY_WATCH"
        return code, CATEGORY_META[code][0], "ETF 加碼但股價短線回檔", CATEGORY_META[code][1]

    if new_count >= 1 and ret5 <= 5 and bucket.etf_action > 0:
        code = "NEW_ENTRY_WATCH"
        return code, CATEGORY_META[code][0], "新進入 ETF 成分股且尚未明顯大漲", CATEGORY_META[code][1]

    if bucket.etf_action >= 25 and ret5 > 5:
        code = "BUY_OBSERVE_NO_CHASE"
        return code, CATEGORY_META[code][0], "ETF 偏多但股價已先漲，不追價", CATEGORY_META[code][1]

    if bucket.etf_action <= -25 and (bucket.price_divergence <= -10 or is_break_ma20 or removed_count > 0):
        code = "SELL_WATCH"
        return code, CATEGORY_META[code][0], "ETF 減持或移除且股價轉弱", CATEGORY_META[code][1]

    if etf_net_direction < 0 and ret5 > 0:
        code = "HIGH_LEVEL_DISTRIBUTION_WATCH"
        return code, CATEGORY_META[code][0], "ETF 減持但股價仍強，留意高檔調節", CATEGORY_META[code][1]

    if -5 <= bucket.etf_action <= 25 and (bucket.institutional + bucket.same_direction) >= 15:
        code = "CHIP_STRENGTH_OBSERVE"
        return code, CATEGORY_META[code][0], "ETF 訊號普通但法人籌碼轉強", CATEGORY_META[code][1]

    code = "NEUTRAL"
    return code, CATEGORY_META[code][0], "未達重點觀察條件", CATEGORY_META[code][1]


def calculate_signals_for_date(data: Dict[str, pd.DataFrame], target_date: str, rule_version: str) -> List[Tuple[Any, ...]]:
    df_etf = data["etf"].copy()
    df_price = data["price"].copy()
    df_inst = data["inst"].copy()
    df_tech = data["tech"].copy()
    df_ind = data["industry"].copy()

    target_ts = pd.to_datetime(target_date)
    target_d = target_ts.date()

    df_today = df_etf[df_etf["date"] == target_d]
    if df_today.empty:
        print(f"⚠️ {target_date} ETF_TRACKING_LIST 無資料，略過訊號計算。")
        return []

    industry_map = {}
    if not df_ind.empty:
        industry_map = dict(zip(df_ind["stock_code"], df_ind["industry_name"]))

    stock_agg: Dict[str, Dict[str, Any]] = {}
    etf_codes = sorted(df_etf["etf_code"].dropna().unique().tolist())

    for etf_code in etf_codes:
        etf_dates = sorted(df_etf[df_etf["etf_code"] == etf_code]["date"].drop_duplicates().tolist())
        if target_d not in etf_dates:
            continue
        idx = etf_dates.index(target_d)
        prev_d = etf_dates[idx - 1] if idx > 0 else None

        curr = df_etf[(df_etf["etf_code"] == etf_code) & (df_etf["date"] == target_d)].copy()
        prev = df_etf[(df_etf["etf_code"] == etf_code) & (df_etf["date"] == prev_d)].copy() if prev_d else pd.DataFrame(columns=curr.columns)

        curr_map = curr.set_index("stock_code").to_dict("index") if not curr.empty else {}
        prev_map = prev.set_index("stock_code").to_dict("index") if not prev.empty else {}
        stock_codes = set(curr_map.keys()) | set(prev_map.keys())

        for code in stock_codes:
            c = curr_map.get(code)
            p = prev_map.get(code)
            curr_shares = safe_float(c.get("shares"), 0) if c else 0
            prev_shares = safe_float(p.get("shares"), 0) if p else 0
            curr_weight = safe_float(c.get("weight"), 0) if c else 0
            prev_weight = safe_float(p.get("weight"), 0) if p else 0
            delta_shares = curr_shares - prev_shares
            delta_weight = curr_weight - prev_weight
            change_pct = (delta_shares / prev_shares) if prev_shares > 0 else None

            stock_name = ""
            if c and c.get("stock_name"):
                stock_name = str(c.get("stock_name"))
            elif p and p.get("stock_name"):
                stock_name = str(p.get("stock_name"))
            else:
                stock_name = code

            if code not in stock_agg:
                stock_agg[code] = {
                    "stock_code": code,
                    "stock_name": stock_name,
                    "held_etf_count": 0,
                    "increased_etf_count": 0,
                    "decreased_etf_count": 0,
                    "new_entry_etf_count": 0,
                    "removed_etf_count": 0,
                    "significant_increased_count": 0,
                    "significant_decreased_count": 0,
                    "total_weight": 0.0,
                    "increased_weight_sum": 0.0,
                    "decreased_weight_sum": 0.0,
                    "total_shares_change": 0.0,
                    "etf_rows": [],
                }

            a = stock_agg[code]
            a["stock_name"] = stock_name or a["stock_name"]
            if curr_shares > 0:
                a["held_etf_count"] += 1
                a["total_weight"] += curr_weight
            if p is None and c is not None and curr_shares > 0:
                a["new_entry_etf_count"] += 1
            if c is None and p is not None and prev_shares > 0:
                a["removed_etf_count"] += 1
            if delta_shares > 0:
                a["increased_etf_count"] += 1
                a["increased_weight_sum"] += curr_weight
                if change_pct is None or change_pct >= SIGNIFICANT_SHARE_CHANGE_PCT:
                    a["significant_increased_count"] += 1
            if delta_shares < 0:
                a["decreased_etf_count"] += 1
                a["decreased_weight_sum"] += max(curr_weight, prev_weight)
                if change_pct is not None and change_pct <= -SIGNIFICANT_SHARE_CHANGE_PCT:
                    a["significant_decreased_count"] += 1
            a["total_shares_change"] += delta_shares
            a["etf_rows"].append(
                {
                    "etf_code": etf_code,
                    "curr_shares": curr_shares,
                    "prev_shares": prev_shares,
                    "delta_shares": delta_shares,
                    "curr_weight": curr_weight,
                    "prev_weight": prev_weight,
                    "delta_weight": delta_weight,
                    "change_pct": change_pct,
                    "is_new": p is None and c is not None,
                    "is_removed": c is None and p is not None,
                }
            )

    records: List[Tuple[Any, ...]] = []

    for code, a in stock_agg.items():
        details: List[Dict[str, Any]] = []
        tags: List[str] = []
        bucket = ScoreBucket()

        held_count = safe_int(a["held_etf_count"])
        inc_count = safe_int(a["increased_etf_count"])
        dec_count = safe_int(a["decreased_etf_count"])
        new_count = safe_int(a["new_entry_etf_count"])
        removed_count = safe_int(a["removed_etf_count"])
        sig_inc_count = safe_int(a["significant_increased_count"])
        sig_dec_count = safe_int(a["significant_decreased_count"])
        etf_net_direction = (inc_count + new_count) - (dec_count + removed_count)

        # ETF 持有廣度分數
        if held_count >= 4:
            bucket.etf_breadth += 20
            add_detail(details, "ETF持有廣度", "4 檔以上 ETF 同步持有", 20)
        elif held_count == 3:
            bucket.etf_breadth += 15
            add_detail(details, "ETF持有廣度", "3 檔 ETF 同步持有", 15)
        elif held_count == 2:
            bucket.etf_breadth += 8
            add_detail(details, "ETF持有廣度", "2 檔 ETF 同步持有", 8)

        total_weight = safe_float(a["total_weight"])
        if total_weight >= 10:
            bucket.etf_breadth += 10
            add_detail(details, "ETF持有廣度", "跨 ETF 總持股權重達 10% 以上", 10)
        elif total_weight >= 5:
            bucket.etf_breadth += 7
            add_detail(details, "ETF持有廣度", "跨 ETF 總持股權重達 5% 以上", 7)
        elif total_weight >= 2:
            bucket.etf_breadth += 5
            add_detail(details, "ETF持有廣度", "跨 ETF 總持股權重達 2% 以上", 5)

        # ETF 操作方向分數
        if new_count >= 1:
            score = min(30, 20 + max(0, new_count - 1) * 5)
            bucket.etf_action += score
            add_detail(details, "ETF操作方向", f"股票被 {new_count} 檔 ETF 新納入", score)
            tags.append("ETF新納入")

        if inc_count >= 3:
            bucket.etf_action += 35
            add_detail(details, "ETF操作方向", "3 檔以上 ETF 同步增持", 35)
            tags.append("多檔ETF同步增持")
        elif inc_count >= 2:
            bucket.etf_action += 25
            add_detail(details, "ETF操作方向", "2 檔以上 ETF 同步增持", 25)
            tags.append("多檔ETF同步增持")
        elif inc_count == 1 and sig_inc_count >= 1:
            bucket.etf_action += 10
            add_detail(details, "ETF操作方向", "單一 ETF 明顯增持股數", 10)

        if dec_count >= 3:
            bucket.etf_action -= 40
            add_detail(details, "ETF操作方向", "3 檔以上 ETF 同步減持", -40)
            tags.append("多檔ETF同步減持")
        elif dec_count >= 2:
            bucket.etf_action -= 30
            add_detail(details, "ETF操作方向", "2 檔以上 ETF 同步減持", -30)
            tags.append("多檔ETF同步減持")
        elif dec_count == 1 and sig_dec_count >= 1:
            bucket.etf_action -= 10
            add_detail(details, "ETF操作方向", "單一 ETF 明顯減持股數", -10)

        if removed_count >= 1:
            score = min(35, 25 + max(0, removed_count - 1) * 5)
            bucket.etf_action -= score
            add_detail(details, "ETF操作方向", f"股票被 {removed_count} 檔 ETF 移除", -score)
            tags.append("ETF移除")

        # ETF 權重調整分數
        for row in a["etf_rows"]:
            if row["delta_shares"] > 0:
                bonus = weight_bonus(row["curr_weight"], 1)
                if bonus:
                    bucket.etf_weight += bonus
                    add_detail(details, "ETF權重調整", f"{row['etf_code']} 高權重持股加碼，權重 {row['curr_weight']:.2f}%", bonus)
            elif row["delta_shares"] < 0:
                bonus = weight_bonus(max(row["curr_weight"], row["prev_weight"]), -1)
                if bonus:
                    bucket.etf_weight += bonus
                    add_detail(details, "ETF權重調整", f"{row['etf_code']} 高權重持股減碼，權重 {max(row['curr_weight'], row['prev_weight']):.2f}%", bonus)
        bucket.etf_weight = max(min(bucket.etf_weight, 25), -25)

        price = get_price_snapshot(df_price, code, target_ts)
        close_price = price["close_price"]
        ret1 = price["price_return_1d"]
        ret3 = price["price_return_3d"]
        ret5 = price["price_return_5d"]
        ret5_safe = ret5 if ret5 is not None else 0
        tech = get_tech_snapshot(df_tech, code, target_ts, close_price)

        # 價格背離分數
        if etf_net_direction > 0:
            if ret5 is not None:
                if ret5 <= -8:
                    bucket.price_divergence += 5
                    bucket.risk -= 5
                    add_detail(details, "價格背離", f"ETF 增持但股價 5 日大跌 {ret5:.2f}%", 5)
                    add_detail(details, "風險", "ETF 增持但股價跌幅超過 8%，降級風險", -5)
                elif -8 < ret5 <= -3:
                    bucket.price_divergence += 20
                    add_detail(details, "價格背離", f"ETF 增持但股價 5 日下跌 {ret5:.2f}%", 20)
                    tags.append("回檔承接")
                elif -3 < ret5 <= -1:
                    bucket.price_divergence += 10
                    add_detail(details, "價格背離", f"ETF 增持但股價 5 日小跌 {ret5:.2f}%", 10)
                elif ret5 > 5:
                    tags.append("不追價")
            if new_count >= 1 and ret5 is not None and ret5 <= 5:
                bucket.price_divergence += 10
                add_detail(details, "價格背離", "ETF 新納入且股價近 5 日尚未大漲", 10)
        elif etf_net_direction < 0:
            if ret5 is not None:
                if ret5 > 0:
                    bucket.price_divergence -= 10
                    add_detail(details, "價格背離", f"ETF 減持但股價 5 日仍上漲 {ret5:.2f}%", -10)
                    tags.append("高檔調節")
                elif ret5 < 0:
                    bucket.price_divergence -= 20
                    add_detail(details, "價格背離", f"ETF 減持且股價 5 日下跌 {ret5:.2f}%", -20)
            if removed_count >= 1 and tech["is_break_ma20"]:
                bucket.price_divergence -= 25
                add_detail(details, "價格背離", "ETF 移除且股價跌破 MA20", -25)

        inst = get_institution_snapshot(df_inst, code, target_ts)
        inst_score, same_score = score_institutional(inst, etf_net_direction, details)
        bucket.institutional += inst_score
        bucket.same_direction += same_score

        final_category, final_label, final_sub, priority = categorize(
            bucket=bucket,
            held_count=held_count,
            increased_count=inc_count,
            decreased_count=dec_count,
            new_count=new_count,
            removed_count=removed_count,
            etf_net_direction=etf_net_direction,
            price_return_5d=ret5,
            is_break_ma20=tech["is_break_ma20"],
        )

        industry_name = industry_map.get(code, "未分類")
        if not tags:
            tags = [final_label]

        records.append(
            (
                target_d,
                code,
                a["stock_name"],
                industry_name,
                final_category,
                final_label,
                final_sub,
                priority,
                bucket.total(),
                bucket.etf_breadth,
                bucket.etf_action,
                bucket.etf_weight,
                bucket.price_divergence,
                bucket.institutional,
                bucket.same_direction,
                bucket.risk,
                held_count,
                inc_count,
                dec_count,
                new_count,
                removed_count,
                round(total_weight, 4),
                round(safe_float(a["increased_weight_sum"]), 4),
                round(safe_float(a["decreased_weight_sum"]), 4),
                round(safe_float(a["total_shares_change"]), 4),
                close_price,
                ret1,
                ret3,
                ret5,
                tech["is_break_ma20"],
                tech["is_above_ma20"],
                inst["foreign_net_qty"],
                inst["itrust_net_qty"],
                inst["dealer_net_qty"],
                inst["total_net_qty"],
                inst["foreign_streak_type"],
                inst["foreign_streak_days"],
                inst["itrust_streak_type"],
                inst["itrust_streak_days"],
                inst["total_inst_streak_type"],
                inst["total_inst_streak_days"],
                Json(details, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, default=str)),
                tags,
                rule_version,
            )
        )

    return records


def write_signal_results(records: List[Tuple[Any, ...]], start_date: str, end_date: str):
    columns = [
        "analysis_date", "stock_code", "stock_name", "industry_name",
        "final_category", "final_category_label", "final_subcategory", "display_priority",
        "total_score", "etf_breadth_score", "etf_action_score", "etf_weight_score",
        "price_divergence_score", "institutional_score", "same_direction_score", "risk_score",
        "held_etf_count", "increased_etf_count", "decreased_etf_count", "new_entry_etf_count", "removed_etf_count",
        "total_weight", "increased_weight_sum", "decreased_weight_sum", "total_shares_change",
        "close_price", "price_return_1d", "price_return_3d", "price_return_5d", "is_break_ma20", "is_above_ma20",
        "foreign_net_qty", "itrust_net_qty", "dealer_net_qty", "total_net_qty",
        "foreign_streak_type", "foreign_streak_days", "itrust_streak_type", "itrust_streak_days", "total_inst_streak_type", "total_inst_streak_days",
        "score_detail_json", "reason_tags", "rule_version",
    ]

    if not records:
        print("⚠️ 沒有訊號結果需要寫入。")
        return

    insert_cols = ", ".join([f'"{c}"' for c in columns])
    update_cols = [c for c in columns if c not in ("analysis_date", "stock_code")]
    update_sql = ",\n                ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
    update_sql += ',\n                "updated_at" = NOW()'

    sql = f'''
        INSERT INTO public."STOCK_DAILY_SIGNAL_RESULT" ({insert_cols})
        VALUES %s
        ON CONFLICT (analysis_date, stock_code) DO UPDATE SET
                {update_sql};
    '''

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 先刪除區間舊資料，避免某股票後續不在 ETF 比較集合時殘留舊結果。
            cur.execute(
                'DELETE FROM public."STOCK_DAILY_SIGNAL_RESULT" WHERE analysis_date BETWEEN %s AND %s',
                (start_date, end_date),
            )
            execute_values(cur, sql, records, page_size=1000)
        conn.commit()

    non_neutral = sum(1 for r in records if r[4] != "NEUTRAL")
    print(f"✅ 已寫入 {len(records)} 筆 STOCK_DAILY_SIGNAL_RESULT，其中重點觀察 {non_neutral} 筆。")


def run_signal_scoring_range(start_date: str, end_date: str, rule_version: str = RULE_VERSION):
    print(f"\n🚀 開始計算每日訊號：{start_date} ~ {end_date}")
    data = load_source_data(start_date, end_date)
    df_etf = data["etf"]
    if df_etf.empty:
        print("❌ ETF_TRACKING_LIST 無資料，流程中斷。")
        return

    all_dates = sorted(
        df_etf[
            (pd.to_datetime(df_etf["date"]) >= pd.to_datetime(start_date))
            & (pd.to_datetime(df_etf["date"]) <= pd.to_datetime(end_date))
        ]["date"].drop_duplicates().tolist()
    )

    all_records: List[Tuple[Any, ...]] = []
    for d in all_dates:
        date_str = pd.to_datetime(d).strftime("%Y-%m-%d")
        records = calculate_signals_for_date(data, date_str, rule_version)
        print(f"  - {date_str}: 計算 {len(records)} 檔股票")
        all_records.extend(records)

    write_signal_results(all_records, start_date, end_date)
    print("✨ 每日訊號計算完成。")


# =========================================================
# 8. 排程友善主程式入口：不使用 input()
# =========================================================

def parse_args():
    parser = argparse.ArgumentParser(description="Colab 排程友善版：ETF / 股價 / 法人 / 技術指標更新 + 每日訊號評分")
    parser.add_argument(
        "--mode",
        default="full-today",
        choices=[
            "full-today",
            "full-range",
            "etf-today",
            "etf-range",
            "data-today",
            "data-range",
            "signal-today",
            "signal-range",
        ],
        help=(
            "full=ETF持股+股票資料+訊號；"
            "etf=只更新ETF持股；"
            "data=只更新股價/法人/技術指標；"
            "signal=只計算訊號。"
        ),
    )
    parser.add_argument("--date", help="單日日期，格式 YYYY-MM-DD。若提供，會覆蓋 start/end。")
    parser.add_argument("--start-date", help="起始日期，格式 YYYY-MM-DD。range mode 使用。")
    parser.add_argument("--end-date", help="結束日期，格式 YYYY-MM-DD。range mode 使用。")
    parser.add_argument("--rule-version", default=RULE_VERSION, help="訊號評分規則版本，預設 v1.0")
    parser.add_argument("--include-ezmoney", action=argparse.BooleanOptionalAction, default=True, help="是否抓 ezmoney ETF。")
    parser.add_argument("--include-capital", action=argparse.BooleanOptionalAction, default=True, help="是否抓群益 ETF API。")
    parser.add_argument("--skip-signal", action="store_true", help="full mode 執行完資料更新後，不計算訊號。")
    parser.add_argument(
        "--use-daily-holdings",
        action="store_true",
        help="股票資料流程改用區間內每天實際 ETF 持股；預設使用 ETF_TRACKING_LIST 全部歷史股票池補齊。",
    )
    parser.add_argument(
        "--continue-on-etf-fail",
        action="store_true",
        help="full mode 若 ETF 持股更新失敗，仍繼續執行股票資料與訊號流程。",
    )
    return parser.parse_args()


def resolve_date_range(args):
    if args.date:
        start_date = end_date = args.date
    else:
        is_range_mode = args.mode.endswith("-range")
        today = today_str()
        if is_range_mode:
            start_date = args.start_date or today
            end_date = args.end_date or start_date
        else:
            start_date = args.start_date or today
            end_date = args.end_date or start_date

    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")
    if start_date > end_date:
        raise ValueError("start_date 不可大於 end_date")
    return start_date, end_date


def run_merged_pipeline(args):
    validate_db_env()
    start_date, end_date = resolve_date_range(args)

    print("\n" + "=" * 72)
    print("📊 Colab 排程友善版：ETF / 股票資料更新 + 每日訊號評分")
    print("=" * 72)
    print(f"Mode        : {args.mode}")
    print(f"Date range  : {start_date} ~ {end_date}")
    print(f"Timezone    : {APP_TIMEZONE}")
    print(f"Rule version: {args.rule_version}")
    print("=" * 72)

    use_all_tracking_codes = not args.use_daily_holdings

    if args.mode.startswith("etf-"):
        update_etf_holdings(
            start_date,
            end_date,
            include_ezmoney=args.include_ezmoney,
            include_capital=args.include_capital,
        )
        return

    if args.mode.startswith("data-"):
        run_stock_data_pipeline(start_date, end_date, use_all_tracking_codes=use_all_tracking_codes)
        return

    if args.mode.startswith("signal-"):
        run_signal_scoring_range(start_date, end_date, args.rule_version)
        return

    if args.mode.startswith("full-"):
        is_success = update_etf_holdings(
            start_date,
            end_date,
            include_ezmoney=args.include_ezmoney,
            include_capital=args.include_capital,
        )

        if not is_success and not args.continue_on_etf_fail:
            print("❌ ETF 持股更新未成功，為避免後續股票清單不完整，本次停止後續流程。")
            return

        run_stock_data_pipeline(start_date, end_date, use_all_tracking_codes=use_all_tracking_codes)

        if not args.skip_signal:
            run_signal_scoring_range(start_date, end_date, args.rule_version)

        print("\n✅ 合併流程執行完成。")
        return

    raise ValueError(f"不支援的 mode: {args.mode}")


def main():
    args = parse_args()
    run_merged_pipeline(args)


if __name__ == "__main__":
    main()
