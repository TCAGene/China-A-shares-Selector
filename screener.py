import json
import os
import traceback
from datetime import date
from collections import defaultdict
from db import get_conn, get_all_stocks, get_daily_prices

_STATE_FILE = os.path.join(os.path.dirname(__file__), "prev_results.json")


def _load_prev_codes():
    """
    返回 (prev_codes_set, prev_date_str)
    prev_codes = 上一天筛选结果的 code 集合（用于判断今日新增）
    today_codes = 今天已跑出的结果（避免同一天多次运行重置新增状态）
    """
    if not os.path.exists(_STATE_FILE):
        return set(), None, None
    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return set(obj.get("prev_codes", [])), obj.get("date"), obj.get("today_codes")
    except Exception:
        traceback.print_exc()
        return set(), None, None


def _save_curr_codes(codes):
    today_str = str(date.today())
    prev_codes, prev_date, _ = _load_prev_codes()

    # 同一天后续运行：today_codes 已是本次结果，无需更新
    if prev_date == today_str:
        return

    # 新的一天：今天的代码成为新的 prev_codes，保存
    try:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "prev_codes": sorted(codes),   # 下一天以此为基准判断新增
                "today_codes": sorted(codes),  # 今天的结果（备用于同一天多次运行）
                "date": today_str,
            }, f, ensure_ascii=False)
    except Exception:
        pass


def _compute_weekly_gain(prices):
    """
    用日线数据推算周涨幅。
    prices: 按日期升序的 list of dict，keys: date, close
    返回 (weekly_gain_pct, last_week_close) 或 (None, None)
    """
    if len(prices) < 10:
        return None, None

    # 按 ISO 周分组: (iso_year, iso_week) -> last close of that week
    weekly_closes = {}
    for r in prices:
        d = r["date"]
        try:
            from datetime import datetime
            dt = datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            continue
        iso_key = (dt.isocalendar()[0], dt.isocalendar()[1])
        weekly_closes[iso_key] = r["close"]

    if len(weekly_closes) < 3:
        return None, None

    sorted_weeks = sorted(weekly_closes.keys())
    last_year, last_week = sorted_weeks[-1]
    prev_year, prev_week = sorted_weeks[-2]

    # 确保上周和上上周是连续的（允许跨年）
    def week_delta(y1, w1, y2, w2):
        return (y2 - y1) * 52 + (w2 - w1)

    if week_delta(prev_year, prev_week, last_year, last_week) != 1:
        return None, None

    last_close = weekly_closes[(last_year, last_week)]
    prev_close = weekly_closes[(prev_year, prev_week)]
    if prev_close <= 0:
        return None, None

    return round((last_close - prev_close) / prev_close * 100, 2), last_close


def _get_profitable_codes(year):
    """规则1：上年度净利润为正的股票代码集合"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT code FROM annual_profit WHERE year = ? AND net_profit > 0", (year,)
    ).fetchall()
    conn.close()
    return {r["code"] for r in rows}


def screen(last_year):
    """
    执行四条筛选规则，返回符合条件的股票列表。
    每项为 dict: code, name, list_open_price, peak_price, peak_date,
                  post_peak_min, current_price, gain_pct,
                  weekly_gain (float or None), is_new (bool)
    排序：is_new=True 置顶，再按 weekly_gain 降序，None 值排最后
    """
    profitable = _get_profitable_codes(last_year)
    if not profitable:
        print(f"[WARN] 未找到 {last_year} 年度净利润数据，跳掉规则1过滤")

    prev_codes, prev_date, today_codes = _load_prev_codes()
    today_str = str(date.today())

    if prev_date == today_str and today_codes is not None:
        # 同一天第二次运行：用今天第一次跑出的结果作基准，保持新增状态
        prev_codes = set(today_codes)
    else:
        # 新的一天首次运行：用上一天结果作基准检测新增
        prev_codes = prev_codes or set()

    stocks = get_all_stocks()
    results = []

    for stock in stocks:
        code = stock["code"]
        name = stock["name"]
        list_open = stock["list_open_price"]

        if profitable and code not in profitable:
            continue
        if not list_open or list_open <= 0:
            continue

        prices = get_daily_prices(code)
        if not prices:
            continue

        prices = [r for r in prices if r["high"] > 0 and r["low"] > 0 and r["close"] > 0 and r["open"] > 0]
        if not prices:
            continue

        max_high = max(r["high"] for r in prices)
        peak_date = next(r["date"] for r in prices if r["high"] == max_high)

        if max_high < list_open * 2:
            continue

        threshold_50 = max_high * 0.5
        post_peak = [r for r in prices if r["date"] >= peak_date]
        if not any(r["low"] < threshold_50 for r in post_peak):
            continue

        post_peak_min = min(r["low"] for r in post_peak if r["low"] > 0)
        current_price = prices[-1]["close"]
        if post_peak_min <= 0 or current_price <= 0:
            continue
        gain_pct = (current_price - post_peak_min) / post_peak_min * 100
        if gain_pct < 100:
            continue

        weekly_gain, _ = _compute_weekly_gain(prices)

        results.append({
            "code": code,
            "name": name,
            "list_open_price": list_open,
            "peak_price": max_high,
            "peak_date": peak_date,
            "post_peak_min": post_peak_min,
            "current_price": current_price,
            "gain_pct": round(gain_pct, 2),
            "weekly_gain": weekly_gain,
            "is_new": code not in prev_codes,
        })

    # 排序：新增股置顶，再按 weekly_gain 降序，None 排最后
    def sort_key(r):
        wg = r["weekly_gain"]
        if r["is_new"]:
            new_priority = 0
        else:
            new_priority = 1
        wg_score = wg if wg is not None else -999999
        return (new_priority, -wg_score)

    results.sort(key=sort_key)

    _save_curr_codes({r["code"] for r in results})
    return results
