import time
import queue
import baostock as bs
import pandas as pd
from db import (
    get_all_stocks, get_latest_price_date,
    insert_daily_prices, replace_all_daily_prices,
    get_close_on_date, get_conn,
    upsert_stocks_batch, _ensure_negative_cache,
)


def _bs_login():
    bs.login()


def _bs_logout():
    bs.logout()


def _to_bs_code(code):
    """000001 -> sz.000001, 600001 -> sh.600001"""
    if code.startswith("6"):
        return f"sh.{code}"
    return f"sz.{code}"


def update_stock_list():
    """同步A股股票列表到 stock_list 表（使用 baostock）"""
    print("正在获取A股股票列表...")
    _bs_login()
    rs = bs.query_stock_basic()
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    _bs_logout()

    df = pd.DataFrame(rows, columns=rs.fields)
    df = df[(df["type"] == "1") & df["code"].str.startswith(("sh.", "sz.")) & (df["outDate"] == "")]

    conn = get_conn()
    all_db_codes = {r[0] for r in conn.execute("SELECT code FROM stock_list").fetchall()}
    valid_codes = set(df["code"].str.split(".").str[1].tolist())
    to_delete = all_db_codes - valid_codes
    if to_delete:
        conn.executemany("DELETE FROM stock_list WHERE code = ?", [(c,) for c in to_delete])
        conn.executemany("DELETE FROM daily_price WHERE code = ?", [(c,) for c in to_delete])
        conn.commit()
        print(f"  已清理 {len(to_delete)} 只不支持/已退市股票")

    # 批量写入，1次连接
    records = [(row["code"].split(".")[1], row.get("code_name", ""))
               for _, row in df.iterrows()]
    conn.executemany(
        """INSERT INTO stock_list (code, name) VALUES (?, ?)
           ON CONFLICT(code) DO UPDATE SET name = excluded.name""",
        records,
    )
    conn.commit()
    conn.close()
    print(f"股票列表同步完成，共 {len(df)} 只股票")


def _adjust_end_date(end_date):
    """
    确保 end_date 是有数据的目标日期（跳过周末/节假日）。
    从候选日期开始向前逐天尝试，直到找到 baostock 有数据的交易日。
    最多回退 7 天。
    返回 (实际日期, 是否因数据未发布而回退, 原始日期).
    """
    from datetime import date, datetime, timedelta
    target = datetime.strptime(end_date, "%Y-%m-%d").date()
    original = target
    for _ in range(7):
        day_of_week = target.weekday()
        if day_of_week >= 5:  # 周六=5，周日=6，跳过
            target -= timedelta(days=1)
            continue
        # 尝试用 baostock 获取该日数据（用 sh.600000 作探针）
        _bs_login()
        rs = bs.query_history_k_data_plus(
            "sh.600000", "date",
            start_date=str(target), end_date=str(target),
            frequency="d", adjustflag="2"
        )
        has_data = rs.next()
        _bs_logout()
        if has_data:
            # 数据存在：若与原始日期相同则无回退；否则是节假日回退
            regressed = target < original
            return str(target), regressed, str(original)
        # baostock 无数据（非交易日或数据未发布），回退一天
        target -= timedelta(days=1)
    return str(target), True, str(original)


def _fetch_bs_history(bs_code, start_date=None, end_date=None, retries=2):
    """通过 baostock 抓取日线数据（前复权 adjustflag=2），失败自动重试"""
    from datetime import date
    if end_date is None:
        end_date = str(date.today())
    actual_start = start_date if start_date else "1990-01-01"
    for attempt in range(retries + 1):
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,close,high,low,volume",
                start_date=actual_start,
                end_date=end_date,
                frequency="d",
                adjustflag="2",
            )
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            return rows
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5)
            else:
                raise


def _rows_to_records(code, rows):
    records = []
    for r in rows:
        try:
            date_, open_, close_, high_, low_, vol_ = r
            if not open_ or open_ in ("", "null") or float(open_) <= 0:
                continue
            records.append((
                code,
                date_,
                float(open_),
                float(close_),
                float(high_),
                float(low_),
                float(vol_) if vol_ not in ("", "null") else 0.0,
            ))
        except (ValueError, TypeError):
            continue
    return records


def fetch_incremental_data():
    """对所有股票进行增量行情抓取（baostock，增量写入防止进度丢失）"""
    from datetime import date, datetime, timedelta
    import queue
    import threading

    today = date.today()
    now = datetime.now()
    if now.hour >= 20:
        end_date = str(today)
        prev_date = str(today - timedelta(days=1))
    else:
        end_date = str(today - timedelta(days=1))
        prev_date = str(today - timedelta(days=2))

    end_date, end_regressed, _ = _adjust_end_date(end_date)
    prev_candidate = datetime.strptime(end_date, "%Y-%m-%d").date() - timedelta(days=1)
    prev_date, _, _ = _adjust_end_date(str(prev_candidate))

    stocks = get_all_stocks()
    total = len(stocks)
    print(f"开始增量抓取行情，共 {total} 只股票（更新至 {end_date}）...")

    if end_regressed and now.hour >= 20 and today.weekday() < 5:
        print(f"  [提示] 今日（{today}）为交易日，但 baostock 尚未发布今日数据（通常 20:00 后发布），已暂时更新至上个交易日 {end_date}")
    elif end_regressed and today.weekday() >= 5:
        print(f"  [提示] 今日（{today}）为周末，上一个交易日为 {end_date}")
    elif not end_regressed and now.hour < 20:
        print(f"  [提示] 现在是 {now.hour}点，今日交易数据一般在 20:00 后发布，请稍后刷新")

    # 分类（内联一次查询）
    conn = get_conn()
    latest_map = {
        r[0]: r[1]
        for r in conn.execute(
            "SELECT code, MAX(date) FROM daily_price GROUP BY code"
        ).fetchall()
    }
    # 增量股票：用各股票自己的 latest_date 的收盘价（用于本地比对是否发生复权）
    latest_close_map = {}
    for code, latest_date in latest_map.items():
        row = conn.execute(
            "SELECT close FROM daily_price WHERE code = ? AND date = ?",
            (code, latest_date)
        ).fetchone()
        if row:
            latest_close_map[code] = row[0]
    conn.close()

    skip_count, new_list, incremental_dict = 0, [], {}
    for stock in stocks:
        code = stock["code"]
        latest = latest_map.get(code)
        if latest is None:
            new_list.append(stock)
        elif latest >= end_date:
            skip_count += 1
        else:
            incremental_dict.setdefault(latest, []).append((code, stock["name"], latest_close_map.get(code)))

    incremental_total = sum(len(v) for v in incremental_dict.values())
    print(f"  跳过(已最新): {skip_count}  新股全量: {len(new_list)}  增量更新: {incremental_total}")
    print(f"  [DIAG] end_date={end_date}, prev_date={prev_date}")
    inc_group_summary = {k: len(v) for k, v in incremental_dict.items()}
    print(f"  [DIAG] 增量分组: {inc_group_summary}")

    total_to_do = len(new_list) + incremental_total
    if total_to_do == 0:
        print("已是最新，无需抓取")
        return

    # ---------- 单 worker：边抓取边写入，每处理完 N 只就写一次 ----------
    job_queue = queue.Queue()
    result_lock = threading.Lock()
    done_count = [0]

    # 累计写入池（每凑满 WRITE_BATCH 条就写入一次数据库）
    WRITE_BATCH = 100
    pending_records = []      # 待写入的 (code, date, ...) 记录
    pending_stock_info = []   # 待写入的 (code, name, list_date, list_open_price)
    pending_replaces = {}    # code -> records（复权全量刷新）
    pending_incremental = {} # code -> [records]（增量记录）

    def _flush():
        """将 pending_records 批量写入数据库"""
        nonlocal pending_records, pending_stock_info, pending_replaces, pending_incremental
        with result_lock:
            if pending_records:
                insert_daily_prices(pending_records)
                pending_records.clear()
            if pending_stock_info:
                upsert_stocks_batch(pending_stock_info)
                pending_stock_info.clear()
            if pending_replaces:
                for code, records in list(pending_replaces.items()):
                    replace_all_daily_prices(code, records)
                pending_replaces.clear()
            if pending_incremental:
                for code, records in list(pending_incremental.items()):
                    insert_daily_prices(records)
                pending_incremental.clear()

    def _worker():
        """单 worker 持有 baostock session，每处理完 WRITE_BATCH 只就写入一次"""
        _bs_login()
        try:
            while True:
                try:
                    item = job_queue.get(timeout=300)
                except queue.Empty:
                    print("[ERROR] Worker 等待任务超时（5分钟无响应）")
                    break
                if item is None:
                    job_queue.task_done()
                    break
                code, name, mode, fetch_start, db_close_prev = item
                bs_code = _to_bs_code(code)
                try:
                    exright_changed = False
                    if mode == 'new':
                        rows = _fetch_bs_history(bs_code, end_date=end_date)
                        records = _rows_to_records(code, rows)
                    else:
                        # 第1次 API：fetch latest_date → end_date
                        rows = _fetch_bs_history(bs_code, start_date=fetch_start, end_date=end_date)
                        records = _rows_to_records(code, rows)
                        # 快速路径：>=2 条说明 fetched[0].date == latest_date，正常增量，跳过 exright 检测
                        # 只有 len==1 时才需要 exright 比对（latest_date 是非交易日触发 baostock 补齐）
                        if len(records) >= 2:
                            exright_changed = False
                            records = records[1:]  # 跳过 latest_date，只写入新数据
                        elif records and db_close_prev is not None:
                            # len==1：fetched[0].date != latest_date，用 fetched[0].close 做 exright 比对
                            fetched_latest_close = records[0][3]
                            exright_changed = abs(fetched_latest_close - db_close_prev) > 0.0001
                            if exright_changed:
                                full_rows = _fetch_bs_history(bs_code, end_date=end_date)
                                records = _rows_to_records(code, full_rows)
                            else:
                                records = []  # 无 exright 变化且无新交易日
                        else:
                            exright_changed = False
                    needs_flush = False
                    with result_lock:
                        if mode == 'new' and records:
                            pending_records.extend(records)
                            pending_stock_info.append((code, name, records[0][1], records[0][2]))
                        elif mode == 'incremental' and records:
                            if exright_changed:
                                # 复权全量刷新：更新行情 + 更新 stock name（IPO 信息不变，传入 None）
                                pending_replaces[code] = records
                                pending_stock_info.append((code, name, None, None))
                            else:
                                pending_incremental.setdefault(code, []).extend(records)
                        if (len(pending_records) >= WRITE_BATCH
                                or len(pending_incremental) >= WRITE_BATCH):
                            needs_flush = True
                    if needs_flush:
                        _flush()
                except Exception as e:
                    print(f"[ERROR] {code} 抓取失败: {e}")
                finally:
                    with result_lock:
                        done_count[0] += 1
                        pct = round(done_count[0] / total_to_do * 100, 1)
                        print(f"[PROGRESS] {done_count[0]}/{total_to_do} ({pct}%)", flush=True)
                    job_queue.task_done()
        finally:
            # 退出前把剩余的记录全部写入
            _flush()
            try:
                _bs_logout()
            except Exception:
                pass

    # 构建任务队列
    for stock in new_list:
        job_queue.put((stock["code"], stock["name"], "new", None, None))
    for latest_date, stock_group in sorted(incremental_dict.items()):
        for code, name, db_close_prev in stock_group:
            job_queue.put((code, name, "incremental", latest_date, db_close_prev))
    job_queue.put(None)

    t = threading.Thread(target=_worker)
    t.start()
    job_queue.join()

    # ---------- 最终验证 ----------
    conn = get_conn()
    count_after = conn.execute(
        "SELECT COUNT(*) FROM daily_price WHERE date = ?", (end_date,)
    ).fetchone()[0]
    conn.close()
    print(f"  [DIAG] {end_date} 最终共 {count_after} 条记录")
    print("行情增量抓取完成")


def fetch_financial_data(target_year, force=False):
    """
    抓取所有股票指定年度的净利润（使用 baostock）。
    - 已有数据的股票直接跳过（不重复查询）
    - 对查询结果为空的股票，记录到 negative_cache 表，后续运行跳过
    - force=True 时强制重新抓取全部股票
    """
    stocks = get_all_stocks()
    total = len(stocks)
    print(f"开始抓取 {target_year} 年度财务数据，共 {total} 只股票...")

    # 初始化 negative_cache 表（记录明确查询为空的结果）
    _ensure_negative_cache()

    conn = get_conn()
    # 已有净利润数据的股票
    existing = {
        r[0] for r in conn.execute(
            "SELECT code FROM annual_profit WHERE year = ?", (target_year,)
        ).fetchall()
    }
    # 已确认无数据的股票（缓存，跳过）
    negative = {
        r[0] for r in conn.execute(
            "SELECT code FROM negative_cache WHERE year = ?", (target_year,)
        ).fetchall()
    }
    conn.close()

    if force:
        todo_stocks = stocks
        print(f"  强制刷新模式，共 {len(todo_stocks)} 只")
        # 清除负面缓存，下次重新查询
        conn = get_conn()
        conn.execute("DELETE FROM negative_cache WHERE year = ?", (target_year,))
        conn.commit()
        conn.close()
    else:
        todo_stocks = [s for s in stocks if s["code"] not in existing and s["code"] not in negative]
        print(f"  已有数据: {len(existing)} 只，已确认无数据: {len(negative)} 只，需抓取: {len(todo_stocks)} 只")

    if not todo_stocks:
        print(f"{target_year} 年度财务数据已是最新，无需更新")
        return

    new_profit_records = []  # (code, year, net_profit) 批量写入
    new_negative_records = []  # (code, year) 批量写入负面缓存
    done = 0
    total_todo = len(todo_stocks)

    _bs_login()
    try:
        for stock in todo_stocks:
            code = stock["code"]
            bs_code = _to_bs_code(code)
            try:
                rs = bs.query_profit_data(code=bs_code, year=target_year, quarter=4)
                rows = []
                while rs.next():
                    rows.append(rs.get_row_data())
                if rows:
                    net_profit_str = rows[0][6]
                    if net_profit_str:
                        val = float(net_profit_str)
                        if val > 0:
                            new_profit_records.append((code, target_year, val))
                else:
                    # 查询为空（非年报发布季），记录到负面缓存
                    new_negative_records.append((code, target_year))
            except Exception as e:
                print(f"[ERROR] {code} 财务数据失败: {e}")
            done += 1
            if done % 100 == 0:
                pct = round(done / total_todo * 100, 1)
                print(f"[PROGRESS] {done}/{total_todo} ({pct}%)")
    finally:
        _bs_logout()

    # 批量写入
    if new_profit_records:
        conn = get_conn()
        conn.executemany(
            """INSERT INTO annual_profit (code, year, net_profit) VALUES (?, ?, ?)
               ON CONFLICT(code, year) DO UPDATE SET net_profit = excluded.net_profit""",
            new_profit_records,
        )
        conn.commit()
        conn.close()
        print(f"  批量写入 {len(new_profit_records)} 条财务数据")

    if new_negative_records:
        conn = get_conn()
        conn.executemany(
            "INSERT OR IGNORE INTO negative_cache (code, year) VALUES (?, ?)",
            new_negative_records,
        )
        conn.commit()
        conn.close()
        print(f"  记录 {len(new_negative_records)} 只确认无数据的股票（后续跳过）")

    print(f"{target_year} 年度财务数据抓取完成，共抓取 {done}/{total_todo} 只")
