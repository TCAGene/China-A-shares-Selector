import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "astock.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS stock_list (
            code             TEXT PRIMARY KEY,
            name             TEXT,
            list_date        TEXT,
            list_open_price  REAL
        );

        CREATE TABLE IF NOT EXISTS daily_price (
            code    TEXT,
            date    TEXT,
            open    REAL,
            close   REAL,
            high    REAL,
            low     REAL,
            volume  REAL,
            PRIMARY KEY (code, date)
        );

        CREATE TABLE IF NOT EXISTS annual_profit (
            code       TEXT,
            year       INTEGER,
            net_profit REAL,
            PRIMARY KEY (code, year)
        );
    """)
    conn.commit()
    conn.close()


def upsert_stock(code, name, list_date=None, list_open_price=None):
    conn = get_conn()
    conn.execute(
        """INSERT INTO stock_list (code, name, list_date, list_open_price)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(code) DO UPDATE SET
               name = excluded.name,
               list_date = COALESCE(excluded.list_date, list_date),
               list_open_price = COALESCE(excluded.list_open_price, list_open_price)
        """,
        (code, name, list_date, list_open_price),
    )
    conn.commit()
    conn.close()


def _ensure_negative_cache():
    """确保 negative_cache 表存在（记录查询结果为空的股票）"""
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS negative_cache (
            code   TEXT,
            year   INTEGER,
            PRIMARY KEY (code, year)
        )
    """)
    conn.commit()
    conn.close()


def upsert_stocks_batch(records):
    """批量 upsert stock_list，records: list of (code, name, list_date, list_open_price)"""
    conn = get_conn()
    conn.executemany(
        """INSERT INTO stock_list (code, name, list_date, list_open_price)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(code) DO UPDATE SET
               name = excluded.name,
               list_date = COALESCE(excluded.list_date, list_date),
               list_open_price = COALESCE(excluded.list_open_price, list_open_price)
        """,
        records,
    )
    conn.commit()
    conn.close()


def get_all_stocks():
    conn = get_conn()
    rows = conn.execute("SELECT code, name, list_date, list_open_price FROM stock_list").fetchall()
    conn.close()
    return rows


def get_latest_price_date(code):
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(date) as d FROM daily_price WHERE code = ?", (code,)
    ).fetchone()
    conn.close()
    return row["d"] if row else None


def insert_daily_prices(records):
    """records: list of (code, date, open, close, high, low, volume)"""
    if not records:
        return
    conn = get_conn()
    conn.executemany(
        """INSERT OR REPLACE INTO daily_price (code, date, open, close, high, low, volume)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        records,
    )
    conn.commit()
    conn.close()


def replace_all_daily_prices(code, records):
    """删除该股票所有历史行情后重新写入（用于复权因子变化后全量刷新）"""
    conn = get_conn()
    conn.execute("DELETE FROM daily_price WHERE code = ?", (code,))
    conn.executemany(
        """INSERT INTO daily_price (code, date, open, close, high, low, volume)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        records,
    )
    conn.commit()
    conn.close()


def get_close_on_date(code, date):
    """获取指定日期的收盘价，不存在返回 None"""
    conn = get_conn()
    row = conn.execute(
        "SELECT close FROM daily_price WHERE code = ? AND date = ?", (code, date)
    ).fetchone()
    conn.close()
    return row["close"] if row else None


def upsert_annual_profit(code, year, net_profit):
    conn = get_conn()
    conn.execute(
        """INSERT INTO annual_profit (code, year, net_profit) VALUES (?, ?, ?)
           ON CONFLICT(code, year) DO UPDATE SET net_profit = excluded.net_profit""",
        (code, year, net_profit),
    )
    conn.commit()
    conn.close()


def has_annual_profit(code, year):
    conn = get_conn()
    row = conn.execute(
        "SELECT 1 FROM annual_profit WHERE code = ? AND year = ?", (code, year)
    ).fetchone()
    conn.close()
    return row is not None


def get_daily_prices(code):
    """返回 (date, open, close, high, low) 按日期升序"""
    conn = get_conn()
    rows = conn.execute(
        "SELECT date, open, close, high, low FROM daily_price WHERE code = ? ORDER BY date ASC",
        (code,),
    ).fetchall()
    conn.close()
    return rows
