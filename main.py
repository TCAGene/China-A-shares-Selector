from datetime import date
from db import init_db
from fetcher import update_stock_list, fetch_incremental_data, fetch_financial_data
from screener import screen


def main():
    today = date.today()
    last_year = today.year - 1

    print("=== A股筛选工具 ===")
    print(f"当前日期: {today}，筛选财年: {last_year}\n")

    # 1. 初始化数据库
    init_db()

    # 2. 同步股票列表
    update_stock_list()

    # 3. 增量抓取行情
    fetch_incremental_data()

    # 4. 抓取上年度财务数据
    fetch_financial_data(last_year)

    # 5. 执行筛选
    print("\n开始执行筛选规则...")
    results = screen(last_year)

    # 6. 输出结果
    print(f"\n{'='*70}")
    print(f"符合条件的股票共 {len(results)} 只：")
    print(f"{'='*70}")
    if results:
        header = f"{'代码':<8}{'名称':<10}{'上市开盘':>10}{'历史最高':>10}{'最高日期':>12}{'峰后最低':>10}{'当前价格':>10}{'涨幅%':>8}"
        print(header)
        print("-" * 70)
        for r in results:
            print(
                f"{r['code']:<8}{r['name']:<10}"
                f"{r['list_open_price']:>10.2f}"
                f"{r['peak_price']:>10.2f}"
                f"{r['peak_date']:>12}"
                f"{r['post_peak_min']:>10.2f}"
                f"{r['current_price']:>10.2f}"
                f"{r['gain_pct']:>8.1f}"
            )
    else:
        print("暂无符合条件的股票。")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
