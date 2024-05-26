from pandas import Series
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from datetime import datetime, time, timedelta
import time
import pandas as pd
import pymssql
from concurrent.futures import ThreadPoolExecutor, as_completed

# stock_id: str = "2330"
# report_date: str = "20240126"
stock_holding_levels = {
    '1-999': 1,
    '1,000-5,000': 2,
    '5,001-10,000': 3,
    '10,001-15,000': 4,
    '15,001-20,000': 5,
    '20,001-30,000': 6,
    '30,001-40,000': 7,
    '40,001-50,000': 8,
    '50,001-100,000': 9,
    '100,001-200,000': 10,
    '200,001-400,000': 11,
    '400,001-600,000': 12,
    '600,001-800,000': 13,
    '800,001-1,000,000': 14,
    '1,000,001以上': 15,
    '差異數調整（說明4）': 16,
    '合　計': 17
}


def get_db_connection() -> pymssql.Connection:
    return pymssql.connect(
        host='172.29.60.101',
        port=1444,
        user='sa',
        password='P@55word',
        database='stock',
        charset='utf8'
    )


def query_stock_id(conn: pymssql.Connection) -> Series:
    cursor: pymssql.Cursor = conn.cursor()
    cursor.execute('SELECT * FROM company_info')
    rows: list = cursor.fetchall()
    columns: list = [column[0] for column in cursor.description]
    df: pd.DataFrame = pd.DataFrame(rows, columns=columns)
    return df["stock_id"]


def open_browser():
    options = webdriver.ChromeOptions()
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36')

    driver = webdriver.Chrome(options=options)
    driver.get("https://www.tdcc.com.tw/portal/zh/smWeb/qryStock")
    return driver


def get_stock_distribution(driver: webdriver.Chrome, stock_id: str, report_date: str, conn: pymssql.Connection):
    try:

        # 輸入股票代號
        input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[1]/div[1]/div/main/div[4]/form/table/tbody/tr[2]/td[2]/input"))
        )
        input_element.clear()
        input_element.send_keys(stock_id)

        # 選擇日期
        select_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 "/html/body/div[1]/div[1]/div/main/div[4]/form/table/tbody/tr[1]/td[2]/select")
            )
        )
        select = Select(select_element)
        select.select_by_visible_text(report_date)

        # 查詢
        search_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div[1]/div/main/div[4]/form/table/tbody/tr[4]/td/input"))
        )
        search_button.click()

        # 取得表格
        table_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[1]/div[1]/div/main/div[6]/div/table"))
        )

        rows = table_element.find_elements(By.TAG_NAME, "tr")

        headers = [header.text for header in rows[0].find_elements(
            By.TAG_NAME, "th")]

        data = []
        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            data.append([cell.text for cell in cells])

        df = pd.DataFrame(data, columns=headers)
        save_to_db(stock_id, report_date, df, conn)
        # print(df)
        print(stock_id, report_date, "成功")
        driver.refresh()
        return 1

    except pymssql.IntegrityError as e:
        print(stock_id, report_date, "資料庫已有資料")
        return 1

    except Exception as e:
        print(stock_id, report_date, "失敗", e)
        element = driver.find_element(
            By.XPATH, "/html/body/div[1]/div[1]/div/main/div[6]/div/table/tbody/tr/td/span")

        print(element.text)
        if element.text == "查無此資料":
            print(stock_id, report_date, "找不到相關資料", e)
            return 1

        driver.refresh()
        return 0
    # finally:


def save_to_db(stock_id, report_date, df, conn):
    # 先做映射
    df['持股/單位數分級'] = df['持股/單位數分級'].map(stock_holding_levels)

    # 去除逗号并转换为整数
    df['人數'] = df['人數'].str.replace(',', '').replace(
        '', '0').apply(lambda x: int(x))
    df['股數/單位數'] = df['股數/單位數'].str.replace(',',
                                            '').replace('', '0').apply(lambda x: int(x))

    report_date = pd.to_datetime(report_date, format='%Y%m%d')

    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stock_distribution (stock_id, stock_unit_range, people_count, stock_unit_count, proportion, report_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (stock_id, row['持股/單位數分級'], row['人數'], row['股數/單位數'], row['占集保庫存數比例 (%)'], report_date))
    conn.commit()


def process_stock(driver, stock_id: str, report_date: str, conn: pymssql.Connection):
    # driver = open_browser()
    # try:
    status = get_stock_distribution(driver, stock_id, report_date, conn)
    driver.quit()
    while status == 0:
        time.sleep(5)
        status = get_stock_distribution(
            driver, stock_id, report_date, conn)
        driver.quit()

    # finally:
    #     driver.quit()


report_dates = [
    # "20240524",
    "20240517",
    # "20240510",
    # "20240503",
    # "20240426",
    # "20240419",
    # "20240412",
    # "20240403",
    # "20240329",
    # "20240322",
    # "20240315",
    # "20240308",
    # "20240301",
    # "20240223",
    # "20240217",
    # "20240207",
    # "20240202",
    # "20240126",
    # "20240119",
    # "20240112",
    # "20240105",
    # "20231229",
    # "20231222",
    # "20231215",
    # "20231208",
    # "20231201",
    # "20231124",
    # "20231117",
    # "20231110",
    # "20231103",
    # "20231027",
    # "20231020",
    # "20231013",
    # "20231006",
    # "20230928",
    # "20230923",
    # "20230915",
    # "20230908",
    # "20230901",
    # "20230825",
    # "20230818",
    # "20230811",
    # "20230804",
    # "20230728",
    # "20230721",
    # "20230714",
    # "20230707",
    # "20230630",
    # "20230621",
    # "20230617",
    # "20230609",
    # "20230602",
    # "20230526"
]

# 資料庫連線
conn = get_db_connection()

# 股票id
stock_ids = query_stock_id(conn)

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    for stock_id in stock_ids:
        for report_date in report_dates:
            print(f"start: {stock_id}_{report_date}")
            futures.append(executor.submit(
                process_stock, open_browser(), stock_id, report_date, conn))

    for future in as_completed(futures):
        future.result()
conn.close()
