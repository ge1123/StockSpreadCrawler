from pandas import Series
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from datetime import datetime, timedelta
import pandas as pd
import pymssql

# stock_id: str = "2330"
# report_date: str = "20240126"


def query_stock_id() -> Series:
    # 连接到 SQL Server 数据库
    conn: pymssql.Connection = pymssql.connect(
        host='172.29.60.101',
        port=1444,
        user='sa',
        password='P@55word',
        database='stock',
        charset='utf8'
    )

    try:
        cursor: pymssql.Cursor = conn.cursor()

        cursor.execute('SELECT * FROM company_info')

        rows: list = cursor.fetchall()

        columns: list = [column[0] for column in cursor.description]

        df: pd.DataFrame = pd.DataFrame(rows, columns=columns)

        return df["stock_id"]
    finally:
        conn.close()


def get_stock_distribution(stock_id: str, report_date: str):
    options = webdriver.ChromeOptions()
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36')

    driver = webdriver.Chrome(options=options)

    try:

        driver.get("https://www.tdcc.com.tw/portal/zh/smWeb/qryStock")

        # 輸入股票代號
        input_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[1]/div[1]/div/main/div[4]/form/table/tbody/tr[2]/td[2]/input"))
        )
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
        print(df)
    finally:
        # driver.quit()
        print("hello")


report_dates = [
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


# 取得stock_id
stock_ids: Series = query_stock_id()

i = 0
for stock_id in stock_ids:
    print(stock_id)
    # for report_date in report_dates:
    #     i += 1
    #     print(stock_id, report_date, i)
