# 匯入必要套件
import requests
import pandas as pd
from loguru import logger
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Text,  # 使用 Text 型別來儲存長文本，如職缺描述
)
from sqlalchemy.dialects.mysql import insert # 專用於 MySQL 的 insert 語法
from requests.exceptions import HTTPError, JSONDecodeError

# --- 複製自 task_fetch_104_data.py 的爬蟲函式 ---
def fetch_104_data(url: str) -> dict:
    """
    根據提供的 104 職缺 URL，抓取其詳細資訊。

    Args:
        url (str): 104 人力銀行的職缺網址。

    Returns:
        dict: 包含職缺詳細資訊的字典，如果失敗則返回空字典。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'referer': 'https://www.104.com.tw/'
    }

    try:
        job_id = url.split('/')[-1].split('?')[0]
    except IndexError:
        logger.error(f"無法從 URL 中解析 job_id: {url}")
        return {}

    url_api = f'https://www.104.com.tw/job/ajax/content/{job_id}'
    
    try:
        response = requests.get(url_api, headers=headers)
        response.raise_for_status()
        data = response.json()
    except (HTTPError, JSONDecodeError) as err:
        logger.error(f"請求 API 時發生錯誤: {err}")
        return {}
    
    job_data = data.get('data', {})
    if not job_data or job_data.get('custSwitch', {}) == "off":
        logger.warning(f"職缺(ID:{job_id})內容不存在或已關閉")
        return {}

    extracted_info = {
        'job_id': job_id,
        'update_date': job_data.get('header', {}).get('appearDate'),
        'title': job_data.get('header', {}).get('jobName'),
        'description': job_data.get('jobDetail', {}).get('jobDescription'),
        'salary': job_data.get('jobDetail', {}).get('salary'),
        'work_type': job_data.get('jobDetail', {}).get('workType'),
        'work_time': job_data.get('jobDetail', {}).get('workPeriod'),
        'location': job_data.get('jobDetail', {}).get('addressRegion'),
        'degree': job_data.get('condition', {}).get('edu'),
        'department': job_data.get('jobDetail', {}).get('department'),
        'working_experience': job_data.get('condition', {}).get('workExp'),
        'qualification_required': job_data.get('condition', {}).get('other'),
        'qualification_bonus': job_data.get('welfare', {}).get('welfare'),
        'company_id': job_data.get('header', {}).get('custNo'),
        'company_name': job_data.get('header', {}).get('custName'),
        'company_address': job_data.get('company', {}).get('address'),
        'contact_person': job_data.get('contact', {}).get('hrName'),
        'contact_phone': job_data.get('contact', {}).get('email', '未提供')
    }

    return extracted_info

if __name__ == "__main__":
    # --- 資料庫設定與資料表定義 ---
    
    # 1. 定義資料庫連線字串
    address = "mysql+pymysql://root:test@127.0.0.1:3306/mydb"
    engine = create_engine(address)
    
    # 2. 定義資料表結構 (Schema)
    metadata = MetaData()
    jobs_104_table = Table(
        "jobs_104", # 資料表名稱
        metadata,
        Column("job_id", String(50), primary_key=True), # 主鍵
        Column("update_date", String(50)),
        Column("title", String(255)),
        Column("description", Text),
        Column("salary", String(255)),
        Column("work_type", String(50)),
        Column("work_time", String(100)),
        Column("location", String(100)),
        Column("degree", String(100)),
        Column("department", String(255)),
        Column("working_experience", String(100)),
        Column("qualification_required", Text),
        Column("qualification_bonus", Text),
        Column("company_id", String(50)),
        Column("company_name", String(255)),
        Column("company_address", String(255)),
        Column("contact_person", String(100)),
        Column("contact_phone", String(255)),
    )
    
    # 3. 自動建立資料表 (如果不存在)
    logger.info("檢查並建立資料表 'jobs_104'...")
    metadata.create_all(engine)
    logger.info("資料表準備完成。")

    # --- 資料抓取與寫入 ---
    
    # 4. 指定要抓取的職缺 URL (可替換成任何有效的 104 職缺網址)
    job_url = "https://www.104.com.tw/job/8863t"
    
    logger.info(f"正在從 {job_url} 抓取資料...")
    job_details = fetch_104_data(job_url)

    if not job_details:
        logger.error("抓取資料失敗，程式終止。")
    else:
        logger.info("資料抓取成功！準備寫入資料庫...")
        
        # 5. 建立 Insert 語句
        # 使用 SQLAlchemy 的 insert() 函式，並傳入要寫入的資料
        insert_stmt = insert(jobs_104_table).values(**job_details)
        
        # 6. 加上 ON DUPLICATE KEY UPDATE 的邏輯
        # 如果主鍵 (job_id) 重複，就更新所有非主鍵的欄位
        # 使用字典推導式來動態產生要更新的欄位
        update_stmt = insert_stmt.on_duplicate_key_update(
            **{
                # col.name 是欄位名稱
                # insert_stmt.inserted[col.name] 是要插入的新值
                col.name: insert_stmt.inserted[col.name]
                for col in jobs_104_table.columns
                if not col.primary_key # 只更新非主鍵的欄位
            }
        )
        
        # 7. 執行 SQL 語句
        try:
            # 使用 with engine.begin() as conn: 可以自動處理交易和連線關閉
            with engine.begin() as conn:
                conn.execute(update_stmt)
            logger.info(f"資料 (job_id: {job_details['job_id']}) 已成功插入或更新至資料庫。")
        except Exception as e:
            logger.error(f"寫入資料庫時發生錯誤: {e}")