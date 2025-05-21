
# Required environment variables:
# - AWS_ACCESS_KEY_ID: AWS_ACCESS_KEY_ID
# - AWS_SECRET_ACCESS_KEY: AWS_SECRET_ACCESS_KEY
# - EMAIL_ADDRESS: G_MAIL_ADDRESS
# - PASSWORD: J_QUANTS_PASSWORD
# - HEROKU_DATABASE_URL: HEROKU_DATABASE_URL
# - RENDER_DATABASE_URL: RENDER_DATABASE_URL
# - External_RENDER_DATABASE_URL: External_RENDER_DATABASE_URL
# - LOCAL_DATABASE_URL: LOCAL_DATABASE_URL

#  Options for overriding defaults:
# - HEROKU_ENV: defaults to "false" for saving
# - RENDER_ENV: defaults to "false" for saving
# - USE_S3: defaults to "false" for determining whether to use S3 or local JSON files
# - S3_BUCKET_NAME: defaults to "jquants-json"
# - LOCAL_JSON_DIR: defaults to "/mnt/c/Users/osamu/OneDrive/jquants_json_data"

# fins_all.py

import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pytz
from fins_all_adjusted import load_and_process_data
from fins_all_bps_opvalues import process_and_save_operation_values
from fins_all_netsales import calculate_and_save_growth_rates
from db_utils import get_database_engine, get_table_names
from datetime import datetime, timedelta, timezone
import logging
from jquants_api import JQuantsAPI
import boto3

# --- Logging setup ---
JST = timezone(timedelta(hours=9))
logging.Formatter.converter = lambda *args: datetime.now(JST).timetuple()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_statements_from_json(root_folder):
    all_statements = []
    total_files = 0
    for year in sorted(os.listdir(root_folder)):
        year_path = os.path.join(root_folder, year)
        if not os.path.isdir(year_path):
            continue
        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue
            for file in sorted(os.listdir(month_path)):
                if file.endswith(".json"):
                    total_files += 1
    logging.info(f"📂 Found {total_files} JSON files in local folder.")

    processed = 0
    for year in sorted(os.listdir(root_folder)):
        year_path = os.path.join(root_folder, year)
        if not os.path.isdir(year_path):
            continue
        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue
            for file in sorted(os.listdir(month_path)):
                if file.endswith(".json"):
                    file_path = os.path.join(month_path, file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        statements = data.get("statements", [])
                        for statement in statements:
                            if 'Foreign' not in statement.get("TypeOfDocument", "") and 'REIT' not in statement.get("TypeOfDocument", ""):
                                statement['CompanyName'] = company_dict.get(statement['LocalCode'], 'Unknown')
                                statement['timestamp'] = datetime.now()
                                all_statements.append(statement)
                    processed += 1
                    if processed % 100 == 0 or processed == total_files:
                        logging.info(f"✅ Processed {processed}/{total_files} local JSON files...")
    return all_statements

def load_statements_from_s3(bucket_name):
    s3 = boto3.client('s3')

    paginator = s3.get_paginator('list_objects_v2')
    all_statements = []
    count = 0
    logging.info(f"📡 Loading JSON files from S3 bucket: {bucket_name}")
    for result in paginator.paginate(Bucket=bucket_name):
        for obj in result.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                data = json.load(file_obj["Body"])
                statements = data.get("statements", [])
                for statement in statements:
                    if 'Foreign' not in statement.get("TypeOfDocument", "") and 'REIT' not in statement.get("TypeOfDocument", ""):
                        statement['CompanyName'] = company_dict.get(statement['LocalCode'], 'Unknown')
                        statement['timestamp'] = datetime.now()
                        all_statements.append(statement)
                count += 1
                if count % 100 == 0:
                    logging.info(f"✅ Processed {count} S3 JSON files...")
    logging.info(f"📦 Total S3 JSON files processed: {count}")
    return all_statements

def transform_TypeOfCurrentPeriod(TypeOfCurrentPeriod):
    return TypeOfCurrentPeriod[:-1] if TypeOfCurrentPeriod.endswith('Q') else TypeOfCurrentPeriod

def find_revisions(doc_name):
    if 'EarnForecastRevision' in doc_name:
        return 'EarnForecastRevision'
    elif 'DividendForecastRevision' in doc_name:
        return 'DividendForecastRevision'
    return None

def transform_fins_dataframe(all_data):
    df = pd.DataFrame(all_data)
    df['revisions'] = df['TypeOfDocument'].apply(find_revisions)
    df['LocalCode'] = df['LocalCode'].str[:4]
    df['quarter'] = df['TypeOfCurrentPeriod'].apply(transform_TypeOfCurrentPeriod)

    column_mapping = {
        'timestamp': 'timestamp',
        'DisclosedDate': 'filingdate',
        'TypeOfDocument': 'docname',
        'LocalCode': 'seccode',
        'CompanyName': 'companyname',
        'CurrentFiscalYearEndDate': 'fiscalyearend',
        'Quarter': 'quarter',
        'CurrentPeriodEndDate': 'quarterenddate',
        'TotalAssets': 'totassets',
        'Equity': 'equity',
        'NetSales': 'netsales',
        'OperatingProfit': 'opprofit',
        'OrdinaryProfit': 'ordprofit',
        'Profit': 'profit',
        'EarningsPerShare': 'earningspershare',
        'ResultDividendPerShareAnnual': 'divannual',
        'ForecastNetSales': 'fcastnetsales',
        'ForecastOperatingProfit': 'fcastopprofit',
        'ForecastOrdinaryProfit': 'fcastordprofit',
        'ForecastProfit': 'fcastprofit',
        'ForecastDividendPerShareAnnual': 'fcastdivannual',
        'NextYearForecastNetSales': 'nextyrfcastnetsales',
        'NextYearForecastOperatingProfit': 'nextyrfcastopprofit',
        'NextYearForecastOrdinaryProfit': 'nextyrfcastordprofit',
        'NextYearForecastProfit': 'nextyrfcastprofit',
        'NextYearForecastDividendPerShareAnnual': 'nextyrfcastdivannual',
        'NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock': 'issuedsharesincltreasury',
        'NumberOfTreasuryStockAtTheEndOfFiscalYear': 'treasuryshares',
        'revisions': 'revisions'
    }

    df.rename(columns=column_mapping, inplace=True)
    missing_cols = [col for col in column_mapping.values() if col not in df.columns]
    for col in missing_cols:
        df[col] = None

    df = df[list(column_mapping.values())]
    df['timestamp'] = datetime.now()
    df['fiscalyearend'] = pd.to_datetime(df['fiscalyearend'], errors='coerce')
    df['filingdate'] = pd.to_datetime(df['filingdate'], errors='coerce')
    df['quarterenddate'] = pd.to_datetime(df['quarterenddate'], errors='coerce')

    numeric_columns = [
        'totassets', 'equity', 'netsales', 'opprofit', 'ordprofit', 'profit', 'earningspershare', 'divannual',
        'fcastnetsales', 'fcastopprofit', 'fcastordprofit', 'fcastprofit', 'fcastdivannual',
        'nextyrfcastnetsales', 'nextyrfcastopprofit', 'nextyrfcastordprofit', 'nextyrfcastprofit',
        'nextyrfcastdivannual', 'issuedsharesincltreasury', 'treasuryshares'
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    df = df[df['companyname'] != 'Unknown']

    return df, list(column_mapping.values())

def save_to_database(fins_df, columns_order, engine, environment, tables):
    with engine.connect() as conn:
        with conn.begin() as transaction:
            logging.info(f"Replacing data in {environment} database...")
            fins_df.to_sql(tables['fins_all'], conn, if_exists='replace', index=False)
            transaction.commit()
            logging.info(f"fins_all replaced in {environment} database.")

def process_new_data():
    try:
        load_and_process_data()
        logging.info("fins_all_adjusted completed successfully.")
    except Exception as e:
        logging.error(f"Error in load_and_process_data: {e}")

    try:
        calculate_and_save_growth_rates()
        logging.info("fins_all_netsales completed successfully.")
    except Exception as e:
        logging.error(f"Error in calculate_and_save_growth_rates: {e}")

    try:
        process_and_save_operation_values()
        logging.info("fins_all_bps_opvalues completed successfully.")
    except Exception as e:
        logging.error(f"Error in process_and_save_operation_values: {e}")

if __name__ == "__main__":
    load_dotenv(dotenv_path="/mnt/c/Users/osamu/OneDrive/onedrive_python_source/.env")
    use_s3 = os.getenv("USE_S3", "true").lower() == "true"
    EMAIL_ADDRESS = os.getenv("G_MAIL_ADDRESS")
    PASSWORD = os.getenv("J_QUANTS_PASSWORD")
    api = JQuantsAPI(EMAIL_ADDRESS, PASSWORD)

    try:
        api.get_refresh_token()
        api.get_id_token()
    except Exception as e:
        logging.error(f"Failed to authenticate: {e}")
        company_dict = {}
    else:
        try:
            company_info = api.fetch_company_info()
            company_dict = {info['Code']: info['CompanyName'] for info in company_info}
        except Exception as e:
            logging.error(f"Failed to fetch company info: {e}")
            company_dict = {}

    if use_s3:
        bucket = os.getenv("S3_BUCKET_NAME", "jquants-json")
        all_statements = load_statements_from_s3(bucket)
    else:
        base_folder = os.getenv("LOCAL_JSON_DIR", "/mnt/c/Users/osamu/OneDrive/jquants_json_data")
        all_statements = load_statements_from_json(base_folder)

    logging.info(f"✅ Loaded {len(all_statements)} statements.")

    if all_statements:
        df, columns_order = transform_fins_dataframe(all_statements)

        engine, environment = get_database_engine()
        tables = get_table_names()

        save_to_database(df, columns_order, engine, environment, tables)
        process_new_data()
    else:
        logging.info("📭 No statements found to process.")
