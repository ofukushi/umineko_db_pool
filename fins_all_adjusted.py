
# fins_all_adjusted.py

import pandas as pd
from datetime import datetime
import logging
from db_utils import get_database_engine, get_table_names

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_and_process_data():
    logging.info(f"🚀Script 'fins_all_adjusted' started...")
    # DBエンジン
    engine, environment = get_database_engine()
    tables = get_table_names()

    logging.info(f"Loading data from {tables['fins_all']} table...")
    df = pd.read_sql_table(tables["fins_all"], engine)
    logging.info(f"✅ Loaded {len(df)} records.")

    if df.empty:
        logging.warning(f"{tables['fins_all']} table is empty.")

    # seccode でグループ化し、filingdate で昇順にソート
    logging.info("Grouping by seccode and sorting by filingdate (ascending)...")
    df_sorted = df.sort_values(['seccode', 'filingdate'], ascending=[True, True])

    # データの読み込み、ソート、グループ化は前と同様
    df_sorted['earn_flag'] = None  # EarnForecastRevision フラグ
    df_sorted['div_flag'] = None   # DividendForecastRevision フラグ

    # 各グループの最終行が EarnForecastRevision または DividendForecastRevision である場合、その行をログに出力
    grouped = df_sorted.groupby('seccode')
    logging.info(f"📊 Grouped by seccode. Total unique seccodes: {len(grouped)}")

    for seccode, group in grouped:
        logging.info(f"🔁 Adjusting seccode: {seccode} with {len(group)} rows")
        last_row = group.iloc[-1]  # 最終行を取得

        # Initialize `previous_value` outside the loop to avoid UnboundLocalError
        previous_value = None

        # 直前の行を探す
        previous_row = None
        for i in range(len(group) - 2, -1, -1):
            potential_prev_row = group.iloc[i]
            if potential_prev_row['docname'] not in ['EarnForecastRevision', 'DividendForecastRevision']:
                previous_row = potential_prev_row
                break

        # EarnForecastRevision の処理
        if last_row['docname'] == 'EarnForecastRevision' and previous_row is not None:
            # quarter が FY であり、かつ fiscalyearend が一致する場合のみ更新
            if last_row['fiscalyearend'] == previous_row['fiscalyearend']: # test 2QとかにFYの修正も載せている会社があるため
                columns_to_update = ['fcastnetsales', 'fcastopprofit', 'fcastordprofit', 'fcastprofit', 'fcastdivannual']
                is_any_field_updated = False  # いずれかのカラムが更新されたかを追跡

                # ゼロ以外の値で上書き
                for col in columns_to_update:
                    original_value = last_row[col]
                    # Only assign `previous_value` if `original_value` is valid to avoid UnboundLocalError
                    if pd.notnull(original_value) and original_value != 0:
                        previous_value = previous_row[col]
                        logging.info(f"Updating {col}: {previous_value} -> {original_value} for seccode {seccode}")
                        group.at[previous_row.name, col] = original_value  # previous_row を更新
                        is_any_field_updated = True  # いずれかのカラムが更新されたことを記録
                    else:
                        # `previous_value` is safely used here because it is initialized as None
                        logging.info(f"No update for {col}: {previous_value} (Original: {original_value}) for seccode {seccode}")

                # いずれかのカラムが更新された場合のみ、filingdate を更新
                if is_any_field_updated:
                    logging.info(f"Updating filingdate: {previous_row['filingdate']} -> {last_row['filingdate']} for seccode {seccode}")
                    group.at[previous_row.name, 'filingdate'] = last_row['filingdate']
                    # Earn_flag を設定
                    group.at[previous_row.name, 'earn_flag'] = 'Updated'
                    logging.info(f"earn_flag set for seccode {seccode}, row {previous_row.name}")
                    
        # DividendForecastRevision の処理
        elif last_row['docname'] == 'DividendForecastRevision' and previous_row is not None:
            # quarter が FY であり、かつ fiscalyearend が一致する場合のみ更新
            if last_row['fiscalyearend'] == previous_row['fiscalyearend']: # test QとかにFYの修正も載せている会社があるため
                columns_to_update = ['fcastdivannual']
                is_any_field_updated = False  # いずれかのカラムが更新されたかを追跡

                # ゼロ以外の値で上書き
                for col in columns_to_update:
                    original_value = last_row[col]
                    # Assign `previous_value` here for dividend updates
                    previous_value = previous_row[col]  # Initialize `previous_value` here
                    logging.info(f"Checking column {col} for seccode {seccode}, Original: {original_value}, Previous: {previous_row[col]}")  # デバッグ用

                    if pd.notnull(original_value) and original_value != 0:
                        previous_value = previous_row[col]
                        logging.info(f"Updating {col}: {previous_value} -> {original_value} for seccode {seccode}")
                        group.at[previous_row.name, col] = original_value  # previous_row を更新
                        is_any_field_updated = True  # いずれかのカラムが更新されたことを記録
                    else:
                        logging.info(f"No update for {col}: {previous_value} (Original: {original_value}) for seccode {seccode}")

                # いずれかのカラムが更新された場合のみ、filingdate を更新
                if is_any_field_updated:
                    logging.info(f"Updating filingdate: {previous_row['filingdate']} -> {last_row['filingdate']} for seccode {seccode}")
                    group.at[previous_row.name, 'filingdate'] = last_row['filingdate']
                    # Div_flag を設定
                    group.at[previous_row.name, 'div_flag'] = 'Updated'
                    logging.info(f"div_flag set for seccode {seccode}, row {previous_row.name}")

        # 更新された group を元の df に反映
        df_sorted.update(group)

    # DataFrameに現在のタイムスタンプを追加
    df_sorted['timestamp'] = datetime.now()
    
    # カラムの順序を再設定
    fins_all_adjusted_columns_order = [
        'timestamp',
        'filingdate',
        'earn_flag',  # EarnForecastRevision のフラグ
        'div_flag',   # DividendForecastRevision のフラグ
        'revisions',
        'docname',
        'seccode',
        'companyname',
        'fiscalyearend',
        'quarter',
        'quarterenddate',
        'totassets',
        'equity',
        'netsales',
        'opprofit',
        'ordprofit',
        'profit',
        'divannual',
        'fcastnetsales',
        'fcastopprofit',
        'fcastordprofit',
        'fcastprofit',
        'fcastdivannual',
        'nextyrfcastnetsales',
        'nextyrfcastopprofit',
        'nextyrfcastordprofit',
        'nextyrfcastprofit',
        'nextyrfcastdivannual',
        'issuedsharesincltreasury',
        'treasuryshares'
    ]

    # カラムの順番を適用
    df_sorted = df_sorted[fins_all_adjusted_columns_order]

    # データベースに保存
    df_sorted.to_sql(tables["fins_all_adjusted"], engine, if_exists='replace', index=False)
    logging.info(f"Updated data with flags saved to '{tables['fins_all_adjusted']}'.")


if __name__ == "__main__":
    try:
        logging.info(f"🚀Script 'fins_all_adjusted' started...")
        load_and_process_data()
    except Exception as e:
        logging.exception(f"Unhandled error: {e}")
        logging.info(f"✅Script'fins_all_adjusted' completed.")



