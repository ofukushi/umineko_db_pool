
# fins_all_netsales.py

import logging
import sys
from datetime import datetime
import pandas as pd
from db_utils import get_database_engine, get_table_names

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # DBエンジン
# engine, environment = get_database_engine()
# tables=get_table_names()

def calculate_and_save_growth_rates():
    # DBエンジン
    engine, environment = get_database_engine()
    tables = get_table_names()

    logging.info(f"📥 Loading '{tables['fins_all_adjusted']}' table...")

    # fins_all_adjusted テーブルを読み込む
    df = pd.read_sql_table(tables['fins_all_adjusted'], engine)
    logging.info(f"✅ Loaded {len(df)} records.")

    # EarnForecastRevision と DividendForecastRevision を削除
    df_filtered = df[~df['docname'].isin(['EarnForecastRevision', 'DividendForecastRevision'])].copy()
    logging.info(f"🧹 Filtered out revision rows. Remaining rows: {len(df_filtered)}")

    # 削除後の行数をログに出力
    logging.info(f"Number of rows after filtering out EarnForecastRevision and DividendForecastRevision: {len(df_filtered)}")

    # Initialize columns with 0.0 (float) using .loc[]
    df_filtered.loc[:, 'growth_amount'] =  0.0
    df_filtered.loc[:, 'growth_percentage'] = 0.0
    df_filtered.loc[:, 'projected_growth_rate'] = 0.0

    # Sort the DataFrame without using inplace
    df_filtered = df_filtered.sort_values(['seccode', 'quarterenddate'])

    grouped = df_filtered.groupby('seccode')
    logging.info(f"📊 Grouped by seccode. Total unique seccodes: {len(grouped)}")

    for name, group in grouped:
        logging.info(f"🔁 Processing NetSales for seccode: {name} with {len(group)} rows")
        for i, row in group.iterrows():
            # QonQ Growth Calculation
            previous_year_row = group[
                (group['fiscalyearend'].dt.year == row['fiscalyearend'].year - 1) &  # 年度で比較
                (group['quarter'] == row['quarter'])  # 四半期で比較
            ]
            
            if not previous_year_row.empty:
                prev_netsales = previous_year_row['netsales'].values[0]
                growth_amount = row['netsales'] - prev_netsales
                growth_percentage = (growth_amount / prev_netsales) * 100 if prev_netsales != 0 else None
                df_filtered.loc[i, 'growth_amount'] = growth_amount
                df_filtered.loc[i, 'growth_percentage'] = growth_percentage
            else:
                df_filtered.loc[i, 'growth_amount'] = 0.0
                df_filtered.loc[i, 'growth_percentage'] = 0.0    

            # Projected Growth Rate Calculation
            if row['quarter'] == 'FY' and pd.notnull(row['nextyrfcastnetsales']) and row['nextyrfcastnetsales'] != 0:
                if row['netsales'] != 0:
                    projected_growth = ((row['nextyrfcastnetsales'] - row['netsales']) / row['netsales']) * 100
                    df_filtered.loc[i, 'projected_growth_rate'] = projected_growth
                else:
                    df_filtered.loc[i, 'projected_growth_rate'] = 0.0

            elif row['quarter'] != 'FY' and pd.notnull(row['fcastnetsales']) and row['fcastnetsales'] != 0:
                previous_fy_row = group[
                    (group['fiscalyearend'].dt.year == row['fiscalyearend'].year - 1) &  # 年度で比較
                    (group['quarter'] == 'FY')
                ]
                if not previous_fy_row.empty:
                    prev_netsales = previous_fy_row['netsales'].values[0]
                    projected_growth = ((row['fcastnetsales'] - prev_netsales) / prev_netsales) * 100 if prev_netsales != 0 else None
                    df_filtered.loc[i, 'projected_growth_rate'] = projected_growth
                    
    # DataFrameに現在のタイムスタンプを追加
    df_filtered['timestamp'] = datetime.now()

    # Define the desired column order
    columns_order = [
        'timestamp',
        'filingdate',
        'earn_flag',  # EarnForecastRevision のフラグ
        'div_flag',   # DividendForecastRevision のフラグ
        'docname', 
        'seccode',
        'companyname',
        'fiscalyearend',
        'quarter',
        'quarterenddate',
        'netsales',
        'growth_amount',
        'growth_percentage',
        'projected_growth_rate',
        'fcastnetsales',
        'nextyrfcastnetsales'
    ]

    # Reorder the dataframe according to the desired column order
    netsales_df  = df_filtered[columns_order]

    # Save to the database
    with engine.connect() as conn:
        with conn.begin():
            logging.info(f"Saving netsales QonQ and projected growth data to the {tables['fins_all_netsales']} table...")
            # Write the number of rows before saving
            logging.info(f"Number of rows to save: {len(netsales_df)}")
            # Save to the database with replace
            netsales_df.to_sql(tables['fins_all_netsales'], conn, if_exists='replace', index=False)
            logging.info(f"✅ netsales data saved to the {tables['fins_all_netsales']} table (replaced).")

if __name__ == "__main__":
    logging.info("🚀 Starting the script 'NetSales'...")
    try:
        calculate_and_save_growth_rates()
    except Exception as e:
        logging.error(f"❌ An error occurred: {e}")

    logging.info("✅ 'NetSales' Growth rates calculated and saved successfully.")   
