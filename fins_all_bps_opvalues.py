
# fins_all_bps_opvalues.py

from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import pandas as pd
import traceback
import logging
from db_utils import get_database_engine, get_table_names

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 理論価値計算のための関数
def calculate_operation_values(company_data):

    # IssuedSharesInclTreasury がゼロや空欄（NaN）の行を除外
    company_data_filtered = company_data.dropna(subset=['issuedsharesincltreasury'])
    company_data_filtered = company_data_filtered[company_data_filtered['issuedsharesincltreasury'] > 0]

    if company_data_filtered.empty:
        logging.info(f"No valid shares data for seccode {company_data['seccode'].iloc[0]}. Skipping this company.")

        return []  # 空のリストを返して、その企業の処理をスキップ
    
    # quarterenddate を ソート
    company_data_filtered = company_data_filtered.sort_values(by='quarterenddate')

    # 最も新しい quarterenddate の IssuedSharesInclTreasury を取得
    latest_shares = company_data_filtered['issuedsharesincltreasury'].iloc[-1]

    results = []

    for i, row in company_data.iterrows():
        bps = 0.0
        bps_eval = 0.0
        opvalue = 0.0
        fcastopvalue = 0.0
        nextyrfcastopvalue = 0.0
        original_divannual_for_chart = 0.0
        adjusted_divannual_for_chart = 0.0
        original_fcastdivannual_for_chart = 0.0
        adjusted_fcastdivannual_for_chart = 0.0
        equityratio = 0.0
        assetevalrate = 0.0
        roaleverage = 0.0
        eps = 0.0
        fcasteps = 0.0
        nextyrfcasteps = 0.0
        roa = 0.0
        fcastroa = 0.0
        nextyrfcastroa = 0.0
        fairvalue = 0.0
        fcastfairvalue = 0.0
        nextyrfcastfairvalue = 0.0
        
        try:
            if row['totassets'] > 0 and row['issuedsharesincltreasury'] > 0:
                previous_shares = row['issuedsharesincltreasury']
                original_divannual_for_chart = row['divannual']
                original_fcastdivannual_for_chart = row['nextyrfcastdivannual']

                adjusted_divannual_for_chart = original_divannual_for_chart
                if pd.isna(adjusted_divannual_for_chart) or adjusted_divannual_for_chart == 0:
                    adjusted_divannual_for_chart = row['fcastdivannual']

                if previous_shares > 0 and previous_shares != latest_shares:
                    adjusted_divannual_for_chart *= (previous_shares / latest_shares)

                adjusted_fcastdivannual_for_chart = original_fcastdivannual_for_chart
                if pd.isna(adjusted_fcastdivannual_for_chart) or adjusted_fcastdivannual_for_chart == 0:
                    adjusted_fcastdivannual_for_chart = row['fcastdivannual']

                if previous_shares > 0 and previous_shares != latest_shares:
                    adjusted_fcastdivannual_for_chart *= (previous_shares / latest_shares)

                equityratio = row['equity'] / row['totassets']
                if equityratio < 0.1:
                    assetevalrate = 0.5
                elif 0.1 <= equityratio < 0.33:
                    assetevalrate = 0.6
                elif 0.33 <= equityratio < 0.5:
                    assetevalrate = 0.65
                elif 0.5 <= equityratio < 0.67:
                    assetevalrate = 0.7
                elif 0.67 <= equityratio < 0.8:
                    assetevalrate = 0.75
                else:
                    assetevalrate = 0.8

                bps_eval = row['equity'] * assetevalrate / latest_shares
                bps = row['equity'] / latest_shares
                roaleverage = 1 / (equityratio + 0.33)
                roaleverage = min(max(roaleverage, 1), 1.5)

                OPERATION_VALUE_MULTIPLIER = 150
                eps_divider = latest_shares
                roa_divider = row['totassets']

                if pd.isna(row['ordprofit']) or row['ordprofit'] == 0:
                    eps = row['profit'] / eps_divider
                    roa = row['profit'] / roa_divider
                else:
                    eps = row['ordprofit'] * 0.7 / eps_divider
                    roa = row['ordprofit'] * 0.7 / roa_divider

                roa = abs(roa)
                roa = min(roa, 0.3)

                opvalue = eps * roa * OPERATION_VALUE_MULTIPLIER * roaleverage
                fairvalue = bps_eval + max(0, opvalue)

                if row['quarter'] == "FY":
                    if pd.isna(row['nextyrfcastordprofit']) or row['nextyrfcastordprofit'] == 0:
                        nextyrfcasteps = row['nextyrfcastprofit'] / eps_divider
                        nextyrfcastroa = row['nextyrfcastprofit'] / roa_divider
                    else:
                        nextyrfcasteps = row['nextyrfcastordprofit'] * 0.7 / eps_divider
                        nextyrfcastroa = row['nextyrfcastordprofit'] * 0.7 / roa_divider

                    nextyrfcastroa = abs(nextyrfcastroa)
                    nextyrfcastroa = min(nextyrfcastroa, 0.3)

                    nextyrfcastopvalue = nextyrfcasteps * nextyrfcastroa * OPERATION_VALUE_MULTIPLIER * roaleverage
                    nextyrfcastfairvalue = bps_eval + max(0, nextyrfcastopvalue)

                else:
                    if pd.isna(row['fcastordprofit']) or row['fcastordprofit'] == 0:
                        fcasteps = row['fcastprofit'] / eps_divider
                        fcastroa = row['fcastprofit'] / roa_divider
                    else:
                        fcasteps = row['fcastordprofit'] * 0.7 / eps_divider
                        fcastroa = row['fcastordprofit'] * 0.7 / roa_divider

                    fcastroa = abs(fcastroa)
                    fcastroa = min(fcastroa, 0.3)

                    fcastopvalue = fcasteps * fcastroa * OPERATION_VALUE_MULTIPLIER * roaleverage
                    fcastfairvalue = bps_eval + max(0, fcastopvalue)

                    nextyrfcastopvalue = fcastopvalue
                    nextyrfcastfairvalue = fcastfairvalue

                result_row = {
                    'timestamp': datetime.now(),
                    'filingdate': row['filingdate'],
                    'seccode': row['seccode'],
                    'companyname': row['companyname'],
                    'quarter': row['quarter'],
                    'quarterenddate': row['quarterenddate'],
                    'bps': bps,
                    'bps_eval':bps_eval,
                    'opvalue': opvalue,
                    'fcastopvalue': fcastopvalue,
                    'nextyrfcastopvalue': nextyrfcastopvalue,
                    'original_divannual_for_chart': original_divannual_for_chart,
                    'adjusted_divannual_for_chart': adjusted_divannual_for_chart,
                    'original_fcastdivannual_for_chart': original_fcastdivannual_for_chart,
                    'adjusted_fcastdivannual_for_chart': adjusted_fcastdivannual_for_chart,
                    'divannual': row['divannual'],  
                    'fcastdivannual': row['fcastdivannual'],  
                    'nextyrfcastdivannual': row['nextyrfcastdivannual'],  
                    'fiscalyearend': row['fiscalyearend'],
                    'issuedsharesincltreasury': row['issuedsharesincltreasury'],
                    'latest_shares': latest_shares,
                    'totassets': row['totassets'],
                    'equity': row['equity'],
                    'equityratio': equityratio,
                    'assetevalrate': assetevalrate,
                    'roaleverage': roaleverage,
                    'eps': eps,
                    'fcasteps': fcasteps,
                    'nextyrfcasteps': nextyrfcasteps,
                    'roa': round(roa, 2),
                    'fcastroa': fcastroa,
                    'nextyrfcastroa': nextyrfcastroa,
                    'fairvalue': fairvalue,
                    'fcastfairvalue': fcastfairvalue,
                    'nextyrfcastfairvalue': nextyrfcastfairvalue,
                    'docname': row['docname']
                }
                results.append(result_row)

        except Exception as e:
            logging.warning(f"Error processing row for seccode {row['seccode']} on {row['filingdate']}: {e}")
            logging.debug(traceback.format_exc())

            continue

    return results

def calculate_and_add_growth_rates(df):
    # fiscalyearendのyearを使用して年度ベースで比較する
    df['fiscalyearend_year'] = df['fiscalyearend'].dt.year

    # Sort by SecCode and QuarterEndDate
    df.sort_values(['seccode', 'quarterenddate'], inplace=True)

    grouped = df.groupby('seccode')
    logging.info(f"📊 Grouped by seccode. Total unique seccodes: {len(grouped)}")

    for name, group in grouped:
        logging.info(f"🔁 Processing BPS OpValue for seccode: {name} with {len(group)} rows")
        for i, row in group.iterrows():
            growth_amount_opvalue = 0.0
            growth_percentage_opvalue = 0.0
            projected_growth_rate_opvalue = 0.0

            # 成長率計算: FY（年度末）の場合
            if row['quarter'] == 'FY':
                if pd.notnull(row['nextyrfcastopvalue']) and row['nextyrfcastopvalue'] != 0:
                    growth_amount_opvalue = row['nextyrfcastopvalue'] - row['opvalue']
                    if row['opvalue'] != 0:
                        growth_percentage_opvalue = (growth_amount_opvalue / abs(row['opvalue'])) * 100
            else:
                # 前年度の同じ四半期データを検索する
                previous_fy_row = group[
                    (group['fiscalyearend_year'] == row['fiscalyearend_year'] - 1) &  # 年度で比較
                    (group['quarter'] == 'FY')  # 四半期で比較
                ]
                if not previous_fy_row.empty:
                    prev_fy_opvalue = previous_fy_row['opvalue'].values[0]
                    growth_amount_opvalue = row['fcastopvalue'] - prev_fy_opvalue
                    if prev_fy_opvalue != 0:
                        growth_percentage_opvalue = (growth_amount_opvalue / abs(prev_fy_opvalue)) * 100

            # 成長率計算: FYの場合
            if row['quarter'] == 'FY':
                if pd.notnull(row['nextyrfcastopvalue']) and row['nextyrfcastopvalue'] != 0:
                    if row['opvalue'] != 0:
                        projected_growth_rate_opvalue = ((row['nextyrfcastopvalue'] - row['opvalue']) / abs(row['opvalue'])) * 100
            else:
                # 前年度のデータを基に予測成長率を計算する
                if pd.notnull(row['fcastopvalue']) and row['fcastopvalue'] != 0:
                    previous_fy_row = group[
                        (group['fiscalyearend_year'] == row['fiscalyearend_year'] - 1) &  # 年度で比較
                        (group['quarter'] == 'FY')  # 四半期で比較
                    ]
                    if not previous_fy_row.empty:
                        prev_fy_opvalue = previous_fy_row['opvalue'].values[0]
                        if prev_fy_opvalue != 0:
                            projected_growth_rate_opvalue = ((row['fcastopvalue'] - prev_fy_opvalue) / abs(prev_fy_opvalue)) * 100

            # 計算結果をDataFrameに格納
            df.at[i, 'growth_amount_opvalue'] = growth_amount_opvalue
            df.at[i, 'growth_percentage_opvalue'] = growth_percentage_opvalue
            df.at[i, 'projected_growth_rate_opvalue'] = projected_growth_rate_opvalue

    return df

def process_and_save_operation_values():
    logging.info("Connecting to the database...")

    # DBエンジン
    engine, environment = get_database_engine()
    tables = get_table_names()

    try:
        with engine.connect() as conn:
            logging.info(f"Loading data from '{tables['fins_all_adjusted']}'...")  
            source_df = pd.read_sql_table(tables['fins_all_adjusted'], engine)
            logging.info(f"Loaded {len(source_df)} rows from '{tables['fins_all_adjusted']}'.")

            # EarnForecastRevision と DividendForecastRevision を削除
            logging.info("Removing 'EarnForecastRevision' and 'DividendForecastRevision' rows...")
            source_df_filtered = source_df[~source_df['docname'].isin(['EarnForecastRevision', 'DividendForecastRevision'])]
            logging.info(f"Remaining rows after filtering: {len(source_df_filtered)}.")

            seccode_list = source_df_filtered['seccode'].unique()
            logging.info(f"Found {len(seccode_list)} unique seccodes.")

            results = []
            for seccode in seccode_list:
                company_data = source_df_filtered[source_df_filtered['seccode'] == seccode]
                seccode_results = calculate_operation_values(company_data)
                results.extend(seccode_results)

            # Convert results to DataFrame and calculate growth rates at the same time
            logging.info(f"Final DataFrame shape before growth calculation: {len(results)} rows.")
            opvalue_growth_df = calculate_and_add_growth_rates(pd.DataFrame(results))

            # カラム順を指定
            localserver_u_fins_all_bps_opvalues_columns_order = [
                'timestamp', 
                'filingdate', 
                'seccode', 
                'companyname', 
                'quarter', 
                'quarterenddate', 
                'bps', 
                'bps_eval',
                'opvalue', 
                'growth_amount_opvalue', 
                'growth_percentage_opvalue', 
                'fcastopvalue', 
                'projected_growth_rate_opvalue', 
                'nextyrfcastopvalue',
                'original_divannual_for_chart', 
                'adjusted_divannual_for_chart',
                'original_fcastdivannual_for_chart', 
                'adjusted_fcastdivannual_for_chart',
                'divannual', 
                'fcastdivannual', 
                'nextyrfcastdivannual',
                'fiscalyearend', 
                'issuedsharesincltreasury', 
                'latest_shares',
                'totassets', 
                'equity', 
                'equityratio', 
                'assetevalrate', 
                'roaleverage', 
                'eps', 
                'fcasteps', 
                'nextyrfcasteps', 
                'roa', 
                'fcastroa', 
                'nextyrfcastroa', 
                'fairvalue', 
                'fcastfairvalue', 
                'nextyrfcastfairvalue', 
                'docname'
            ]
            
            # カラム順を再設定
            #logging.info(f"Reordering columns for the new DataFrame: {opvalue_growth_df.columns}")
            opvalue_growth_df = opvalue_growth_df[localserver_u_fins_all_bps_opvalues_columns_order]

            # テーブルに保存
            logging.info(f"Final DataFrame shape after growth calculation: {opvalue_growth_df.shape}.")
            logging.info(f"Writing to table: {tables['fins_all_bps_opvalues']}")
            opvalue_growth_df.to_sql(tables['fins_all_bps_opvalues'], conn, if_exists='replace', index=False)
            logging.info(f"✅Operation values written to '{tables['fins_all_bps_opvalues']}'.")

    except SQLAlchemyError as e:
        logging.error(f"Database error occurred: {e}")
        logging.error(traceback.format_exc())
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    logging.info("🚀BPS OpValue Process started...")

    try:
        process_and_save_operation_values()
    except Exception as e:
        logging.exception(f"Unhandled error: {e}")
        logging.error(traceback.format_exc())

    logging.info("✅ BPS OpValue Process completed.")




