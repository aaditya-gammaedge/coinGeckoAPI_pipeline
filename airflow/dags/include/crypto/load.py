
import pandas as pd
from sqlalchemy import create_engine, text
import logging


from airflow.utils.email import send_email

THRESHOLD = 0.01


logger = logging.getLogger(__name__)

def load_to_postgres(**context):
    ti = context["ti"]

    coins = ti.xcom_pull(task_ids="transform_data", key="coins_table")
    prices = ti.xcom_pull(task_ids="transform_data", key="prices_table")
    market = ti.xcom_pull(task_ids="transform_data", key="market_table")



    # coins = ti.xcom_pull(task_ids="transform_data", key="coins_table")
    print("Coins XCom:",coins)
    print("prices", prices)
    print("market", market)


    if not coins or not prices or not market:
        logger.error("No transformed data is available in XCom")
        raise ValueError("No transformed data available in XCom")


    coins_df = pd.DataFrame(coins)
    prices_df = pd.DataFrame(prices)
    market_df = pd.DataFrame(market)
    

   
    # postgres_url = "postgresql://postgres:Aaditya%405689@db.pfrlgshyrsrnuvkbynau.supabase.co:5432/postgres"
    # engine = create_engine(postgres_url)
    # postgres_url = "postgresql://postgres:Aaditya%405689@aws-1-ap-northeast-2.pooler.supabase.com:5432/postgres"
    postgres_url = "postgresql://postgres.pfrlgshyrsrnuvkbynau:Aaditya%405689@aws-1-ap-northeast-2.pooler.supabase.com:5432/postgres"
    engine = create_engine(postgres_url, connect_args={"options": "-4"})

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS coins (
                coin_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS prices (
                price_id SERIAL PRIMARY KEY,
                coin_id TEXT REFERENCES coins(coin_id),
                price_usd NUMERIC(18,8) NOT NULL,
                ingestion_timestamp TIMESTAMP,
                UNIQUE(coin_id, ingestion_timestamp)
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS market_data (
                market_id SERIAL PRIMARY KEY,
                coin_id TEXT REFERENCES coins(coin_id),
                market_cap NUMERIC(18,2),
                volume NUMERIC(18,2),
                ingestion_timestamp TIMESTAMP,
                UNIQUE(coin_id, ingestion_timestamp)
            );
        """))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_prices_coin ON prices(coin_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_market_coin ON market_data(coin_id);"))


#     last_prices = pd.read_sql("SELECT coin_id, price_usd FROM prices ORDER BY ingestion_timestamp DESC", engine)
#     last_prices = last_prices.groupby('coin_id').first().reset_index()

    
#     merged = prices_df.merge(last_prices, on='coin_id', how='left', suffixes=('_current', '_last'))

#     if not merged.empty:
#         merged.loc[0, 'price_usd_current'] *= 2

#     merged['relative_change'] = abs(merged['price_usd_current'] - merged['price_usd_last']) / merged['price_usd_last']


#     major_changes = merged.loc[
#     merged['price_usd_last'].notna() & (merged['relative_change'] >= THRESHOLD)
# ]
   
#     major_changes = merged[(merged['price_usd_last'].notna()) &
#                            (abs(merged['price_usd_current'] - merged['price_usd_last']) / merged['price_usd_last'] >= THRESHOLD)]
    
#     if not major_changes.empty:
        
#         logger.info("Email alert")
        
#         msg = "Major price changes detected in:\n" + major_changes.to_string(index=False)
#         send_email(
#             to="aadityajaiswal797@gmail.com",
#             subject="Price Alert",
#             html_content=msg
#         )

        last_prices = pd.read_sql(
    "SELECT coin_id, price_usd FROM prices ORDER BY ingestion_timestamp DESC",
    engine
)
    last_prices = last_prices.groupby('coin_id').first().reset_index()

    merged = prices_df.merge(
    last_prices,
    on='coin_id',
    how='left',
    suffixes=('_current', '_last')
)

    if not merged.empty:
        merged.loc[0, 'price_usd_current'] *= 2


    merged = merged[merged['price_usd_last'] > 0]

    merged['relative_change'] = abs(
    merged['price_usd_current'] - merged['price_usd_last']
) / merged['price_usd_last']

    major_changes = merged[
    (merged['price_usd_last'].notna()) &
    (merged['relative_change'] >= THRESHOLD)
]

    if not major_changes.empty:

        logger.info("Email alert")

        msg = "Major price changes detected in:\n" + major_changes.to_string(index=False)

        send_email(
        to="aadityajaiswal797@gmail.com",
        subject="Price Alert",
        html_content=msg
        
    )

    coins_df.to_sql('coins', engine, if_exists='append', index=False)
    prices_df.to_sql('prices', engine, if_exists='append', index=False)
    market_df.to_sql('market_data', engine, if_exists='append', index=False)

    logger.info("Data loaded successfully")




