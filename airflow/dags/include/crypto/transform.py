import pandas as pd
from datetime import datetime
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def clean_and_normalize(raw_data):
    logger.info("Starting cleaning & normalization")

    df = pd.DataFrame(raw_data)
    logger.info(f"Initial shape: {df.shape}")

    
    df = df.dropna(subset=["id", "symbol", "name", "current_price"])
    df = df[df["current_price"] > 0]
    df = df.drop_duplicates(subset=["id"])

    logger.info(f"Shape after cleaning: {df.shape}")

    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

   
    coins_df = df[["id", "symbol", "name"]].copy()
    coins_df.rename(columns={"id": "coin_id"}, inplace=True)

    prices_df = df[["id", "current_price", "ingestion_timestamp"]].copy()
    prices_df.rename(
        columns={
            "id": "coin_id",
            "current_price": "price_usd"
        },
        inplace=True
    )

    market_df = df[
        ["id", "market_cap", "total_volume", "ingestion_timestamp"]
    ].copy()

    market_df.rename(
        columns={
            "id": "coin_id",
            "total_volume": "volume"
        },
        inplace=True
    )

    logger.info(f"Coins table shape: {coins_df.shape}")
    logger.info(f"Prices table shape: {prices_df.shape}")
    logger.info(f"Market table shape: {market_df.shape}")

    logger.info("Transformation completed ")

    return coins_df, prices_df, market_df


def transform_crypto_data(**context):
    ti = context["ti"]

    logger.info("Pulling data from XCom")

    raw_data = ti.xcom_pull(
        task_ids="fetch_top_50",
        key="raw_crypto_data"
    )

    if not raw_data:
        logger.error("No data received from XCom")
        raise ValueError("No data received from XCom")

    coins_df, prices_df, market_df = clean_and_normalize(raw_data)

    ti.xcom_push(key="coins_table", value=coins_df.to_dict())
    ti.xcom_push(key="prices_table", value=prices_df.to_dict())
    ti.xcom_push(key="market_table", value=market_df.to_dict())

    logger.info("Data pushed to XCom successfully")