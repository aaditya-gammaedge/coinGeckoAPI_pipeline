import requests
import logging

logger = logging.getLogger(__name__)

def fetch_top_50_coins(**context):
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        "?vs_currency=usd"
        "&order=market_cap_desc"
        "&per_page=50"
        "&page=1"
        "&sparkline=false"
    )

    response = requests.get(url)



    if response.status_code != 200:
        raise Exception(f"api request failed: {response.status_code}")


    data = response.json()
    logger.info(f"fetched {len(data)} coins successfully")

    context["ti"].xcom_push(key="raw_crypto_data", value=data)