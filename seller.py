import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Gets the list of goods from Ozon

    Args:
        last_id (str): last id of a product that was obtained from the store
        client_id (str): client id from the environment
        seller_token (str): seller token for the API

    Returns:
        list: list of up to 1000 objects from the store, starting from the last id.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Gets ids of the goods from Ozon
    
    Args:
        client_id (str): client id from the environment
        seller_token (str): seller token for the API
        
    Returns:
        list of str: ids of goods from Ozon marketplace
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Update prices of the goods
    
    Args:
        prices (list of str): Prices of the goods from the marketplace.
        client_id (str): Client id from the environment.
        seller_token (str): Seller token for the API.
        
    Returns:
        dict: Result of the POST response that updates the prices.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Updates goods in stock

    Args:
        stocks (list of dict): List of information on goods, their ids and how many in stock there are
        client_id (str): Client id from the environment.
        seller_token (str): Seller token for the API.


    Returns:
        dict: Result of the POST response that updates the prices.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Gets information from file ostatki from casio website

    Returns:
        dict: Information on goods in stock from the file
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Arranges information about goods in stock based on the remaining items.

    Args:
        watch_remnants (dict): Information on items in stock.
        offer_ids (list of str): ids of necessary items

    Returns:
        list of dict: information on each necessary item's remains in stock - if more than 10, assume 100, otherwise
        how many are left; for all necessary items that were not found in the remaining, assume 0.
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Returns properly formatted prices for items

    Args:
        watch_remnants (dict): Information on items in stock.
        offer_ids (list of str): ids of necessary items.

    Returns:
        list of dict: Formatted information on the items.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Function converts price to an integer-format string
    
    Args:
        price (str): Initial price of an item
        
    Returns:
        str: The same price ready to be used as an integer (no currency, decimal places etc)
    Raises:
        AttributeError: if parameter is not an str.
    Examples:
        >> price = "5'990.00 руб"
        >> price_conversion(price)
        5990
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Split a list into equal segments.

    Args:
        lst (list): Initial list.
        n (int): How many elements should be in each segment of the list.

    Returns:
        Generator: Returns a split into n segments initial list
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Updates prices of goods asynchronously

    Args:
        watch_remnants (dict): Information on items in stock.
        client_id (str): Client id from the environment.
        seller_token (str): Seller token for the API.

    Returns:
        list: list of updated prices.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Updates stocks of goods asynchronously

    Args:
        watch_remnants (dict): Information on items in stock.
        client_id (str): Client id from the environment.
        seller_token (str): Seller token for the API.

    Returns:
        non_empty (list): list of items that are still in stock.
        stocks (list): updated list of all stocks.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Updates stocks and prices of the items.

    Raises:
        ReadTimeout: in case of time out.
        ConnectionError: in case connection fails.
        Exception: in all other cases
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
