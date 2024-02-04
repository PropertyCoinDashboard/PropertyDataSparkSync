"""
pyspark udf 
"""

import datetime
import numpy as np
from typing import Any

from schema.data_constructure import CoinPrice, AverageCoinPriceData


def get_utc_time() -> int:
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    return int(utc_now.timestamp())


def streaming_preprocessing(name: str, *data: tuple) -> dict | dict[str, Any]:
    """average coin price normalization in spark python udf

    Args:
        - name (str): coin_symbol \n
    Returns:
        ex)
        >>> "average_price": {
                "name": "ETH",
                "timestamp": 1689633864.89345,
                "data": {
                    "opening_price": 2455000.0,
                    "closing_price": 2439000.0,
                    "trade_price": 38100000.0,
                    "max_price": 2462000.0,
                    "min_price": 2431000.0,
                    "prev_closing_price": 2455000.0,
                    "acc_trade_volume_24h": 11447.928,
                }
            }

    """
    if not data or any(item is None for item in data):
        return {}

    try:
        row: list[tuple[dict[str, Any]]] = list(data)

        # 문자열을 float로 변환
        value: list[tuple[float,]] = [tuple(map(float, item)) for item in zip(*row)]
    except (ValueError, TypeError) as error:
        print(error)
        return {}

    average: list[dict[str, int]] = np.mean(value, axis=1).tolist()
    data_dict = CoinPrice(
        opening_price=average[0],
        closing_price=average[1],
        max_price=average[2],
        min_price=average[3],
        prev_closing_price=average[4],
        acc_trade_volume_24h=average[5],
    )

    streaming_data = AverageCoinPriceData(
        name=name, time=get_utc_time(), data=data_dict
    )
    return streaming_data.model_dump(mode="json")
