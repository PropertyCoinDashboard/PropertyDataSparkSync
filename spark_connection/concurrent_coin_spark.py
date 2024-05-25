"""
spark 
"""

from concurrent.futures import ThreadPoolExecutor
from streaming_connection import SparkStreamingCoinAverage
from config.properties import (
    BTC_TOPIC_NAME,
    BTC_AVERAGE_TOPIC_NAME,
    UPBIT_BTC_REAL_TOPIC_NAME,
    ETH_TOPIC_NAME,
    ETH_AVERAGE_TOPIC_NAME,
)


def run_spark_streaming1(
    coin_name: str, topics: str, retrieve_topic: str, type_
) -> None:
    SparkStreamingCoinAverage(
        coin_name, topics, retrieve_topic, type_
    ).run_spark_streaming()


def run_spark_streaming2(
    coin_name: str, topics: str, retrieve_topic: str, type_
) -> None:
    SparkStreamingCoinAverage(
        coin_name, topics, retrieve_topic, type_
    ).run_spark_streaming()


def spark_in_start() -> None:
    """
    multi-Threading in SPARK application
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(
            run_spark_streaming1,
            "BTC",
            UPBIT_BTC_REAL_TOPIC_NAME,
            BTC_AVERAGE_TOPIC_NAME,
            "rest",
        )
        executor.submit(
            run_spark_streaming2,
            "ETH",
            ETH_TOPIC_NAME,
            ETH_AVERAGE_TOPIC_NAME,
            "rest",
        )


if __name__ == "__main__":
    #spark_in_start()
    run_spark_streaming1("BTC",
            BTC_TOPIC_NAME,
            BTC_AVERAGE_TOPIC_NAME,
            "rest")
