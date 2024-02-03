"""
spark 
"""

from concurrent.futures import ThreadPoolExecutor
from streaming_connection import SparkStreamingCoinAverage
from util.properties import (
    BTC_TOPIC_NAME,
    ETH_TOPIC_NAME,
    BTC_AVERAGE_TOPIC_NAME,
    ETH_AVERAGE_TOPIC_NAME,
)


def run_spark_streaming1(coin_name: str, topics: str, retrieve_topic: str) -> None:
    SparkStreamingCoinAverage(coin_name, topics, retrieve_topic).run_spark_streaming()


def run_spark_streaming2(coin_name: str, topics: str, retrieve_topic: str) -> None:
    SparkStreamingCoinAverage(coin_name, topics, retrieve_topic).run_spark_streaming()


def spark_in_start() -> None:
    """
    multi-Threading in SPARK application
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(
            run_spark_streaming1, "BTC", BTC_TOPIC_NAME, BTC_AVERAGE_TOPIC_NAME
        )
        executor.submit(
            run_spark_streaming2, "ETH", ETH_TOPIC_NAME, ETH_AVERAGE_TOPIC_NAME
        )


if __name__ == "__main__":
    spark_in_start()
