"""
spark 
"""
from concurrent.futures import ThreadPoolExecutor
from streaming_connection import run_spark_streaming
from util.properties import (
    BTC_TOPIC_NAME,
    ETH_TOPIC_NAME,
    BTC_AVERAGE_TOPIC_NAME,
    ETH_AVERAGE_TOPIC_NAME,
)


def spark_in_start() -> None:
    """
    multi-Threading in SPARK application
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(
            run_spark_streaming, "BTC", BTC_TOPIC_NAME, BTC_AVERAGE_TOPIC_NAME
        )
        executor.submit(
            run_spark_streaming, "ETH", ETH_TOPIC_NAME, ETH_AVERAGE_TOPIC_NAME
        )


if __name__ == "__main__":
    spark_in_start()
