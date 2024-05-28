from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from schema.udf_util import streaming_preprocessing
from schema.data_constructure import (
    average_schema,
    final_schema,
    socket_schema,
)


def field_generator(field: Column) -> list[DataFrame]:
    fields = [
        "opening_price",
        "trade_price",
        "max_price",
        "min_price",
        "prev_closing_price",
        "acc_trade_volume_24h",
    ]
    return [col(f"{field}.{market}") for market in fields]


def column_generator(data: DataFrame, markets: list[str]) -> DataFrame:
    for market in markets:
        data = data.withColumn(market, *field_generator(market))
    return data


class SparkCoinAverageQueryOrganization:

    def __init__(self, kafka_data: DataFrame) -> None:
        self.kafka_cast_string = kafka_data.selectExpr("CAST(value AS STRING)")
        self.markets = ["upbit", "bithumb", "coinone", "korbit", "gopax"]

    # fmt: off
    def coin_main_columns(self) -> DataFrame:
        return (
            self.kafka_cast_string.select(
                F.from_json("value", schema=final_schema).alias("crypto")
            )
            .select(
                F.to_timestamp(
                    F.from_unixtime(
                        F.least(
                            col("crypto.upbit.time"),
                            col("crypto.bithumb.time"),
                            col("crypto.coinone.time"),
                            col("crypto.korbit.time"),
                            col("crypto.gopax.time"),
                        )
                    )
                ).alias("timestamp"),
                *[col(f"crypto.{market}.data").alias(f"{market}") for market in self.markets],
            )
        )

    def coin_colum_window(self):
        columns_selection = self.coin_main_columns()
        data: DataFrame = column_generator(columns_selection, self.markets)
        return (
            data
            .groupby(
                F.window(col("timestamp"), "1 second", "1 second"),
                col("timestamp"),
                *[col(f"{market}_time") for market in self.markets],
            )
            .agg(
                # *[self.get_avg_field(field) for field in self.fields],
                *[F.count(col(field)).alias(f"{field}_count") for field in self.fields],
                *[F.first(col(field)).alias(field) for field in self.fields],
            )
            .select(
                *[col(f"{market}_time") for market in self.markets],
                F.current_timestamp().alias("processed_time"),
                col("timestamp"),
                *[col(field) for field in self.fields],
                *[col(f"{field}_count") for field in self.fields],
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
            )
        )

    def socket_preprocessing(self) -> DataFrame:
        """웹소켓 처리"""
        return (
            self.kafka_cast_string.selectExpr("CAST(value as STRING)")
            .select(F.from_json("value", schema=socket_schema).alias("crypto"))
            .select(F.explode(col("crypto")).alias("crypto"))
            .select(
                F.split(col("crypto.market"), "-")[1].alias("name"),
                col("crypto.data").alias("price_data"),
            )
            .withColumn(
                "socket_average_price",
                self.average_price(col("name"), col("price_data")).alias(
                    "socket_average_price"
                ),
            )
            .select(F.to_json(F.struct(col("socket_average_price"))).alias("value"))
        )
