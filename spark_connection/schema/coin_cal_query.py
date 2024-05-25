from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from schema.udf_util import streaming_preprocessing
from schema.data_constructure import (
    average_schema,
    final_schema,
    socket_schema,
)


class SparkCoinAverageQueryOrganization:

    def __init__(self, kafka_data: DataFrame) -> None:
        self.kafka_cast_string = kafka_data.selectExpr("CAST(value AS STRING)")
        self.average_price = F.udf(streaming_preprocessing, average_schema)
    def coin_preprocessing(self) -> DataFrame:
        """데이터 처리 pythonUDF사용"""
        return (
            self.kafka_cast_string
            .select(F.from_json("value", schema=final_schema).alias("crypto"))
            .select(
                F.split(col("crypto.upbit.market"), "-")[1].alias("name"),
                col("crypto.upbit.data.trade_price").alias("upbit_price"),
                col("crypto.bithumb.data.trade_price").alias("bithumb_price"),
                col("crypto.coinone.data.trade_price").alias("coinone_price"),
                col("crypto.korbit.data.trade_price").alias("korbit_price"),
                F.to_timestamp(F.from_unixtime(col("crypto.upbit.time"))).alias("timestamp")
            )
            #.withColumn(
            #    "average_price",
            #    self.average_price(
            #        col("name"),
            #        col("upbit_price"),
            #        col("bithumb_price"),
            #        col("coinone_price"),
            #        col("korbit_price"),
            #        )
            #)
            .withColumn(
                "average_price",
                (col("upbit_price") + col("bithumb_price") + col("coinone_price") + col("korbit_price")) / 4
            )
            .withWatermark("timestamp", "30 second")
            .groupBy(
                F.window(col("timestamp"), "1 second", "1 second"),
                col("name")
            )
            .agg(F.avg(col("average_price")).alias("average_price"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("name"),
                col("average_price")

            )  
            .select("*")
        
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
