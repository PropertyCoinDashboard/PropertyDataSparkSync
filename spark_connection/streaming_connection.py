"""
Spark streaming coin average price 
"""

from __future__ import annotations
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, udf, to_json, struct
from schema.data_constructure import average_schema, final_schema, schema
from schema.udf_util import streaming_preprocessing
from util.properties import KAFKA_SERVER, MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD


class _SparkSettingOrganization:
    def __init__(self, coin_name: str, topics: str, retrieve_topic: str):
        self._coin_name = coin_name
        self._topics = topics
        self._retrieve_topic = retrieve_topic
        self._spark = self._create_spark_session()
        self._streaming_kafka_session = self._stream_kafka_session()

    def _create_spark_session(self) -> SparkSession:
        # 환경 설정
        return (
            SparkSession.builder.appName("myAppName")
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,mysql:mysql-connector-java:8.0.28",
            )
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.consumer.config.auto.offset.reset", "latest")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.cores.max", "16")
            .getOrCreate()
        )

    def _stream_kafka_session(self) -> DataFrame:
        """spark streaming multithreading

        Args:
            - topic (str): topic \n
        Returns:
            - DataFrame: query
        """

        return (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", "".join(self._topics))
            .option("startingOffsets", "earliest")
            .load()
        )

    def _topic_to_spark_streaming(self, data_format: DataFrame):
        """KAFKA interaction TOPIC Sending data

        Args:
            name (str): coin_symbol
            topics (str): topic
            retrieve_topic (str): retrieve_topic
        """

        return (
            data_format.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092")
            .option("topic", self._retrieve_topic)
            .option("checkpointLocation", f".checkpoint_{self._coin_name}")
            .option(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .start()
        )

    def _write_to_mysql(self, data_format: DataFrame, table_name: str):
        checkpoint_dir = f".checkpoint_{table_name}"

        def _write_batch_to_mysql(batch_df: Any, batch_id) -> None:
            (
                batch_df.write.format("jdbc")
                .option("url", MYSQL_URL)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", table_name)
                .option("user", MYSQL_USER)
                .option("password", MYSQL_PASSWORD)
                .mode("append")
                .save()
            )

        return (
            data_format.writeStream.outputMode("update")
            .foreachBatch(_write_batch_to_mysql)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="1 minute")
            .start()
        )


class SparkStreamingCoinAverage:
    def __init__(self, coin_name: str, topics: str, retrieve_topic: str):
        self.coin_name = coin_name
        self._spark = _SparkSettingOrganization(self.coin_name, topics, retrieve_topic)
        self._stream_kafka_session = self._spark._stream_kafka_session()
        self._spark_streaming = self._spark._topic_to_spark_streaming
        self._mysql_micro = self._spark._write_to_mysql

    def preprocessing(self) -> DataFrame:
        average_udf = udf(streaming_preprocessing, average_schema)

        return (
            self._stream_kafka_session.selectExpr("CAST(value AS STRING)")
            .select(from_json("value", schema=final_schema).alias("crypto"))
            .selectExpr(
                "split(crypto.upbit.market, '-')[1] as name",
                "crypto.upbit.time as time",
                "crypto.upbit.data as upbit_price",
                "crypto.bithumb.data as bithumb_price",
                "crypto.coinone.data as coinone_price",
                "crypto.korbit.data as korbit_price",
            )
            .withColumn(
                "average_price",
                average_udf(
                    col("name"),
                    col("upbit_price"),
                    col("bithumb_price"),
                    col("coinone_price"),
                    col("korbit_price"),
                ).alias("average_price"),
            )
            .select(to_json(struct(col("average_price"))).alias("value"))
        )

    def saving_to_mysql_round_query(self):
        data_df: DataFrame = self.preprocessing()

        return data_df.select(from_json("value", schema).alias("value")).select(
            col("value.average_price.name").alias("name"),
            col("value.average_price.time").alias("time"),
            col("value.average_price.data.opening_price").alias("opening_price"),
            col("value.average_price.data.max_price").alias("max_price"),
            col("value.average_price.data.min_price").alias("min_price"),
            col("value.average_price.data.prev_closing_price").alias(
                "prev_closing_price"
            ),
            col("value.average_price.data.acc_trade_volume_24h").alias(
                "acc_trade_volume_24h"
            ),
        )

    def run_spark_streaming(self):
        """
        Spark Streaming 실행 함수

        Args:
            coin_name (str): 코인 이름
            topics (str): 수신할 Kafka 토픽 목록
            retrieve_topic (str): 결과를 보낼 Kafka 토픽
        Returns:
            StreamingQuery: 실행된 스트리밍 쿼리
        """

        query1 = self._mysql_micro(
            self.saving_to_mysql_round_query(), f"coin_average_price_{self.coin_name}"
        )
        query2 = self._spark_streaming(self.preprocessing())

        query1.awaitTermination()
        query2.awaitTermination()
