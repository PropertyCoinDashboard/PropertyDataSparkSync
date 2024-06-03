"""
Spark streaming coin average price 
"""

from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import from_json, col

from schema.data_constructure import average_price_schema
from schema.abstruct_class import AbstructSparkSettingOrganization
from schema.coin_cal_query import (
    SparkCoinAverageQueryOrganization as SparkStructCoin,
)

from config.properties import (
    KAFKA_BOOTSTRAP_SERVERS,
    SPARK_PACKAGE,
    COIN_MYSQL_URL,
    COIN_MYSQL_USER,
    COIN_MYSQL_PASSWORD,
)


class _ConcreteSparkSettingOrganization(AbstructSparkSettingOrganization):
    """SparkSession Setting 모음"""

    def __init__(self, name: str, retrieve_topic: str) -> None:
        """생성자

        Args:
            topics (str): 토픽
            retrieve_topic (str): 처리 후 다시 카프카로 보낼 토픽
        """
        self.name = name
        self.retrieve_topic = retrieve_topic
        self._spark: SparkSession = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """
        Spark Session Args:
            - spark.jars.packages : 패키지
                - 2024년 2월 4일 기준 : Kafka-connect, mysql-connector
            - spark.streaming.stopGracefullyOnShutdown : 우아하게 종료 처리
            - spark.streaming.backpressure.enabled : 유압 밸브
            - spark.streaming.kafka.consumer.config.auto.offset.reset : kafka 스트리밍 경우 오프셋이 없을때 최신 메시지 부터 처리
            - spark.sql.adaptive.enabled : SQL 실행 계획 최적화
            - spark.executor.memory : Excutor 할당되는 메모리 크기를 설정
            - spark.executor.cores : Excutor 할당되는 코어 수 설정
            - spark.cores.max : Spark 에서 사용할 수 있는 최대 코어 수
        """
        return (
            SparkSession.builder.appName("myAppName")
            .master("local[*]")
            .config("spark.jars.packages", f"{SPARK_PACKAGE}")
            # .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            # .config("spark.kafka.consumer.cache.capacity", "")
            .config("spark.sql.session.timeZone", "Asia/Seoul") 
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.consumer.config.auto.offset.reset", "latest")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.cores.max", "16")
            .getOrCreate()
        )

    def _topic_to_spark_streaming(self, data_format: DataFrame) -> StreamingQuery:
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
            - checkpointLocation: 체크포인트
            - value.serializer: 직렬화 종류
        """
        checkpoint_dir: str = f".checkpoint_{self.name}"

        return (
            data_format.writeStream.outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("topic", self.retrieve_topic)
            .option("checkpointLocation", checkpoint_dir)
            .option("startingOffsets", "earliest")
            .option(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .start()
        )


class SparkStreamingCoinAverage(_ConcreteSparkSettingOrganization):
    """
    데이터 처리 클래스
    """

    def __init__(self, name: str, topics: str, retrieve_topic: str, type_: str) -> None:
        """
        Args:
            coin_name (str): 코인 이름
            topics (str): 토픽
            retrieve_topic (str): 처리 후 다시 카프카로 보낼 토픽
        """
        super().__init__(name, retrieve_topic)
        self.topic = topics
        self._streaming_kafka_session: DataFrame = self._stream_kafka_session()
        self.type_ = type_

    def _stream_kafka_session(self) -> DataFrame:
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
        """

        return (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def _coin_write_to_mysql(
        self, data_format: DataFrame, table_name: str
    ) -> StreamingQuery:
        """
        Function Args:
            - data_format (DataFrame): 저장할 데이터 포맷
            - table_name (str): 체크포인트 저장할 테이블 이름
                - ex) .checkpoint_{table_name}

        MySQL Setting Args (_write_batch_to_mysql):
            - url : JDBC MYSQL connention URL
            - driver : com.mysql.cj.jdbc.Driver
            - dbtable : table
            - user : user
            - password: password
            - mode : append
                - 추가로 들어오면 바로 넣기

        - trigger(processingTime="1 minute")
        """
        checkpoint_dir: str = f".checkpoint_{table_name}"

        def _write_batch_to_mysql(batch_df: DataFrame, batch_id) -> None:
            (
                batch_df.write.format("jdbc")
                .option("url", COIN_MYSQL_URL)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", table_name)
                .option("user", COIN_MYSQL_USER)
                .option("password", COIN_MYSQL_PASSWORD)
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

    def saving_to_mysql_query(
        self, data_frame_type: DataFrame, connect: str
    ) -> DataFrame:
        """데이터 처리 pythonUDF사용"""

        return data_frame_type.select(
            from_json("value", average_price_schema(connect)).alias("value")
        ).select(
            col(f"value.{connect}.name").alias("name"),
            col(f"value.{connect}.time").alias("time"),
            col(f"value.{connect}.data.opening_price").alias("opening_price"),
            col(f"value.{connect}.data.max_price").alias("max_price"),
            col(f"value.{connect}.data.min_price").alias("min_price"),
            col(f"value.{connect}.data.prev_closing_price").alias("prev_closing_price"),
            col(f"value.{connect}.data.acc_trade_volume_24h").alias(
                "acc_trade_volume_24h"
            ),
        )

    def run_spark_streaming(self) -> None:
        """
        Spark Streaming 실행 함수
        """
        # if self.type_ == "socket":
        #    connect = "socket_average_price"
        #    query_collect = SparkStructCoin(
        #        self._streaming_kafka_session
        #    ).socket_preprocessing()
        # elif self.type_ == "rest":
        #    connect = "average_price"
        #     query_collect = SparkStructCoin(
        #         self._streaming_kafka_session
        #     ).coin_preprocessing()

        # query1 = self._coin_write_to_mysql(
        #    self.saving_to_mysql_query(query_collect, connect), f"table_{self.name}"
        # )
        # query2 = self._topic_to_spark_streaming(query_collect)

        # query1.awaitTermination()
        # query2.awaitTermination()
        query = (
            SparkStructCoin(self._stream_kafka_session())
            .coin_colum_window()
            .writeStream.outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .start()
        )
        query.awaitTermination()
