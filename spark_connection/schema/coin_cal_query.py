from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from schema.data_constructure import (
    final_schema,
    socket_schema,
)


def get_avg_field(markets: list[str], field: str) -> Column:
    """각 거래소 별 평균

    Args:
        markets (list[str]): 마켓
        field (str): 각 필드 이름

    Returns:
        Column: 각 Col Fields
    """
    columns: list[Column] = [
        F.col(f"crypto.{market}.data.{field}").cast("double") for market in markets
    ]
    avg_column = sum(columns) / len(markets)
    return F.round(avg_column, 3).alias(field)


def generate_function(process: str, field: list[str]) -> list[Column]:
    """function 1급 객체 생성
    Args:
        process (str): 프로세스
        field (list[str]): 각 필드

    Returns:
        list[Column]: [col]
    """

    def generate_column(field: str) -> Column:
        """
        Args:
            field (str): 필드

        Returns:
            Column: 필드 에 따른 함수
        """
        match process:
            case "count":
                return F.first(col(field)).alias(field)
            case "first":
                return F.count(col(field)).alias(f"{field}_count")

    return list(map(generate_column, field))


def market_time_geneator(market: list[str], type_: str) -> list[Column]:
    """
    Args:
        market (list[str]): list 마켓
        type_ (str): 각 타입마다 col를 다르게 생성

    Returns:
        list[Column]: _description_
    """

    def generate_column(market: str) -> Column:
        match type_:
            case "time":
                return col(f"crypto.{market}.time").alias(f"{market}_time")
            case "data":
                return col(f"crypto.{market}.data").alias(market)
            case "w_time":
                return col(f"{market}_time")
            case _:
                ValueError("No Type")

    return list(map(generate_column, market))


def time_instructure() -> Column:
    market_time = col(f"crypto.timestamp").cast("long")
    return F.to_timestamp(F.from_unixtime(market_time)).alias("timestamp")


class SparkCoinAverageQueryOrganization:
    """spark query injection Organization"""

    def __init__(self, kafka_data: DataFrame) -> None:
        self.kafka_cast_string = kafka_data.selectExpr("CAST(value AS STRING)")
        self.markets = ["upbit", "bithumb","korbit", "coinone", "gopax"]
        self.fields = [
            "opening_price",
            "trade_price",
            "max_price",
            "min_price",
            "prev_closing_price",
            "acc_trade_volume_24h",
        ]

    def coin_main_columns(self) -> DataFrame:
        """watermark injectional testing"""
        return (
            self.kafka_cast_string.select(
                F.from_json("value", schema=final_schema).alias("crypto")
            )
            .select(
                "*",
                time_instructure(),
                #*market_time_geneator(self.markets, "data"),
                #*market_time_geneator(self.markets, "time"),
                #*[get_avg_field(self.markets, field) for field in self.fields],
            )
            .withWatermark("timestamp", "1 minutes")
        )

    def coin_colum_window(self) -> DataFrame:
        """코인 window injectional testing"""
        columns_selection = self.coin_main_columns()
        return (
            columns_selection.groupby(
                F.window(col("timestamp"), "1 minutes", "1 minutes"),
                #col("timestamp"),
                #*market_time_geneator(self.markets, "w_time"),
            )
            .agg(
                F.count("*").alias("window_count"),
                #*generate_function("count", self.fields),
                #*generate_function("first", self.fields)
            )
            .select(
                #*market_time_geneator(self.markets, "w_time"),
                F.current_timestamp().alias("processed_time"),
                #col("timestamp"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("window_count"),
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
