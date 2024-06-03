"""
API에 필요한것들
"""

import configparser
from pathlib import Path


path = Path(__file__).parent.parent
parser = configparser.ConfigParser()
parser.read(f"{path}/config/setting.conf")

KAFKA_BOOTSTRAP_SERVERS = parser.get("KAFKASPARK", "KAFKA_BOOTSTRAP_SERVERS")
SPARK_PACKAGE = parser.get("KAFKASPARK", "SPARK_PACKAGE")


# ------------------------------------------------------------------------------
# Coin setting
# ------------------------------------------------------------------------------
BTC_TOPIC_NAME = parser.get("RESTTOPICNAME", "BTC_TOPIC_NAME")
ETH_TOPIC_NAME = parser.get("RESTTOPICNAME", "ETHER_TOPIC_NAME")
OTHER_TOPIC_NAME = parser.get("RESTTOPICNAME", "OTHER_TOPIC_NAME")
BTC_AVERAGE_TOPIC_NAME = parser.get("AVERAGETOPICNAME", "BTC_REST_AVERAGE_TOPIC_NAME")
ETH_AVERAGE_TOPIC_NAME = parser.get("AVERAGETOPICNAME", "ETHER_REST_AVERAGE_TOPIC_NAME")


UPBIT_BTC_REAL_TOPIC_NAME = parser.get("REALTIMETOPICNAME", "UPBIT_BTC_REAL_TOPIC_NAME")
BITHUMB_BTC_REAL_TOPIC_NAME = parser.get(
    "REALTIMETOPICNAME", "BITHUMB_BTC_REAL_TOPIC_NAME"
)
KORBIT_BTC_REAL_TOPIC_NAME = parser.get(
    "REALTIMETOPICNAME", "KORBIT_BTC_REAL_TOPIC_NAME"
)
COINONE_BTC_REAL_TOPIC_NAME = parser.get(
    "REALTIMETOPICNAME", "COINONE_BTC_REAL_TOPIC_NAME"
)

COIN_MYSQL_URL = parser.get("MYSQL", "COIN_MYSQL_URL")
COIN_MYSQL_USER = parser.get("MYSQL", "COIN_MYSQL_USER")
COIN_MYSQL_PASSWORD = parser.get("MYSQL", "COIN_MYSQL_PASSWORD")


# # AWS Credentials 설정
# AWS_ACCESS_KEY_ID = parser.get("KAFKA", "AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = parser.get("KAFKA", "AWS_SECRET_ACCESS_KEY")
# S3_LOCATION = parser.get("KAFKA", "S3_LOCATION")
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
# spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
