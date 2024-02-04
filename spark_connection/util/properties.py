"""
전역으로 사용할 수 있게 환경 변수 값들 
"""

import configparser
from pathlib import Path

path = Path(__file__).parent.parent
parser = configparser.ConfigParser()
parser.read(f"{path}/util/urls.conf")

BTC_TOPIC_NAME: str = parser.get("TOPICNAME", "BTC_TOPIC_NAME")
ETH_TOPIC_NAME: str = parser.get("TOPICNAME", "ETHER_TOPIC_NAME")
OTHER_TOPIC_NAME: str = parser.get("TOPICNAME", "OTHER_TOPIC_NAME")

BTC_AVERAGE_TOPIC_NAME: str = parser.get("AVERAGETOPICNAME", "BTC_AVERAGE_TOPIC_NAME")
ETH_AVERAGE_TOPIC_NAME: str = parser.get("AVERAGETOPICNAME", "ETHER_AVERAGE_TOPIC_NAME")


BTC_TOPIC_NAME: str = parser.get("TOPICNAME", "BTC_TOPIC_NAME")
ETH_TOPIC_NAME: str = parser.get("TOPICNAME", "ETHER_TOPIC_NAME")


UPBIT_BTC_REAL_TOPIC_NAME: str = parser.get(
    "REALTIMETOPICNAME", "UPBIT_BTC_REAL_TOPIC_NAME"
)
BITHUMB_BTC_REAL_TOPIC_NAME: str = parser.get(
    "REALTIMETOPICNAME", "BITHUMB_BTC_REAL_TOPIC_NAME"
)
KORBIT_BTC_REAL_TOPIC_NAME: str = parser.get(
    "REALTIMETOPICNAME", "KORBIT_BTC_REAL_TOPIC_NAME"
)


KAFKA_SERVER: str = parser.get("KAFKA", "SERVERS")

MYSQL_URL: str = parser.get("MYSQL", "MYSQL_URL")
MYSQL_USER: str = parser.get("MYSQL", "MYSQL_USER")
MYSQL_PASSWORD: str = parser.get("MYSQL", "MYSQL_PASSWORD")
