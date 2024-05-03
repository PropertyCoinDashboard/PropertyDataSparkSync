from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class AbstructSparkSettingOrganization(ABC):
    """Abstract class for SparkSession settings"""

    @abstractmethod
    def _create_spark_session(self):
        """Abstract method to create SparkSession"""
        pass

    @abstractmethod
    def _topic_to_spark_streaming(self, data_format: DataFrame):
        """Abstract method to convert topic to Spark streaming"""
        pass
