from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, split, count, hour


def sparkSe():
    spark = SparkSession.builder\
        .config("spark.driver.host", "localhost")\
        .getOrCreate()
    return spark

def logfile(spark):
    logfile_Schema = StructType([
        StructField("Logging",StringType(),True),
        StructField("timestamp",StringType(),True),
        StructField("ghtorrent_id",StringType(),True)
          ])
    sparkDF = spark.read.option("header",True).schema(logfile_Schema).csv('../../res/logs.txt')
    return sparkDF

def Withcolumn_Log(sparkDF):

        split_Column=sparkDF\
            .withColumn("ghtorrent",split(col("ghtorrent_id"),"--").getItem(0))\
            .withColumn("ghtorrent_remain",split(col("ghtorrent_id"),"--").getItem(1))\
            .withColumn("api_client",split(col("ghtorrent_remain"),":").getItem(0)) \
            .withColumn("url", split(col("ghtorrent_remain"), ":").getItem(1))

        return split_Column

def find_warn(df,Logging):
     find_warn_log = df.filter(col(Logging) == "WARN")\
         .agg(count("*").alias("warn_count"))
     return find_warn_log

def total_line(df):
    total_line_count = df.agg(count("*")
                              .alias("total_line"))
    return total_line_count

def api_Client(df):
    api_Client_count = df.filter(col("api_client")
                                 .like("%api_client%")).agg(count("*")
                                                            .alias("Total_api_Client"))
    return api_Client_count


def most_Http(df):
    most_Http_count=df.groupBy("ghtorrent")\
        .agg(count("ghtorrent")
             .alias("Most_Http"))
    most_Http_count\
.sort(col("Most_Http").desc())
    return most_Http_count


def faild_Request(df):
    failed_Request_count = df.filter(col("url")
        .like("%Failed%"))\
        .agg(count("*")
        .alias("failed_Request_count"))
    return failed_Request_count


def most_Acite_Hour(df):
    most_Active_Hour_Count = df.withColumn("active_hour",hour(col("timestamp")))\
             .groupBy("active_hour")\
             .agg(count("*")
             .alias("moar_Active_Hour"))
    return most_Active_Hour_Count

def most_Active_Repository(df):
    active_Repository = df.groupBy("ghtorrent")\
        .agg(count("*").alias("most_Active_Repository"))
    active_Repository.sort("most_Active_Repository")
    return active_Repository