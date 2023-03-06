
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,LongType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col, avg, sum, min, max, row_number

def sparkSe():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark


data=   {("james","sales",3000),
        ("michael","sales",4600),
        ("robert","sales",4100),
        ("maria","finance",3000),
        ("raman","finance",3000),
        ("scott","finance",3300),
        ("jen","marketing",3000),
        ("kumar","marketinng",2000)}


schema=StructType([
    StructField("emp_name",StringType(),True),
    StructField("job",StringType(),True),
    StructField("salary",LongType(),True)
])


def emp_details(spark):
    data = {("james", "sales", 3000),
            ("michael", "sales", 4600),
            ("robert", "sales", 4100),
            ("maria", "finance", 3000),
            ("raman", "finance", 3000),
            ("scott", "finance", 3300),
            ("jen", "marketing", 3000),
            ("kumar", "marketinng", 2000)}
    schema = StructType([
        StructField("emp_name", StringType(), True),
        StructField("job", StringType(), True),
        StructField("salary", LongType(), True)
    ])
    emp_df=spark.createDataFrame(data=data,schema=schema)
    return emp_df
def empwindows_details(emp_df):
    user_data = Window.partitionBy("job").orderBy("salary")
    emp_detail_row_number=emp_df.withColumn("row_number", row_number().over(user_data))
    emp_detail_row_number.show(truncate=False)
    return emp_detail_row_number

def emp_aggfunc(emp_agg):
    user_data = Window.partitionBy("job").orderBy("salary")
    Aggfunc = Window.partitionBy("job")
    emp_agg_df=emp_agg.withColumn("row", row_number().over(user_data)) \
        .withColumn("avg", avg(col("salary")).over(Aggfunc)) \
        .withColumn("sum", sum(col("salary")).over(Aggfunc)) \
        .withColumn("min", min(col("salary")).over(Aggfunc)) \
        .withColumn("max", max(col("salary")).over(Aggfunc)) \
        .where(col("row") == 1).select("job", "avg", "sum", "min", "max") \
        .show(truncate=False)
    return emp_agg_df
