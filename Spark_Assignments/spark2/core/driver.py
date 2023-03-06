from Spark_Assignments.spark2.core.util import *
from pyspark.sql import SparkSession



spark = sparkSe()
spark = SparkSession


log_Df = logfile(spark)





logs_file =Withcolumn_Log(log_Df)





warn_count = find_warn(logs_file,"Logging")




total_line_count = total_line(log_Df)




total_Api_Client =api_Client(logs_file)




most_Http_count = most_Http(logs_file)




failed_Request_count = faild_Request(logs_file)




most_Active_Hour_Count =most_Acite_Hour(logs_file)


active_Repository_count = most_Active_Repository(logs_file)
