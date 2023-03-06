from Spark_Assignments.spark1.core.util import *


spark = SparkSe()

tran_DF = transactionDF(spark)

user_DF = userDF(spark)

join_DF = joinDF(user_DF,tran_DF)

cont_loc = count_location(join_DF,"location","product_description")

product = product_Bought(join_DF,"user_id")

total_price = total_spending(join_DF,"userid","price")
