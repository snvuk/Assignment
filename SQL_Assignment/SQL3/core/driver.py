from SQL_Assignment.SQL3.core.util import *

spark = sparkSe()

emp_details_df=emp_details(spark)


emp_window_df=empwindows_details(emp_details_df)


emp_aggfunc_df=emp_aggfunc(emp_window_df)
