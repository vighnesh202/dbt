# You use Spark SQL in a "SparkSession" to create DataFrames
from pyspark.sql import SparkSession, DataFrame
# PySpark functions
from pyspark.sql.functions import avg, col, count, desc, round, size, udf, length, concat_ws, trim, substring
# These allow us to create a schema for our data
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructType, StructField, IntegerType, StringType
from functools import reduce

# default values
dv_scrubbed_serial_num_threshold = 2000
dv_scrubbed_serial_num_string_limit = 2000

# a list to hold all the individual recursion results
recursive_result_list = []

recursive_count  = 0


def recusrion(recursion_df, base_table_df):
    # Execute recursive logic with Ri as an input to return the result set Ri+1 as the output, until
    # an empty result set is returned
    global recursive_count
    global recursive_result_list
    recursion_df = recursion_df.alias("recursion_df")
    base_table_df = base_table_df.alias("base_table_df")
    recursive_count += 1

    # xyz recursive logic
    # An empty dataframe to hold the recursive result set Ri
    schema = StructType([
    StructField("BK_POS_TRANSACTION_ID_INT", IntegerType(), True),
    StructField("DV_SCRUBBED_SERIAL_NUM", StringType(), True),
    StructField("BK_DETAIL_ID_INT", IntegerType(), True),
    StructField("RNK", IntegerType(), True)
    ])

    recursive_result = spark.createDataFrame([], schema)
    
    # the first part is to do the from statement
    # Inner join between the recursive result and base table
    p1 = recursion_df.join(base_table_df, on=["BK_POS_TRANSACTION_ID_INT"], how="inner")
    
    # second part is to execute where clause
    p2 = p1.filter((col("base_table_df.RNK") == (col("recursion_df.RNK") + 1)) &
               ((length(col("recursion_df.DV_SCRUBBED_SERIAL_NUM")) + length(col("base_table_df.DV_SCRUBBED_SERIAL_NUM"))) < dv_scrubbed_serial_num_threshold))
    
    # select clause
    recursive_result = p2.select(
      col("BK_POS_TRANSACTION_ID_INT").alias("BK_POS_TRANSACTION_ID_INT"),
      substring(concat_ws(",", trim(col("recursion_df.DV_SCRUBBED_SERIAL_NUM")), trim(col("base_table_df.DV_SCRUBBED_SERIAL_NUM"))), 1, dv_scrubbed_serial_num_string_limit).alias("DV_SCRUBBED_SERIAL_NUM"),
      col("base_table_df.BK_DETAIL_ID_INT").alias("BK_DETAIL_ID_INT"),
      col("base_table_df.RNK").alias("RNK")
    )
    
    # check for possibility for next iteration
    if recursive_result.count():
        recursive_result_list.append(recursive_result)
        return recusrion(recursive_result, base_table_df)
        
    return recursive_result_list
    

def model(dbt, session):
    dbt.config(
        materialized = 'table',
        submission_method = 'serverless'
    )
    global recursive_result_list
    global recursive_count
    
    # get the base table into a dataframe
    base_table_df = dbt.source('dbt_workdb', 'base_table_v1')
    
    # execution of non recusrive logic
    non_recursive_df = base_table_df.filter(base_table_df['RNK'] == 31)
    
    # first append the non-recursive term result as it forms the base of the union all statement
    recursive_result_list.append(non_recursive_df)
    
    # start the recursion by calling the recursive function
    recursive_result_list = recusrion(non_recursive_df, base_table_df)
    
    final_df = reduce(DataFrame.union, recursive_result_list)
    
    return final_df