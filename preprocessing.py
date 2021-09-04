import pandas as pd
import requests
from datetime import datetime

eth_tweets = pd.read_csv("eth_final.csv")
btc_tweets = pd.read_csv("btc_final.csv")
axie_tweets = pd.read_csv("axie_final.csv")
doge_tweets = pd.read_csv("doge_final.csv")

import pyspark as spark
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,udf,monotonically_increasing_id,unix_timestamp,round,avg
import re

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("yarn").appName("MyApp").getOrCreate()

sc = spark.SparkContext()
sqlContext = spark.SQLContext(sc)

# from pyspark.context import SparkContext
# sc = SparkContext('local', 'test')

import preprocessor as p 
def function_udf(input_str):
    input_str = re.sub(r'RT', '', input_str)
    p.set_options(p.OPT.URL, p.OPT.EMOJI,p.OPT.MENTION)
    input_str = p.clean(input_str)
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", input_str).split())
func_udf = udf(function_udf, StringType())


# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Pandas to spark df
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)


df = pandas_to_spark(eth_tweets)

CleanDF = df.withColumn('CleanedTweets', func_udf(df['text']))
CleanDF.show(3)
