#Extract 2019-2020 data as a df  ETL
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
import pandas as pd
import numpy as np
#from pyspark.sql.functions import pandas_udf, PandasUDFType
#from fbprophet import Prophet
spark = SparkSession.builder.master('local').config("spark.driver.memory", "10g").appName('speed-analysis').getOrCreate() 
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
n = 1000
#from pyspark.sql.functions import pandas_udf, PandasUDFType
df =spark.read.csv('hdfs://babar.es.its.nyu.edu/user/yz7413/project/Uber_data/Uber_timeseries_2019.txt')
df  = df.withColumn("end_id_2019",split("_c1", "\t").getItem(0))
df = df.withColumn("_c1",split("_c1","\t").getItem(1))
df = df.withColumn("start_id_2019",df["_c0"])
df = df.withColumn("id_2019",concat("start_id_2019",lit("-"),"end_id_2019")) 
ds = pd.date_range('2019-01-01', '2020-01-01', freq='1H', closed='left',tz='US/Eastern')  
ds = np.array([ds[i].replace(tzinfo=None) for i in range(len(ds))])
new_columns = [str(ds[i]) for i in range(len(ds))]
new_columns.append('end_id_2019')
new_columns.append('start_id_2019')
new_columns.append('id_2019')
new_columns.insert(0,"start_2019")
changed = False
for i in range(len(new_columns)):
    if (not changed) and (new_columns[i] == "2019-11-03 01:00:00"):
        new_columns[i] = "2019-11-03 01:30:00"
        changed = True       
df = df.toDF(*new_columns)
df2 = spark.read.csv('hdfs://babar.es.its.nyu.edu/user/yz7413/project/Uber_data/Uber_timeseries_2020.txt')
df2  = df2.withColumn("end_id_2020",split("_c1", "\t").getItem(0))
df2 = df2.withColumn("_c1",split("_c1","\t").getItem(1))
df2 = df2.withColumn("start_id_2020",df2["_c0"])
df2 = df2.withColumn("id",concat("start_id_2020",lit("-"),"end_id_2020")) 
ds2 = pd.date_range('2020-01-01', '2020-04-01', freq='1H', closed='left',tz='US/Eastern')  
ds2 = np.array([ds2[i].replace(tzinfo=None) for i in range(len(ds2))])
new_columns2 = [str(ds2[i]) for i in range(len(ds2))]
new_columns2.append('end_id_2020')
new_columns2.append('start_id_2020')
new_columns2.append('id')
new_columns2.insert(0,"start_2020")
new_columns2.insert(0,"start_2020_1")
df2 = df2.toDF(*new_columns2)
df.createOrReplaceTempView("speeds")
df2.createOrReplaceTempView("speeds2")
df3 = (spark.sql("select * from speeds inner join speeds2 on speeds.id_2019 = speeds2.id order by speeds2.id limit "+ str(n)).repartition(spark.sparkContext.defaultParallelism,['speeds2.id'])).cache()
df3.write.csv("2019-2020_1000")