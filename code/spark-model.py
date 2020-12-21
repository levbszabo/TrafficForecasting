#Evaluate selected forecasting model using spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf, PandasUDFType
from fbprophet import Prophet
spark = SparkSession.builder.master('local').config("spark.driver.memory", "10g").appName('speed-analysis').getOrCreate() 
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
n = 10
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

result_schema =StructType([
  StructField('id',StringType()),
  StructField('RMSE')
  ])   

@pandas_udf(result_schema, PandasUDFType.GROUPED_MAP )
def forecast_speed(df):
    growth = "linear"
    seasonality_mode = "multiplicative"
    regressor_mode = "multiplicative"
    time_series1 = df.values[1:8761]
    time_series2 = df.values[8765:-4]
    end_id = df.values[-3]
    start_id = df.values[-2]
    street_id = df.values[-1]
    y = np.concatenate((time_series1,time_series2))
    ds = pd.date_range('2019-01-01','2020-04-01',freq='1H',closed='left',tz='US/Eastern')
    ds = np.array([ds[i].replace(tzinfo=None) for i in range(len(ds))])
    formatted_df = pd.DataFrame(data=[ds,y]).T
    formatted_df.columns = ["ds","y"]
    train_ind = 365
    formatted_df = formatted_df.set_index(pd.DatetimeIndex(formatted_df["ds"]))
    daily_average = formatted_df[["y"]].astype(np.double).resample("D").mean()
    column_set = ["visibility","rain","snow","freezing"]
    weather_df_average = weather_df[column_set].resample("D").max()
    left = len(daily_average) - 365
    weather_df_average = weather_df_average.append(weather_df_average[:left])
    weather_df_average = weather_df_average.set_index(daily_average.index)
    weather_score = weather_df_average.sum(axis=1).values
    train_df = daily_average[:train_ind]
    train_df.insert(0,"ds",train_df.index)
  #train_df.insert(0,"weather",weather_score[:train_ind])
    train_df.insert(0,"rain",weather_df_average["rain"].values[:train_ind])
    train_df.insert(0,"snow",weather_df_average["snow"].values[:train_ind])
    train_df.insert(0,"freezing",weather_df_average["freezing"].values[:train_ind])
    train_df.insert(0,"visibility",weather_df_average["visibility"].values[:train_ind])
    train_df.insert(0,"crashes",crash_df["crashes"].values[:train_ind])
    test_df = daily_average[train_ind:]
    test_df.insert(0,"ds",test_df.index)  
  #test_df.insert(0,"weather",weather_score[train_ind:])   
    test_df.insert(0,"rain",weather_df_average["rain"].values[train_ind:])
    test_df.insert(0,"snow",weather_df_average["snow"].values[train_ind:])
    test_df.insert(0,"freezing",weather_df_average["freezing"].values[train_ind:])
    test_df.insert(0,"visibility",weather_df_average["visibility"].values[train_ind:])
    test_df.insert(0,"crashes",crash_df["crashes"].values[train_ind:])
    ts = train_df["y"].values
    model = Prophet(growth=growth,interval_width = 0.95,weekly_seasonality=True,yearly_seasonality=True,seasonality_mode = seasonality_mode)
    for col in weather_df_average.columns:
    if col == "temp":
        continue
    #model.add_regressor(col,prior_scale=1,mode="multiplicative")
  #model.add_regressor("weather",mode="additive")
    model.add_regressor("rain",mode=regressor_mode)
    model.add_regressor("snow",mode=regressor_mode)
    model.add_regressor("freezing",mode=regressor_mode)
    model.add_regressor("visibility",mode=regressor_mode)
    model.add_regressor("crashes",mode=regressor_mode)
    model.fit(train_df)
  #future_pd = model.make_future_dataframe(periods=len(test_df), freq='d') 
  #future_pd.insert(1,"temp",daily_average_temp_total)
    future_pd = train_df.append(test_df)
    future_pd["ds"] = daily_average.index
  #future_pd = future_pd.set_index("ds")
    forecast_pd = model.predict(future_pd)
    forecast_pd = forecast_pd.set_index("ds")
    forecast_pd["ds"] = forecast_pd.index
    target = pd.merge(daily_average, forecast_pd, how='outer', left_index=True, right_index=True)
    test_vals = target[["yhat","y"]][train_ind:-1]
    error = RMSE(test_vals)
    out = pd.DataFrame()
    out.insert(0,"id",street_id)
    out.insert(1,"RMSE",error)
    return out

results = (df3.groupBy('id').apply(forecast_speed).withColumn('training_date', current_date()))
#results.write.parquet("hdfs://babar.es.its.nyu.edu/user/yz7413/project/model_evaluation_10.parquet")