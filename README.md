# TrafficForecasting
Forecasting Street Speed using Alternative Data

This repository contains code, data and analysis regarding traffic forecasting. 

## code: contains all data preprocessing and pyspark scripts for performing forecasting
    data-processing folders include scripts used to process raw data
    2019-2020_nodes.csv  validation street speed time series
    Road_id.txt          validation street ids
    data-etl-validation.py python script for extracting validation set
    data-etl.py            python script for extracting arbitrary sized dataset
    spark-model.py         python script for performing modelling directly on data in HDFS
    weather-2019.csv       weather data 


data: contains variety of background literature


traffic-forecasting-modelling.ipynb   notebook used to prototype models 
traffic-forecasting.pdf               project article and analysis of results
