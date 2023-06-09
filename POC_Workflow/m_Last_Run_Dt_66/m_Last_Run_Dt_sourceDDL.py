# Databricks notebook source
# COMMAND ----------

CREATE DATABASE IF NOT EXISTS DELTA_TRAINING;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.DAYS(DAY_DT TIMESTAMP,
BUSINESS_DAY_FLAG STRING,
HOLIDAY_FLAG STRING,
DAY_OF_WK_NAME STRING,
DAY_OF_WK_NAME_ABBR STRING,
DAY_OF_WK_NBR INT,
CAL_DAY_OF_MO_NBR INT,
CAL_DAY_OF_YR_NBR INT,
CAL_WK INT,
CAL_WK_NBR INT,
CAL_MO INT,
CAL_MO_NBR INT,
CAL_MO_NAME STRING,
CAL_MO_NAME_ABBR STRING,
CAL_QTR INT,
CAL_QTR_NBR INT,
CAL_HALF INT,
CAL_YR INT,
FISCAL_DAY_OF_MO_NBR INT,
FISCAL_DAY_OF_YR_NBR INT,
FISCAL_WK INT,
FISCAL_WK_NBR INT,
FISCAL_MO INT,
FISCAL_MO_NBR INT,
FISCAL_MO_NAME STRING,
FISCAL_MO_NAME_ABBR STRING,
FISCAL_QTR INT,
FISCAL_QTR_NBR INT,
FISCAL_HALF INT,
FISCAL_YR INT,
LYR_WEEK_DT TIMESTAMP,
LWK_WEEK_DT TIMESTAMP,
WEEK_DT TIMESTAMP,
EST_TIME_CONV_AMT INT,
EST_TIME_CONV_HRS INT,
ES0_TIME_CONV_AMT INT,
ES0_TIME_CONV_HRS INT,
CST_TIME_CONV_AMT INT,
CST_TIME_CONV_HRS INT,
CS0_TIME_CONV_AMT INT,
CS0_TIME_CONV_HRS INT,
MST_TIME_CONV_AMT INT,
MST_TIME_CONV_HRS INT,
MS0_TIME_CONV_AMT INT,
MS0_TIME_CONV_HRS INT,
PST_TIME_CONV_AMT INT,
PST_TIME_CONV_HRS INT) USING DELTA;