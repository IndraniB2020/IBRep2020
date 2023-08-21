# Databricks notebook source
# MAGIC %run "/Repos/CCT_Dashboard/databricks_cct/Common Utilities/helper_utils"

# COMMAND ----------

config = get_config()

# COMMAND ----------

dbutils.fs.cp("wasbs://input@cctdash0000prd0504blob.blob.core.windows.net/haib_on_case_linelist_ODS.zip", "dbfs:/FileStore/OutcomeV3/", True) 

# COMMAND ----------

file_location = "dbfs:/FileStore/OutcomeV3/linelist.zip/haib_on_case_linelist_ODS.csv"
fileN = spark.read.options(header=True, inferSchema=True).csv(file_location)
fileN = fileN.orderBy("CLIENT_DEATH_DATE", ascending=False)
dates = fileN.select("CLIENT_DEATH_DATE").distinct()
dates = dates.na.drop()
dates = dates.withColumnRenamed("CLIENT_DEATH_DATE","Date") ###getting the unique client death dates to merge with final dataframe as all client death dates doesnt actually indicate a death
dates_case_reported = fileN.select("CASE_REPORTED_DATE").distinct() ###to calculate average, we need every date
dates_case_reported = dates_case_reported.na.drop()

# COMMAND ----------

from datetime import timezone, datetime, timedelta
import os.path
import pandas as pd
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import when, to_date, col, lag, lit, monotonically_increasing_id, row_number, count, lead, coalesce
from pyspark.sql import functions as F, Window as W
import datetime
from pyspark.sql import DataFrame
from functools import reduce
date = datetime.date.today()

Pres = date.today()
offset = (Pres.weekday() - 1) % 7
Date_T = str(Pres - timedelta(days=offset, hours=0))
path = Date_T  ##print(type(path))
path = pd.to_datetime(path).date()

fileN = fileN.filter(fileN.COVID_DEATH.contains("YES"))
fileN = fileN.withColumn("CLIENT_DEATH_DATE", when(fileN["CLIENT_DEATH_DATE"].isNull(), fileN.CASE_REPORTED_DATE).otherwise(fileN["CLIENT_DEATH_DATE"]))
fileN = fileN.withColumn("CLIENT_DEATH_DATE", when(fileN["CLIENT_DEATH_DATE"] < "2020-01-01", fileN.CASE_REPORTED_DATE).otherwise(fileN["CLIENT_DEATH_DATE"]))
#####Date should be less than latest Tuesday
fileN = fileN.filter(fileN["CLIENT_DEATH_DATE"] < path)

fileN = fileN.orderBy("CLIENT_DEATH_DATE", ascending=False)
###sorting columns as per requirements
_df = fileN[["CLIENT_DEATH_DATE","AGE_AT_TIME_OF_ILLNESS"]]

# COMMAND ----------

# MAGIC %md
# MAGIC ##etl

# COMMAND ----------

_df = _df.withColumn("age_group_80_and_over_daily", when((_df.AGE_AT_TIME_OF_ILLNESS >= 80) & \
                                                         (_df.AGE_AT_TIME_OF_ILLNESS <= 114), "1" ))\
         .withColumn("age_group_60_79_daily", when((_df.AGE_AT_TIME_OF_ILLNESS >= 60) & \
                                                         (_df.AGE_AT_TIME_OF_ILLNESS <= 79), "1" ))\
         .withColumn("age_group_40_59_daily", when((_df.AGE_AT_TIME_OF_ILLNESS >= 40) & \
                                                         (_df.AGE_AT_TIME_OF_ILLNESS <= 59), "1" ))\
         .withColumn("age_group_20_39_daily", when((_df.AGE_AT_TIME_OF_ILLNESS >= 20) & \
                                                         (_df.AGE_AT_TIME_OF_ILLNESS <= 39), "1" ))\
         .withColumn("age_group_0_19_daily", when((_df.AGE_AT_TIME_OF_ILLNESS >= 0) & \
                                                         (_df.AGE_AT_TIME_OF_ILLNESS <= 19), "1" ))    

_df = _df.withColumn("age_group_80_and_over_daily", _df["age_group_80_and_over_daily"].cast(IntegerType()))\
         .withColumn("age_group_60_79_daily", _df["age_group_60_79_daily"].cast(IntegerType()))\
         .withColumn("age_group_40_59_daily", _df["age_group_40_59_daily"].cast(IntegerType()))\
         .withColumn("age_group_20_39_daily", _df["age_group_20_39_daily"].cast(IntegerType()))\
         .withColumn("age_group_0_19_daily", _df["age_group_0_19_daily"].cast(IntegerType()))

_df = _df.groupBy("CLIENT_DEATH_DATE").sum("age_group_80_and_over_daily","age_group_60_79_daily", "age_group_40_59_daily","age_group_20_39_daily","age_group_0_19_daily")  

_df =  _df.withColumnRenamed("sum(age_group_80_and_over_daily)","age_group_80_and_over_sum")\
        .withColumnRenamed("sum(age_group_0_19_daily)","age_group_0_19_sum")\
        .withColumnRenamed("sum(age_group_20_39_daily)","age_group_20_39_sum")\
        .withColumnRenamed("sum(age_group_40_59_daily)","age_group_40_59_sum")\
        .withColumnRenamed("sum(age_group_60_79_daily)","age_group_60_79_sum")

w = Window.orderBy('CLIENT_DEATH_DATE').rowsBetween(Window.unboundedPreceding, Window.currentRow)
_df = _df.withColumn('age_group_80_and_over_cumulative', F.sum('age_group_80_and_over_sum').over(w))\
         .withColumn('age_group_60_79_cumulative', F.sum('age_group_60_79_sum').over(w))\
         .withColumn('age_group_40_59_cumulative', F.sum('age_group_40_59_sum').over(w)) \
         .withColumn('age_group_20_39_cumulative', F.sum('age_group_20_39_sum').over(w))\
         .withColumn('age_group_0_19_cumulative', F.sum('age_group_0_19_sum').over(w))  

# COMMAND ----------

final_df = _df.join(dates, dates.Date == _df.CLIENT_DEATH_DATE,"full")
final_df = final_df.drop(final_df['CLIENT_DEATH_DATE'])
final_df = final_df.fillna(0)
final_df = final_df.select(sorted(final_df.columns))

final_df = final_df.withColumn('age_group_80_and_over_cumulative',F.last(F.when(F.col('age_group_80_and_over_cumulative') != 0, F.col('age_group_80_and_over_cumulative')), True).over(W.orderBy('Date')))\
                   .withColumn('age_group_60_79_cumulative',F.last(F.when(F.col('age_group_60_79_cumulative') != 0, F.col('age_group_60_79_cumulative')), True).over(W.orderBy('Date')))\
                    .withColumn('age_group_40_59_cumulative',F.last(F.when(F.col('age_group_40_59_cumulative') != 0, F.col('age_group_40_59_cumulative')), True).over(W.orderBy('Date')))\
                    .withColumn('age_group_20_39_cumulative',F.last(F.when(F.col('age_group_20_39_cumulative') != 0, F.col('age_group_20_39_cumulative')), True).over(W.orderBy('Date')))\
                     .withColumn('age_group_0_19_cumulative',F.last(F.when(F.col('age_group_0_19_cumulative') != 0, F.col('age_group_0_19_cumulative')), True).over(W.orderBy('Date')))
final_df = final_df.fillna(0)


# COMMAND ----------

spec = Window.orderBy("Date")

####Agegroup 80-114
final_df = final_df.withColumn("Age_80_above",lag("age_group_80_and_over_cumulative",7).over(spec))
final_df = final_df.withColumn("age_group_80_and_over_avg7day", F.when(F.isnull(final_df.age_group_80_and_over_cumulative - final_df.Age_80_above), 0).otherwise(final_df.age_group_80_and_over_cumulative - final_df.Age_80_above)/7)
final_df = final_df.withColumn("age_group_80_and_over_avg7day", func.round(final_df["age_group_80_and_over_avg7day"], 1)).drop(final_df['Age_80_above'])

####Agegroup 60-79
final_df = final_df.withColumn("Age_60_79",lag("age_group_60_79_cumulative",7).over(spec))
final_df = final_df.withColumn("age_group_60_79_avg7day", F.when(F.isnull(final_df.age_group_60_79_cumulative - final_df.Age_60_79), 0).otherwise(final_df.age_group_60_79_cumulative - final_df.Age_60_79)/7)
final_df = final_df.withColumn("age_group_60_79_avg7day", func.round(final_df["age_group_60_79_avg7day"], 1)).drop(final_df['Age_60_79'])

####Agegroup 40-59
final_df = final_df.withColumn("Age_40_59",lag("age_group_40_59_cumulative",7).over(spec))
final_df = final_df.withColumn("age_group_40_59_avg7day", F.when(F.isnull(final_df.age_group_40_59_cumulative - final_df.Age_40_59), 0).otherwise(final_df.age_group_40_59_cumulative - final_df.Age_40_59)/7)
final_df = final_df.withColumn("age_group_40_59_avg7day", func.round(final_df["age_group_40_59_avg7day"], 1)).drop(final_df['Age_40_59'])

####Agegroup 20-39
final_df = final_df.withColumn("Age_20_39",lag("age_group_20_39_cumulative",7).over(spec))
final_df = final_df.withColumn("age_group_20_39_avg7day", F.when(F.isnull(final_df.age_group_20_39_cumulative - final_df.Age_20_39), 0).otherwise(final_df.age_group_20_39_cumulative - final_df.Age_20_39)/7)
final_df = final_df.withColumn("age_group_20_39_avg7day", func.round(final_df["age_group_20_39_avg7day"], 1)).drop(final_df['Age_20_39'])

####Agegroup 0-19
final_df = final_df.withColumn("Age_0_19",lag("age_group_0_19_cumulative",7).over(spec))
final_df = final_df.withColumn("age_group_0_19_avg7day", F.when(F.isnull(final_df.age_group_0_19_cumulative - final_df.Age_0_19), 0).otherwise(final_df.age_group_0_19_cumulative - final_df.Age_0_19)/7)
final_df = final_df.withColumn("age_group_0_19_avg7day", func.round(final_df["age_group_0_19_avg7day"], 1)).drop(final_df['Age_0_19'])

final_df = final_df.select(sorted(final_df.columns))

# COMMAND ----------

###To Calculate Cuulative difference, we need all dates, hence Case_REPORTED_DATe was choosen and the calculations below
_final_df = final_df[["Date", "age_group_80_and_over_avg7day","age_group_60_79_avg7day","age_group_40_59_avg7day","age_group_20_39_avg7day","age_group_0_19_avg7day"]]

_df_dates = _final_df.join(dates_case_reported, dates_case_reported.CASE_REPORTED_DATE == _final_df.Date,"full")
_df_dates = _df_dates.fillna(0)
_df_dates = _df_dates.select(sorted(_df_dates.columns))
_df_dates = _df_dates.filter(_df_dates["CASE_REPORTED_DATE"] < path)

new_spec = Window.orderBy("CASE_REPORTED_DATE")

_df_dates = _df_dates.withColumn('age_group_80_and_over_avg7day',F.last(F.when(F.col('age_group_80_and_over_avg7day') != 0, F.col('age_group_80_and_over_avg7day')), True).over(W.orderBy('CASE_REPORTED_DATE')))\
                   .withColumn('age_group_60_79_avg7day',F.last(F.when(F.col('age_group_60_79_avg7day') != 0, F.col('age_group_60_79_avg7day')), True).over(W.orderBy('CASE_REPORTED_DATE')))\
                    .withColumn('age_group_40_59_avg7day',F.last(F.when(F.col('age_group_40_59_avg7day') != 0, F.col('age_group_40_59_avg7day')), True).over(W.orderBy('CASE_REPORTED_DATE')))\
                    .withColumn('age_group_20_39_avg7day',F.last(F.when(F.col('age_group_20_39_avg7day') != 0, F.col('age_group_20_39_avg7day')), True).over(W.orderBy('CASE_REPORTED_DATE')))\
                     .withColumn('age_group_0_19_avg7day',F.last(F.when(F.col('age_group_0_19_avg7day') != 0, F.col('age_group_0_19_avg7day')), True).over(W.orderBy('CASE_REPORTED_DATE')))

####Agegroup 80-114, difference in cumulatives
_df_dates = _df_dates.withColumn("lag_age_group_80_and_over_avg7day",lag("age_group_80_and_over_avg7day",7).over(new_spec))
_df_dates = _df_dates.withColumn("diff_age_group_80_and_over_avg7day", F.when(F.isnull(_df_dates.age_group_80_and_over_avg7day - _df_dates.lag_age_group_80_and_over_avg7day), 0).otherwise(_df_dates.age_group_80_and_over_avg7day - _df_dates.lag_age_group_80_and_over_avg7day)).drop(_df_dates['lag_age_group_80_and_over_avg7day'])

####Agegroup 60-79, difference in cumulatives
_df_dates = _df_dates.withColumn("lag_age_group_60_79_avg7day",lag("age_group_60_79_avg7day",7).over(new_spec))
_df_dates = _df_dates.withColumn("diff_age_group_60_79_avg7day", F.when(F.isnull(_df_dates.age_group_60_79_avg7day - _df_dates.lag_age_group_60_79_avg7day), 0).otherwise(_df_dates.age_group_60_79_avg7day - _df_dates.lag_age_group_60_79_avg7day)).drop(_df_dates['lag_age_group_60_79_avg7day'])

####Agegroup 40-59, difference in cumulatives
_df_dates = _df_dates.withColumn("lag_age_group_40_59_avg7day",lag("age_group_40_59_avg7day",6).over(new_spec))
_df_dates = _df_dates.withColumn("diff_age_group_40_59_avg7day", F.when(F.isnull(_df_dates.age_group_40_59_avg7day - _df_dates.lag_age_group_40_59_avg7day), 0).otherwise(_df_dates.age_group_40_59_avg7day - _df_dates.lag_age_group_40_59_avg7day)).drop(_df_dates['lag_age_group_40_59_avg7day'])

####Agegroup 20-39, difference in cumulatives
_df_dates = _df_dates.withColumn("lag_age_group_20_39_avg7day",lag("age_group_20_39_avg7day",6).over(new_spec))
_df_dates = _df_dates.withColumn("diff_age_group_20_39_avg7day", F.when(F.isnull(_df_dates.age_group_20_39_avg7day - _df_dates.lag_age_group_20_39_avg7day), 0).otherwise(_df_dates.age_group_20_39_avg7day - _df_dates.lag_age_group_20_39_avg7day)).drop(_df_dates['lag_age_group_20_39_avg7day'])

#####Agegroup 0-19,  difference in cumulatives
_df_dates = _df_dates.withColumn("lag_age_group_0_19_avg7day",lag("age_group_0_19_avg7day",6).over(new_spec))
_df_dates = _df_dates.withColumn("diff_age_group_0_19_avg7day", F.when(F.isnull(_df_dates.age_group_0_19_avg7day - _df_dates.lag_age_group_0_19_avg7day), 0).otherwise(_df_dates.age_group_0_19_avg7day - _df_dates.lag_age_group_0_19_avg7day)).drop(_df_dates['lag_age_group_0_19_avg7day'])
_df_dates = _df_dates.drop(_df_dates["CASE_REPORTED_DATE"])
_df_dates = _df_dates.filter(_df_dates.Date.isNotNull())

new_final_df = _df_dates[["Date","diff_age_group_80_and_over_avg7day","diff_age_group_60_79_avg7day","diff_age_group_40_59_avg7day","diff_age_group_20_39_avg7day","diff_age_group_0_19_avg7day"]]
new_final_df = new_final_df.withColumnRenamed("Date","Client_death_date")

# COMMAND ----------

####file loaded to Public.
final_df = final_df.join(new_final_df, final_df.Date == new_final_df.Client_death_date,"full")
final_df = final_df.drop(final_df["Client_death_date"])

demo_death_by_age = config["cct_db_schema"] + ".Demographics_Deaths"
spark.sql(f"DROP TABLE IF EXISTS {demo_death_by_age}")
final_df.write.saveAsTable(demo_death_by_age)
print("table loaded")
