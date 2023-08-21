# Databricks notebook source
# MAGIC %run "/Shared/Common Utilities/helper_utils"

# COMMAND ----------

config = get_config()

# COMMAND ----------

# only updated the container name and filename
# sas-extracts and Daily Tracker Dashboard for exporting.xlsx
Sheet_name = 'Results(2)'
file_location = "wasbs://sas-extracts@cctdash0000prd0504blob.blob.core.windows.net/Daily Tracker Dashboard for exporting.xlsx"
vaccine_mohsas_df=read_excel_file(file_location, Sheet_name)

vaccine_mohsas_df=make_col_names_safe(vaccine_mohsas_df)   ##rename columns


# a = dbutils.fs.ls('dbfs:/FileStore/Daily_Tracker_Dashboard_for_exporting.xlsx')
# import pandas as pd

# # print(test.sheet_names)
# print(a)
# test = pd.ExcelFile(a)
new_vaccine_mohsas_df = vaccine_mohsas_df
new_vaccine_mohsas_df = new_vaccine_mohsas_df.toPandas()
n = 1
new_df = new_vaccine_mohsas_df.iloc[:-n]
new_df = spark.createDataFrame(new_df)

# COMMAND ----------

###Transformed original Vacfile/sheet-Results(2)
from pyspark.sql.functions import *
from pyspark.sql.functions import when, regexp_replace, col, concat, to_date
import pyspark.sql.functions as f
from pyspark.sql.functions import concat_ws
vaccine_mohsas_df = vaccine_mohsas_df.withColumn("Date",to_date('Date'))
last_date = vaccine_mohsas_df.select('Date').orderBy(desc("date")).take(1)
last_date = pd.DataFrame(last_date, columns=['Date'])
last_date = spark.createDataFrame(last_date)
display(last_date)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###ETl
# MAGIC

# COMMAND ----------

##cumulatives_doses calculate
from pyspark.sql.functions import sum, avg
cumulatives_doses = vaccine_mohsas_df.select(sum("daily_dose_count_total"))
cumulatives_doses = cumulatives_doses.withColumnRenamed("sum(daily_dose_count_total)","Cumulative doses")

# COMMAND ----------

###cumulatives_doses-7 days
from pyspark.sql.functions import desc
import pandas as pd
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType,FloatType
cumu_doses_ = vaccine_mohsas_df.select('daily_dose_count_total').orderBy(desc("date")).take(7)
cumu_doses_ = pd.DataFrame(cumu_doses_, columns=['daily_dose_count_total'])
cumulativedoses_7dayAvg = cumu_doses_.sum().to_frame('dailydose_count_total')
cumulativedoses_7dayAvg = spark.createDataFrame(cumulativedoses_7dayAvg)
final_df=cumulatives_doses.join(cumulativedoses_7dayAvg)

# COMMAND ----------

#Daily doses = last date of daily dose count
daily_doses = vaccine_mohsas_df.select('daily_dose_count_total').orderBy(desc("date")).take(1)
daily_doses = pd.DataFrame(daily_doses, columns=['daily_dose_count_total'])
daily_doses = daily_doses.sum().to_frame('Daily_Doses_lastdate')
daily_doses = spark.createDataFrame(daily_doses)
final_df=final_df.join(daily_doses)

# COMMAND ----------

#7 day rolling daily dose count
_daily_doses = vaccine_mohsas_df.select('daily_dose_count_total').orderBy(desc("date")).take(8)
_daily_doses = pd.DataFrame(_daily_doses, columns=['daily_dose_count_total'])
_daily_doses = spark.createDataFrame(_daily_doses)
daily_doses_8thAvg = _daily_doses.select('daily_dose_count_total').orderBy(desc("daily_dose_count_total")).take(1)
daily_doses_8thAvg = pd.DataFrame(daily_doses_8thAvg, columns=['Daily_Doses_8thDate'])
daily_doses_lastdate=daily_doses.select(daily_doses.Daily_Doses_lastdate).toPandas()
daily_doses_lastdate = (list(daily_doses_lastdate['Daily_Doses_lastdate']))
daily_doses8thAvg = (list(daily_doses_8thAvg['Daily_Doses_8thDate']))
sub_list = []
zip_obj = zip(daily_doses_lastdate, daily_doses8thAvg)
for daily_doses_lastdate, daily_doses8thAvg in zip_obj:
    sub_list.append(daily_doses_lastdate - daily_doses8thAvg)
daily_dose_diff = pd.DataFrame(sub_list, columns=['daily_dose_diff_'])
daily_dose_diff = spark.createDataFrame(daily_dose_diff)
final_df=final_df.join(daily_dose_diff)

# COMMAND ----------

#7day Average doses = last date of doses administered
day_Avgdoses = vaccine_mohsas_df.select('doses_administered_7-day_avg').orderBy(desc("date")).take(1)
day_Avgdoses = pd.DataFrame(day_Avgdoses, columns=['doses_administered_7dayavg'])
day_Avgdoses = day_Avgdoses.sum().to_frame('doses_administered_7dayavg')
day_Avgdoses = spark.createDataFrame(day_Avgdoses)
final_df=final_df.join(day_Avgdoses)

# COMMAND ----------

##last date of doses administered - 8th day
dose_adim_= vaccine_mohsas_df.select('doses_administered_7-day_avg').orderBy(desc("date")).take(8)
dose_adim_ = pd.DataFrame(dose_adim_, columns=['doses_administered_7-day_avg'])
dose_adim_ = spark.createDataFrame(dose_adim_)
dose_adim_8th = dose_adim_.select('doses_administered_7-day_avg').orderBy(desc("doses_administered_7-day_avg")).take(1)
dose_adim_8th = pd.DataFrame(dose_adim_8th, columns=['doses_administered_7dayavg'])
dose_adim_8th = spark.createDataFrame(dose_adim_8th)
dose_7th_lastdate=day_Avgdoses.select(day_Avgdoses.doses_administered_7dayavg).toPandas()
dose_doses8thAvg=dose_adim_8th.select(dose_adim_8th.doses_administered_7dayavg).toPandas()
doses_lastdate = (list(dose_7th_lastdate['doses_administered_7dayavg']))
doses8thAvg = (list(dose_doses8thAvg['doses_administered_7dayavg']))
sub_list = []
zip_obj = zip(doses_lastdate, doses8thAvg)
for doses_lastdate, doses8thAvg in zip_obj:
    sub_list.append(doses_lastdate - doses8thAvg)
dose_diff=pd.DataFrame(sub_list, columns=['daily_dose_diff'])
dose_diff=spark.createDataFrame(dose_diff)
final_df=final_df.join(dose_diff)

# COMMAND ----------

##minimum 1 dose 
dose_min1 = vaccine_mohsas_df.select('people_w/_at_least_one_dose').orderBy(desc("date")).take(1)
dose_min1 = pd.DataFrame(dose_min1, columns=['people_least1dose'])
dose_min1 = spark.createDataFrame(dose_min1)

# COMMAND ----------

##lastdate of people with least1dose-8thDay
dose_adim_= vaccine_mohsas_df.select('people_w/_at_least_one_dose').orderBy(desc("date")).take(8)
dose_adim_ = pd.DataFrame(dose_adim_, columns=['people_w/_at_least_one_dose'])
dose_adim_ = spark.createDataFrame(dose_adim_)
dose_min1dose_8th = dose_adim_.select('people_w/_at_least_one_dose').orderBy("people_w/_at_least_one_dose").take(1)
dose_min1dose_8th = pd.DataFrame(dose_min1dose_8th, columns=['day7_rolling_Min_1dose'])
dose_min1dose_8th = spark.createDataFrame(dose_min1dose_8th)
dose_lastday_min1dose=dose_min1.select(dose_min1.people_least1dose).toPandas()
dose_doses8thAvg=dose_min1dose_8th.select(dose_min1dose_8th.day7_rolling_Min_1dose).toPandas()
doses_lastdate = (list(dose_lastday_min1dose['people_least1dose']))
doses8thAvg = (list(dose_doses8thAvg['day7_rolling_Min_1dose']))
sub_list = []
zip_obj = zip(doses_lastdate, doses8thAvg)
for doses_lastdate, doses8thAvg in zip_obj:
    sub_list.append(doses_lastdate - doses8thAvg)
dose_diff_min1dose=pd.DataFrame(sub_list, columns=['7day_rolling_min1dose'])
dose_diff_min1dose=spark.createDataFrame(dose_diff_min1dose)
_final_df=dose_min1.join(dose_diff_min1dose)

# COMMAND ----------

###completed primary series = last date of the people fully vaccinated
%time
df_fullvac = vaccine_mohsas_df.select('people_fully_vaccinated').orderBy(desc("date")).take(1)
df_fullvac = pd.DataFrame(df_fullvac, columns=['p_fully_vaccinated'])
df_fullvac = spark.createDataFrame(df_fullvac)
_final_df=_final_df.join(df_fullvac)

# COMMAND ----------

##completed primary series, 7 day rolling
%time
dose_adim_= vaccine_mohsas_df.select('people_fully_vaccinated').orderBy(desc("date")).take(8)
dose_adim_ = pd.DataFrame(dose_adim_, columns=['people_fully_vaccinated'])
dose_adim_ = spark.createDataFrame(dose_adim_)
comp_p_ = dose_adim_.select('people_fully_vaccinated').orderBy("people_fully_vaccinated").take(1)
comp_p_ = pd.DataFrame(comp_p_, columns=['peopefullvac_7dayroll'])
comp_p_ = spark.createDataFrame(comp_p_)
fullvac=df_fullvac.select(df_fullvac.p_fully_vaccinated).toPandas()
comp_p_=comp_p_.select(comp_p_.peopefullvac_7dayroll).toPandas()
latest = (list(fullvac['p_fully_vaccinated']))
day7roll = (list(comp_p_['peopefullvac_7dayroll']))
sub_list = []
zip_obj = zip(latest, day7roll)
for latest, day7roll in zip_obj:
    sub_list.append(latest - day7roll)
peop_fullvac=pd.DataFrame(sub_list, columns=['7day_fullVac'])
peop_fullvac=spark.createDataFrame(peop_fullvac)
_final_df=_final_df.join(peop_fullvac)

# COMMAND ----------

##Spring booster  #people_w/_fall_booster
%time
fallb = vaccine_mohsas_df.select('people_w/_fall_booster').orderBy(desc("date")).take(1)
fallb = pd.DataFrame(fallb, columns=['people_fall_booster'])
fallb = spark.createDataFrame(fallb)
_final_df=_final_df.join(fallb)

# COMMAND ----------

##fall booster 7 day rolling
%time
fallboosterdf= vaccine_mohsas_df.select('people_w/_fall_booster').orderBy(desc("date")).take(8)
fallboosterdf = pd.DataFrame(fallboosterdf, columns=['people_w/_fall_booster'])
fallboosterdf = spark.createDataFrame(fallboosterdf)
fallboosterdf = fallboosterdf.select('people_w/_fall_booster').orderBy("people_w/_fall_booster").take(1)
fallboosterdf = pd.DataFrame(fallboosterdf, columns=['Fall_booster_7dayroll'])
fallboosterdf = spark.createDataFrame(fallboosterdf)
fallb=fallb.select(fallb.people_fall_booster).toPandas()
fallboosterdf=fallboosterdf.select(fallboosterdf.Fall_booster_7dayroll).toPandas()
latest = (list(fallb['people_fall_booster']))
fallbooster_7dayroll = (list(fallboosterdf['Fall_booster_7dayroll']))
sub_list = []
zip_obj = zip(latest, fallbooster_7dayroll)
for latest, fallbooster_7dayroll in zip_obj:
    sub_list.append(latest - fallbooster_7dayroll)
dose_diff_fall_springbooster=pd.DataFrame(sub_list, columns=['Spring/Fall_booster_7dayroll'])
dose_diff_fall_springbooster=spark.createDataFrame(dose_diff_fall_springbooster)
_final_df=_final_df.join(dose_diff_fall_springbooster)

# COMMAND ----------

##combine all dfs
# cumulatives_doses, cumulativedoses_7dayAvg, daily_doses, daily_dose_diff, day7_Avgdoses, dose_diff,
# dose_min1, dose_diff_min1dose,df_fullvac, peop_fullvac, fallb, dose_diff_fall_springbooster
# display(final_df)
vaccinesasphu_cal=make_col_names_safe(final_df)
vaccinesasphu_cal_=make_col_names_safe(_final_df)

# COMMAND ----------

from pyspark.sql.window import *
from pyspark.sql.functions import *
sqlContext.setConf("spark.sql.shuffle.partitions", "10")
vaccinesasphu_cal = vaccinesasphu_cal.withColumn("id", monotonically_increasing_id())
vaccinesasphu_cal_ = vaccinesasphu_cal_.withColumn("id", monotonically_increasing_id())
vaccine_mohsas_df = vaccine_mohsas_df.withColumn("id", monotonically_increasing_id())
last_date = last_date.withColumn("id", monotonically_increasing_id())
window = Window.orderBy(col('id'))
vaccinesasphu_cal = vaccinesasphu_cal.withColumn('id', row_number().over(window))
vaccinesasphu_cal_ = vaccinesasphu_cal_.withColumn('id', row_number().over(window))
vaccine_mohsas_df = vaccine_mohsas_df.withColumn('id', row_number().over(window))
last_date = last_date.withColumn('id', row_number().over(window))

_results = vaccinesasphu_cal.join(vaccinesasphu_cal_,['id'],how='inner')
final_vaccinesasphu = last_date.join(_results,['id'],how='inner')
final_vaccinesasphu_ = vaccine_mohsas_df.join(final_vaccinesasphu,['Date'],how='full')
final_vaccinesasphu_ = final_vaccinesasphu_.drop("id", "id")
# display(final_vaccinesasphu_)
a = final_vaccinesasphu_
# a.printSchema()
# b = a.toPandas()
# c = b.bfill(axis ='rows')

 file = "dbfs:/FileStore/tables/Vactest.csv"
_file = spark.read.options(header=True, inferSchema=True).csv(file)
 b = a.unionByName(_file)
 b = a.join(_file,['Date'],how='full')
display(b)

z = 
c = c.union(z)

# COMMAND ----------

#####write Table to Public/ist_0504_cctdash_0000_uc
%time
vaccine_output = config["cct_db_schema"] + ".vaccineSASPHU_calulations"
spark.sql(f"DROP TABLE IF EXISTS {vaccine_output}")
final_vaccinesasphu_.write.saveAsTable(vaccine_output)

# COMMAND ----------

print("2 tables loaded")  ## from sheet-Results(2)

# COMMAND ----------

##############Reading sheet-Results(1)
file_location = "wasbs://sas-extracts@cctdash0000prd0504blob.blob.core.windows.net/Daily Tracker Dashboard for exporting.xlsx"
Sheet_name = 'Results(1)'
vaccine_mohsas_df_sheet2=read_excel_file(file_location, Sheet_name)
# vaccine_mohsas_df = read_excel_file(file_location) #helper_utils.py
vaccine_mohsas_df_sheet2.createOrReplaceTempView("daily_tracker1")

##rename columns
vaccine_mohsas_df_sheet2=make_col_names_safe(vaccine_mohsas_df_sheet2)

# COMMAND ----------

###Transformations------timestamp to date - Sheet-Results(1)
##-----------Column- age_group_new
from pyspark.sql.functions import *
from pyspark.sql.window import *

vaccine_mohsas_df_sheet2 = vaccine_mohsas_df_sheet2.withColumn("current_date:",to_date('current_date:'))\
                                                   .withColumn("yesterday_date:",to_date('yesterday_date:'))
vaccine_mohsas_df_sheet2 = vaccine_mohsas_df_sheet2.withColumn("id", monotonically_increasing_id())
window = Window.orderBy(col('id'))
vaccine_mohsas_df_sheet2 = vaccine_mohsas_df_sheet2.withColumn('id', row_number().over(window))

# COMMAND ----------

_age = vaccine_mohsas_df_sheet2.select("age_group_new")
split_col = f.split(_age['age_group_new'], 'to')
minA = _age.withColumn('Min_Age', split_col.getItem(0))
maxA = _age.withColumn('Max_Age', split_col.getItem(1))
output_age = minA.join(maxA,['age_group_new'],how='inner')
output_age = output_age.withColumn("id", monotonically_increasing_id())
output_ = output_age.select(concat_ws("-",output_age.Min_Age,output_age.Max_Age).alias("age_group"))
output_ = output_.withColumn("id", monotonically_increasing_id())
_coutput_age = output_age.join(output_,['id'],how='inner')
_coutput_age = _coutput_age.drop(*['id','Min_Age', 'Max_Age'])
vaccine_mohsas_df_sheet2_ = _coutput_age.join(vaccine_mohsas_df_sheet2,['age_group_new'],how='inner')

# COMMAND ----------

###write agegroup-Sheet- Results1 to Table/IST/public
%time
agegroup_update = config["cct_db_schema"] + ".vaccines_agegroup_update"
spark.sql(f"DROP TABLE IF EXISTS {agegroup_update}")
vaccine_mohsas_df_sheet2_.write.saveAsTable(agegroup_update)

# COMMAND ----------

#####Sheet-Results/ renaming column - Public Health Unit
file_location = "wasbs://sas-extracts@cctdash0000prd0504blob.blob.core.windows.net/Daily Tracker Dashboard for exporting.xlsx"
Sheet_name = 'Results'
vaccine_mohsas_df_sheet3=read_excel_file(file_location, Sheet_name)
##rename columns
vaccine_mohsas_df_sheet3=make_col_names_safe(vaccine_mohsas_df_sheet3)

# COMMAND ----------

pHU_names = vaccine_mohsas_df_sheet3.select("public_health_unit")
pHU_names = pHU_names.withColumn("id", monotonically_increasing_id())
from pyspark.sql.types import StringType
newPHUnames = spark.createDataFrame([("Algoma"), ("Brant"), ("Chatham-Kent"), ("Durham"),("Eastern"),("Grey Bruce"),\
                           ("Haldimand-Norfolk"), ("HKPR"), ("Halton"),("Hamilton"),("HPE"),("Huron-Perth"),\
                           ("KFLA"),("Lambton"), ("LGL"), ("Middlesex-London"),("Niagara"), ("NBPS"),("Northwestern"),\
                           ("Ottawa"),("Peel"), ("Peterborough"), ("Porcupine"),("Renfrew"), ("Simcoe Muskoka"),\
                           ("Southwestern"),("Sudbury"), ("Thunder Bay"), ("Timiskaming"),("Toronto"),("Unknown"),\
                           ("Waterloo"),("WDG"),("Windsor-Essex"), ("York")], StringType())
newPHUnames = newPHUnames.withColumnRenamed("value","new_PHUnames")
newPHUnames = newPHUnames.withColumn("id", monotonically_increasing_id())
_newPHUnames = pHU_names.join(newPHUnames,['id'],how='inner')
_newPHUnames = _newPHUnames.drop(*['id'])
vaccine_mohsas_sheet3_ = vaccine_mohsas_df_sheet3.join(_newPHUnames,['public_health_unit'],how='inner')

# COMMAND ----------

###write agegroup-Sheet- Results1 to Table/IST/public
%time
PHUname_update = config["cct_db_schema"] + ".vaccines_PHUname_update"
spark.sql(f"DROP TABLE IF EXISTS {PHUname_update}")
vaccine_mohsas_sheet3_.write.saveAsTable(PHUname_update)

print("All tables loaded")
