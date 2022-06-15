# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Creator: Taru Vaid, Colby Carrilo, Kushal Kapadia
# Date Created: 02/23/2020
# Date Updated: 

# Modified by: Sanjo Jose
# Date Modified: 19/08/2021
# Modifications: Phase 4 Improvements, Test run for Phase 4 BUs, Converted widgets to parameters, Other Minor modifications

# Description: 
# A notebook to be used for imputation of Regular Price for NA BUs
#This is adapted from accentures notebook and made suitable for the may refresh. 
# user imputs needed in cmd 4 & 10
# this notebook is reading in notebook 99 . The path to that will need to be updated for any new refresh

# COMMAND ----------

#enabling delta caching to improve performance
spark.conf.set("spark.app.name","localized_pricing")
sqlContext.clearCache()

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# !pip uninstall xlrd
!pip install xlrd==1.2.0

# COMMAND ----------

import pandas as pd
import datetime
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from operator import add
from functools import reduce
from pyspark.sql.functions import when, sum, avg, col, concat, lit
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import greatest
from pyspark.sql.functions import least
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType
import xlrd

# COMMAND ----------

# Define parameters used in notebook
business_units = dbutils.widgets.get("business_units")
bu_code = dbutils.widgets.get("bu_code")

phase = dbutils.widgets.get("phase") ##Input whether 'phase3' or 'phase4'

# Define the database for tables and directory paths for input & output files
db = dbutils.widgets.get("db")
file_directory = dbutils.widgets.get("file_directory")
wave = dbutils.widgets.get("wave") #Modify for each new wave

# Input files - From Finalised scoping after CatM review
# final_inscope_file = '/dbfs/{0}/{1}/{2}_Repo/Modelling_Inputs/{2}_Inscope_May2021_Refresh_Final_dbfs.csv'.format(file_directory, wave, bu_code) #Use for Phase 3

# final_inscope_file = '/dbfs/{0}/Accenture_Refresh/final_inscope_phase_4_BUs.xlsx'.format(file_directory) #Use for Phase 4

# Inscope reference column used in file #wherever applicable
#final_inscope_reference_col = dbutils.widgets.get("final_inscope_reference_col")

# Notebook used for computing PDI regular price
pdi_price_notebook_name = dbutils.widgets.get("pdi_price_notebook_name")

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Creating Widget
# #Create widgets for business unit selection

# Business_Units = ["1400 - Florida Division",
#                   "1600 - Coastal Carolina Division",
#                   "1700 - Southeast Division",
#                   "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division",
#                   "2600 - West Coast Division",
#                   "2800 - Texas Division",
#                   "2900 - South Atlantic Division",
#                   "3100 - Grand Canyon Division",
#                   "3800 - Northern Tier Division",
#                   "4100 - Midwest Division",
#                   "4200 - Great Lakes Division",
#                   "4300 - Heartland Division",
#                   "QUEBEC OUEST",
#                   "QUEBEC EST - ATLANTIQUE",
#                   "Central Division",
#                   "Western Division"]

# bu_abb = [["1400 - Florida Division", 'FL'],
#           ["1600 - Coastal Carolina Division", "CC"],
#           ["1700 - Southeast Division", "SE"],
#           ["1800 - Rocky Mountain Division", 'RM'],
#           ["1900 - Gulf Coast Division", "GC"],
#           ["2600 - West Coast Division", "WC"],
#           ["2800 - Texas Division", 'TX'],
#           ["2900 - South Atlantic Division", "SA"],
#           ["3100 - Grand Canyon Division", "GR"],
#           ["3800 - Northern Tier Division", "NT"],
#           ["4100 - Midwest Division", 'MW'],
#           ["4200 - Great Lakes Division", "GL"],
#           ["4300 - Heartland Division", 'HLD'],
#           ["QUEBEC OUEST", 'QW'],
#           ["QUEBEC EST - ATLANTIQUE", 'QE'],
#           ["Central Division", 'CE'],
#           ["Western Division", 'WC']]

# dbutils.widgets.dropdown("01.Business Unit", "3100 - Grand Canyon Division", Business_Units)

# COMMAND ----------

# DBTITLE 1,Reading the Widget
# bu = dbutils.widgets.get("01.Business Unit")
# bu

# #get bu 2 character code
# bu_abb_ds = sqlContext.createDataFrame(bu_abb, ['bu', 'bu_abb']).filter('bu = "{}"'.format(bu))
# bu_abb = bu_abb_ds.collect()[0][1]

# print("BU_Abb:", bu_abb, "; Business Unit:", bu)

# COMMAND ----------

##USER INPUT
#read in the final inscope itmes
# we just need the BU name and tempe product key

# scoped_items = pd.read_csv('/dbfs/Phase3_extensions/Elasticity_Modelling/JUNE2021_Refresh/CC_Repo/Modelling_Inputs/RM_Inscope_May2021_Refresh_Final_dbfs.csv')
# scoped_items = spark.createDataFrame(scoped_items)
# scoped_items = scoped_items.filter(col("in_scope_may2021_final") == 'Y').withColumn('BU',sf.lit('1800 - Rocky Mountain Division')).select(['BU','tempe_product_key'])

# There is a potential improvement to avoid the manual process for Finalising Scoping. But keeping the finalised scoping file from CM for now.
if phase == 'phase3':
  #scoped_items = pd.read_csv(final_inscope_file)
  #scoped_items = spark.createDataFrame(scoped_items)
  #scoped_items = scoped_items.filter(col(final_inscope_reference_col) == 'Y').withColumn('BU', sf.lit(business_units)).select(['BU','tempe_product_key'])
  
  scoped_items = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
  scoped_items = scoped_items.filter(col("BU") == business_units)#.withColumn('Scope',sf.lit('Y'))
  scoped_items = scoped_items.groupBy("BU",'product_key').agg(sf.max('last_update'))
  scoped_items = scoped_items.drop('max(last_update)').withColumnRenamed("product_key","tempe_product_key")
  
elif phase == 'phase4':
  #scoped_items = pd.read_excel(final_inscope_file)
  #scoped_items = spark.createDataFrame(scoped_items)
  #scoped_items = scoped_items.filter(col('BU') == business_units).select(['BU','tempe_product_key']).distinct()  
  # filter(col(final_inscope_reference_col) == 'Y')
  
  scoped_items = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
  scoped_items = scoped_items.filter(col("BU") == business_units)#.withColumn('Scope',sf.lit('Y'))
  scoped_items = scoped_items.groupBy("BU",'product_key').agg(sf.max('last_update'))
  scoped_items = scoped_items.drop('max(last_update)').withColumnRenamed("product_key","tempe_product_key")
  #.withColumnRenamed("Scope","inscope_prev_refresh")

display(scoped_items)

# COMMAND ----------

# keep only acc_cat and only relevant Items according to scoping
# join(sqlContext.sql("select * from localized_pricing.acc_cat"),["category_desc"],"left").\
#                filter(sf.col("Acc_Cat") != 'Out of Scope').\

txn_data = sqlContext.sql("""
select
  BU,
  upc,
  sell_unit_qty,
  product_key,
  tempe_prod_item_env_key,
  upc_sell_unit_key,
  promotion_env_key,
  prod_item_env_key,
  site_env_key,
  sys_environment_name,
  transaction_date,
  business_date,
  transaction_time,
  transaction_number,
  promotion_id,
  promotion_description,
  promotion_type,
  quantity_sold,
  sales_amount,
  loyalty_card_number,
  marketbasket_header_key,
  organization_key,
  timeofday_key,
  loyalty_key,
  businessdate_calendar_key,
  transaction_type_key,
  site_number_corporate,
  site_state_id,
  site_city,
  site_address,
  site_zip,
  gps_latitude,
  gps_longitude,
  region_id,
  region_desc,
  BU_ID,
  upc_new,
  sell_unit_qty_new,
  tempe_product_key,
  nacs_category_desc,
  department_desc,
  category_desc,
  sub_category_desc,
  item_desc,
  manufacturer_desc,
  brand_desc,
  package_desc,
  item_number,
  private_brand_desc,
  size_unit_of_measure,
  UPC_Sell_Unit_desc,
  mb_product_key,
  mb_product_env_key
  from {0}.{1}_master_merged_ads_{2}""".format(db, bu_code.lower(), wave.lower())).\
               filter(~sf.col("BU").isin(*business_units)).\
               withColumn("Promo_Flag", sf.when(col("promotion_id") == 0, 0).otherwise(1)).\
               join(scoped_items.select(['BU','tempe_product_key']).distinct().withColumn("keep", sf.lit(1)),['BU','tempe_product_key'],'left').\
               filter(col("keep").isNotNull()).\
               drop("keep").\
               withColumnRenamed("site_number_corporate","site")

# ##june2021_refresh
# Replace above table with {0}.{1}_master_merged_ads_

# COMMAND ----------

## Pause and check the distinct product keys in txn_date vs. scoped items
# txn_data.select('tempe_product_key').distinct().count() - scoped_items.count() #should be 0

# Difference in count might be due to the difference in period considered between phase 4 actual scoping and current test run - To be checked

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Regular Price Imputation

# COMMAND ----------

# %sql
# drop table if exists {}.{}_na_bus_master_raw_basket_data_with_prod_{}

# COMMAND ----------

# saving master data with relevant columns with only inscope acc cats
txn_data.write.mode("overwrite").saveAsTable("{0}.{1}_na_bus_master_raw_basket_data_with_prod_{2}".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

# MAGIC %sql
# MAGIC clear cache

# COMMAND ----------

# display(sqlContext.sql("select min(business_date),max(business_date) from {0}.{1}_na_bus_master_raw_basket_data_with_prod_{2}".\
#                         format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

# DBTITLE 1,Imputation Start
# create file at upc x site x date x txn_id level where sales >0 and sales_amount/sales_qty != 0.01 and promo flag = 0 
sqlContext.sql("""
select
  *,
  round((Sales_Amount / Quantity), 2) as Reg_Price
from
  (
    Select
      BU,
      upc,
      tempe_product_key,
      site,
      business_date,
      Promo_Flag,
      transaction_number,
      sum(quantity_sold) as Quantity,
      sum(Sales_Amount) as Sales_Amount
    from
      {0}.{1}_na_bus_master_raw_basket_data_with_prod_{2}
    where
      sales_amount > 0
      and quantity_sold > 0
      and sales_amount / quantity_sold != 0.01
      and promo_flag = 0
    group by
      BU,
      upc,
      tempe_product_key,
      site,
      business_date,
      Promo_Flag,
      transaction_number
  )
""".format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("imputation_source_master_1")

# COMMAND ----------

#create a subset of the above file where there are two different reg prices on the same day vs only 1
sqlContext.sql(""" select BU, upc, tempe_product_key, site, business_date, promo_flag, transaction_number, Quantity, Sales_Amount, Reg_Price from
                    (select A.*, B.count_prices from imputation_source_master_1 A 
                    left join 
                    (select BU, upc, tempe_product_key, site, business_date, count(distinct Reg_Price) as count_prices from imputation_source_master_1 group by BU, upc, 
                    tempe_product_key, site, business_date having count(distinct Reg_Price) > 1) B
                    on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date) 
                    where count_prices is not null                
                """).createOrReplaceTempView("imputation_source_master_multi")

sqlContext.sql("""select  BU, upc, tempe_product_key, site, business_date, sum(Quantity) as Quantity, sum(Sales_Amount) as Sales_Amount, round(avg(Reg_Price),2) as 
                  Reg_Price 
                  from
                  (select A.*, B.count_prices from imputation_source_master_1 A 
                  left join 
                  (select BU, upc, tempe_product_key, site, business_date, count(distinct Reg_Price) as count_prices from imputation_source_master_1 group by BU, upc, 
                  tempe_product_key, site, business_date having count(distinct Reg_Price) = 1) B
                  on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date) 
                  where count_prices is not null group by  BU, tempe_product_key, upc,site, business_date             
              """).createOrReplaceTempView("imputation_source_master_single")

# COMMAND ----------

sqlContext.sql(""" select BU, tempe_product_key, business_date, site, Reg_Price, count(*) as count_price, sum(Sales_Amount) as Sales, sum(Quantity) as 
                    Quantity from imputation_source_master_multi group by 
                    BU, tempe_product_key, business_date, site, Reg_Price order by BU, tempe_product_key, business_date, site
               """).createOrReplaceTempView("check_multi_mode_method")

#create variation flags - basically to indicate which product x site x day could be fixed with mode value and which could be fixed with max quantity method
sqlContext.sql(""" select *, case when mode_significance != 0 then 'Use Mode' else 'Go to Next Level' end as mode_flag from 
                    (select BU, tempe_product_key, business_date, site, stddev(count_price) as mode_significance from 
                    check_multi_mode_method group by BU, tempe_product_key, business_date, site)
               """).createOrReplaceTempView("check_multi_mode_method_2")

# COMMAND ----------

#qc cases where we are not getting a single mode for 'Use Mode' flag 
sqlContext.sql(""" select * from 
                     (select A.*, B.mode_flag from check_multi_mode_method A left join check_multi_mode_method_2 B on 
                     A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date)
                     where mode_flag = 'Use Mode'
                """).createOrReplaceTempView("qc_mode_1")

sqlContext.sql(""" select BU, tempe_product_key, site, business_date, Reg_Price, count_price, count_price_max as count_mode from 
                    (select A.BU, A.tempe_product_key, A.site, A.business_date, A.Reg_Price, A.count_price, B.count_price_max from qc_mode_1 A left join 
                    (select BU, tempe_product_key, site, business_date, max(count_price) as count_price_max
                    from qc_mode_1 group by BU, tempe_product_key,business_date, site) B
                    on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date
                    ) where count_price = count_price_max
               """).createOrReplaceTempView("qc_mode_2")

# to find out if a prouct x site x date is having multiple modes
sqlContext.sql(""" select BU, tempe_product_key, site, business_date, count(Reg_Price) as mode_count
                   from qc_mode_2 group by BU, tempe_product_key, site, business_date having count(Reg_Price)>= 2        
                """).createOrReplaceTempView("qc_mode_3")

#merge this above table to check_multi_mode_method_2 and wherever not null those cases also need to pass through our next flow of imputation
sqlContext.sql("""select BU, tempe_product_key, business_date, site, mode_flag , case when mode_count is not null then 'Go to Next Level' else mode_flag end as 
                  final_flag from 
                  (select A.*, B.mode_count from check_multi_mode_method_2 A left join 
                  qc_mode_3 B on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.business_date = B.business_date and A.site = B.site)
                """).createOrReplaceTempView("check_multi_mode_method_3")

# COMMAND ----------

##Now join back the above table and split the multi table into two parts - one where we would use the mode to get the single day's price and other which goes through next imputation tech

sqlContext.sql("""select * from 
                  (select A.*, B.final_flag from imputation_source_master_multi A left join
                  check_multi_mode_method_3 B on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.business_date = B.business_date and A.site = B.site)
                  where final_flag = 'Use Mode' 
                """).createOrReplaceTempView("imputation_source_master_multi_mode")

sqlContext.sql("""select * from 
                  (select A.*, B.final_flag from imputation_source_master_multi A left join
                  check_multi_mode_method_3 B on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.business_date = B.business_date and A.site = B.site)
                  where final_flag != 'Use Mode' 
                """).createOrReplaceTempView("imputation_source_master_multi_next")

#now for mode cases simply use the mode price for a product x site x date
sqlContext.sql("""select BU, upc, tempe_product_key, site, business_date, promo_flag, transaction_number, Quantity, Sales_Amount, Reg_Price_Final as Reg_Price, "Use 
                  Mode" as Impute_Flag  from
                  (Select A.*, B.Reg_Price_Final from 
                  imputation_source_master_multi_mode A left join (select BU, tempe_product_key, site, business_date, Reg_Price as Reg_Price_Final from qc_mode_2) B 
                  on 
                  A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.business_date = B.business_date and A.site = B.site)
                """).createOrReplaceTempView("single_daily_date_from_mode_fin")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Go through recent past and then recent future logic if past fails and if all fails use the most frequent price for that day for that item across all stores to decide the single price for the day

# COMMAND ----------

#now for multi prices same day product key x sites - first need to get recent past single price 
sqlContext.sql(""" Select A.BU, A.site,A.upc, A.tempe_product_key, A.business_date, 
                   datediff(A.business_date,B.business_date) Date_Diff,B.business_date as Impute_from_date  
                   from 
                       (select distinct BU, site, upc, tempe_product_key, business_date 
                       from imputation_source_master_multi_next) A 
                   Left Join 
                       (Select distinct BU, site, upc, tempe_product_key, business_date 
                       from imputation_source_master_single) B 
                    On A.BU = B.BU and A.site = B.site and A.upc=B.upc and A.tempe_product_key = B.tempe_product_key 
                    and datediff(A.business_date,b.business_date)>=0 
                """).createOrReplaceTempView("source_cleanup_history_1")

# then we need to take the maximum of the dates at product key x site level to find the most recent historical date from which we can get the single daily price
sqlContext.sql("""Select BU, upc, tempe_product_key, site, business_date,max(Impute_from_date) Impute_from_date 
                  from source_cleanup_history_1 
                  group by BU, upc, tempe_product_key, site, business_date
               """).createOrReplaceTempView("source_cleanup_history_2nd")

#join this with the imputation_source_master_multi table and keep only rows where Impute_from_date is not null - meaning these have single daily prices from recent history
sqlContext.sql("""select *, 'From Past' as Impute_Flag 
                  from (select A.*, B.Impute_from_date 
                        from imputation_source_master_multi_next A 
                        left outer join source_cleanup_history_2nd B \
                        on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = b.site and A.business_date = b.business_date) 
                  where Impute_from_date is not null
               """).createOrReplaceTempView("single_daily_date_from_past")

#need to now subset cases which could find single daily price from past and need te tried for future
sqlContext.sql("""select * 
                  from 
                      (select A.*, B.Impute_from_date 
                      from imputation_source_master_multi_next A 
                      left outer join source_cleanup_history_2nd B 
                      on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = b.site and A.business_date = b.business_date) 
                  where Impute_from_date is null
               """).createOrReplaceTempView("single_daily_try_from_future")

#now for multi prices same day product key x sites -  need to get recent future single price 
sqlContext.sql("""Select A.BU, A.site, A.upc, A.tempe_product_key, A.business_date, 
                  datediff(A.business_date,B.business_date) Date_Diff, B.business_date as Impute_from_date  
                  from (select distinct BU, site, upc, tempe_product_key, business_date from single_daily_try_from_future) A 
                  Left Join (Select distinct BU, site, upc, tempe_product_key, business_date from imputation_source_master_single) B 
                  On A.BU = B.BU and A.site = B.site and A.upc=B.upc and A.tempe_product_key = B.tempe_product_key 
                  and datediff(A.business_date,b.business_date) < 0 
               """).createOrReplaceTempView("source_cleanup_future_1")

# then we need to take the minimum of the dates at product key x site level to find the most recent future date from which we can get the single daily 
# price
sqlContext.sql("""Select BU , upc, tempe_product_key, site, business_date, min(Impute_from_date) Impute_from_date 
                  from source_cleanup_future_1 
                  group by BU, upc, tempe_product_key, site, business_date
               """).createOrReplaceTempView("source_cleanup_future_2nd")

#join this with the imputation_source_master_multi table and keep only rows where Impute_from_date is not null - meaning these have single daily prices from recent history
sqlContext.sql("""select *, 'From Future' as Impute_Flag 
                  from 
                      (select A.BU, A.upc, A.tempe_product_key, A.site, A.business_date, A.promo_flag, A.transaction_number, 
                      A.Quantity, A.Sales_Amount, A.Reg_Price, A.Final_Flag, B.Impute_from_date 
                      from single_daily_try_from_future A 
                      left outer join source_cleanup_future_2nd B 
                      on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date) 
                   where Impute_from_date is not null
                """).createOrReplaceTempView("single_daily_date_from_future")

#combine these past and future tables, then based on the imppute_from_date get the price from singles master table
# get the closest price which would be the price for that day
sqlContext.sql("""  (select *, abs(Reg_Price_mod - Reg_Price) as abs_diff from
                    (select A.*, B.Reg_Price_mod from
                    (select * from single_daily_date_from_past union all select * from single_daily_date_from_future) A
                    left join (select distinct BU, tempe_product_key, site, business_date, Reg_Price as Reg_Price_mod from imputation_source_master_single) B
                    on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.Impute_from_date = B.business_date))
""").createOrReplaceTempView("single_daily_date_from_past_fut_fin_1")   

# get the closest price which would be the price for that day
sqlContext.sql("""  select A.*, B.abs_diff_min 
                    from single_daily_date_from_past_fut_fin_1 A
                    left join
                    (select BU, tempe_product_key, site, business_date, min(abs_diff) as abs_diff_min 
                    from single_daily_date_from_past_fut_fin_1 
                    group by BU, tempe_product_key, site, business_date) B
                    on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date  
""").createOrReplaceTempView("single_daily_date_from_past_fut_fin_2")
# display(sqlContext.sql("select * from single_daily_date_from_past_fut_fin_2"))

# now basically take case where abs_diff = min_abs_diff 
# then taking average as there could be multiple same reg prices which we could not use mode for e.g. - if frequency is 1,2,2,2 there are 3 modes - these might cuase duplication here and this should sort it
sqlContext.sql("""  select BU, upc, tempe_product_key, site, business_date , promo_flag, Impute_Flag,sum(Quantity) as Quantity, sum(Sales_Amount) as 
                    Sales_Amount, round(avg(Reg_Price),2) as Reg_Price from
                    (select *,Reg_Price as Reg_Price_Mod from single_daily_date_from_past_fut_fin_2 where abs_diff = abs_diff_min )  
                    group by BU, upc, tempe_product_key, site, business_date , promo_flag, Impute_Flag
""").createOrReplaceTempView("single_daily_date_from_past_fut_fin")

# COMMAND ----------

## get the cases where we still could not get a single daily price
sqlContext.sql(""" select * 
                   from 
                       (select A.BU, A.upc, A.tempe_product_key, A.site,A.business_date, A.promo_flag, A.transaction_number,A.Quantity,A.Sales_Amount, 
                        A.Reg_Price, A.Final_Flag ,B.Impute_from_date 
                        from single_daily_try_from_future A 
                        left outer join source_cleanup_future_2nd B 
                        on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = b.site and A.business_date = b.business_date) 
                    where Impute_from_date is null
                """).createOrReplaceTempView("single_daily_date_from_all_stores")

#get the mode of reg price at item x day acorss stores from the master table of both single and multiple
sqlContext.sql(""" select * 
                   from 
                      (select A.*, B.count_price_max 
                       from
                          (select BU, tempe_product_key, business_date, Reg_Price, count(*) as count_price 
                            from imputation_source_master_1 
                            group by  BU, tempe_product_key, business_date, Reg_Price) A 
                       left join
                          (select BU, tempe_product_key, business_date, max(count_price) as count_price_max 
                            from
                                (select BU, tempe_product_key, business_date, Reg_Price, count(*) as count_price 
                                  from imputation_source_master_1 
                                  group by BU, tempe_product_key, business_date, Reg_Price) 
                            group by BU, tempe_product_key, business_date) B
                       on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.business_date = B.business_date)
                    where  count_price = count_price_max     
              """).createOrReplaceTempView("mode_data_all_stores")

## now left join the mode table with single_daily_date_from_all_stores table and then take the price where there is min abs difference b/w the two
# on top there is an average taken to make sure if there are same min abs diff b/w the current price at that store and the mode price acorss stores we get a single value which is the same

sqlContext.sql("""select *, abs(mode_price - Reg_Price) as abs_diff 
                  from
                      (select C.*, D.mode_price 
                      from single_daily_date_from_all_stores C 
                      left join 
                            (select BU, tempe_product_key, business_date, Reg_Price as mode_price 
                              from mode_data_all_stores) D 
                       on C.BU = D.BU and C.tempe_product_key = D.tempe_product_key and C.business_date = D.business_date)
                """).createOrReplaceTempView("single_daily_date_from_all_stores_fin_1")

# get the closest price which would be the price for that day
sqlContext.sql("""  select A.*,B.abs_diff_min 
                    from single_daily_date_from_all_stores_fin_1 A
                    left join
                         (select BU,tempe_product_key, site, business_date, min(abs_diff) as abs_diff_min 
                          from  single_daily_date_from_all_stores_fin_1 
                          group by BU, tempe_product_key, site, business_date) B
                    on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date  
                """).createOrReplaceTempView("single_daily_date_from_all_stores_fin_2")

# display(sqlContext.sql("select * from single_daily_date_from_all_stores_fin_2"))

# now basically take case where abs_diff = min_abs_diff 
# also average over it to get unique values where we have multiple cases where min_abs_diff = abs_diff just so that we end up with an unique number for
# a product x site x day
sqlContext.sql(""" select  BU, upc, tempe_product_key, site, business_date , promo_flag, Impute_Flag, 
                    sum(Quantity) as Quantity, sum(Sales_Amount) as Sales_Amount, round(avg(Reg_Price),2) as Reg_Price 
                    from 
                      (select *, 'From All Stores' as Impute_Flag 
                      from single_daily_date_from_all_stores_fin_2 
                      where abs_diff = abs_diff_min )
                    group by  BU, upc, tempe_product_key, site, business_date , promo_flag, Impute_Flag
               """).createOrReplaceTempView("single_daily_date_from_all_stores_fin")

# COMMAND ----------

# %sql
# drop table if exists {}.{}_na_bus_Reg_Price_Impute_source_Final_{}

# COMMAND ----------

#NOW FINALLY UNION THE BOTH TABLES
sqlContext.sql(""" (select BU, upc, tempe_product_key, site, business_date , 0 as Promo_flag, 'no imputation' as Impute_Flag, Quantity,Sales_Amount, Reg_Price 
                    from imputation_source_master_single) 
                    union all 
                      select * from single_daily_date_from_past_fut_fin 
                    union all 
                      select * from single_daily_date_from_all_stores_fin 
                    union all 
                      (select BU, upc, tempe_product_key, site, business_date , promo_flag, Impute_Flag, 
                        sum(Quantity) as Quantity, sum(Sales_Amount) as Sales_Amount, round(avg(Reg_Price),2) as Reg_Price 
                       from single_daily_date_from_mode_fin 
                       group by BU, upc, tempe_product_key, site, business_date , promo_flag, Impute_Flag)
              """).write.mode("overwrite").saveAsTable("{0}.{1}_na_bus_Reg_Price_Impute_source_Final_{2}".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

# MAGIC %sql
# MAGIC clear cache

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 2 Outlier Treatment

# COMMAND ----------

#calculate mu+- 3 sigma
sql_text = """Select BU, tempe_product_key
                    ,site
                    ,Avg(Reg_Price)-3*(stddev(Reg_Price)) Neg_Outliers
                    ,Avg(Reg_Price)+3*(stddev(Reg_Price)) Pos_outlier 
                    from {0}.{1}_na_bus_Reg_Price_Impute_source_Final_{2}
                    group by 
                    BU,tempe_product_key
                    ,site"""
sqlContext.sql(sql_text.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("OutlierTreatmentTable")

#get the outliers by mu +- 3 sigma
sqlContext.sql("""Select A.BU, A.tempe_product_key
                       ,A.site
                       ,A.business_date
                       ,A.Reg_Price as Reg_Price_Mod 
                 from {0}.{1}_na_bus_Reg_Price_Impute_source_Final_{2} A 
                 Left outer join OutlierTreatmentTable B 
                 on 
                 A.BU = B.BU and
                 A.tempe_product_key = B.tempe_product_key 
                 and A.site = B.site 
                 where (A.Reg_Price <B.Neg_Outliers
                 or A.Reg_Price> B.Pos_outlier)""".format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("multiple_reg_price_outlier_reg")

#separate the table of non-outliers
sql_text = """
            select BU, upc,	tempe_product_key,site,	business_date,	Promo_flag,	Impute_Flag, Quantity ,Sales_Amount, Reg_Price 
            from 
               (select A.*, B.Reg_Price_Mod 
               from {0}.{1}_na_bus_Reg_Price_Impute_source_Final_{2} A 
               left join multiple_reg_price_outlier_reg B 
               on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date)
            where Reg_Price_Mod is null
           """
sqlContext.sql(sql_text.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("Reg_Price_Impute_source_Final_non_out")

#find recent past non-outlier value by product key x site
sql_text = """Select A.BU, A.tempe_product_key
                    ,A.site
                    ,A.business_date outlier_date
                    ,A.Reg_Price_Mod Outlier_price
                    ,B.business_date  
                    ,B.Reg_Price 
                    ,Count(*) over (partition by A.BU, A.tempe_product_key,A.site,A.business_date order by B.business_date desc) count_trns
              from multiple_reg_price_outlier_reg A  
              join Reg_Price_Impute_source_Final_non_out B 
              on 
              A.BU = B.BU and
              A.tempe_product_key = B.tempe_product_key
              and A.site = B.site
              and A.business_date > B.business_date
              and A.Reg_Price_Mod <> B.Reg_Price
              """
sqlContext.sql(sql_text).createOrReplaceTempView("last_non_outlier_value")

#find recent past non-outlier value by product key x site - keeping the 1st one - which denotes most recent
sqlContext.sql("Select * from last_non_outlier_value where count_trns =1").createOrReplaceTempView("Recent_non_outlier_value")

# only treat cases where we see atleast 30% difference b/w outlier and non-outlier prices
sqlContext.sql("""Select *, 
                        (abs(Reg_Price-Outlier_price)/Reg_Price)*100 per_diff, 
                        case when(abs(Reg_Price-Outlier_price)/Reg_Price)*100>30 then 'Treat' else 'Not Treating' end as Flag 
                   from Recent_non_outlier_value
                """).createOrReplaceTempView("treat_not_treat")

#selecting important columns from above file
sqlContext.sql("""Select  BU, tempe_product_key,site, Outlier_Price, Outlier_date, business_date, Flag,Reg_Price as Regu_Price 
                  from treat_not_treat
               """).createOrReplaceTempView("treat_not_treatz")

#join the treat flag from above in original dataset and treat only those cases
sql_text = """select BU, upc,tempe_product_key,	site,	business_date,	Promo_flag,	Impute_Flag, Quantity ,Sales_Amount, Regular_Price 
              from 
                  (Select A.*,case when B.Flag = 'Treat' then B.Regu_Price 
                          else A.Reg_Price
                          end as Regular_Price
                          ,B.Flag OutlierFlag
                  from {0}.{1}_na_bus_Reg_Price_Impute_source_Final_{2} A
                  left  join treat_not_treatz B
                  on 
                  A.BU = B.BU and A.tempe_product_key = B.tempe_product_key
                  and A.site = B.site
                  and A.business_date =B.outlier_date)
            """
sqlContext.sql(sql_text.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("Reg_Price_Impute_source_Final_outlier")

# COMMAND ----------

# MAGIC %md
# MAGIC Stage 2 of treatment -  where we replace cases where a price is more than 100% of previous price and next price. 
# MAGIC 
# MAGIC Also, cases where there is 100% price change but consistently that item has been on at least 2 prices consecutively - we have not removed thinking it might be some legit price change

# COMMAND ----------

# %sql
# drop table if exists {}.{}_na_bus_Reg_Price_Impute_source_imputed_treated_{}

# COMMAND ----------

# take lag and lead
sqlContext.sql(""" select *, 
                      Lag(Regular_Price) over (partition by BU, tempe_product_key, site order by business_date) as lagged_price, 
                      Lead(Regular_Price) over (partition by BU,tempe_product_key, site order by business_date) as lead_price 
                   from Reg_Price_Impute_source_Final_outlier
                """).createOrReplaceTempView("lag_after_treatment")

#create flag for more than 100% variation from previous price
sqlContext.sql(""" (select *, 
                          case when ((Regular_Price - lagged_price)/lagged_price > 1 or (Regular_Price - lagged_price)/lagged_price < -0.5) and
                                    ((Regular_Price - lead_price)/lead_price > 1 or (Regular_Price - lead_price)/lead_price < -0.5) 
                                then 'more than 100% deviation' 
                                else null 
                           end as second_impute_flag 
                    from lag_after_treatment )
                """).createOrReplaceTempView("lag_after_treatment_1")

#create table of only issues
#create table which is devoid of anomalies
sqlContext.sql(""" select * from lag_after_treatment_1 where second_impute_flag is not null
               """).createOrReplaceTempView("lag_after_issues")

#create table which is devoid of anomalies
# also fix the lead and lag issue cases here

sqlContext.sql(""" select *, null as Impute_from_date 
                   from  
                       (select * from lag_after_treatment_1 where second_impute_flag is null)
                """).createOrReplaceTempView("lag_after_source_1")

#take care of 1st and last row of tempe_product_key x cluster cases - where lead/lag is null - if there are issues there as well
sqlContext.sql(""" select BU, upc,tempe_product_key, site ,business_date, 
                     case when (lagged_price is null and ((Regular_Price - lead_price2)/lead_price2 > 1 or (Regular_Price - lead_price2)/lead_price2 < -0.5)) 
                            then lead_price2 
                          when (lead_price is null and ((Regular_Price - lagged_price2)/lagged_price2 > 1 or (Regular_Price - lagged_price2)/lagged_price2 < -0.5)) 
                            then lagged_price2 
                          else Regular_Price 
                      end as Regular_Price_Modded, 
                      case when ((lagged_price is null and 
                                  ((Regular_Price - lead_price2)/lead_price2 > 1 or (Regular_Price - lead_price2)/lead_price2 < -0.5)) or 
                                (lead_price is null and 
                                  ((Regular_Price - lagged_price2)/lagged_price2 > 1 or (Regular_Price - lagged_price2)/lagged_price2 < -0.5))) 
                           then business_date
                           else null 
                       end as Impute_from_date
                    from
                      (select BU, upc, tempe_product_key, site ,business_date, Promo_flag, Impute_Flag, Quantity, Sales_Amount,Regular_Price, lagged_price, lead_price,
                        Lag(Regular_Price) over (partition by BU, tempe_product_key, site order by business_date) as lagged_price2, 
                        Lead(Regular_Price) over (partition by BU, tempe_product_key, site order by  business_date) as lead_price2 
                       from lag_after_source_1)
               """).createOrReplaceTempView("lag_after_source_2")

#join the above two tables
sqlContext.sql("""
                select BU, upc, tempe_product_key, site, business_date, Promo_flag, Impute_Flag, Quantity, Sales_Amount, Regular_Price, lagged_price,	
                lead_price, second_impute_flag, case when Regular_Price_Modded is not null then Regular_Price_Modded else Regular_Price end as 
                Reg_Price_Mod, Impute_from_date 
                from 
                (select A.*, B.Regular_Price_Modded from lag_after_source_1 A left join 
                lag_after_source_2 B on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date)
              """).createOrReplaceTempView("lag_after_source")

# join back to create a skeleton where all dates prior to issue are there
sqlContext.sql(""" Select A.BU, A.site,A.upc,A.tempe_product_key, A.business_date, 
                     datediff(A.business_date,B.business_date) Date_Diff,B.business_date as Impute_from_date 
                   from lag_after_issues A 
                   Left Join 
                         (Select BU,upc, tempe_product_key, site, business_date 
                          from lag_after_source) B 
                    On A.BU = B.BU and A.site = B.site and A.upc=B.upc and A.tempe_product_key=B.tempe_product_key 
                    and datediff(A.business_date,b.business_date)<0 
               """).createOrReplaceTempView("lag_after_issues_past_dates_1st")

# then we need to take the minimum of the dates at product x site level to find the most recent past date from which we can impute
sqlContext.sql(""" (Select BU,upc, tempe_product_key,site, business_date,
                    min(Impute_from_date) Impute_from_date from lag_after_issues_past_dates_1st group by BU, upc, tempe_product_key,site, 
                    business_date) 
                """).createOrReplaceTempView("lag_after_issues_past_dates_2nd")

#add the impute from another date to fix for 1st and last row cases
sqlContext.sql(""" (select * from lag_after_issues_past_dates_2nd union all (select BU, upc, tempe_product_key,site, business_date, Impute_from_date from 
                    lag_after_source_2 where 
                    Impute_from_date is not null )) 
                """).createOrReplaceTempView("lag_after_issues_past_dates_3rd")

#now join back the price from source table on this above table basis impute_from_date
sqlContext.sql(""" Select A.*, B.Reg_Price_Mod as Reg_Price_Mod 
                   from lag_after_issues_past_dates_3rd A 
                   left join lag_after_source B 
                   on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.Impute_from_date = B.business_date
               """).createOrReplaceTempView("lag_after_issues_past_dates_fin")

#Finally Replace by Pre-Price
# also fix the lead and lag issue cases here
sqlContext.sql(""" select BU, upc, tempe_product_key, site, business_date, Promo_flag, Impute_Flag, second_impute_flag, Quantity, Sales_Amount,
                      case when Reg_Price_Mod is not null then Reg_Price_Mod else Regular_Price end as Regular_Price
                    from (select A.*, B.Reg_Price_Mod from lag_after_treatment_1 A 
                    left join lag_after_issues_past_dates_fin B
                    on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date)
               """).write.mode("overwrite").saveAsTable("{0}.{1}_na_bus_Reg_Price_Impute_source_imputed_treated_{2}".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

# MAGIC %sql
# MAGIC clear cache

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Impute the On-promo cases using the fixed regular prices from step 1 & 2
# MAGIC We first use the inscope file based on category to subset raw basket data for only inscope items which would be our staring base dataset

# COMMAND ----------

#creating inscope file from full basket data after rolling up at upc x item_desc x site x business day x promo flag level
sqlContext.sql(""" Select BU, upc, tempe_product_key, site, business_date, Promo_Flag, sum(Quantity) as Quantity, sum(Sales_Amount) as Sales_Amount 
                   from {0}.{1}_na_bus_Reg_Price_Impute_source_imputed_treated_{2}
                   group by BU, upc,tempe_product_key, site, business_date, Promo_Flag
               """.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("basket_day_promoflag_inscope_upc")

# COMMAND ----------

# Reg price imputation summaries
# Create Imputation Flag
sqlContext.sql(""" Select BU, upc,tempe_product_key,Site,business_date,Promo_flag, Quantity,Sales_Amount, 
                    case when Promo_flag = 1 then 'Impute due to on Promo' when (Promo_flag = 0) and (Sales_Amount <= 0 or Quantity <= 0) 
                         then 'Impute due zero or neg reg sales or 0.01' 
                         else 'no imputation'  
                    end as Impute_Flag 
                   from basket_day_promoflag_inscope_upc 
               """).createOrReplaceTempView("Reg_Imp_skel_day_promo_NM")

# also need to add flag of cases where we see if that day has seen any txn which is of 0.01 reg price
# this needs to be done at granular level and then joined to the above table
# the negative or 0 sales flag should also come from txn level cause if in a day even a single bad txn might be contributing to a roll-up level bad price
sqlContext.sql(""" select distinct BU, tempe_product_key, site, business_date, Promo_Flag, Flag 
                   from 
                      (select BU, tempe_product_key, site, business_date, promo_flag,
                          case when sales_amount/quantity_sold = 0.01 then 'Impute due zero or neg reg sales or 0.01' 
                            when (sales_amount <= 0 or quantity_sold <= 0) then 'Impute due zero or neg reg sales or 0.01' 
                            else null end as Flag 
                       from {0}.{1}_na_bus_master_raw_basket_data_with_prod_{2}) 
                   where Flag is not null and Promo_Flag = 0
               """.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("other_flag")

#join and create extra flag
sqlContext.sql(""" select BU, upc , tempe_product_key, site, business_date, Promo_Flag, Quantity, Sales_Amount, Impute_Flag 
                   from 
                      (select BU, upc , tempe_product_key, site, business_date, Promo_Flag, Quantity, Sales_Amount, Flag, 
                       case when Flag is not null then Flag else Impute_Flag end as Impute_Flag 
                      from 
                          (select A.*, B.Flag 
                            from Reg_Imp_skel_day_promo_NM A 
                            left join other_flag B 
                            on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.Site 
                            and A.business_date = B.business_date and A.promo_flag = B.promo_flag ))
               """).createOrReplaceTempView("Reg_Imp_skel_day_promo_NM")

# COMMAND ----------

sqlContext.sql(""" select BU, upc, tempe_product_key, Site, business_date, Quantity, Sales_Amount, Regular_Price as Reg_Price 
                   from {0}.{1}_na_bus_Reg_Price_Impute_source_imputed_treated_{2}
               """.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("Reg_Price_Impute_source_Final_temp")

# create the to be imputed cases
sqlContext.sql("select * from Reg_Imp_skel_day_promo_NM where Impute_Flag != 'no imputation'").createOrReplaceTempView("Tot_to_be_imputed_NM")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage A
# MAGIC Try to impute from the recent past or same day

# COMMAND ----------

#Create a table where have the most recent date in history of matching tempe_product_key x site in Imptation Source file
sqlContext.sql(""" Select A.BU, A.site,A.upc, A.tempe_product_key,A.business_date, 
                    datediff(A.business_date,B.business_date) Date_Diff,B.business_date as Impute_from_date
                   from Tot_to_be_imputed_NM A 
                   Left Join 
                       (Select * from Reg_Price_Impute_source_Final_temp) B 
                   On A.BU = B.BU and A.site = B.site and A.upc=B.upc and A.tempe_product_key = B.tempe_product_key 
                   and datediff(A.business_date,b.business_date)>=0 
               """).createOrReplaceTempView("upc_site_history_dates_1st")

# then we need to take the minimum of the dates at upc x site level to find the most recent historical date from which we can impute
sqlContext.sql(""" Select BU, upc, tempe_product_key, site, business_date,max(Impute_from_date) Impute_from_date 
                   from upc_site_history_dates_1st 
                   group by BU, upc, tempe_product_key, site, business_date
               """).createOrReplaceTempView("upc_site_history_dates_2nd")

#join this with the total_to_be_imputed table and keep only rows where Impute_from_date is not null - meaning these can be imputed from recent history
sqlContext.sql(""" select * 
                   from 
                       (select A.*, B.Impute_from_date 
                        from Tot_to_be_imputed_NM A 
                        left outer join upc_site_history_dates_2nd B 
                        on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = b.site and A.business_date = b.business_date) 
                    where Impute_from_date is not null
                """).createOrReplaceTempView("To_be_imputed_from_recent_hist_or_same_day")

#need to now subset cases which could not be imputed from past and need te tried for future
sqlContext.sql(""" select * from 
                    (select A.*, B.Impute_from_date 
                     from Tot_to_be_imputed_NM A 
                     left outer join upc_site_history_dates_2nd B 
                     on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = b.site and A.business_date = b.business_date) 
                   where Impute_from_date is null
               """).createOrReplaceTempView("impute_try_from_future")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage B
# MAGIC Try to impute from the future

# COMMAND ----------

#Create a table where have the most recent date in history of matching tempe_product_key x site in Imptation Source file
sqlContext.sql(""" Select A.BU, A.site,A.upc,A.tempe_product_key, A.business_date, 
                     datediff(A.business_date,B.business_date) Date_Diff, B.business_date as Impute_from_date  
                   from impute_try_from_future A 
                   Left Join (Select * from Reg_Price_Impute_source_Final_temp) B 
                   On A.BU = B.BU and A.site = B.site and A.upc=B.upc and A.tempe_product_key=B.tempe_product_key 
                   and datediff(A.business_date,b.business_date)<0 
               """).createOrReplaceTempView("upc_site_future_dates_1st")

# then we need to take the minimum of the dates at upc x site level to find the most recent future date from which we can impute
sqlContext.sql(""" Select BU, upc, tempe_product_key,site, business_date,min(Impute_from_date) Impute_from_date 
                   from upc_site_future_dates_1st 
                   group by BU, upc, tempe_product_key,site, business_date
               """).createOrReplaceTempView("upc_site_future_dates_2nd")

#join this with the impute_try_from_future table and keep only rows where Impute_from_date is not null - meaning these can be imputed from recent future
sqlContext.sql(""" select * from 
                      (select A.BU, A.upc,A.tempe_product_key,A.site,A.business_date,A.Promo_flag,A.Quantity, A.Sales_Amount, A.Impute_Flag, B.Impute_from_date 
                       from impute_try_from_future A 
                       left outer join upc_site_future_dates_2nd B 
                       on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = b.site and A.business_date = b.business_date) 
                   where Impute_from_date is not null
               """).createOrReplaceTempView("To_be_imputed_from_future")

#finally cases where we can't impute as that tempe_product_key x site has been on continuous promotion/has never seen non zero regular sale
sqlContext.sql(""" select * from 
                      (select A.BU, A.upc,A.tempe_product_key,A.site,A.business_date,A.Promo_flag,A.Quantity, A.Sales_Amount, A.Impute_Flag, B.Impute_from_date 
                       from impute_try_from_future A 
                       left outer join upc_site_future_dates_2nd B 
                       on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = b.site and A.business_date = b.business_date) 
                   where Impute_from_date is null
               """).createOrReplaceTempView("Cant_be_imputed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage C
# MAGIC a.Create an uninon of all 3 cases - past or same day impute, future impute and cant be imputed to get back original imputation skeleton
# MAGIC 
# MAGIC b.Left join from Reg_Price_Imputation Source - basis impute_from_date column to get final reg price
# MAGIC 
# MAGIC c.add the no imputation cases to the table to get the final price table

# COMMAND ----------

# %sql
# drop table if exists {}.{}_na_bus_total_imputation_combined_{}

# COMMAND ----------

#create combined imputation file for later summaries 
sqlContext.sql(""" select * from 
                      (select * from To_be_imputed_from_recent_hist_or_same_day 
                       union 
                       select * from To_be_imputed_from_future 
                       union 
                       select * from Cant_be_imputed)
               """).write.mode("overwrite").saveAsTable("{0}.{1}_na_bus_total_imputation_combined_{2}".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

#left join this table with Imputation Source basis Impute from date
sqlContext.sql(""" select A.*, B.Reg_Price 
                   from {0}.{1}_na_bus_total_imputation_combined_{2} A 
                   left outer join Reg_Price_Impute_source_Final_temp B 
                   on A.BU = B.BU and A.upc = B.upc and A.tempe_product_key = B.tempe_product_key and A.site = B.site and  A.Impute_from_date = B.business_date
               """.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("Complete_Imputation_combined_with_price")

#now rolling the above table up to tempe_product_key x site x day level which we need the final data
sqlContext.sql(""" select BU, upc,tempe_product_key,site, business_date, sum(Quantity) Quantity, sum(Sales_Amount) Sales_Amount, max(Reg_Price) Reg_Price 
                   from Complete_Imputation_combined_with_price 
                   group by BU, upc, tempe_product_key ,site, business_date
               """).createOrReplaceTempView("Regular_Price_Imputed_only")

# COMMAND ----------

#finally need to union this table with the Imputation source table to get final reg price at tempe_product_key x site x business day
sqlContext.sql(""" select * from 
                      (select A.*, B.Reg_Price from 
                          (select BU, upc,tempe_product_key,site,business_date, sum(Quantity) as Quantity, sum(Sales_Amount) as Sales_Amount 
                           from  Reg_Imp_skel_day_promo_NM 
                           where Impute_flag = 'no imputation' 
                           group by BU, upc,tempe_product_key,site,business_date) A 
                       left join Reg_Price_Impute_source_Final_temp B 
                       on A.BU = B.BU and A.tempe_product_key = B.tempe_product_key and A.site = B.site and A.business_date = B.business_date) 
                   union all 
                   select * from Regular_Price_Imputed_only 
               """).createOrReplaceTempView("Regular_Price_all_final_with_duplicate")

#need to group the above data at tempe_product_key x site x date level to arrive at final table
sqlContext.sql(""" select BU, upc,tempe_product_key ,site, business_date, sum(Quantity) Quantity, sum(Sales_Amount) Sales_Amount, max(Reg_Price) Reg_Price 
                   from Regular_Price_all_final_with_duplicate 
                   group by BU,upc,tempe_product_key ,site, business_date
               """).createOrReplaceTempView("Regular_Price_all_finalized_master")

# COMMAND ----------

sqlContext.sql("select * from Regular_Price_all_finalized_master").write.mode('overwrite').\
           saveAsTable("{0}.{1}_Regular_Price_all_finalized_master_{2}".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Other Variables of discount and net price with renaming as Sweden BU

# COMMAND ----------

# %sql 
# drop table if exists {}.{}_na_bus_Basket_raw_final_SWEDEN_Nomenclature_{}

# COMMAND ----------

#create BU Sweden nomenclature and also join the reg price as well to get some metrics
sqlContext.sql("""
select
  D.*,
  E.month_year_name
from
  (
    select
      transaction_date as Other_date,
      business_date as trn_transaction_date,
      transaction_time as trn_transaction_time,
      transaction_number as trn_transaction_id,
      BU,
      tempe_product_key,
      tempe_prod_item_env_key,
      product_key,
      upc as trn_item_sys_id,
      item_desc as item_name,
      nacs_category_desc,
      department_desc,
      category_desc,
      sub_category_desc,
      manufacturer_desc,
      brand_desc,
      sell_unit_qty,
      package_desc,
      private_brand_desc,
      size_unit_of_measure,
      UPC_Sell_Unit_Desc,
      promotion_env_key,
      promotion_id as trn_promotion_id,
      promotion_description,
      promotion_type,
      site as trn_site_sys_id,
      site_state_id,
      site_zip,
      gps_latitude,
      gps_longitude,
      region_id,
      region_desc,
      quantity_sold as trn_sold_quantity,
      Sales_Amount as trn_gross_amount,
      Reg_Price as avg_trn_item_unit_price,
      loyalty_card_number as trn_loy_unique_account_id,
      Promo_Flag
    from
      (
        select
          A.*,
          B.Reg_Price
        from
          {0}.{1}_na_bus_master_raw_basket_data_with_prod_{2} A
          left outer join Regular_Price_all_finalized_master B on A.BU = B.BU
          and A.tempe_product_key = B.tempe_product_key
          and A.upc = B.upc
          and A.site = B.site
          and A.business_date = B.business_date
      )
  ) D
  left outer join calendar E on D.trn_transaction_date = E.Calendar_date
""".format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("Basket_raw_final_SWED_Nomenclature")

#remove (sales_amount >0 but qty <= 0 ) or (sales_amount <= 0 but qty > 0 ) rows and 0.01 price rows
sqlContext.sql("""
select
  *
from
  Basket_raw_final_SWED_Nomenclature
where
  (
    (
      trn_gross_amount > 0
      and trn_sold_quantity > 0
    )
    or (
      trn_gross_amount < 0
      and trn_sold_quantity < 0
    )
  )
  and trn_gross_amount / trn_sold_quantity != 0.01       
"""
).write.mode('overwrite').saveAsTable("{0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}_temp".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

# DBTITLE 1,Run PDI Reg Price Notebook
dbutils.notebook.run(pdi_price_notebook_name, 0, \
        {"business_units": "{}".format(business_units), \
         "bu_code": "{}".format(bu_code), \
         "db": "{}".format(db), \
         "wave": "{}".format(wave) })

# COMMAND ----------

# DBTITLE 1,Column Logic To Get Correct Reg Price
## Note : Canadian BUs alert : check if you need to change category here
#Implement logic of avg_trn_item_unit_price
post_pdi_table_name = '{0}.{1}_Basket_raw_final_post_pdi_SWEDEN_Nomenclature_{2}'.format(db, bu_code.lower(), wave.lower())
post_pdi_df = sqlContext.sql("SELECT * FROM {0}".format(post_pdi_table_name))

################################################
## change cig category if this is not correct ##
################################################
## Note : Canadian BUs alert : update the appropirae categories for cigs here
# cig_cat = "002-CIGARETTES"

Canadian_BUs = ['QE', 'QW', 'WD', 'CD','CE']
if bu_code in (Canadian_BUs):
  cig_cat = "Cigarettes (60)"
else:
  cig_cat = "002-CIGARETTES"

################################################
################################################

post_pdi_df_modified = post_pdi_df.withColumn("final_reg_price", when(col("gross_price").isNull(), col("avg_trn_item_unit_price")).\
                                                                 otherwise(when(col("category_desc")==cig_cat,col("net_price")).\
                                                                 otherwise(when(col("gross_price")==col("net_price"),col("gross_price")).\
                                                                 otherwise(col("gross_price")))))

# COMMAND ----------

# Just a output to show any null values within the data
# display(post_pdi_df_modified.filter(col("final_reg_price").isNull()))

# COMMAND ----------

# DBTITLE 1,Renaming Columns
# Grab final columns from old dataframe 
# rename column names replacing final_reg_price with the reg price column they have
final_columns = sqlContext.sql("SELECT * FROM {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}_temp".format(db, bu_code.lower(), wave.lower())).columns
final_pdi_write_df = post_pdi_df_modified.withColumnRenamed("avg_trn_item_unit_price", "old_reg_price")\
                                         .withColumnRenamed("final_reg_price", "avg_trn_item_unit_price")\
                                         .select(final_columns)

# COMMAND ----------

# DBTITLE 1,Write Out Transformed Table
# THIS IS FINAL WRITE COMMAND
# Made a alteration changing the name of the previous table to have _temp so we can keep the final table name
final_pdi_write_df.write.mode('overwrite').saveAsTable("{0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

# MAGIC %sql
# MAGIC clear cache

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Daily Table from Basket Table After removing (sales_amount >0 but qty <= 0 ) or (sales_amount <= 0 but qty > 0 ) rows and 0.01 price rows - rows mean txns

# COMMAND ----------

# %sql 
# drop table if exists {}.{}_Daily_txn_final_SWEDEN_Nomenclature_{}

# COMMAND ----------

#first roll up from basket data
sqlContext.sql("""
                select
  Other_date,
  trn_transaction_date,
  BU,
  tempe_product_key,
  tempe_prod_item_env_key,
  product_key,
  trn_item_sys_id,
  item_name,
  nacs_category_desc,
  department_desc,
  category_desc,
  sub_category_desc,
  manufacturer_desc,
  brand_desc,
  sell_unit_qty,
  package_desc,
  private_brand_desc,
  size_unit_of_measure,
  UPC_Sell_Unit_Desc,
  trn_site_sys_id,
  site_state_id,
  site_zip,
  gps_latitude,
  gps_longitude,
  region_id,
  region_desc,
  month_year_name,
  sum(trn_sold_quantity) as trn_sold_quantity,
  sum(trn_gross_amount) as trn_gross_amount,
  round(avg(nullif(avg_trn_item_unit_price, 0)), 2) as avg_trn_item_unit_price
from
  {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}
group by
  Other_date,
  trn_transaction_date,
  BU,
  tempe_product_key,
  tempe_prod_item_env_key,
  product_key,
  trn_item_sys_id,
  item_name,
  nacs_category_desc,
  department_desc,
  category_desc,
  sub_category_desc,
  manufacturer_desc,
  brand_desc,
  sell_unit_qty,
  package_desc,
  private_brand_desc,
  size_unit_of_measure,
  UPC_Sell_Unit_Desc,
  trn_site_sys_id,
  site_state_id,
  site_zip,
  gps_latitude,
  gps_longitude,
  region_id,
  region_desc,
  month_year_name
""".format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("Daily_txn_final_GC_Nomenclature_1")

#calculate important metrics
sqlContext.sql("""
Select
  *,
  round(
    (
      (avg_trn_item_unit_price * trn_sold_quantity) - trn_gross_amount
    ),
    2
  ) trn_total_discount,
  round(
    (
      (
        (avg_trn_item_unit_price * trn_sold_quantity) - trn_gross_amount
      ) /(avg_trn_item_unit_price * trn_sold_quantity)
    ) * 100,
    2
  ) Discount_Perc,
  round((trn_gross_amount / trn_sold_quantity), 2) trn_net_price
from
  Daily_txn_final_GC_Nomenclature_1
""").createOrReplaceTempView("Daily_txn_final_GC_Nomenclature_2")

#create BU Sweden nomenclature and impute nulls
if bu_code == "ce":
  sqlContext.sql("""
  select
    Other_date,
    trn_transaction_date,
    BU,
    tempe_product_key,
    tempe_prod_item_env_key,
    product_key,
    trn_item_sys_id,
    item_name,
    nacs_category_desc,
    department_desc,
    category_desc,
    sub_category_desc,
    manufacturer_desc,
    brand_desc,
    sell_unit_qty,
    package_desc,
    private_brand_desc,
    size_unit_of_measure,
    UPC_Sell_Unit_Desc,
    cast(trn_site_sys_id as int) - 3000000 as trn_site_sys_id,
    site_state_id,
    site_zip,
    gps_latitude,
    gps_longitude,
    region_id,
    region_desc,
    month_year_name,
    trn_sold_quantity,
    trn_gross_amount,
    case
      when avg_trn_item_unit_price is null then 0
      else round(avg_trn_item_unit_price, 2)
    end as avg_trn_item_unit_price,
    case
      when trn_total_discount is null then 0
      else trn_total_discount
    end as trn_total_discount,
    case
      when discount_perc is null then 0
      else discount_perc
    end as discount_perc,
    case
      when trn_net_price is null then 0
      else trn_net_price
    end as trn_net_price
  from
    Daily_txn_final_GC_Nomenclature_2
  """).write.mode('overwrite').saveAsTable("{0}.{1}_Daily_txn_final_SWEDEN_Nomenclature_{2}".format(db, bu_code.lower(), wave.lower()))

else:
  sqlContext.sql("""
select
  Other_date,
  trn_transaction_date,
  BU,
  tempe_product_key,
  tempe_prod_item_env_key,
  product_key,
  trn_item_sys_id,
  item_name,
  nacs_category_desc,
  department_desc,
  category_desc,
  sub_category_desc,
  manufacturer_desc,
  brand_desc,
  sell_unit_qty,
  package_desc,
  private_brand_desc,
  size_unit_of_measure,
  UPC_Sell_Unit_Desc,
  trn_site_sys_id,
  site_state_id,
  site_zip,
  gps_latitude,
  gps_longitude,
  region_id,
  region_desc,
  month_year_name,
  trn_sold_quantity,
  trn_gross_amount,
  case
    when avg_trn_item_unit_price is null then 0
    else round(avg_trn_item_unit_price, 2)
  end as avg_trn_item_unit_price,
  case
    when trn_total_discount is null then 0
    else trn_total_discount
  end as trn_total_discount,
  case
    when discount_perc is null then 0
    else discount_perc
  end as discount_perc,
  case
    when trn_net_price is null then 0
    else trn_net_price
  end as trn_net_price
from
  Daily_txn_final_GC_Nomenclature_2
""").write.mode('overwrite').saveAsTable("{0}.{1}_Daily_txn_final_SWEDEN_Nomenclature_{2}".format(db, bu_code.lower(), wave.lower()))

# COMMAND ----------

# display(sqlContext.sql("select min(trn_transaction_date), max(trn_transaction_date) from {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}".format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regular Price Imputation Summaries

# COMMAND ----------

# #of states
# display(sqlContext.sql("""select site_state_id, count(distinct trn_site_sys_id) as site, sum(trn_gross_amount) as revenue 
#                           from {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2} 
#                           group by site_state_id
#                        """.format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

# #same day promotion and regular price summary
# multi_promo_day = sqlContext.sql("""
#                      select * from {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}""".format(db, bu_code.lower(), wave.lower())).\
#                   groupBy(['BU','tempe_product_key','trn_site_sys_id','trn_transaction_date','Promo_Flag']).\
#                   agg(sf.sum("trn_gross_amount").alias('revenue')).\
#                   groupBy(['BU','tempe_product_key','trn_site_sys_id','trn_transaction_date']).\
#                   agg(sf.mean("Promo_Flag").alias('same_day_ind')).\
#                   filter(col("same_day_ind") == 0.5)
                 
# same_day_promo_data = sqlContext.sql("""
#                      select * from {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}""".format(db, bu_code.lower(), wave.lower())).\
#                      join(multi_promo_day,['BU','tempe_product_key','trn_site_sys_id','trn_transaction_date'],'left')

# COMMAND ----------

# display(same_day_promo_data.filter(col("same_day_ind") == 0.5).\
#        groupBy(['BU','department_desc','nacs_category_desc',	'category_desc',	'sub_category_desc',	\
#                 'trn_item_sys_id',	'item_name',	'trn_site_sys_id',	'sell_unit_qty',	'trn_transaction_date',	\
#                 'Promo_Flag',	'promotion_description','tempe_product_key']).\
#        agg(sf.sum('trn_gross_amount').alias('sales'),
#           sf.sum('trn_sold_quantity').alias('qty')))

# COMMAND ----------

# #oveall_level
# overall_summary = same_day_promo_data.\
#                  groupBy(['BU']).\
#                  pivot("same_day_ind").\
#                  agg(sf.sum(col("trn_gross_amount")).alias("Revenue"))
# display(overall_summary)

# COMMAND ----------

# #by category
# display(same_day_promo_data.\
#         groupBy(['BU','category_desc','promotion_description']).\
#         agg(sf.sum(col("trn_gross_amount")).alias("Revenue")))

# COMMAND ----------

# #overall Promo summary
# display(sqlContext.sql("""select BU, Promo_Flag, sum(trn_gross_amount) as revenue 
#                            from {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2}  
#                            where trn_sold_quantity >0 and trn_gross_amount > 0  
#                            group by BU, Promo_Flag
#                        """.format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

# #summary by imputation beware null does not mean cant be imputed

# display(sqlContext.sql("""
#         select BU, Promo_flag, Impute_Flag, count(distinct tempe_product_key, trn_site_sys_id, trn_transaction_date, 
#         Promo_flag) as Count_Rows, sum(trn_gross_amount) as Revenue 
#         from       
#            (select BU, Promo_flag,  tempe_product_key, trn_site_sys_id, trn_transaction_date, trn_gross_amount, 
#            case 
#                 when Impute_date is null then 'Not or Cant be Imputed' 
#                 when Impute_date = trn_transaction_date then 'Same Day' 
#                 when Impute_date > trn_transaction_date then 'From Future'
#                 when Impute_date < trn_transaction_date then 'From Past' else 'Not or Cant be Imputed' end as Impute_Flag 
#                 from 
#                       (select A.*,B.Impute_from_date as Impute_date from 
#                             (select * from {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2} where trn_transaction_date >= '2018-05-30' and 
#                              trn_transaction_date <= '2020-06-01' and trn_sold_quantity >0 and trn_gross_amount > 0
#                              )A
#                              left join {0}.{1}_na_bus_total_imputation_combined_{2} B 
#                              on A.BU = B.BU and
#                              A.trn_item_sys_id = B.upc and
#                              A.tempe_product_key = B.tempe_product_key and
#                              A.trn_site_sys_id = B.site and
#                              A.trn_transaction_date = B.business_date and
#                              A.Promo_flag = B.Promo_flag))
                       
#         group by BU, Promo_flag, Impute_Flag
# """.format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

# #percentage of days on promotion
# from pyspark.ml.feature import Bucketizer
# bucketizer = Bucketizer(splits=[ 0, 0.0000000001, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, float('Inf') ],inputCol="promo_days_perc", outputCol="buckets_perc_days_promo")

# perc_on_promo  = sqlContext.sql("""select * 
#                                    from {0}.{1}_Basket_raw_final_SWEDEN_Nomenclature_{2} 
#                                    where trn_transaction_date >= '2019-02-04' and 
#                                    trn_transaction_date <= '2021-01-31' and trn_sold_quantity >0 and trn_gross_amount > 0
#                                 """.format(db, bu_code.lower(), wave.lower())).\
#         groupBy(['BU','tempe_product_key', 'trn_site_sys_id','trn_transaction_date']).\
#         agg(sf.max("Promo_flag").alias("Final_Promo"), sf.sum("trn_gross_amount").alias("Revenue")).\
#         groupBy(['BU','tempe_product_key', 'trn_site_sys_id','Final_Promo']).\
#         agg(sf.sum("Revenue").alias("Revenue"),sf.count(sf.lit(1)).alias("count_days")).\
#         withColumn("tot_days", sf.sum(col('count_days')).over((Window.partitionBy(['BU','tempe_product_key','trn_site_sys_id'])))).\
#         withColumn("perc_days_on_promo", sf.when((col("Final_Promo") == 1), col("count_days")/col("tot_days")).otherwise(0)).\
#         groupBy(['BU','tempe_product_key', 'trn_site_sys_id']).\
#         agg(sf.sum("Revenue").alias("Revenue"),sf.max("perc_days_on_promo").alias("promo_days_perc"))

# perc_on_promo_bucketed = bucketizer.setHandleInvalid("keep").transform(perc_on_promo).\
#         groupBy(['BU','buckets_perc_days_promo']).\
#         agg(sf.sum("Revenue").alias("Revenue"), sf.count(sf.lit(1)).alias("count_pro_site"))

# COMMAND ----------

# display(perc_on_promo_bucketed)

# COMMAND ----------

# # to check Variation in Reg Price
# from pyspark.ml.feature import Bucketizer
# bucketizer = Bucketizer(splits=[ 0, 0.0000000001, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, float('Inf') ],inputCol="cov_avg_price", outputCol="buckets_cov_avg_price")

# CoV_reg_P = sqlContext.sql("""select * from {0}.{1}_Daily_txn_final_SWEDEN_Nomenclature_{2} 
#                               where trn_transaction_date >= '2019-02-04' and trn_transaction_date <= '2021-01-31' and trn_sold_quantity >0 and trn_gross_amount > 0
#                             """.format(db, bu_code.lower(), wave.lower())).\
#             groupBy(['BU','tempe_product_key', 'trn_site_sys_id']).\
#             agg(sf.sum('trn_gross_amount').alias('revenue'),
#                           sf.stddev('avg_trn_item_unit_price').alias('sd_avg_price'),sf.mean('avg_trn_item_unit_price').alias('mean_avg_price')).\
#                           withColumn('cov_avg_price', sf.col('sd_avg_price')/sf.col('mean_avg_price'))

# CoV_reg_P_bucketed =  bucketizer.setHandleInvalid("keep").transform(CoV_reg_P).\
#         groupBy(['BU','buckets_cov_avg_price']).\
#         agg(sf.sum("revenue").alias("revenue"), sf.count(sf.lit(1)).alias("count_pro_site"))    

# COMMAND ----------

# display(CoV_reg_P_bucketed)#.filter(col("buckets_cov_avg_price").isNull())

# COMMAND ----------

# # to check variation amogh different product keys within same tempe product key for an a day at a store
# all_daily_data = sqlContext.sql("select * from {0}.{1}_Daily_txn_final_SWEDEN_Nomenclature_{2}".format(db, bu_code.lower(), wave.lower())).\
#                 filter((sf.col("trn_gross_amount")>0) & (sf.col("trn_sold_quantity")>0))

# all_daily_data_col = all_daily_data.select(['BU','tempe_product_key','item_name','product_key','trn_site_sys_id','trn_transaction_date',
#                                             'trn_gross_amount','trn_sold_quantity','avg_trn_item_unit_price','category_desc','site_state_id'])

# metric_store = all_daily_data_col.groupBy(['BU','category_desc','item_name','tempe_product_key','trn_site_sys_id','trn_transaction_date']).\
#                           agg(sf.sum('trn_gross_amount').alias('revenue'),
#                               sf.stddev('avg_trn_item_unit_price').alias('sd_avg_price'),sf.mean('avg_trn_item_unit_price').alias('mean_avg_price')).\
#                           withColumn('cov_avg_price', sf.col('sd_avg_price')/sf.col('mean_avg_price'))

# from pyspark.ml.feature import Bucketizer
# bucketizer = Bucketizer(splits=[ 0, 0.0000000001, 0.02, 0.05, 0.1, 0.25, 0.5, 1, float('Inf') ],inputCol="cov_avg_price", outputCol="buckets")
# metric_store_bucket = bucketizer.setHandleInvalid("keep").transform(metric_store)

# display(metric_store_bucket.groupBy(['BU','category_desc','buckets']).agg(sf.count(sf.lit(1)),sf.sum('revenue').alias('bucket_revenue')))

# COMMAND ----------

# # to check daily Variation in Reg Price for same item across stores
# metric_transaction_date= all_daily_data_col.groupBy(['BU','tempe_product_key','trn_transaction_date','category_desc','item_name']).\
#                           agg(sf.sum('trn_gross_amount').alias('revenue'),\
#                           sf.stddev('avg_trn_item_unit_price').alias('sd_avg_price'),sf.mean('avg_trn_item_unit_price').alias('mean_avg_price')).\
#                           withColumn('cov_avg_price', sf.col('sd_avg_price')/sf.col('mean_avg_price'))
# bucketizer = Bucketizer(splits=[ 0, 0.0000000001, 0.02, 0.05, 0.1, 0.25, 0.5, 1, float('Inf') ],inputCol="cov_avg_price", outputCol="buckets")
# metric_transaction_date_bucket = bucketizer.setHandleInvalid('keep').transform(metric_transaction_date)

# display(metric_transaction_date_bucket.groupBy(['BU','category_desc','buckets']).agg(sf.count(sf.lit(1)),sf.sum('revenue').alias('bucket_revenue')))

# COMMAND ----------

# display(metric_transaction_date_bucket.groupBy(['BU','category_desc','item_name','tempe_product_key','buckets']).\
#                agg(sf.count(sf.lit(1)),sf.sum('revenue').alias('bucket_revenue')))

# COMMAND ----------

# display(metric_transaction_date_bucket.groupBy(['BU','tempe_product_key']).agg(sf.mean('cov_avg_price').alias('avg_CoV')))

# COMMAND ----------

# # to check daily Variation in Reg Price for same item across states
# # metric_transaction_date_state = all_daily_data_col.groupBy(['BU','tempe_product_key','trn_transaction_date',
# #                                                             'category_desc','site_state_id']).\
# #                           agg(sf.sum('trn_gross_amount').alias('revenue'), 
# #                               (sf.sum(col('avg_trn_item_unit_price')*col('trn_gross_amount'))/sf.sum('trn_gross_amount')).alias('w_avg_price'))


# # metric_transaction_date_state2 = metric_transaction_date_state.groupBy(['BU','tempe_product_key','trn_transaction_date',
# #                                                             'category_desc','site_state_id']).\
# #                                 agg(sf.sum('revenue'),sf.stddev('w_avg_price').alias('sd_w_avg_price'),
# #                                     sf.mean('w_avg_price').alias('mean_w_avg_price'),sf.countDistinct("site_state_id").alias('no_state')).\
# #                                 withColumn('cov_w_avg_price', sf.col('sd_w_avg_price')/sf.col('mean_w_avg_price'))

# # bucketizer2 = Bucketizer(splits=[ 0, 0.0000000001, 0.02, 0.05, 0.1, 0.25, 0.5, 1, float('Inf') ],inputCol="cov_w_avg_price", outputCol="buckets")
# # metric_transaction_date_state_bucket = bucketizer2.setHandleInvalid("keep").transform(metric_transaction_date_state2)
# # display(metric_transaction_date_state_bucket.groupBy(['BU','category_desc','buckets']).\
# #         agg(sf.count(sf.lit(1)),sf.sum('sum(revenue)').alias('bucket_revenue')))


# metric_transaction_state= all_daily_data_col.groupBy(['BU','tempe_product_key','trn_transaction_date','category_desc','item_name','site_state_id']).\
#                           agg(sf.sum('trn_gross_amount').alias('revenue'),
#                           sf.stddev('avg_trn_item_unit_price').alias('sd_avg_price'),sf.mean('avg_trn_item_unit_price').alias('mean_avg_price')).\
#                           withColumn('cov_avg_price', sf.col('sd_avg_price')/sf.col('mean_avg_price'))
# bucketizer = Bucketizer(splits=[ 0, 0.0000000001, 0.02, 0.05, 0.1, 0.25, 0.5, 1, float('Inf') ],inputCol="cov_avg_price", outputCol="buckets")
# metric_transaction_state_bucket = bucketizer.setHandleInvalid('keep').transform(metric_transaction_state)

# display(metric_transaction_state_bucket.groupBy(['BU','category_desc','buckets','site_state_id']).agg(sf.count(sf.lit(1)),sf.sum('revenue').alias('bucket_revenue')))
