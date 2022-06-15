# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Creator:Taru,Anuj,Jagurti
# Date Created: 01/15/2021
# Date Updated: This notebook is a clone of Accentures notebook with a similar name. This maps product keys from an older enviroment to the new enviroment. We are trying to replace this process with a static table ( work under progress ) . This notebook can be used for all North American BUs except RM & GC. For RM/GC run the other notebook

# Modified by: Karan Kumar
# Date Modified: 19/08/2021

#Please clone this notebook in the appropriate BU's workspace, update its name if you would like and run it after making the appropirate changes.

# Our understanding is that the following files are simply read in and are NOT to be updated
# multi_to_promo.csv
# Problem_type_3.csv
# Problem_type_1_2.csv

#Once this notebook is run, do spend sometime validating the output. Checkout the quality checks within this notebook and also the separate Validation notebook for more checks.

#Note of Grand Canyon (GR): if we want we could bypass this notebook/ and only run the outlier piece. Worth reviewing when we get to the refresh.

#Output: phase3_extensions.{bu}_master_merged_ads_may2021_refresh

# COMMAND ----------

# MAGIC %md
# MAGIC #### Meta Data
# MAGIC 
# MAGIC **Creator: *Amogh Gupta* | Updated by: *Nayan Mallick, Deepankar Singh Rao* **
# MAGIC 
# MAGIC **Date Created: 06/25/2020**
# MAGIC 
# MAGIC **Date Updated: 07/17/2020**
# MAGIC 
# MAGIC **Description: A notebook to be used to validate the upc x sell unit qty roll-up**
# MAGIC 
# MAGIC Main Code to do the following
# MAGIC   1. Remove Out of Scope Categories as deemed by looking at names/metrics(sales & names) and BU GC via xlsx input 
# MAGIC   2. Remove Unknown BU 
# MAGIC   3. Roll up data at all hierarchy level - remove all anomalous sales rows
# MAGIC   4. Check if an upc x sell qty has multiple items - do analysis for those cases only
# MAGIC   5. do the 4 flags to check price - described in the code blocks 14 onwards
# MAGIC   6. Do the cross cat rollup analysis - if acc dep cat 1 or not
# MAGIC 
# MAGIC * **Update Nayan - Added outlier detection and deletion before consolidation, Multi-Pack Cigarettes Logic**
# MAGIC * **Update Deepankar - Generalised the code to create ADS for BUs with/without Multiple Environments, with/without Cig-MultiPack Issue; Added QC Points and widgets**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Functions

# COMMAND ----------

import pandas as pd
import datetime
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from operator import add
from functools import reduce
from pyspark.sql.functions import when, sum, avg, col, concat, lit, count, countDistinct, max, min
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import greatest
from pyspark.sql.functions import least
from pyspark.ml.feature import Bucketizer
import json

#enabling delta caching to improve performance
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
sqlContext.clearCache()

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------


#Defining parameters

#wave
wave = dbutils.widgets.get("refresh")

#phase 3 extension db location
db = dbutils.widgets.get("db")

#Apply BU 
business_unit = dbutils.widgets.get("business_unit")

#specify date ranges
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

#static file path location for the different issues, which have been identified in the hierarchy validation
# multi_to_promo.csv
# Problem_type_3.csv
# Problem_type_1_2.csv

static_file_path = dbutils.widgets.get("static_file_path")

#db parameter
na_DB_txn = "dl_localized_pricing_all_bu"

# Test Control Store Map
# test_control_map_name = dbutils.widgets.get("test_control_map_name")

# multipack_promo_bu = ["1800 - Rocky Mountain Division", "1900 - Gulf Coast Division", "2800 - Texas Division"]

product_cols = [
  'product_key',
  'category_desc'
]

#BU abbreviation
bu_code = dbutils.widgets.get("bu_code").lower()

# old to new upc mapping
new_upc_pk_file = dbutils.widgets.get("new_upc_pk_file")
new_upc_pk = spark.createDataFrame(pd.read_excel(new_upc_pk_file)).filter(col('BU')==business_unit)

print("Start Date:", start_date, "; End Date:", end_date, "; BU_Abb:", bu_code, "; Business Unit:", business_unit, "; Static path:", static_file_path)

# COMMAND ----------

#derived parameters
#table used for analysis 
merged_table = "na_all_bu_transaction_full_table_hierarchy_validation_{0}".format(wave.lower())
na_item_table = "{}.product".format(na_DB_txn)

# COMMAND ----------

# MAGIC  %md
# MAGIC #### Creating Widgets 

# COMMAND ----------

# #Create widgets for business unit selection

# Business_Units = ["1400 - Florida Division",
#                   "1600 - Coastal Carolina Division",final
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

# dbutils.widgets.dropdown("01.Business Unit", "1400 - Florida Division", Business_Units)
# dbutils.widgets.text("02.Start Date", "2018-05-30")
# dbutils.widgets.text("03.End Date", "2020-06-01")

## Run only if widgets has to be removed/ recreated
## dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading the Widget Parameters

# COMMAND ----------

# wave = "june2021_refresh"

# #Apply BU 
# business_unit = dbutils.widgets.get("01.Business Unit")

# #date ranges
# start_date = getArgument('02.Start Date')
# end_date = getArgument('03.End Date')

# #get bu 2 character code
# bu_abb_ds = sqlContext.createDataFrame(bu_abb, ['bu', 'bu_abb']).filter('bu = "{}"'.format(business_unit))
# bu_code = bu_abb_ds.collect()[0][1].lower()

# print("Start Date:", start_date, "; End Date:", end_date, "; BU_Abb:", bu_code, "; Business Unit:", business_unit)

# COMMAND ----------

# test_control_stores = spark.read.csv("/Phase3_extensions/Elasticity_Modelling/JUNE2021_Refresh/test_control_map_updated_04_22_2021.csv",inferSchema=True,header=True)
# test_control_stores.createOrReplaceTempView("test_control_stores")
test_control_stores = spark.sql("""Select business_unit as BU, site_id as Site, group as Type, cluster as Cluster from circlek_db.grouped_store_list""")
# Where business_unit = '{}'
# and group = 'Test' """.format(business_units)
test_control_stores.createOrReplaceTempView("test_control_stores")


test_control_stores = test_control_stores.filter(sf.col("BU").isin(["Central Canada"])).filter(sf.col("Type").isin(["Test", "Control"])).select(sf.col("Site"))
store_list = tuple(test_control_stores.select('Site').toPandas()['Site'].astype(int) - 3000000)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pre Requisite Table Save

# COMMAND ----------

if bu_code == 'ce':
  table_1 = sqlContext.sql("""
  select
    mit.*,
    s.site_number_corporate,
    s.site_address,
    s.site_city,
    s.site_state_id,
    s.site_zip,
    s.gps_latitude,
    s.gps_longitude,
    s.division_id,
    s.division_desc,
    s.region_id,
    s.region_desc,
    p.sell_unit_qty,
    p.category_desc,
    p.sub_category_desc,
    p.department_desc,
    p.package_desc,
    p.item_number
  FROM
    dl_localized_pricing_all_bu.merchandise_item_transactions mit
    LEFT JOIN (SELECT * FROM dl_localized_pricing_all_bu.site s WHERE site_number_corporate IN (SELECT Site - 3000000 FROM test_control_stores WHERE BU = "Central Canada")) s
      ON mit.site_env_key = s.site_env_key     
    LEFT JOIN dl_localized_pricing_all_bu.product p
      ON mit.prod_item_env_key = p.product_item_env_key
  WHERE
    s.division_desc = '{}' AND
    s.Region_desc != "Central - Closed Stores"
  """.format(business_unit))
else:
  table_1 = sqlContext.sql("""
  select
   mit.*,
   s.site_number_corporate,
   s.site_address,
   s.site_city,
   s.site_state_id,
   s.site_zip,
   s.gps_latitude,
   s.gps_longitude,
   s.division_id,
   s.division_desc,
   s.region_id,
   s.region_desc,
   p.sell_unit_qty,
   p.category_desc,
   p.sub_category_desc,
   p.department_desc,
   p.package_desc,
   p.item_number
  FROM
   dl_localized_pricing_all_bu.merchandise_item_transactions mit
   LEFT JOIN dl_localized_pricing_all_bu.site s
     ON mit.site_env_key = s.site_env_key     
   LEFT JOIN dl_localized_pricing_all_bu.product p
     ON mit.prod_item_env_key = p.product_item_env_key
  WHERE
   s.division_desc = "{}"
  """.format(business_unit))

table_1.createOrReplaceTempView("na_all_bu_transaction_full_table_hierarchy_validation_{}".format(wave.lower()))

# COMMAND ----------

# display(table_1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Conversion of Multi-Pack (Includes multi-pack Cigarettes conversion for Old Environment in BU Texas)

# COMMAND ----------

#Cig cats
cig_cats = ['Generic Cig R02',
            'Premium Cig R02',
            'Cigarettes-open',
            'Private Lbl Cig R02',
            '002-CIGARETTES',
            'Premium Cigarettes',
            'Sub Generic Pl Cigarettes',
            'Discount Cigarettes'
           ]

multipack_conversion = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').\
                               load('{}multi_to_promo.csv'.format(static_file_path))

# COMMAND ----------

if business_unit in ("2800 - Texas Division"):
  print(business_unit)
  #keep data for only BU TX and only Cigarette UPCs
  txn_data_cig_multi_raw = sqlContext.sql("select * from {}".format(merged_table)).\
                           withColumnRenamed("division_desc","BU").\
                           withColumnRenamed("division_id","BU_ID").\
                           drop(col('category_desc')).\
                           filter(sf.col("BU").isin(business_unit)).\
                           join(sf.broadcast(sqlContext.sql("select * from {}".format(na_item_table)).select(*product_cols)),'product_key','left').\
                           filter(col("category_desc").isin(*cig_cats))

  # find cases where upc x sell_unit_qty does not span both envs but same upc does
  single_env_upc_sell = txn_data_cig_multi_raw.\
                        groupBy(['BU','upc','sell_unit_qty']).\
                        agg(sf.countDistinct(col("sys_environment_name")).alias("count_env")).\
                        filter(col("count_env") == 1)

  #join this table with all_env and keep only cases where sys_env is not Tempe

  single_env_old = txn_data_cig_multi_raw.\
                    join(single_env_upc_sell,['BU','upc','sell_unit_qty'],'left').\
                    filter(~sf.col("count_env").isNull()).\
                    filter(sf.col("sys_environment_name") != 'Tempe')

  #upcs which exist in tempe
  tempe_env_cases = txn_data_cig_multi_raw.\
                    filter(col("sys_environment_name") == 'Tempe').\
                    select(['BU','upc']).\
                    distinct().\
                    withColumn("In", sf.lit(1))

  #keep only cases upc x sell_unit_qty does not span both envs but same upc does
  single_env_old_tempe_upc = single_env_old.\
                             join(tempe_env_cases,['BU','upc'],'left').\
                             filter(~sf.col("In").isNull()).\
                             drop('In')

  #split main table & add changed cases # keep upc x sell unit qty from prior changed sell qty table
  changed_cases = single_env_old_tempe_upc.\
                  select(['BU','upc','sell_unit_qty']).distinct().\
                  withColumnRenamed("BU","division_desc").\
                  withColumn("Out", sf.lit(1))

  ##1. Adjust these cases for promo columns and qty sold column - keeping revenue same - is all we need to do
  # change promotion_id',promotion_description,promotion_type,quantity_sold,sell_unit_qty

  merged_table_df = sqlContext.sql("select * from {}".format(merged_table)).\
                    join(changed_cases,['division_desc','upc','sell_unit_qty'],'left').\
                    withColumn("promotion_id", sf.when((sf.col("sell_unit_qty") == 1) & (col("Out") == 1), 0).
                               otherwise(sf.when((col("Out") == 1),1).otherwise(col("promotion_id")))).\
                    withColumn("promotion_description", sf.when((sf.col("sell_unit_qty") != 1) & (col("Out") == 1),
                                                                 sf.concat(sf.col('sell_unit_qty'),sf.lit('_for'))).
                                otherwise(sf.when((col("Out") == 1),"Regular Price").otherwise(col("promotion_description")))).\
                    withColumn("promotion_type", sf.when((col("sell_unit_qty").isin([2,3,4,5])) & (col("Out") == 1), "non-carton promotion").
                                otherwise(sf.when((col("sell_unit_qty") > 5) & (col("Out") == 1), "carton promotion").\
                                          otherwise(col("promotion_type")))).\
                    withColumn("quantity_sold", sf.when((col("Out") == 1),sf.col("quantity_sold")*col("sell_unit_qty")).\
                               otherwise(col("quantity_sold"))).\
                    withColumn("sell_unit_qty", sf.when((col("Out") == 1),sf.lit(1)).otherwise(col("sell_unit_qty"))).\
                    drop("Out")
  
else:
  print(business_unit)
  #split main table & add changed cases # keep upc x sell unit qty from prior changed sell qty table
  multipack_conversion_changed_cases = multipack_conversion.\
                                        select(['division_desc','upc','sell_unit_qty','check']).distinct().\
                                        withColumnRenamed("Check", "Out")

  ##1. Adjust these cases for promo columns and qty sold column - keeping revenue same - is all we need to do
  # change promotion_id',promotion_description,promotion_type,quantity_sold,sell_unit_qty

  merged_table_df = sqlContext.sql("select * from {}".format(merged_table)).\
                    join(multipack_conversion_changed_cases,['division_desc','upc','sell_unit_qty'],'left').\
                    withColumn("promotion_id", sf.when((sf.col("sell_unit_qty") == 1) & (col("Out") == 1), 0).
                               otherwise(sf.when((col("Out") == 1),1).otherwise(col("promotion_id")))).\
                    withColumn("promotion_description", sf.when((sf.col("sell_unit_qty") != 1) & (col("Out") == 1),
                                                                 sf.concat(sf.col('sell_unit_qty'),sf.lit('_for'))).
                                otherwise(sf.when((col("Out") == 1),"Regular Price").otherwise(col("promotion_description")))).\
                    withColumn("promotion_type", sf.when((col("sell_unit_qty").isin([2,3,4,5])) & (col("Out") == 1), "non-carton promotion").
                                otherwise(sf.when((col("sell_unit_qty") > 5) & (col("Out") == 1), "carton promotion").\
                                          otherwise(col("promotion_type")))).\
                    withColumn("quantity_sold", sf.when((col("Out") == 1),sf.col("quantity_sold")*col("sell_unit_qty")).\
                               otherwise(col("quantity_sold"))).\
                    withColumn("sell_unit_qty", sf.when((col("Out") == 1),sf.lit(1)).otherwise(col("sell_unit_qty"))).\
                    drop("Out")

# COMMAND ----------

# display(merged_table_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outlier Analysis
# MAGIC - If a Product x site has seen >= 50 abs(quantity_sold) in a single txn in its lifetime - do a mu +6 sigma outlier treatment >> delete all such outlier-txns with are >=50
# MAGIC - If a Product x site has seen < 50 abs(quantity_sold) in a single txn in its lifetime - do a mu +6 sigma outlier treatment >> delete all such outlier-txns with are >=20

# COMMAND ----------

# # If the BU to run is Rocky Mountains or Texas, then we take the processed table for Multi Pack Cigarettes
# if business_unit in (multipack_promo_bu):
#   print("Merged Table is the Processed Table (Accounting Multi-Pack Promotion)")
# else:
#   print("Merged Table is the Initial Table")
#   merged_table_df = sqlContext.sql("select * from {}".format(merged_table))

# COMMAND ----------

# sigma_limit = 6

# COMMAND ----------

# Code explanation
# row 1-5 -Read all transactions, edits two columns, filter to BU and between business dates
# row 6-8 -Create column for transaction identification, join in product table (only pk and cat)
# row 9 -Group by: BU, pk, cat_desc, site, date, transaction_identifier from earlier
# row 10 -Aggregate: sum of quantity_sold, sum of sales
# row 11-12 -Join in accenture category table and filter "out of scope" (determines if they are out of scope or not - generic removal of categories)
# 13-22 -Create columns:
# 	-qty - absolute value of quantity_sold
# 	-max_qty - max of qty (above) over BU, PK, site
# 	-tot_qty - sum of qty over BU
# 	-tot_rev - sum of sales_amount over BU
# 	-tot_txn - count of txn_proxy (transaction identifier) over BU
# 	-type - when max_qty < 50 = "< 50" otherwise "> 50"
# 	-mean_qty - mean of qty over BU, pk, site
# 	-sdev_qty - standard deviation of qty over BU, PK, site
# 	-mu_plus_xsig - mean_qty + (sigma * sdev_qty)
# 	-outlier_flag - when qty > mu_plus_xsig then "outlier" otherwise "none"
# row 23 -filter to outlier_flag == "outlier"
# row 24-25 -Create column delete_flag: when (type == "< 50" & qty >= 20) then 1, otherwise when (type == '> 50' & qty >= 50) then 1, otherwise 0
# row 26 -Filter delete_flag == 1



# txn_data_outlier_raw = merged_table_df.\
#                        withColumnRenamed("division_desc","BU").\
#                        withColumnRenamed("division_id","BU_ID").\
#                        filter(sf.col("BU").isin(business_unit)).\
#                        filter((col("business_date") >= start_date) & (col("business_date") <= end_date)).\
#                        withColumn("txn_proxy", sf.concat(sf.col('transaction_number'),sf.lit('_'),sf.col('transaction_time'),
#                                                          sf.lit('_'),sf.col('business_date'))).\
#                        join(sf.broadcast(sqlContext.sql("select * from {}".format(na_item_table)).select(*product_cols)),'product_key','left').\
#                        groupBy(['BU','product_key','category_desc','site_number_corporate','business_date','txn_proxy']).\
#                        agg(sf.sum("quantity_sold").alias("quantity_sold"),sf.sum("sales_amount").alias("sales_amount")).\
#                        join(sf.broadcast(sqlContext.sql("select * from {}".format('localized_pricing.acc_cat'))),'category_desc','left').\
#                        filter(sf.col("Acc_Cat") != 'Out of Scope').\
#                        withColumn("qty", sf.abs(col("quantity_sold"))).\
#                        withColumn("max_qty", sf.max(col('qty')).over((Window.partitionBy(['BU','product_key','site_number_corporate'])))).\
#                        withColumn("tot_qty", sf.sum(col('qty')).over((Window.partitionBy(['BU'])))).\
#                        withColumn("tot_rev", sf.sum(col('sales_amount')).over((Window.partitionBy(['BU'])))).\
#                        withColumn("tot_txn", sf.count(col('txn_proxy')).over((Window.partitionBy(['BU'])))).\
#                        withColumn("type", sf.when(col("max_qty") < 50, "< 50").otherwise(">= 50")).\
#                        withColumn("mean_qty", sf.mean(col('qty')).over((Window.partitionBy(['BU','product_key','site_number_corporate'])))).\
#                        withColumn("sdev_qty", sf.stddev(col('qty')).over((Window.partitionBy(['BU','product_key','site_number_corporate'])))).\
#                        withColumn("mu_plus_xsig", (sf.col("mean_qty")+ sigma_limit*(sf.col("sdev_qty")))).\
#                        withColumn("outlier_flag", sf.when((sf.col("qty") > sf.col('mu_plus_xsig')),"outlier").otherwise("none")).\
#                        filter(col("outlier_flag") == "outlier").\
#                        withColumn("delete_flag", sf.when(((col("type") == '< 50') & (col("qty") >= 20)), 1).\
#                                   otherwise(sf.when(((col("type") == '>= 50') & (col("qty") >= 50)), 1).otherwise(0))).\
#                        filter(col("delete_flag") == 1)

# COMMAND ----------

# row 1 -Start with base table or one manipulated with cig information above
# row 2-3 - rename division_desc and division_id
# row 4-5 - Filter to our BU and dates between our target
# row 6-7 -Create column for transaction identifier --> txn_proxy
# row 8-9 -Join in above txn_data_outlier_raw on: pk, site, date, transaction identifier
# row 10-11 -Filter down to transactions that have delete_flag of null and then drop the delete_flag column

# outlier_treated = merged_table_df.\
#                        withColumnRenamed("division_desc","BU").\
#                        withColumnRenamed("division_id","BU_ID").\
#                        filter(sf.col("BU").isin(business_unit)).\
#                        filter((col("business_date") >= start_date) & (col("business_date") <= end_date)).\
#                        withColumn("txn_proxy", sf.concat(sf.col('transaction_number'),sf.lit('_'),sf.col('transaction_time'),
#                                                          sf.lit('_'), sf.col('business_date'))).\
#                        join((txn_data_outlier_raw.select(["product_key","site_number_corporate","business_date","txn_proxy","delete_flag"])),
#                             ["product_key","site_number_corporate","business_date","txn_proxy"],'left').\
#                        filter(col("delete_flag").isNull()).\
#                        drop(col('delete_flag'))

# COMMAND ----------

# # to be used to check down-stream processes
# print((outlier_treated.count(), len(outlier_treated.columns)))

# COMMAND ----------

###########################################################
# Kushal's Outlier Process Start

# COMMAND ----------

# DBTITLE 1,Read In Data - Filter Down Transactions Types - Filter Down Categories
#merged_table_df.createOrReplaceTempView("merged_table_df")

#static_table = "merged_table_df"

# Transaction Type Filter
transaction_type_key_ENV = ["1||Columbus",\
                            "10||Columbus",\
                            "2||Holiday",\
                            "6||Holiday",\
                            "2||Laval",\
                            "3||Laval",\
                            "3||San_Antonio",\
                            "4||San_Antonio",\
                            "5||Sanford",\
                            "8||Sanford",\
                            "1||Tempe",\
                            "6||Tempe",\
                            "2||Toronto",\
                            "3||Toronto"]

Acc_cat = sqlContext.sql("""SELECT distinct category_desc from localized_pricing.acc_cat WHERE Acc_cat != 'Out of Scope'""")
Acc_cat = Acc_cat.select("category_desc").rdd.flatMap(lambda x: x).collect()
data = merged_table_df.filter((col("business_date") >= start_date) & (col("business_date") <= end_date)).filter(concat(col("transaction_type_key"), lit("||"), col("sys_environment_name")).isin(transaction_type_key_ENV)).filter(col("category_desc").isin(Acc_cat))

#display(data)

# COMMAND ----------

# DBTITLE 1,A quick look at all the in-scope categories:
# display(data.select(col("category_desc")).distinct())

# COMMAND ----------

# DBTITLE 1,Roll Up Data To marketbasket_header_key and product_key
# # * Figure out what the level of aggregation is best to roll up market basket too

data_mb_roll_up = data.groupBy("division_desc",\
                           "division_id",\
                           "promotion_env_key",\
                           "prod_item_env_key",\
                           "site_env_key",\
                           "sys_environment_name",\
                           "transaction_date",\
                           "transaction_time",\
                           "transaction_number",\
                           "promotion_id",\
                           "promotion_description",\
                           "promotion_type",\
                           "site_number",\
                           "loyalty_card_number",\
                           "organization_key",\
                           "timeofday_key",\
                           "loyalty_key",\
                           "businessdate_calendar_key",\
                           "marketbasket_header_key",\
                           "product_key",\
                           "upc",\
                           "transaction_type_key",\
                           "site_number_corporate",\
                           "site_address",\
                           "site_city",\
                           "site_state_id",\
                           "site_zip",\
                           "region_id",\
                           "region_desc",\
                           "gps_latitude",\
                           "gps_longitude",\
                           "business_date",\
                           "item_description",\
                           "department_desc",\
                           "category_desc",\
                           "sub_category_desc",\
                           "sell_unit_qty",\
                           "package_desc",\
                           "item_number")\
                  .agg(sum(col("sales_amount")).alias("sum_of_sales_amount"),\
                       sum(col("quantity_sold")).alias("sum_of_quantity_sold"))


# COMMAND ----------

# DBTITLE 1,Preserving the needed records 
data_mb_roll_up_condition = data_mb_roll_up.filter(((col("sum_of_sales_amount")>0) & (col("sum_of_quantity_sold")>0)) | ((col("sum_of_sales_amount")<0) & (col("sum_of_quantity_sold")<0)))

# COMMAND ----------

# DBTITLE 1,Excluding the Open key items 
data_mb_agg = data_mb_roll_up_condition.\
              withColumnRenamed("division_desc","BU").\
              withColumnRenamed("division_id","BU_ID").\
              withColumnRenamed("sum_of_sales_amount","sales_amount").\
              withColumnRenamed("sum_of_quantity_sold","quantity_sold").\
              filter(~((col("item_description").like('%OPEN KEY%')) | (col("item_description").like("%open key%")))).cache()

# COMMAND ----------

# Determine Categories To Loop Over
if (bu_code == 'ce'):
  cat_list = data_mb_agg.select('department_desc').distinct().collect()
else:
  cat_list = data_mb_agg.select('category_desc').distinct().collect()
final_cat_list = [c[0] for c in cat_list]

final_test = final_cat_list[0]

for ea in final_cat_list:
  print(ea)

# COMMAND ----------

###########################################################################
# High level Logic
###########################################################################
# * Figure out what the cutoff quantile is
# Do loop

  # Filter Data down to loop criteria (maybe category?)
  # Capture data that is > quantile value (we want mb_header_key)
  # Create some sort of outside list to capture mb_header_keys to remove
  
# Filter all data down to remove out mb_header_keys that are outliers

###########################################################################


quantile_cutoff = .9999
upper_bound = quantile_cutoff
lower_bound = 1-quantile_cutoff
remove_mb_header_list = []


for category in final_cat_list:
  # Subset data
  if (bu_code == 'ce'):
    tmp_data = data_mb_agg.filter(col("department_desc")==category).select("marketbasket_header_key", "product_key", "sales_amount", "quantity_sold")
  else:
    tmp_data = data_mb_agg.filter(col("category_desc")==category).select("marketbasket_header_key", "product_key", "sales_amount", "quantity_sold")
  
  # Capture quantile metrics - upper
  tmp_category_metrics_upper = tmp_data.approxQuantile(["sales_amount","quantity_sold"],[upper_bound],0)
  # Capture quantile metrics - lower
  tmp_category_metrics_lower = tmp_data.approxQuantile(["sales_amount","quantity_sold"],[lower_bound],0)
  
  
  # Save metrics to other variable (not required) - upper
  tmp_sales_cutoff_upper = tmp_category_metrics_upper[0][0]
  tmp_qty_cutoff_upper = tmp_category_metrics_upper[1][0]
  
  # Save metrics to other variable (not required) - lower
  tmp_sales_cutoff_lower = tmp_category_metrics_lower[0][0]
  tmp_qty_cutoff_lower = tmp_category_metrics_lower[1][0]

  
  # Filter data down to records above cutoff - upper
  tmp_remove_df_upper = tmp_data.filter(col("quantity_sold")>tmp_qty_cutoff_upper)
  # Filter data down to records above cutoff - lower
  tmp_remove_df_lower = tmp_data.filter(col("quantity_sold")<tmp_qty_cutoff_lower)
  
  # Capture marketbasket_header_key to remove - upper
  tmp_remove_mb_upper = tmp_remove_df_upper.select("marketbasket_header_key").distinct().collect() 
  tmp_final_remove_mb_upper = [mbu[0] for mbu in tmp_remove_mb_upper]
  # Capture marketbasket_header_key to remove - lower
  tmp_remove_mb_lower = tmp_remove_df_lower.select("marketbasket_header_key").distinct().collect() 
  tmp_final_remove_mb_lower = [mbl[0] for mbl in tmp_remove_mb_lower]
  
  # Combine above dfs to add to outside list of mb header to remove
  
  # Try to not have errors if no records removed
  if ((len(tmp_final_remove_mb_upper) > 0) | (len(tmp_final_remove_mb_lower) > 0)):
    
    if (len(tmp_final_remove_mb_upper)>0):
      remove_mb_header_list.extend(tmp_final_remove_mb_upper)
      print(category, "number mb_header_key removed (upper):", len(tmp_final_remove_mb_upper))
      
    if (len(tmp_final_remove_mb_lower)>0):
      remove_mb_header_list.extend(tmp_final_remove_mb_lower)
      print(category, "number mb_header_key removed (lower):", len(tmp_final_remove_mb_lower))
      
  else:
    print(category, "none to remove")
    
    
    
xyz = set(remove_mb_header_list)
print(len(xyz))

# 2467 - 0.99999
# 24966 - 0.9999

# COMMAND ----------

outlier_treated = data_mb_agg.filter(~col("marketbasket_header_key").isin(xyz))
# outlier_treated = merged_table_df.\
#               withColumnRenamed("division_desc","BU").\
#               withColumnRenamed("division_id","BU_ID")

# COMMAND ----------

################
# Kushal's Outlier Process End

# COMMAND ----------

# Defining Columns to Keep in the ADS from the Transactions Table
cols_to_keep = [
                'promotion_env_key',
                'prod_item_env_key',
                'site_env_key',
                'sys_environment_name',
                'transaction_date',
                'business_date',
                'transaction_time',
                'transaction_number',
                'product_key',
                'upc',
                'sell_unit_qty',
                'promotion_id',
                'promotion_description',
                'promotion_type',
                'quantity_sold',
                'sales_amount',
                'loyalty_card_number',
                'marketbasket_header_key',
                'organization_key',
                'timeofday_key',
                'loyalty_key',
                'businessdate_calendar_key',
                'transaction_type_key',
                'site_number_corporate',
                'site_state_id',
                'site_city',
                'site_address',
                'site_zip',
                'gps_latitude',
                'gps_longitude',
                'region_id',
                'region_desc',
                'BU_ID',
                'BU'
               ]

# COMMAND ----------

# Defining the Products Table to be joined with the Transactions Table for the Hierarchy Columns
product_table = sqlContext.sql("""  
                           Select product_item_env_key as tempe_prod_item_env_key,
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
                           from dl_localized_pricing_all_bu.PRODUCT
                                  """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinguishing between Multiple Environment BUs and Single Environment BUs

# COMMAND ----------

# Determining whether the given BU is Single Environment or Multiple Environment and follow the Message from the Output of this Code Block
environments_in_bu = sqlContext.sql("select * from {}".format(merged_table)).\
                        withColumnRenamed("division_desc","BU").\
                        filter(sf.col("BU").isin(business_unit)).\
                        select('sys_environment_name').\
                        distinct().rdd.flatMap(lambda x: x).collect()

#print(environments_in_bu)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADS Creation for Single Environment BU 

# COMMAND ----------

# Final ADS for the Single Environment BU, do not run if it is a Multiple Environment BU
final_ADS = outlier_treated.\
            select(*cols_to_keep).\
            withColumn("upc_sell_unit_key", concat(col("upc"),lit("_"), col("sell_unit_qty"))).\
            withColumn('tempe_prod_item_env_key', sf.col('prod_item_env_key')).\
            withColumn('tempe_product_key', sf.col('product_key')).\
            withColumn('upc_new',sf.col('upc')).\
            withColumn('sell_unit_qty_new', sf.col('sell_unit_qty')).\
            join(product_table, ['tempe_prod_item_env_key'], 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC #### New upc fix/ time series stitch

# COMMAND ----------

# stitching time series of old and new upcs
final_ADS = final_ADS.join(new_upc_pk.select('upc','new_upc'), ['upc'], 'left')
final_ADS = final_ADS.withColumn("upc", when(col('new_upc').isNull(), col("upc")).otherwise(col("new_upc")))

final_ADS = final_ADS.join(new_upc_pk.select('product_key','new_product_key'), ['product_key'], 'left')
final_ADS = final_ADS.withColumn("product_key", when(col('new_product_key').isNull(), col("product_key")).otherwise(col("new_product_key")))

# COMMAND ----------

# writing BU ADS to dbfs
if len(environments_in_bu) == 1:
  final_ADS.write.mode('overwrite').saveAsTable('{0}.{1}_master_merged_ads_{2}'.format(db,bu_code, wave.lower()))
else:
  print("It is a Multiple Environment BU")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADS Creation for Multiple Environment BU 
# MAGIC * Creating Table where we are taking the latest tempe key for a UPC x sell_unit_qty on the basis of Rank Variable
# MAGIC * Rank variable is created by sorting both business_date and sales_amount in descending order for each UPC x sell_unit_qty

# COMMAND ----------

# Creation of RANK TABLE - TableA
# row 1 - Start with base table with filtered out transactions that were outliers
# row 2 - Create column upc_sell_unit_key which is concat of upc and sell_unit_qty
# row 3 - group by: sys_env, BU, prod_item_env_key, product_key, upc, sell_unit_qty, upc_sell_unit_key
# row 4-5 - aggreagte: max of business_date and sum of sales_amount
# row 6-7 - create column Rank - rank over BU, upc, sell_unit_qty - ordered by max_business_date descending and then sales_amount desc
# row 8 - filter to where the rank == 1 and sys_env is tempe
# row 9-10 - Renamed prod_item_env_key and product_key

TableA= outlier_treated.\
        withColumn("upc_sell_unit_key",concat(col("upc"),lit("_"), col("sell_unit_qty"))).\
        groupBy("sys_environment_name","BU","prod_item_env_key","product_key","upc","sell_unit_qty","upc_sell_unit_key").\
        agg(sf.max("business_date").alias("max_business_date"),\
            sf.sum("sales_amount").alias("sales_Amount")).\
        withColumn("rank",sf.rank().over(Window.partitionBy(["BU","upc","sell_unit_qty"]).
                                         orderBy(sf.col("max_business_date").desc(),sf.col("sales_amount").desc()))).\
        filter((col('rank') == '1') & (col('sys_environment_name')=='Tempe')).\
        withColumnRenamed('prod_item_env_key','tempe_prod_item_env_key').\
        withColumnRenamed('product_key','tempe_product_key')

# COMMAND ----------

# MAGIC %md
# MAGIC ####For every upc sell unit qty combination we should have a tempe product key by doing a left join with the Rank table i.e. TableA

# COMMAND ----------

# TableX -  Take above table, selecting BU, upc, sell_unit_qty, product_key, and tempe_prod_item_env_key

# row 1 - TableB - Start with post outlier dataframe
# row 2 - select columns specified above
# row 3-4 - create collumns upc_sell_unit_key and suq (sell_unit_qty)
# row 5-6 - join Table x dropping suq column


# In summary, take the above commands table that contains only the latest selling pk by date and highest sales amount
# Thenin second table, grab the same outlier treated dataframe, and join in the ideal pk dataframe on BU, upc, and suq


# Take above table selecting only BU, upc, suq, pk and pk identifier
TableX = TableA.select('BU','upc',sf.col('sell_unit_qty').alias("suq"),'tempe_product_key','tempe_prod_item_env_key')

# Join in new ranked df to post outlier treatment, to get a tempe_pk which is joined by UPC and SUQ not PK
TableB = outlier_treated.\
         select(*cols_to_keep).\
         withColumn("upc_sell_unit_key",concat(col("upc"),lit("_"), col("sell_unit_qty"))).\
         withColumn("suq", col("sell_unit_qty")).\
         join(TableX,['BU','upc','suq'],"left").\
         drop('suq')

# COMMAND ----------

# print((TableB.count(), len(TableB.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Handling the Problem Type 3 Cases i.e. wherever we have a upc sell unit quantity combination valid in old environment but it is selling with come other UPC sell unit combination in new environment
# MAGIC * Only top 90% cases in terms of sales taken and an external file is created from which we map the tempe product environment key

# COMMAND ----------

# Cmd 1-2 Load in above csv file
# Cmd 3-5 renamed columns - upc --> upc_new, division_desc --> BU, sell_unit_qty_new --> sell_unit_qty
# Cmd 6-7 Join in table A (rank table), on BU, upc, sell_unit_qty

# Renamed again, upc, sell_unit_qty, tempe_product_key, tempe_prod_item_env_key

Problem_type_3 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').\
                                 load('{}Problem_type_3.csv'.format(static_file_path)).\
                 withColumnRenamed('upc_new','upc').\
                 withColumnRenamed('division_desc','BU').\
                 withColumnRenamed('sell_unit_qty_new','sell_unit_qty').\
                 join(TableA.select('BU','upc','sell_unit_qty','tempe_product_key','tempe_prod_item_env_key'),
                      ['BU','upc','sell_unit_qty'],"left").\
                 withColumnRenamed('upc','upc_new').\
                 withColumnRenamed('sell_unit_qty','sell_unit_qty_new').\
                 withColumnRenamed('tempe_product_key','tempe_product_key_2').\
                 withColumnRenamed('tempe_prod_item_env_key','tempe_prod_item_env_key_2')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Again taging the Problem 3 cases with the new tempe key as calculated in the above step

# COMMAND ----------

# NEEDED TO RENAME THESE DATAFRAMES
# Join in Table B with problem_type_3 (has rank table joined to it)

TableB_p3 = TableB.join(Problem_type_3.withColumnRenamed('upc_old','upc').withColumnRenamed('sell_unit_qty_old','sell_unit_qty'),
                     ['BU','upc','sell_unit_qty'],"left")

# Take columns with correct pk based off of rank file and problem_type_3 file
# create columns: 
#   - final_tempe_prod_item_env_key --> takes tempe_prod_item_env_key_2 over tempe_prod_item_env_key
#   - Final_tempe_product_key --> takes tempe_product_key_2 over tempe_product_key
# drop unneeded duplicate columns (brought in through files and rank table)
# Rename final columns to name format we need to continue

TableB_corr_pk = TableB_p3.\
           withColumn('Final_tempe_prod_item_env_key', sf.when((col('tempe_prod_item_env_key_2').isNotNull()),\
                                                               (col('tempe_prod_item_env_key_2'))).otherwise(sf.col('tempe_prod_item_env_key'))).\
           withColumn('Final_tempe_product_key', sf.when((col('tempe_product_key_2').isNotNull()),\
                                                               (col('tempe_product_key_2'))).otherwise(sf.col('tempe_product_key'))).\
           withColumn('upc', sf.when((col('tempe_product_key_2').isNotNull()),\
                                               (col('upc_new'))).otherwise(sf.col('upc'))).\
           withColumn('sell_unit_qty', sf.when((col('tempe_product_key_2').isNotNull()),\
                                                               (col('sell_unit_qty_new'))).otherwise(sf.col('sell_unit_qty'))).\
           drop(('tempe_prod_item_env_key'),('tempe_prod_item_env_key_2'),('tempe_product_key'),('tempe_product_key_2'),('Problem_Type')).\
           withColumnRenamed('Final_tempe_prod_item_env_key','tempe_prod_item_env_key').\
           withColumnRenamed('Final_tempe_product_key','tempe_product_key')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Associating the product heirarchy columns to the UPC x sell_unit_qty based on the new product environment key

# COMMAND ----------

# Join backin the product table on the new choosen tempe_prod_item_env_key

TableB_final = TableB_corr_pk.join(product_table, ['tempe_prod_item_env_key'], 'left')

# COMMAND ----------

# print((TableB._final.count(), len(TableB_final.columns)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removing Problem Type 1 and Problem type 2 cases from the table

# COMMAND ----------

# Load in problem type 1 and 2 csv and alter division_desc to BU
Problem_type_1_2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').\
                            load('{}Problem_type_1_2.csv'.format(static_file_path)).\
                            withColumnRenamed("division_desc","BU")

display(Problem_type_1_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Table C below is the final ADS on which we can roll up on the tempe  product key

# COMMAND ----------

# Bring in the problem type 1 and 2 and filter down to records that matched

TableC = TableB_final.join(Problem_type_1_2,['BU','upc','sell_unit_qty','product_key'],"left").\
             filter(col('Problem_Type').isNull()).\
             drop(('Problem_Type'))

# COMMAND ----------

# display(TableC.select('upc').distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC #### New upc fix/ time series stitch

# COMMAND ----------

# stitching time series of old and new upcs
TableC = TableC.join(new_upc_pk.select('upc','new_upc'), ['upc'], 'left')
TableC = TableC.withColumn("upc", when(col('new_upc').isNull(), col("upc")).otherwise(col("new_upc")))

TableC = TableC.join(new_upc_pk.select('product_key','new_product_key'), ['product_key'], 'left')
TableC = TableC.withColumn("product_key", when(col('new_product_key').isNull(), col("product_key")).otherwise(col("new_product_key")))

# COMMAND ----------

# writing BU ADS to dbfs
if len(environments_in_bu) != 1:
  TableC.write.mode('overwrite').saveAsTable('{0}.{1}_master_merged_ads_{2}'.format(db,bu_code,wave.lower()))
else:
  print("It is a Single Environment BU")

# COMMAND ----------

# MAGIC %md
# MAGIC #### QC's

# COMMAND ----------

# Reading the saved ADS Table
bu_ADS = sqlContext.sql("""Select * FROM {0}.{1}_master_merged_ads_{2}""".format(db,bu_code,wave.lower()))
print(bu_ADS.count(),len(bu_ADS.columns))

# COMMAND ----------

# First 5 Rows of the Master ADS
display(bu_ADS.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comparing Sales Numbers of Initial Table and the Final ADS

# COMMAND ----------

# Total Sales Amount in the Initial Transactions Table
display(sqlContext.sql("""
                        Select * FROM na_all_bu_transaction_full_table_hierarchy_validation_{3}
                        where division_desc = '{0}'
                        and business_date >= '{1}' and business_date <= '{2}'
                        """.format(business_unit, start_date, end_date, wave.lower())).\
       groupBy("division_desc").\
       agg(sf.sum("sales_amount").alias("sales_all")))

# COMMAND ----------

# Total Sales Amount in Final ADS
display(bu_ADS.\
       groupBy("BU").\
       agg(sf.sum("sales_amount").alias("sales_ads")))

# COMMAND ----------

# Total Sales Amount of the Problem 1 and 2 Cases which are removed
if len(environments_in_bu) != 1:
  Problem_type_1_2.filter(sf.col("BU") == business_unit).createOrReplaceTempView("Problem_type_1_2")
  display(sqlContext.sql("""
                        Select * FROM na_all_bu_transaction_full_table_hierarchy_validation_{3} 
                        where division_desc = '{0}'
                          and business_date >= '{1}' 
                          and business_date <= '{2}'
                          and product_key in (select product_key from Problem_type_1_2)
                        """.format(business_unit, start_date, end_date, wave.lower())).\
          groupBy("division_desc").agg(sf.sum("sales_amount").alias("sales_problem_1_2")))
else:
  print("It is a Single Environment BU. There are no Problem Cases.")

# COMMAND ----------

# # Outlier Summary
# # A bucket defined by splits x,y holds values in the range [x,y) except the last bucket, which also includes y
# splits=[-float("inf"), 0, 20, 50, 100, 200, 500, 1000, 5000, 10000,float('Inf') ]
# splits_dict = {i:splits[i] for i in range(len(splits))}
# bucketizer = Bucketizer(splits = splits,inputCol="qty", outputCol="buckets_outlier_abs_qty")

# bucketed_check = bucketizer.setHandleInvalid('skip').transform(txn_data_outlier_raw)
# outlier_summary = bucketed_check.replace(to_replace=splits_dict, subset=['buckets_outlier_abs_qty'])
# outlier_summary = outlier_summary.groupBy(['BU','buckets_outlier_abs_qty']).\
#         agg(sf.sum("sales_amount").alias("revenue"),sf.count('site_number_corporate').alias("num_txns"),
#             sf.mean("tot_rev").alias("BU_tot_rev"),sf.mean("tot_txn").alias("BU_tot_txn")).\
#         withColumn("perc_rev", (col("revenue")/col("BU_tot_rev"))).\
#         withColumn("perc_txn", (col("num_txns")/col("BU_tot_txn"))).\
#         cache()

# # Total Sum of the Outlier Transactions which are removed
# display(outlier_summary.\
#         groupBy("BU").\
#         agg(sf.sum("revenue").alias("sales_outliers")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking One to One Mapping between upc_sell_unit_key, tempe_prod_item_env_key and tempe_product_key

# COMMAND ----------

display(sqlContext.sql("""
                        Select distinct upc_sell_unit_key, tempe_prod_item_env_key, tempe_product_key
                        FROM {0}.{1}_master_merged_ads_{2}
                          """.format(db,bu_code,wave.lower())).\
       groupBy("tempe_product_key").agg(sf.countDistinct("tempe_prod_item_env_key").alias("count_env_keys"),
                                        sf.countDistinct("upc_sell_unit_key").alias("count_sell_unit_keys")).\
       filter((sf.col("count_env_keys") > 1) | (sf.col("count_sell_unit_keys") > 1)))

# COMMAND ----------

display(sqlContext.sql("""
                          Select count(distinct upc_sell_unit_key) as Distinct_UPC_Sell_Unit_Keys, 
                                 count(distinct tempe_prod_item_env_key) as Distinct_Item_Env_Keys, 
                                 count(distinct tempe_product_key) as Distinct_Tempe_Prod_Keys,
                                 count(distinct product_key) as Distinct_product_key
                          FROM {0}.{1}_master_merged_ads_{2}
                          """.format(db,bu_code,wave.lower())))

# COMMAND ----------

display(sqlContext.sql("""Select count(distinct upc_sell_unit_key) as Distinct_UPC_Sell_Unit_Keys, 
                                 count(distinct tempe_prod_item_env_key) as Distinct_Item_Env_Keys, 
                                 count(distinct tempe_product_key) as Distinct_Tempe_Prod_Keys,
                                 count(distinct product_key) as Distinct_product_key
                          FROM {0}.{1}_master_merged_ads_{2}
                          WHERE tempe_product_key is not null""".format(db,bu_code,wave.lower())))

# COMMAND ----------

# Count of Unique upc_sell_unit_key for which Tempe Environment Exists but is not the Latest Environment
display(sqlContext.sql("""Select count(Distinct upc_sell_unit_key) as Distinct_Keys
                            from {0}.{1}_master_merged_ads_{2}
                            where sys_environment_name != 'Tempe'
                            and tempe_product_key is null""".format(db,bu_code,wave.lower())))

# COMMAND ----------

# Count of Unique upc_sell_unit_key for which Tempe Environment Exists but is not the Latest Environment
display(sqlContext.sql("""Select count(Distinct upc_sell_unit_key) as Distinct_Keys
                            from {0}.{1}_master_merged_ads_{2}
                            where sys_environment_name = 'Tempe'
                            and tempe_product_key is null""".format(db,bu_code,wave.lower())))

# COMMAND ----------

# Max Date for those UPC Sell Unit Keys where Tempe Environment does not exist or is not the Latest Environment
display(sqlContext.sql("""
                       select max(business_date) as Max_Business_Date 
                       from {0}.{1}_master_merged_ads_{2}
                       where tempe_product_key is null
                       """.format(db,bu_code,wave.lower())))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Outlier Summary

# COMMAND ----------

# print(outlier_summary.is_cached)

# COMMAND ----------

#display(outlier_summary)

# COMMAND ----------


