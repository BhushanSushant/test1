# Databricks notebook source
# MAGIC %md
# MAGIC # Read Libraries

# COMMAND ----------

import pandas as pd
import datetime
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from operator import add
from functools import reduce
from pyspark.sql.functions import when, sum, avg, col,concat,lit
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import greatest
from pyspark.sql.functions import least

#enabling delta caching to improve performance
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
spark.conf.set("spark.databricks.io.cache.enabled", "false")
sqlContext.clearCache()

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %r
# MAGIC library(tidyverse)

# COMMAND ----------

dbutils.library.installPyPI('pandas','1.1.5')
dbutils.library.installPyPI('xlrd','1.2.0')
#dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Inputs

# COMMAND ----------

na_DB_txn = dbutils.widgets.get("db")
BU = dbutils.widgets.get('BU')
business_units = dbutils.widgets.get('business_units')
wave = dbutils.widgets.get("wave")
bu_daily_txn_table = dbutils.widgets.get("bu_daily_txn_table")
start_week = dbutils.widgets.get('start_week')
end_week = dbutils.widgets.get('end_week')
weight_date = dbutils.widgets.get('weight_date')
phase = dbutils.widgets.get('phase')
bu_name_tc_map = dbutils.widgets.get('bu_name_tc_map')
#"phase3"

bu_repo = dbutils.widgets.get("bu_repo")
state = dbutils.widgets.get("state")
# bu_test_control = dbutils.widgets.get("bu_test_control")

#cluster_map = dbutils.widgets.get("cluster_map")
#zone_map = dbutils.widgets.get("zone_map")
#model_scope = dbutils.widgets.get("model_scope")
#test_control_map = dbutils.widgets.get("test_control_map")

# COMMAND ----------

# MAGIC %r
# MAGIC #### USER INPUTS #######
# MAGIC wave = dbutils.widgets.get("wave")
# MAGIC bu = dbutils.widgets.get("bu_repo")
# MAGIC end_week = dbutils.widgets.get('end_week')
# MAGIC weight_date = dbutils.widgets.get('weight_date')
# MAGIC # = dbutils.widgets.get('no_smoothen_after')
# MAGIC # no_smoothen_after = '2021-01-18' # TV:not being used for this refresh. Revaluate need in the next refresh
# MAGIC state = '' #'site_state_id'

# COMMAND ----------


# gl_map = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').\
#                           load('/Phase4_extensions/Elasticity_Modelling/Nov21_Refresh/gl_refresh_zoning_file.csv')
# qe_map = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').\
#                           load('/Phase4_extensions/Elasticity_Modelling/Nov21_Refresh/qe_refresh_zoning_file.csv')

# zone_map_df = qe_map.union(gl_map)

# zone_map_df.write.mode("overwrite").saveAsTable("circlek_db.phase4_gl_qe_beer_wine_zone")

# display(zone_map_df)

# COMMAND ----------

#/dbfs/Phase3_extensions/Elasticity_Modelling/JUNE2021_Refresh/CE_Repo/Modelling_Inputs/ce_zones_updated_final.csv
#Phase3_extensions/Elasticity_Modelling/{}/{}/Modelling_Inputs/{}_zones_updated_final.csv'.format(wave,bu_repo,BU))\
# zone_map = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true')\
# .load('/Phase3_extensions/Elasticity_Modelling/JUNE2021_Refresh/CE_Repo/Modelling_Inputs/ce_zones_updated_final.csv')\
# .select('BU',\
#         'category_desc',\
#         'tempe_product_key',\
#         'trn_site_sys_id',\
#         'price_zones')\
#.withColumnRenamed('zone_final','price_zones')

zone_map = spark.sql(""" SELECT BU, tempe_product_key, trn_site_sys_id, price_zones FROM circlek_db.phase3_beer_wine_zone WHERE BU = '{}' """.format(business_units))


#get the test_control stores mapping file
## TV: this will have to be manually created and uploaded in the input folder while doing an actual refresh
# cluster_map = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true')\
# .load('dbfs:/Phase3_extensions/Elasticity_Modelling/JUNE2021_Refresh/test_control_map_updated_04_22_2021.csv'.format(wave))\
# .filter(col('BU')=="Central Canada")\
# .withColumnRenamed('Site','trn_site_sys_id')\
# .withColumnRenamed('BU','BU_old')\
# .withColumn('BU',sf.lit(business_units))\
# .select('BU','trn_site_sys_id','Cluster')\
cluster_map = spark.sql("""Select business_unit, site_id as trn_site_sys_id, cluster as Cluster from circlek_db.grouped_store_list
Where business_unit = '{}'
And group = 'Test'
And exclude_from_lp_price = 'N'
And  test_launch_date <= '2020-11-09' """.format(bu_name_tc_map))
cluster_map = cluster_map.withColumn("BU",lit(business_units))
# and group = 'Test'


#if BU == "Central Division":
#  cluster_map.withColumn(col("trn_site_sys_id"), col("trn_site_sys_id") - 3000000)
#display(cluster_map)

#test_control_map to keep only test stores
#review if this is accurate (test control mapping to be updated for each refresh)
# test_control_map = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true')\
# .load('dbfs:/Phase3_extensions/Elasticity_Modelling/JUNE2021_Refresh/test_control_map_updated_04_22_2021.csv'.format(wave))\
# .filter(sf.col('BU')=="Central Canada")\
# .withColumnRenamed('Site','site_number_corporate')\
# .withColumnRenamed('BU','BU_old')\
# .withColumnRenamed('Type','test_control_map')\
# .withColumn('BU',sf.lit(business_units))\
# .select('BU','site_number_corporate','test_control_map')
test_control_map = spark.sql("""Select business_unit, site_id as site_number_corporate, group as test_control_map from circlek_db.grouped_store_list
Where business_unit = '{}'
And exclude_from_lp_price = 'N'
AND  test_launch_date <= '2020-11-09' """.format(bu_name_tc_map))
test_control_map = test_control_map.withColumn("BU",lit(business_units))
#if BU == "Central Division":
#  test_control_map.withColumn(col("trn_site_sys_id"), col("trn_site_sys_id") - 3000000)

#display(test_control_map)

#to keep item x clusters based on model scope
#TV: assuming all clusters are inscope for an item. If we are modelling an item for only selected clusters then we must have cluster info in this table and the join should adjust to include the cluster aswell.
#model_scope = pd.read_csv('/dbfs/Phase3_extensions/Elasticity_Modelling/JUNE2021_Refresh/SE_Repo/Input/SE_Scope_List_Wave3_final_v17.csv')
#model_scope = spark.createDataFrame(model_scope)
#model_scope = model_scope.withColumn('BU',lit(business_units)).select('BU','tempe_product_key','Scope').filter(col('Scope')=='Y').distinct().withColumnRenamed('Scope','model_scope').distinct()

model_scope = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
model_scope = model_scope.filter(col("BU") == business_units).filter(col('Scope') == 'Y')
model_scope = model_scope.groupBy("BU",'product_key', 'Scope').agg(sf.max('last_update'))
model_scope = model_scope.drop('max(last_update)').withColumnRenamed("product_key","tempe_product_key").withColumnRenamed("Scope","model_scope")

#display(model_scope)
display(test_control_map)

# COMMAND ----------

#if BU == "ce":
 # sqlContext.sql("""select Other_date,
 # trn_transaction_date,
 # BU,
 # tempe_product_key,
 # tempe_prod_item_env_key,
 # product_key,
 # trn_item_sys_id,
 # item_name,
 # nacs_category_desc,
 # department_desc,
 # category_desc,
 # sub_category_desc,
 # manufacturer_desc,
 # brand_desc,
 # sell_unit_qty,
 # package_desc,
 # private_brand_desc,
 # size_unit_of_measure,
 # UPC_Sell_Unit_Desc,
 # cast(trn_site_sys_id as int) - 3000000 as trn_site_sys_id,
 # site_state_id,
 # site_zip,
 # gps_latitude,
 # gps_longitude,
 # region_id,
 # region_desc,
 # month_year_name,
 # trn_sold_quantity,
 # trn_gross_amount,
 # avg_trn_item_unit_price,
 # trn_total_discount,
 # discount_perc,
 # trn_net_price
 # from {} """.format(bu_daily_txn_table)).write.mode('overwrite').saveAsTable("temp1_testing") 
 # sqlContext.sql("""select * from default.temp1_testing""").write.mode('overwrite').saveAsTable("phase3_extensions.ce_daily_txn_final_sweden_nomenclature_testing")

# COMMAND ----------

# MAGIC %md
# MAGIC # TBD

# COMMAND ----------

#TV: Ideally we should filter out categories like beer,cig, OTP & wine because we create different ADS's for them. However this is done in the 4th notebook which collates the ADS's
grouping_cols=[
              'product_key',
              'trn_site_sys_id','Cluster',
              'week_start_date']

if business_units in ['4200 - Great Lakes Division']:
  issue_categories = ['004-BEER','002-CIGARETTES','CIGARETTES-C',
                          'Discount/vfm 20s (6002)','Subgener/budget 20s (6003)','Premium Cigarettes 20s (6001)','Cigarettes (60)','085-SOFT DRINKS','BOISSONS GAZEUSES-C','003-OTHER TOBACCO PRODUCTS',
                          'BIERE-C','005-WINE', '006-LIQUOR','Tobacco (65)']
elif business_units =='Western Division':
  issue_categories = []#'Cigarettes (60)','Tobacco (65)'
elif business_units =='QUEBEC EST - ATLANTIQUE':
  issue_categories = ['004-BEER','002-CIGARETTES','CIGARETTES-C','BIERE-C',
                          'Discount/vfm 20s (6002)','Subgener/budget 20s (6003)','Premium Cigarettes 20s (6001)','Cigarettes (60)','085-SOFT DRINKS','BOISSONS GAZEUSES-C','AUTRES PRODUITS DU TABAC','VIN']
else:
  issue_categories = ['004-BEER','002-CIGARETTES','CIGARETTES-C',
                    'Discount/vfm 20s (6002)','Subgener/budget 20s (6003)','Premium Cigarettes 20s (6001)','Cigarettes (60)']

# COMMAND ----------

if state == '':
      data_rolled_up = sqlContext.sql("""select * from {} """.format(bu_daily_txn_table)).\
                                filter(~sf.col('category_desc').isin(issue_categories)).\
                                withColumn('avg_trn_item_unit_price', sf.when(col('avg_trn_item_unit_price').isNull(),0).otherwise(col('avg_trn_item_unit_price'))).\
                                drop("product_key").\
                                join(cluster_map,['BU','trn_site_sys_id'], "left").\
                                join(test_control_map.withColumnRenamed('site_number_corporate','trn_site_sys_id'),['BU','trn_site_sys_id'], "left").\
                                filter(col('test_control_map') == "Test").\
                                drop('test_control_map').\
                                withColumnRenamed("tempe_product_key","product_key").\
                                filter(col("avg_trn_item_unit_price") > 0.1).\
                                withColumn("week_no",sf.dayofweek("trn_transaction_date")).\
                                withColumn("adj_factor", sf.col("week_no") - 4).\
                                withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
                                withColumn("week_start_date", sf.expr("date_sub(trn_transaction_date,adj_factor)")).\
                                filter((sf.col("week_start_date") <= end_week) & (sf.col("week_start_date") >= start_week)).\
                                groupBy(*grouping_cols).\
                                agg(sf.sum("trn_sold_quantity").alias("trn_sold_quantity"),
                                    sf.sum("trn_gross_amount").alias("trn_gross_amount"),
                                    sf.mean("avg_trn_item_unit_price").alias("reg_price")).\
                                withColumn('reg_price',sf.round('reg_price',2)).\
                                 join(model_scope.drop(*['BU']).withColumnRenamed("tempe_product_key","product_key").distinct(),['product_key'],'left').\
                                 filter(col('model_scope').isNotNull()).\
                                 drop('model_scope')
else:
      data_rolled_up = sqlContext.sql("""select * from {} """.format(bu_daily_txn_table)).\
                                filter(~sf.col('category_desc').isin(issue_categories)).\
                                withColumn('avg_trn_item_unit_price', sf.when(col('avg_trn_item_unit_price').isNull(),0).otherwise(col('avg_trn_item_unit_price'))).\
                                drop("product_key").\
                                join(cluster_map,['BU','trn_site_sys_id'], "left").\
                                join(test_control_map.withColumnRenamed('site_number_corporate','trn_site_sys_id'),['BU','trn_site_sys_id'], "left").\
                                filter(col('test_control_map') == "Test").\
                                drop('test_control_map').\
                                withColumn("Cluster",sf.concat(col("Cluster"),sf.lit("_"),col("site_state_id"))).\
                                withColumnRenamed("tempe_product_key","product_key").\
                                filter(col("avg_trn_item_unit_price") > 0.1).\
                                withColumn("week_no",sf.dayofweek("trn_transaction_date")).\
                                withColumn("adj_factor", sf.col("week_no") - 4).\
                                withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
                                withColumn("week_start_date", sf.expr("date_sub(trn_transaction_date,adj_factor)")).\
                                filter((sf.col("week_start_date") <= end_week) & (sf.col("week_start_date") >= start_week)).\
                                groupBy(*grouping_cols).\
                                agg(sf.sum("trn_sold_quantity").alias("trn_sold_quantity"),
                                    sf.sum("trn_gross_amount").alias("trn_gross_amount"),
                                    sf.mean("avg_trn_item_unit_price").alias("reg_price")).\
                                withColumn('reg_price',sf.round('reg_price',2)).\
                                 join(model_scope.drop(*['BU']).withColumnRenamed("tempe_product_key","product_key").distinct(),['product_key'],'left').\
                                 filter(col('model_scope').isNotNull()).\
                                 drop('model_scope')
    


if len(issue_categories)!=0:
  if (phase == 'phase3'):
    if business_units in ['Western Division','Central Division']:
      data_rolled_up_zoned = sqlContext.sql("""select * from {} """.format(bu_daily_txn_table)).\
      withColumn('category_desc',col('department_desc')).\
          filter(sf.col('category_desc').isin(issue_categories)).\
          withColumn('avg_trn_item_unit_price', sf.when(col('avg_trn_item_unit_price').isNull(),0).otherwise(col('avg_trn_item_unit_price'))).\
          drop("product_key").\
          join(cluster_map,['BU','trn_site_sys_id'], "left").\
          join(zone_map.distinct(), ['BU','tempe_product_key','trn_site_sys_id'], "left").\
          filter(col("price_zones").isNotNull()).\
          withColumnRenamed("tempe_product_key","product_key").\
          withColumn("price_zones", when((col('BU')=="4200 - Great Lakes Division") & (col('category_desc')=='003-OTHER TOBACCO PRODUCTS') &\
                                         (col('price_zones').like('%OH%')),"OH").otherwise(col('price_zones'))).\
          withColumn("Cluster", sf.concat(col("Cluster"),sf.lit("_"),col("price_zones"))).\
          join(test_control_map.withColumnRenamed('site_number_corporate','trn_site_sys_id'),['BU','trn_site_sys_id'], "left").\
          filter(col('test_control_map') == "Test").\
          drop('test_control_map').\
          drop("price_zones").\
          filter(col("avg_trn_item_unit_price") > 0.1).\
          withColumn("week_no",sf.dayofweek("trn_transaction_date")).\
          withColumn("adj_factor", sf.col("week_no") - 4).\
          withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
          withColumn("week_start_date", sf.expr("date_sub(trn_transaction_date,adj_factor)")).\
          filter((sf.col("week_start_date") <= end_week) & (sf.col("week_start_date") >= start_week)).\
          groupBy(*grouping_cols).\
          agg(sf.sum("trn_sold_quantity").alias("trn_sold_quantity"),
              sf.sum("trn_gross_amount").alias("trn_gross_amount"),
              sf.mean("avg_trn_item_unit_price").alias("reg_price")).\
          withColumn('reg_price',sf.round('reg_price',2)).\
          join(model_scope.drop(*['BU']).withColumnRenamed("tempe_product_key","product_key").distinct(),['product_key'],'left').\
          filter(col('model_scope').isNotNull()).\
          drop('model_scope')

    else:
      data_rolled_up_zoned = sqlContext.sql("""select * from {} """.format(bu_daily_txn_table)).\
          filter(sf.col('category_desc').isin(issue_categories)).\
          withColumn('avg_trn_item_unit_price', sf.when(col('avg_trn_item_unit_price').isNull(),0).otherwise(col('avg_trn_item_unit_price'))).\
          drop("product_key").\
          join(cluster_map,['BU','trn_site_sys_id'], "left").\
          join(zone_map.distinct(), ['BU','tempe_product_key','trn_site_sys_id'], "left").\
          filter(col("price_zones").isNotNull()).\
          withColumnRenamed("tempe_product_key","product_key").\
          withColumn("price_zones", when((col('BU')=="4200 - Great Lakes Division") & (col('category_desc')=='003-OTHER TOBACCO PRODUCTS') &\
                                         (col('price_zones').like('%OH%')),"OH").otherwise(col('price_zones'))).\
          withColumn("Cluster", sf.concat(col("Cluster"),sf.lit("_"),col("price_zones"))).\
          join(test_control_map.withColumnRenamed('site_number_corporate','trn_site_sys_id'),['BU','trn_site_sys_id'], "left").\
          filter(col('test_control_map') == "Test").\
          drop('test_control_map').\
          drop("price_zones").\
          filter(col("avg_trn_item_unit_price") > 0.1).\
          withColumn("week_no",sf.dayofweek("trn_transaction_date")).\
          withColumn("adj_factor", sf.col("week_no") - 4).\
          withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
          withColumn("week_start_date", sf.expr("date_sub(trn_transaction_date,adj_factor)")).\
          filter((sf.col("week_start_date") <= end_week) & (sf.col("week_start_date") >= start_week)).\
          groupBy(*grouping_cols).\
          agg(sf.sum("trn_sold_quantity").alias("trn_sold_quantity"),
              sf.sum("trn_gross_amount").alias("trn_gross_amount"),
              sf.mean("avg_trn_item_unit_price").alias("reg_price")).\
          withColumn('reg_price',sf.round('reg_price',2)).\
          join(model_scope.drop(*['BU']).withColumnRenamed("tempe_product_key","product_key").distinct(),['product_key'],'left').\
          filter(col('model_scope').isNotNull()).\
          drop('model_scope')

  elif (phase == 'phase4'):
    if business_units in ['Western Division','Central Division']:
      data_rolled_up_zoned = sqlContext.sql("""select * from {} """.format(bu_daily_txn_table)).\
      withColumn('category_desc',col('department_desc')).\
          filter(sf.col('category_desc').isin(issue_categories)).\
          withColumn('avg_trn_item_unit_price', sf.when(col('avg_trn_item_unit_price').isNull(),0).otherwise(col('avg_trn_item_unit_price'))).\
          drop("product_key").\
          join(cluster_map,['BU','trn_site_sys_id'], "left").\
          join(zone_map.drop(*['site_state_id','Store_group']).distinct(), ['BU','trn_site_sys_id'], "left").\
          filter(col("price_zones").isNotNull()).\
          withColumnRenamed("tempe_product_key","product_key").\
          withColumn("price_zones", when((col('BU')=="4200 - Great Lakes Division") & (col('category_desc')=='003-OTHER TOBACCO PRODUCTS') &\
                                         (col('price_zones').like('%OH%')),"OH").otherwise(col('price_zones'))).\
          withColumn("Cluster", sf.concat(col("Cluster"),sf.lit("_"),col("price_zones"))).\
          join(test_control_map.withColumnRenamed('site_number_corporate','trn_site_sys_id'),['BU','trn_site_sys_id'], "left").\
          filter(col('test_control_map') == "Test").\
          drop('test_control_map').\
          drop("price_zones").\
          filter(col("avg_trn_item_unit_price") > 0.1).\
          withColumn("week_no",sf.dayofweek("trn_transaction_date")).\
          withColumn("adj_factor", sf.col("week_no") - 4).\
          withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
          withColumn("week_start_date", sf.expr("date_sub(trn_transaction_date,adj_factor)")).\
          filter((sf.col("week_start_date") <= end_week) & (sf.col("week_start_date") >= start_week)).\
          groupBy(*grouping_cols).\
          agg(sf.sum("trn_sold_quantity").alias("trn_sold_quantity"),
              sf.sum("trn_gross_amount").alias("trn_gross_amount"),
              sf.mean("avg_trn_item_unit_price").alias("reg_price")).\
          withColumn('reg_price',sf.round('reg_price',2)).\
          join(model_scope.drop(*['BU']).withColumnRenamed("tempe_product_key","product_key").distinct(),['product_key'],'left').\
          filter(col('model_scope').isNotNull()).\
          drop('model_scope')

    else:
      data_rolled_up_zoned = sqlContext.sql("""select * from {} """.format(bu_daily_txn_table)).\
          filter(sf.col('category_desc').isin(issue_categories)).\
          withColumn('avg_trn_item_unit_price', sf.when(col('avg_trn_item_unit_price').isNull(),0).otherwise(col('avg_trn_item_unit_price'))).\
          drop("product_key").\
          join(cluster_map,['BU','trn_site_sys_id'], "left").\
          join(zone_map.drop(*['site_state_id','Store_group']).distinct(), ['BU','trn_site_sys_id'], "left").\
          filter(col("price_zones").isNotNull()).\
          withColumnRenamed("tempe_product_key","product_key").\
          withColumn("price_zones", when((col('BU')=="4200 - Great Lakes Division") & (col('category_desc')=='003-OTHER TOBACCO PRODUCTS') &\
                                         (col('price_zones').like('%OH%')),"OH").otherwise(col('price_zones'))).\
          withColumn("Cluster", sf.concat(col("Cluster"),sf.lit("_"),col("price_zones"))).\
          join(test_control_map.withColumnRenamed('site_number_corporate','trn_site_sys_id'),['BU','trn_site_sys_id'], "left").\
          filter(col('test_control_map') == "Test").\
          drop('test_control_map').\
          drop("price_zones").\
          filter(col("avg_trn_item_unit_price") > 0.1).\
          withColumn("week_no",sf.dayofweek("trn_transaction_date")).\
          withColumn("adj_factor", sf.col("week_no") - 4).\
          withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
          withColumn("week_start_date", sf.expr("date_sub(trn_transaction_date,adj_factor)")).\
          filter((sf.col("week_start_date") <= end_week) & (sf.col("week_start_date") >= start_week)).\
          groupBy(*grouping_cols).\
          agg(sf.sum("trn_sold_quantity").alias("trn_sold_quantity"),
              sf.sum("trn_gross_amount").alias("trn_gross_amount"),
              sf.mean("avg_trn_item_unit_price").alias("reg_price")).\
          withColumn('reg_price',sf.round('reg_price',2)).\
          join(model_scope.drop(*['BU']).withColumnRenamed("tempe_product_key","product_key").distinct(),['product_key'],'left').\
          filter(col('model_scope').isNotNull()).\
          drop('model_scope')
  
if len(issue_categories)!=0:
  data_rolled_up_weekly = data_rolled_up.union(data_rolled_up_zoned)
else:
  data_rolled_up_weekly = data_rolled_up

display(data_rolled_up_weekly)

# COMMAND ----------

## incase you lose sites, uncomment this code to identify which ones
x= test_control_map.filter(col('test_control_map')=='Test' ).filter(col('BU')== bu_name_tc_map)\
.select('site_number_corporate')\
.distinct()\
.withColumnRenamed('site_number_corporate','trn_site_sys_id')

#if BU == "Central Division":
#  x.withColumn(col("trn_site_sys_id"), col("trn_site_sys_id") - 3000000)
  
y=data_rolled_up_weekly[['trn_site_sys_id']]\
.distinct()\
.withColumn('check',sf.lit('ok'))

z=x.join(y,['trn_site_sys_id'],'left')

display(z.filter(col('check').isNull()))

## RM: these 3 sites did not sell beer, they only contained zones for cigs and OTP

# COMMAND ----------

data_rolled_up_weekly.createOrReplaceTempView("data_rolled_up_weekly_r")

# COMMAND ----------

# MAGIC %r
# MAGIC weekly_item_store_data <- as.data.frame(SparkR::collect(SparkR::sql("select * from data_rolled_up_weekly_r")))

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC wide_prices = weekly_item_store_data %>%
# MAGIC     select(product_key, trn_site_sys_id, Cluster, week_start_date, reg_price) %>%
# MAGIC     arrange(product_key, trn_site_sys_id, Cluster, week_start_date) %>%
# MAGIC     spread(week_start_date, reg_price)
# MAGIC 
# MAGIC # Impute Missing Values
# MAGIC for(k in 1:nrow(wide_prices)){
# MAGIC   for(l in 5:ncol(wide_prices)){
# MAGIC     wide_prices[k,l] = ifelse(is.na(wide_prices[k,l]),wide_prices[k,l-1],wide_prices[k,l])
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC no_col = ncol(wide_prices)+1
# MAGIC 
# MAGIC for(k in 1:nrow(wide_prices)){
# MAGIC   for(l in 1:ncol(wide_prices)){
# MAGIC     wide_prices[k,no_col-l] = ifelse(is.na(wide_prices[k,no_col-l]),wide_prices[k,no_col-l+1],wide_prices[k,no_col-l])
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC long_prices <- wide_prices %>%
# MAGIC   gather("week_start_date", "reg_price", c(-product_key, -trn_site_sys_id, -Cluster)) %>%
# MAGIC   mutate(week_start_date = as.Date(week_start_date))
# MAGIC 
# MAGIC item_store_clust_weights <- weekly_item_store_data %>%
# MAGIC   filter(week_start_date >= weight_date) %>%               # Weights to be taken on the basis of last One Year
# MAGIC   group_by(product_key, Cluster, trn_site_sys_id) %>%
# MAGIC   summarise(item_clust_store_rev = sum(trn_gross_amount, na.rm = T)) %>%
# MAGIC   ungroup() %>%
# MAGIC   group_by(product_key, Cluster) %>%
# MAGIC   mutate(item_clust_rev = sum(item_clust_store_rev, na.rm = T)) %>%
# MAGIC   ungroup() %>%
# MAGIC   mutate(weights = item_clust_store_rev/item_clust_rev) %>%
# MAGIC   select(-item_clust_store_rev,-item_clust_rev)
# MAGIC 
# MAGIC clust_level_price <- long_prices %>%
# MAGIC   left_join(weekly_item_store_data %>% select(-reg_price), c("product_key", "trn_site_sys_id", "Cluster", "week_start_date")) %>%
# MAGIC   left_join(item_store_clust_weights, c("product_key", "Cluster", "trn_site_sys_id")) %>%
# MAGIC   filter(!is.na(weights)) %>%
# MAGIC   mutate(price_x_weights = reg_price*weights) %>%
# MAGIC   group_by(product_key, Cluster, week_start_date) %>%
# MAGIC   summarise(trn_sold_quantity = sum(trn_sold_quantity, na.rm = T), 
# MAGIC             trn_gross_amount = sum(trn_gross_amount, na.rm = T), 
# MAGIC             reg_price = round(sum(price_x_weights, na.rm = T),2)) %>%
# MAGIC   ungroup()

# COMMAND ----------

# MAGIC %r
# MAGIC #before imputation
# MAGIC wide_prices_initial = weekly_item_store_data %>%
# MAGIC     select(product_key, trn_site_sys_id, Cluster, week_start_date, reg_price) %>%
# MAGIC     arrange(product_key, trn_site_sys_id, Cluster, week_start_date) %>%
# MAGIC     spread(week_start_date, reg_price)
# MAGIC 
# MAGIC 
# MAGIC #display(wide_prices_initial)

# COMMAND ----------

# %r

########## UNCOMMENT TO IDENTIFY WHICH ITEM-CLUSTERS HAVE BEEN DROPPED ##############

# x= clust_level_price %>% dplyr::select(product_key,Cluster)%>%unique() %>% dplyr::mutate(check=1)
# x1= weekly_item_store_data %>% dplyr::select(product_key,Cluster)%>%unique()
# x1= weekly_item_store_data %>% dplyr::group_by(product_key,Cluster)%>%summarize(max_wk_start_dt=max(week_start_date))
# x2=x1 %>% left_join(x)
# x2$check = ifelse(is.na(x2$check), "item-cluster dropped", x2$check)
# x3=x2 %>% dplyr::filter(check=="item-cluster dropped")
# display(x3)
# ## legnth of the resultin data frameshould be equal to the no of item-clusters lost # the reason for this will need to be investigated(could be disontinued products)

# COMMAND ----------

# MAGIC %r
# MAGIC ##code to remove weeks that have no sales from the start of our time period.
# MAGIC df_cumsum<- clust_level_price %>% arrange(product_key,Cluster,week_start_date) %>%group_by(product_key,Cluster)%>% dplyr::mutate(trn_sold_quantity2=abs(trn_sold_quantity),cs=cumsum(trn_sold_quantity2)) %>%ungroup()
# MAGIC clust_level_price_final <- df_cumsum %>% dplyr::filter(cs!=0)  
# MAGIC clust_level_price_no_sales_from_start <- df_cumsum %>% dplyr::filter(cs==0)
# MAGIC #display(clust_level_price_final)

# COMMAND ----------

# MAGIC %r
# MAGIC suppressPackageStartupMessages(library(tidyverse))
# MAGIC source_data_main <- clust_level_price_final%>% dplyr::select(product_key,Cluster,week_start_date,trn_sold_quantity,trn_gross_amount,reg_price) %>%dplyr::rename(WEEK_SDATE = week_start_date,
# MAGIC                                                  REG_PRICE = reg_price)%>%
# MAGIC                                          dplyr::mutate(func_key = paste0(product_key,"_",Cluster))%>%
# MAGIC                                          dplyr::filter(!is.na(REG_PRICE))%>%
# MAGIC                                          dplyr::filter(!is.na(Cluster))
# MAGIC 
# MAGIC if(nrow(clust_level_price_final%>%dplyr::filter(!is.na(Cluster))) == nrow(source_data_main)){
# MAGIC   print('All Good!')
# MAGIC }else{
# MAGIC    print(paste0('Originally NAs in the price - cause of full promotion history or no sale: ',nrow(clust_level_price%>%dplyr::filter(!is.na(Cluster)))-nrow(source_data_main), ' item x store x weeks'))
# MAGIC }

# COMMAND ----------

# MAGIC %r
# MAGIC ######### Updated Regular Price Fix core FUnc ##########
# MAGIC price_fix_function <- function(to_fix_regime, stable_regimes, price_regimes, fix_type = 1){
# MAGIC         stable_future <- min(stable_regimes$PRICE_REGIME[which(stable_regimes$PRICE_REGIME > to_fix_regime)])
# MAGIC 
# MAGIC         if(is.infinite(stable_future)){
# MAGIC           stable_future = max(price_regimes$PRICE_REGIME)
# MAGIC         }
# MAGIC         stable_past <- max(stable_regimes$PRICE_REGIME[which(stable_regimes$PRICE_REGIME < to_fix_regime)])
# MAGIC 
# MAGIC         if(is.infinite(stable_past)){
# MAGIC           stable_past = min(price_regimes$PRICE_REGIME)
# MAGIC         }
# MAGIC 
# MAGIC        if(fix_type == 1){   #based on price variaton 1st
# MAGIC             stable_future_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_future),"REG_PRICE"]
# MAGIC             stable_past_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_past),"REG_PRICE"]
# MAGIC             issue_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == to_fix_regime),"REG_PRICE"]
# MAGIC             #boundary_condition_fix
# MAGIC             if((to_fix_regime == min(price_regimes$PRICE_REGIME)) | (to_fix_regime == max(price_regimes$PRICE_REGIME))){
# MAGIC               fixed_price <- stable_future_price
# MAGIC             }else{
# MAGIC               if(abs(issue_price - stable_future_price) <= abs(issue_price - stable_past_price)){
# MAGIC                 fixed_price <- stable_future_price
# MAGIC               }else{
# MAGIC                 fixed_price <- stable_past_price
# MAGIC               }
# MAGIC             }
# MAGIC        }else{ #if that has failed try distance from closest price regime for type other than 1
# MAGIC           
# MAGIC           if(abs(to_fix_regime - stable_future) <= abs(to_fix_regime - stable_past)){
# MAGIC               fixed_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_future),"REG_PRICE"]$REG_PRICE
# MAGIC             }else{
# MAGIC               fixed_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_past),"REG_PRICE"]$REG_PRICE
# MAGIC             }
# MAGIC          
# MAGIC          #boundary condition fix
# MAGIC           
# MAGIC           if((to_fix_regime == max(price_regimes$PRICE_REGIME)) & (stable_past == to_fix_regime-1) & (price_regimes[which(price_regimes["PRICE_REGIME"] == max(price_regimes["PRICE_REGIME"])),]$WEEKS_ON_PRICE < 3)){
# MAGIC             fixed_price <- stable_regimes[which(stable_regimes["PRICE_REGIME"] == stable_past),"REG_PRICE"]$REG_PRICE
# MAGIC           }
# MAGIC           
# MAGIC           if(to_fix_regime == min(price_regimes$PRICE_REGIME) & stable_future == to_fix_regime+1){
# MAGIC             fixed_price <- stable_regimes[which(stable_regimes["PRICE_REGIME"] == stable_future),"REG_PRICE"]$REG_PRICE
# MAGIC           }
# MAGIC           
# MAGIC        }
# MAGIC         return(data.frame(PRICE_REGIME = to_fix_regime,
# MAGIC                           FIXED_PRICE = fixed_price))
# MAGIC }

# COMMAND ----------

# MAGIC %r
# MAGIC ####### overall cleaning func ########
# MAGIC cleanup_price <- function(key){
# MAGIC #   log_smooth = paste0("/dbfs/","Phase_3/Data_Validation_All_BU/regP_Smooth_log.txt")
# MAGIC #   cat(paste0(key,"_processing_start\n"),file=paste0(log_smooth), append=TRUE)
# MAGIC   source_data <- source_data_main %>% dplyr::filter(func_key == key)%>%dplyr::arrange(product_key,Cluster,WEEK_SDATE)%>%
# MAGIC                              dplyr::mutate(REG_PRICE= round(REG_PRICE,2)) %>%
# MAGIC                              dplyr::mutate(LAG_PRICE = dplyr::lag(REG_PRICE))%>%
# MAGIC                              dplyr::mutate(CHANGE_IND = ifelse((REG_PRICE == LAG_PRICE) | (is.na(LAG_PRICE)),0,1))%>%
# MAGIC                              dplyr::mutate(PRICE_REGIME = cumsum(CHANGE_IND))%>%
# MAGIC                              dplyr::group_by(PRICE_REGIME)%>%
# MAGIC                              dplyr::mutate(WEEKS_ON_PRICE = dplyr::n())%>%ungroup()
# MAGIC       source_data <- as.data.frame(source_data)
# MAGIC 
# MAGIC 
# MAGIC       #only keep unique price regimes
# MAGIC       price_regimes <- source_data%>%dplyr::select(REG_PRICE, PRICE_REGIME, WEEKS_ON_PRICE)%>%unique()%>%dplyr::arrange(PRICE_REGIME)
# MAGIC       stable_regimes <- price_regimes%>%dplyr::filter(WEEKS_ON_PRICE > 3)
# MAGIC       to_fix_regimes <- sort(unique((price_regimes%>%dplyr::filter(WEEKS_ON_PRICE <= 3))$PRICE_REGIME))
# MAGIC       to_fix_regimes_padded <- c(to_fix_regimes,rep(0,length(price_regimes$PRICE_REGIME)-length(to_fix_regimes)))
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC       #remove weeks if initial turbulence
# MAGIC       if(length(to_fix_regimes) >=1){
# MAGIC                   conti_issue_till <- max(which(to_fix_regimes_padded - (price_regimes$PRICE_REGIME) == 0))
# MAGIC                   if(is.infinite(conti_issue_till) == FALSE ){
# MAGIC                       remove_issue_weeks <- sum((price_regimes%>%head(conti_issue_till))$WEEKS_ON_PRICE)
# MAGIC                     }else{
# MAGIC                     remove_issue_weeks <- 0
# MAGIC                   }
# MAGIC               }else{
# MAGIC               remove_issue_weeks <- 0
# MAGIC              }
# MAGIC 
# MAGIC       if(nrow(stable_regimes) == 0){
# MAGIC                source_data <- source_data %>%
# MAGIC                                dplyr::mutate(FIXED_PRICE = REG_PRICE)
# MAGIC             }else{  
# MAGIC 
# MAGIC                   ## add in flag if the latest price is not stable
# MAGIC                   if((max(price_regimes$PRICE_REGIME) != max(stable_regimes$PRICE_REGIME))){
# MAGIC                       source_data$REGULAR_PRICE_FLAG <- "Issue"
# MAGIC                       source_data$ISSUE_COMMENT <- "Latest Price not stable"
# MAGIC                   }
# MAGIC 
# MAGIC                   if(length(to_fix_regimes) >=1){
# MAGIC                     fixed_prices_dat <- bind_rows(lapply(to_fix_regimes, function(x) {
# MAGIC             #           print(x)
# MAGIC                       price_fix_function(x,stable_regimes, price_regimes,1)
# MAGIC                     }))
# MAGIC                     source_data <- source_data %>%dplyr::left_join(fixed_prices_dat)%>%
# MAGIC                                    dplyr::mutate(FIXED_PRICE = ifelse(is.na(FIXED_PRICE), REG_PRICE, FIXED_PRICE))
# MAGIC                   }else{
# MAGIC                     source_data <- source_data %>%
# MAGIC                                      dplyr::mutate(FIXED_PRICE = REG_PRICE)
# MAGIC                    }
# MAGIC 
# MAGIC                   #check if things have been fixed or not - if not then try the distance method
# MAGIC 
# MAGIC                   #check again based on fixed price if all anomalies have not been fixed
# MAGIC                    check3 <- source_data %>%dplyr::mutate(LAG_PRICE = dplyr::lag(FIXED_PRICE))%>%
# MAGIC                                    dplyr::mutate(CHANGE_IND = ifelse((FIXED_PRICE == LAG_PRICE) | (is.na(LAG_PRICE)),0,1))%>%
# MAGIC                                    dplyr::mutate(PRICE_REGIME = cumsum(CHANGE_IND))%>%
# MAGIC                                    dplyr::group_by(PRICE_REGIME)%>%
# MAGIC                                    dplyr::mutate(WEEKS_ON_PRICE = dplyr::n())%>%ungroup()%>%
# MAGIC                                    dplyr::mutate(REG_PRICE = FIXED_PRICE)
# MAGIC 
# MAGIC                   if(min(check3$WEEKS_ON_PRICE) <= 3){
# MAGIC                     price_regimes <- check3%>%dplyr::select(REG_PRICE, PRICE_REGIME, WEEKS_ON_PRICE)%>%unique()%>%dplyr::arrange(PRICE_REGIME)
# MAGIC                     stable_regimes <- price_regimes%>%dplyr::filter(WEEKS_ON_PRICE > 3)
# MAGIC                     to_fix_regimes <- unique((price_regimes%>%dplyr::filter(WEEKS_ON_PRICE <= 3))$PRICE_REGIME)
# MAGIC 
# MAGIC                     fixed_prices_dat2 <- bind_rows(lapply(to_fix_regimes, function(x) {
# MAGIC                     #           print(x)
# MAGIC                       price_fix_function(x,stable_regimes, price_regimes,2)
# MAGIC                     }))
# MAGIC                     temp_price <- check3 %>%dplyr::left_join(fixed_prices_dat2%>%dplyr::rename(FIXED_PRICE2 = FIXED_PRICE))%>%
# MAGIC                                    dplyr::mutate(FIXED_PRICE2 = ifelse(is.na(FIXED_PRICE2), FIXED_PRICE, FIXED_PRICE2))%>%dplyr::select(WEEK_SDATE, FIXED_PRICE2)
# MAGIC                     source_data <- source_data %>% dplyr::left_join(temp_price)%>%
# MAGIC                                    dplyr::mutate(FIXED_PRICE = ifelse(is.na(FIXED_PRICE2), FIXED_PRICE, FIXED_PRICE2))
# MAGIC 
# MAGIC                     }
# MAGIC         }
# MAGIC 
# MAGIC       #renaming
# MAGIC       source_data <- source_data%>%
# MAGIC                              dplyr::rename(REG_PRICE_OLD = REG_PRICE,
# MAGIC                                     reg_price = FIXED_PRICE,
# MAGIC                                     week_start_date = WEEK_SDATE)
# MAGIC 
# MAGIC        # making the series truncated if the initial continuous unstable prices are more than 5 regimes
# MAGIC        #source_data <- source_data %>%
# MAGIC        #                  tail(nrow(source_data) - remove_issue_weeks)
# MAGIC      
# MAGIC        return(source_data)
# MAGIC      
# MAGIC  }

# COMMAND ----------

# MAGIC %r
# MAGIC keys = unique(source_data_main$func_key)
# MAGIC 
# MAGIC #Uncomment if we need to unsmoothen part of the series. Not needed for this may refresh
# MAGIC # source_data_main_post <- source_data_main %>% dplyr::filter(WEEK_SDATE >= no_smoothen_after)%>% dplyr::rename(week_start_date = WEEK_SDATE, reg_price = REG_PRICE) %>% dplyr::select(-func_key)
# MAGIC # source_data_main <- source_data_main %>% dplyr::filter(WEEK_SDATE < no_smoothen_after)
# MAGIC 
# MAGIC source_data_cleaned <- bind_rows(lapply(keys, function(x) {
# MAGIC                                             cleanup_price(x)}))
# MAGIC 
# MAGIC # #Uncomment if we need to unsmoothen part of the series. Not needed for this may refresh
# MAGIC # source_data_cleaned <- bind_rows(source_data_cleaned,source_data_main_post)

# COMMAND ----------

# MAGIC %md
# MAGIC # Now need to re-calculate those variables based on updated regular price and also a weekly and monthly dummy variable

# COMMAND ----------

# MAGIC %r
# MAGIC #remove any anomalous sales issue if there - only 1 row was of propane there which is removed below
# MAGIC inscope_smoothened <- source_data_cleaned%>%dplyr::filter((trn_sold_quantity >= 0 & trn_gross_amount >= 0)|(trn_sold_quantity < 0 & trn_gross_amount <0))
# MAGIC #display(inscope_smoothened)

# COMMAND ----------

# MAGIC %r
# MAGIC ##look at the rows that have been removed
# MAGIC inscope_smoothened_removed <- source_data_cleaned%>%dplyr::filter(!((trn_sold_quantity >= 0 & trn_gross_amount >= 0)|(trn_sold_quantity < 0 & trn_gross_amount <0)))
# MAGIC #nrow(inscope_smoothened_removed)

# COMMAND ----------

# MAGIC %r
# MAGIC #calculate vars
# MAGIC calendar_dat <- readRDS(paste0("/dbfs/Phase4_extensions/Elasticity_Modelling/",wave,"/calendar.Rds"))%>%
# MAGIC                            dplyr::select(week_start_date,Week_No)%>%unique()
# MAGIC inscope_smoothened_vars <- inscope_smoothened%>%
# MAGIC                             dplyr::select(product_key,Cluster,week_start_date,trn_sold_quantity,trn_gross_amount,reg_price)%>%
# MAGIC                             dplyr::left_join(calendar_dat)%>%
# MAGIC                            dplyr::rename(avg_trn_item_unit_price = reg_price)%>%
# MAGIC                            dplyr::mutate(trn_net_price = round(trn_gross_amount/trn_sold_quantity,2),
# MAGIC                                   trn_total_discount = avg_trn_item_unit_price - trn_net_price,
# MAGIC                                   discount_perc = round((trn_total_discount*100)/avg_trn_item_unit_price,2),
# MAGIC                                   ln_sales_qty = ifelse(trn_sold_quantity > 0,log(trn_sold_quantity),0),      
# MAGIC                                   ln_reg_price = ifelse(avg_trn_item_unit_price  > 0,log(avg_trn_item_unit_price),0),
# MAGIC                                   mon_var = paste0("MONTH_DUMMY_",lubridate::month(week_start_date,label = T,abbr = T)),
# MAGIC                                   mon_val = 1,
# MAGIC                                   WEEK_DUM = paste0("WEEK_DUMMY_",Week_No),
# MAGIC                                   week_val = 1)%>%
# MAGIC                             dplyr::select( -Week_No)%>%
# MAGIC                             dplyr::mutate(discount_perc = ifelse(discount_perc < 5 ,0,discount_perc)) %>%
# MAGIC                             tidyr::spread(mon_var,mon_val) %>%
# MAGIC                             tidyr::spread(WEEK_DUM,week_val) %>% ungroup()%>%
# MAGIC                             dplyr::rename(weekdate = week_start_date,
# MAGIC                                   itemid = product_key,
# MAGIC                                   reg_price = avg_trn_item_unit_price,
# MAGIC                                   sales_qty = trn_sold_quantity,
# MAGIC                                   revenue = trn_gross_amount,
# MAGIC                                   discount = trn_total_discount, 
# MAGIC                                   net_price = trn_net_price
# MAGIC                                   ) %>% 
# MAGIC                              dplyr::mutate(discount = ifelse(is.na(discount),0,discount),
# MAGIC                                          discount_perc = ifelse(is.na(discount_perc),0,discount_perc),
# MAGIC                                          net_price= ifelse(is.na(net_price),reg_price,(ifelse(is.infinite(net_price),reg_price,net_price)))
# MAGIC                                          )
# MAGIC                           

# COMMAND ----------

# MAGIC %r
# MAGIC #final validation
# MAGIC df_valdiation = inscope_smoothened_vars %>% group_by(itemid,Cluster) %>% dplyr::summarise (min_reg_price= min(reg_price),
# MAGIC                                                                                    max_reg_price=max(reg_price),
# MAGIC                                                                                    mean_reg_price=round(mean(reg_price),2),
# MAGIC                                                                                    min_net_price= min(net_price),
# MAGIC                                                                                    max_net_price=max(net_price),
# MAGIC                                                                                    mean_net_price=round(mean(net_price),2),
# MAGIC                                                                                    reg_price_check=round(max_reg_price/min_reg_price,2),
# MAGIC                                                                                    net_price_check=round(max_net_price/min_net_price,2),
# MAGIC                                                                                    compare=round(mean_reg_price/mean_net_price,2),
# MAGIC                                                                                    )
# MAGIC 
# MAGIC #display(df_valdiation)
# MAGIC #Note:spend some time reviewing the results and ratios

# COMMAND ----------

# MAGIC %md
# MAGIC #### Finally save Modelling Rds's with cluster column being removed and the name suffix of the Rds denoting cluster

# COMMAND ----------

# MAGIC %r
# MAGIC if(state == 'site_state_id'){
# MAGIC   saveRDS(inscope_smoothened_vars,
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC           paste0("/dbfs/Phase3_extensions/Elasticity_Modelling/",wave,"/",bu,"/Modelling_Inputs/modelling_ads_cluster_state_zone_new.Rds"))
# MAGIC }else{
# MAGIC   saveRDS(inscope_smoothened_vars,
# MAGIC           paste0("/dbfs/Phase3_extensions/Elasticity_Modelling/",wave,"/",bu,"/Modelling_Inputs/modelling_ads_cluster_stateless_zone_new.Rds"))
# MAGIC 
# MAGIC 
# MAGIC }
