# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Modified by: Sanjo Jose
# Date Modified: 19/08/2021
# Modifications: Test run for Phase 4 BUs, Modified output table for selected BU

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import necessary libraries

# COMMAND ----------

from datetime import *
import datetime
import pyspark.sql.functions as sf

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
spark.conf.set("spark.databricks.io.cache.enabled", "false")

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

spark.conf.set("spark.sql.broadcastTimeout", 36000)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read widget values

# COMMAND ----------

wave = dbutils.widgets.get("wave")

business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

db = dbutils.widgets.get("db")

eu_txn_table = dbutils.widgets.get("transaction_table")
eu_site_table = dbutils.widgets.get("station_table")
eu_item_table = dbutils.widgets.get("item_table")

output_txn_table = "{0}.{1}_txn_data_{2}".format(db, bu_code.lower(), wave.lower())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and save master data (transactions + items + sites)

# COMMAND ----------

#join all hiearchies 
query_txt_txn = "select * from {} \
                 where trn_transaction_date >= '{}' and trn_transaction_date <= '{}' ".format(eu_txn_table, start_date, end_date)

query_txt_item = "select sys_id as trn_item_sys_id, item_name, item_subcategory, item_category, item_category_group, item_subcategory_code, \
                  item_category_code, item_category_group_code, item_segment, item_segment_code, item_brand, item_brand_code, \
                  item_manufacturer, item_manufacturer_code, item_line_of_business \
                  from {} ".format(eu_item_table)

query_txt_site = "select sys_id as trn_site_sys_id, site_bu, site_bu_name,site_country, site_name, site_number, site_ownership, site_brand, site_format, site_status  \
                  from {} ".format(eu_site_table)

# COMMAND ----------

master_joined = sqlContext.sql(query_txt_txn).\
                join(sqlContext.sql(query_txt_item),['trn_item_sys_id'],"left").\
                join(sqlContext.sql(query_txt_site),['trn_site_sys_id'],"left").\
                filter(sf.col('site_bu_name')==business_units)

master_joined.write.mode("overwrite").saveAsTable(output_txn_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run validations on master data

# COMMAND ----------

# Check the count in DB table
lower_bu_code = bu_code.lower()
output_txn_table = "{}.{}_txn_data_{}".format(db, lower_bu_code, wave)
x = sqlContext.sql("select * from {}".format(output_txn_table))
if x.count() > 0:
  print("ok")
else:
  raise ValueError('zero rows in output table, check parameters')

# COMMAND ----------

# Check the count in DB table vs Txn table
y = sqlContext.sql(query_txt_txn).\
                join(sqlContext.sql(query_txt_site),['trn_site_sys_id'],"left").\
                filter(sf.col('site_bu_name')==business_units)
if x.count() == y.count():
  print("ok")
else:
  raise ValueError('rows in output table not matching rows in txn table')
