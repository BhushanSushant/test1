# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Creator: Taru Vaid, Anuj Rewale, Jagruti Joshi
# Date Created: 01/14/2021
# Updated by: Nikhil Soni
# Date Updated: 08/13/2021
# Description: 
# This is a clone of the notebook data_validation_north_america located in Localised-Pricing/Master_Codes with some adjustments to make it more suitable for all north american BUs

# Test control map, inscope items list, cluster map : these files will need to be updated for the latest refresh. Please discuss these with the manager. IF they are not available then you can use the file from an earlier refresh for this data validation piece, and comeback and rerun any impacted cmds when the files are ready.

# if you note some weirdness in the data, please discuss with the manager and reach a decision of what are the next steps.

# GR & Sweden are unique and slightly different from the other new BU's. It will be prudent to discuss their data prep beforehand and any associated changes.

# COMMAND ----------

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

import pyspark.sql.functions as sf
import pandas as pd
from pyspark.sql import *
from dateutil.relativedelta import relativedelta
from datetime import *
import datetime
from pyspark.sql.functions import when, sum, avg, col, concat, lit
import math

# COMMAND ----------

business_units = dbutils.widgets.get("business_unit") #BU name
start_date = dbutils.widgets.get("start_date") #Start Date
end_date = dbutils.widgets.get("end_date") #End Date
bu_code = dbutils.widgets.get("bu_code").lower()

acc_cat_table = "localized_pricing.acc_cat" #Accenture category table
transactions_table = "dl_localized_pricing_all_bu.merchandise_item_transactions" #Transactions Table
site_table = "dl_localized_pricing_all_bu.site" #Site Table
product_table = "dl_localized_pricing_all_bu.product" #Product Table
extreme_value_sales_indicator = 40 #Extreme value indicator for sales
# bu_zone_map_repo = bu_code.upper()+"_Repo"
# wave_zone_map = "Wave_1"
# bu_zone_map = dbutils.widgets.get("zone_map")
#Uncomment for Phase 3
# phase = "Phase3" #When Phase 3
# wave = "MAY2021_Refresh" #Current wave
# base_directory_dbfs = "/dbfs/Phase3_extensions/Elasticity_Modelling" #dbfs directory
# base_directory_spark = "/Phase3_extensions/Elasticity_Modelling" #Spark directory
# scope_file_name = "Final_Inscope_All_BU.csv" #Scope file name
# store_cluster_map_name = "store_cluster_mapping_active_stores_wave3.csv" #Store cluster map name
# test_control_map_name = "test_control_map_updated_03_03_2021.csv" #Test control map name

#Uncomment for Phase 4
#phase = dbutils.widgets.get("phase") #When Phase 4
wave = dbutils.widgets.get("wave") #Current wave
base_directory_dbfs = dbutils.widgets.get("base_directory_dbfs") #dbfs directory
base_directory_spark = dbutils.widgets.get('base_directory_spark') #Spark directory



# COMMAND ----------

# Derived Parameters
# Uncomment for Phase 3
# scope_file_location = base_directory_dbfs+'/'+ wave +'/'+scope_file_name
# store_cluster_map_file_location = base_directory_spark+'/'+ wave +'/'+ store_cluster_map_name
# test_control_map_file_location = base_directory_spark + "/" + wave + "/" + test_control_map_name

# Uncomment for Phase 4
# scope_file_location = base_directory_dbfs+'/'+ wave +'/'+scope_file_name
# store_cluster_map_file_location = base_directory_spark+'/'+ wave +'/'+ store_cluster_map_name
# test_control_map_file_location = base_directory_spark + "/" + wave + "/" + test_control_map_name
#bu_zone_map = '/Phase_3/Elasticity_Modelling/{}/{}/Modelling_Inputs/{}_item_store_zones.csv'.format(wave_zone_map, bu_zone_map_repo, bu_code)

# COMMAND ----------

# DBTITLE 1,List of Accenture's Category Generic Scope Definition 
acc_cat = sqlContext.sql("select distinct category_desc, Acc_Cat from {0}".format(acc_cat_table))

# COMMAND ----------

# DBTITLE 1,Data Pull (output temp view) - If GR only take tempe sys_environment
transactions_query = """
  SELECT   
  MT.transaction_line_item,
  MT.promotion_env_key,
  MT.prod_item_env_key,
  MT.site_env_key,
  MT.sys_environment_name,
  MT.transaction_date,
  MT.business_date,
  MT.transaction_time,
  MT.transaction_number,
  MT.product_key,
  MT.upc,
  MT.promotion_id,
  MT.promotion_description,
  MT.promotion_type,
  MT.quantity_sold,
  MT.sales_amount,                
  MT.loyalty_card_number,
  MT.marketbasket_header_key,
  MT.organization_key,
  MT.timeofday_key,
  MT.loyalty_key,
  MT.businessdate_calendar_key,               
  MT.transaction_type_key,
  ST.site_number_corporate,
  ST.site_address,
  ST.site_city,
  ST.site_state_id,
  ST.site_zip,
  ST.gps_latitude,
  ST.gps_longitude,
  ST.division_id,
  ST.division_desc,
  ST.region_id,
  ST.region_desc,
  PD.sell_unit_qty,
  PD.product_item_env_key,
  PD.nacs_category_desc, 
  PD.department_desc,
  PD.category_desc,
  PD.sub_category_desc,
  PD.item_desc,
  PD.manufacturer_desc,
  PD.brand_desc,
  PD.package_desc,
  PD.item_number,
  PD.private_brand_desc, 
  PD.size_unit_of_measure,
  PD.UPC_Sell_Unit_desc,
  PD.mb_product_key,
  PD.mb_product_env_key 
  
  FROM {0} as MT 
  LEFT JOIN {1} as ST
      ON MT.site_env_key == ST.site_env_key
  LEFT JOIN {2} as PD
      ON MT.prod_item_env_key == PD.product_item_env_key
  LEFT JOIN {3} AC
      ON PD.category_desc==AC.category_desc
  
  WHERE business_date BETWEEN '{4}' and '{5}' 
  AND Acc_Cat NOT IN ("Out of Scope")
  """.format(transactions_table, site_table, product_table, acc_cat_table, start_date, end_date)


#filtering transactions for data validation
if business_units!="3100 - Grand Canyon Division":
   merchandise_item_transactions = sqlContext.sql(transactions_query)\
  .withColumnRenamed("division_desc","BU")\
  .withColumnRenamed("division_id","BU_ID")\
  .withColumn("upc_sell_unit_key", concat(col("upc"),lit("_"), col("sell_unit_qty")))\
  .filter(sf.col("BU").isin(business_units))
else :
  merchandise_item_transactions = sqlContext.sql(transactions_query)\
  .withColumnRenamed("division_desc","BU")\
  .withColumnRenamed("division_id","BU_ID")\
  .withColumn("upc_sell_unit_key", concat(col("upc"),lit("_"), col("sell_unit_qty")))\
  .filter(sf.col("BU").isin(business_units))\
  .filter(sf.col("sys_environment_name")=='Tempe')
  
print("business unit is: "+business_units)

merchandise_item_transactions.createOrReplaceTempView("filtered_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC # Start of Data Validation

# COMMAND ----------

# DBTITLE 1,Unique BU/BU_ID Within Data
display(sqlContext.sql("""SELECT distinct BU, BU_ID 
                          FROM  filtered_transactions  """))

# COMMAND ----------

# DBTITLE 1,Unique States Within Data
display(sqlContext.sql("""SELECT distinct(site_state_id) 
                          FROM filtered_transactions"""))

# COMMAND ----------

# DBTITLE 1,Sales, Qty, Min/Max Business Date and Transaction Date by Environment
display(sqlContext.sql("""
SELECT
  BU,
  sys_environment_name,
  sum(sales_amount) as total_sales,
  sum(quantity_sold) as total_qty,
  min(transaction_date),
  min(business_date),
  max(transaction_date),
  max(business_date)
FROM
  filtered_transactions
GROUP BY
  BU,
  sys_environment_name
"""))

# COMMAND ----------

# DBTITLE 1,Check for null values in the transaction table (normal in some columns)
display(merchandise_item_transactions.select([sf.count(when(col(c).isNull(), c)).alias(c) for c in merchandise_item_transactions.columns]))

# COMMAND ----------

# DBTITLE 1,Verify Date Range of Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC   min(business_date) as start_date,
# MAGIC   max(business_date),
# MAGIC   min(transaction_date) as start_date,
# MAGIC   max(transaction_date) as end_date
# MAGIC FROM
# MAGIC   filtered_transactions

# COMMAND ----------

# DBTITLE 1,Unique Sites in Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT site_number_corporate) as Unique_Count_Of_Sites
# MAGIC FROM
# MAGIC   filtered_transactions

# COMMAND ----------

# DBTITLE 1,Data Summary
# Statistical summaries of sales_amount
# merchandise_item_transactions.describe(['sales_amount', 'quantity_sold']).show()

display(merchandise_item_transactions.describe(['sales_amount', 'quantity_sold']))

# COMMAND ----------

# DBTITLE 1,Count of Records in Data
# Get the count of rows in the transaction data
nrow_transactions=merchandise_item_transactions.count()
print(nrow_transactions)

# COMMAND ----------

# DBTITLE 1,Investigate Sales > X (extreme_value_sales_indicator)
# Investigate high amount transactions, for now take 3 standard deviations away from the mean from the above cell, so we'll use that as the tenative definition of extreme value
count_high_sales= merchandise_item_transactions.filter(col('sales_amount') > extreme_value_sales_indicator).count()

high_sales = (count_high_sales/nrow_transactions)*100

print("{}% of transactions are >$40 with {} rows of {}".format(round(high_sales,4), count_high_sales,nrow_transactions))

# COMMAND ----------

# Get the counts of items that have high transaction amounts
# merchandise_item_transactions.filter(col('sales_amount') > 40).groupby('item_desc').count().orderBy(sf.desc("count")).show()

display(merchandise_item_transactions.filter(col('sales_amount') > extreme_value_sales_indicator).groupby('item_desc').count().orderBy(sf.desc("count")))

# COMMAND ----------

# DBTITLE 1,Brief look at largest sales amount per item on transaction
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM filtered_transactions
# MAGIC ORDER BY sales_amount DESC

# COMMAND ----------

# DBTITLE 1,Transactions with Sales Amount of 1 cent
# Investigate transactions at 0.01
count_low_sales= merchandise_item_transactions.filter(col('sales_amount') == 0.01).count()

low_sales = (count_low_sales/nrow_transactions)*100

print("{}% of transactions are $0.01 with {} rows of {}".format(round(low_sales,4), count_low_sales,nrow_transactions))

# COMMAND ----------

# DBTITLE 1,Count of line items with 1 cent by item description
# Get the counts of items that have negative transaction amounts
# merchandise_item_transactions.filter(col('sales_amount') == 0.01).groupby('item_desc').count().orderBy(sf.desc("count")).show()

display(merchandise_item_transactions.filter(col('sales_amount') == 0.01).groupby('item_desc').count().orderBy(sf.desc("count")))

# COMMAND ----------

# DBTITLE 1,Look at distribution of transaction types - just FYI for now
# MAGIC %sql
# MAGIC SELECT trans.transaction_type_key, trans.sys_environment_name, tt.Transaction_Type_Desc, sum(sales_amount) as Sum_Of_Sales, count(*) as Count_Of_Records
# MAGIC FROM filtered_transactions trans JOIN dl_market_basket.transaction_type tt
# MAGIC   ON trans.transaction_type_key = tt.Transaction_Type_Key AND
# MAGIC   trans.sys_environment_name = tt.sys_environment_name
# MAGIC GROUP BY trans.transaction_type_key, trans.sys_environment_name, tt.Transaction_Type_Desc

# COMMAND ----------

# DBTITLE 1,Percent of line items with negative sales amount
count_low_sales= merchandise_item_transactions.filter(col('sales_amount') <= 0).count()
low_sales = (count_low_sales/nrow_transactions)*100
print("{}% of transactions are < 0 with {} rows of {}".format(round(low_sales,4), count_low_sales,nrow_transactions))

# COMMAND ----------

# DBTITLE 1,Count of line items with negative sales by item description
# Get the counts of items that have negative transaction amounts
# merchandise_item_transactions.filter(col('sales_amount') <= 0).groupby('item_desc').count().orderBy(sf.desc("count")).show()

display(merchandise_item_transactions.filter(col('sales_amount') <= 0).groupby('item_desc').count().orderBy(sf.desc("count")))

# COMMAND ----------

# DBTITLE 1,Brief look at smallest sales (most negative) amount per item on transaction
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM filtered_transactions
# MAGIC ORDER BY sales_amount ASC

# COMMAND ----------

# DBTITLE 1,Get data ready for the the times series visualization

timeseries_df1= merchandise_item_transactions.select('business_date','quantity_sold','sales_amount')
timeseries_df2= timeseries_df1.groupBy("business_date").agg(sum('quantity_sold'),sum('sales_amount')).withColumnRenamed("sum(quantity_sold)","quantity").withColumnRenamed("sum(sales_amount)","revenue").orderBy( "business_date")

# COMMAND ----------

# DBTITLE 1,High level view of BU's timeseries
# Plot the time series of quantity and revenue
display(timeseries_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Static and Dimension Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE dl_localized_pricing_all_bu

# COMMAND ----------

# DBTITLE 1,Check to see if Site table has nulls (mainly checking if table has changed) 
# Check for NULL values in the site data
site = sqlContext.sql("SELECT * FROM site")
display(site.select([sf.count(when(col(c).isNull(), c)).alias(c) for c in site.columns]))

# COMMAND ----------

# DBTITLE 1,Check to see if Promotions table has changed
# Check for NULL values in the promotions data
promotions = sqlContext.sql("SELECT * FROM promotions")
display(promotions.select([sf.count(when(col(c).isNull(), c)).alias(c) for c in promotions.columns]))

# COMMAND ----------

# DBTITLE 1,Check to see if Product table has changed
# Check for NULL values in the product data
product = sqlContext.sql("SELECT * FROM product")
display(product.select([sf.count(when(col(c).isNull(), c)).alias(c) for c in product.columns]))

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviewing Inscope Data 
# MAGIC <h2> (if available typically use last iterations inscope until we have new inscope list) </h2>

# COMMAND ----------

# DBTITLE 1,Load inscope and filter to specific BU (under impression inscope is single sheet for all BU)
# Get the inscope mapping file 
# This will be updated based on inputs from the Product expansion team
# will need to review the apporpirate inscope file for each BU. If the file is not available yet, then proceed with daa validation using the file from the last refresh. Come back to update these commands when you get the latest file

#get final inscope itmes
scoped_items = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
scoped_items = scoped_items.filter(col("BU") == business_units)
scoped_items = scoped_items.groupBy('product_key', 'item_number', 'UPC', 'Scope', 'department_name', 'category_name', 'item_name', 'price_family', 'sell_unit_qty', 'wave', 'date_live', 'BU').agg(sf.max('last_update'))
scoped_items = scoped_items.drop('max(last_update)').withColumnRenamed("product_key","tempe_product_key")
#scoped_items = pd.read_csv(scope_file_location)
#scoped_items = spark.createDataFrame(scoped_items)
#scoped_items = scoped_items.filter(col("BU") == business_units)


##############################################################
# DO WE NEED TO PULL PRODUCTS AGAIN WHEN WE PULLED IT ABOVE? #
##############################################################

# product=sqlContext.sql("select * from dl_localized_pricing_all_bu.product where environment_name=='Tempe'") #filtering for Tempe might not work for Canadian or single environment BUs
product=sqlContext.sql("select * from {0} ".format(product_table))
scoped_items.count()



inscope_items= scoped_items.join(product.select('product_key','category_desc'),[scoped_items['tempe_product_key']==product['product_key']],'left')
inscope_items.count()

inscope_items.createOrReplaceTempView("inscope_items")

# display(scoped_items)

# display(inscope_items)

# COMMAND ----------

# DBTITLE 1,Check if scoped_items joins cleanly to product table - Should result in 0
scoped_items.count()-inscope_items.count() ## validation : should be 0

# COMMAND ----------

# DBTITLE 1,Overview of Old Inscope Records by category
display(sqlContext.sql("""
select
  category_desc,
  count(*)
from
  inscope_items
group by
  category_desc
order by
  count(*) desc
""")) 

# COMMAND ----------

# DBTITLE 1,Overview of Old Inscope Distinct Product Key by category - Should match above
display(sqlContext.sql("""
select
  category_desc,
  count(distinct tempe_product_key)
from
  inscope_items
group by
  category_desc
order by
  count(*) desc
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Control Stores for All BUs

# COMMAND ----------

## Get the latest test control file from the manager of each BU

#test_control_stores= spark.read.csv(test_control_map_file_location,inferSchema=True,header=True)

test_control_stores = spark.sql("""Select business_unit as BU, site_id as Site, group as test_control_map, cluster as Cluster from circlek_db.grouped_store_list
""")
# Where business_unit = '{}'
# and group = 'Test' 
# .format(business_units)
display(test_control_stores)

# COMMAND ----------

display(test_control_stores[["BU"]].distinct())

# COMMAND ----------

# DBTITLE 1,What is the purpose?
#get the test_control stores mapping file
test_control_stores=test_control_stores\
.withColumn("test_site_fixed", when(test_control_stores['BU'] == 'Central Canada', test_control_stores['Site'] - 3000000).otherwise(test_control_stores['Site']))\
.select("test_site_fixed","Cluster")\
.distinct()


test_control_stores=test_control_stores.select([sf.col(x).alias(x.lower()) for x in test_control_stores.columns])

display(test_control_stores)

# .withColumn("control_site_fixed", when(test_control_stores['Test BU'] == 'Central Canada',test_control_stores['Control Site'] - 3000000).otherwise(test_control_stores['Control Site']))\
# .withColumnRenamed("Test Cluster","Cluster")\

# COMMAND ----------

# DBTITLE 1,Count of Test_Control stores
test_control_stores.count()

# COMMAND ----------

# DBTITLE 1,Joined test_control_stores to Transactions Data
joined_df=merchandise_item_transactions.join(test_control_stores,[ merchandise_item_transactions.site_number_corporate == test_control_stores.test_site_fixed],'left')

joined_df.createOrReplaceTempView("joined_df")

# display(joined_df)

# COMMAND ----------

joined_df.count() #501252486

# COMMAND ----------

# DBTITLE 1,Show stores in data without cluster
display(sqlContext.sql("""
SELECT
  cluster,
  count(distinct site_number_corporate)
FROM
  joined_df
GROUP BY
  cluster
"""))

# COMMAND ----------

# DBTITLE 1,Find metrics on missing stores from list
display(sqlContext.sql("""
select
  site_number_corporate,
  min(business_date) as Min_Business_Date,
  max(business_date) as Max_Business_Date,
  sum(sales_amount) as Sum_Of_Sales
FROM
  dl_localized_pricing_all_bu.merchandise_item_transactions mit
  left join dl_localized_pricing_all_bu.site s on mit.site_env_key = s.site_env_key
WHERE
  site_number_corporate IN (
    SELECT
      distinct site_number_corporate
    FROM
      joined_df
    WHERE
      Cluster IS NULL
  )
GROUP BY
  site_number_corporate
"""))

# COMMAND ----------

# DBTITLE 1,Show categories x stores in data without cluster
display(sqlContext.sql("""
SELECT
  category_desc,
  cluster,
  count(distinct site_number_corporate)
FROM
  joined_df
GROUP BY
  category_desc,
  cluster
"""))

# COMMAND ----------

# DBTITLE 1,Phase 3 Cluster to Store Map - Accenture File
#test store file used by accenture in theier All BU run. We can compare this to the new file uploaded above
#make sure to update the path
# Discuss this file with the manager and Update this file for the new refresh 

# cluster_map = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').\
#                           load(store_cluster_map_file_location)
cluster_map = spark.sql("""Select business_unit as BU, site_id as trn_site_sys_id, cluster as Cluster from circlek_db.grouped_store_list
Where business_unit = '{}'
and group = 'Test' """.format(business_units))
# display(cluster_map)

# COMMAND ----------

display((cluster_map.groupBy('Cluster').agg(sf.countDistinct('trn_site_sys_id'))))

# COMMAND ----------

# MAGIC %md
# MAGIC # Closer Look at Environments for All BUs

# COMMAND ----------

display(sqlContext.sql("""
SELECT * 
FROM dl_localized_pricing_all_bu.site 
"""))

# COMMAND ----------

# DBTITLE 1,What is the purpose?
sqlContext.sql("""
SELECT
  a.*,
  b.site_number_corporate,
  b.site_address,
  b.site_city,
  b.site_state_id,
  b.site_zip,
  b.gps_latitude,
  b.gps_longitude,
  b.division_id,
  b.division_desc,
  b.region_id,
  b.region_desc
FROM
  dl_localized_pricing_all_bu.merchandise_item_transactions a
  left join dl_localized_pricing_all_bu.site b on a.site_env_key = b.site_env_key           
""").createOrReplaceTempView("na_all_bu_trnsaction_store_data")

sqlContext.sql("""
SELECT
  a.*,
  b.sell_unit_qty,
  b.category_desc
FROM
  na_all_bu_trnsaction_store_data a
  left join dl_localized_pricing_all_bu.product b on a.prod_item_env_key = b.product_item_env_key            
""").createOrReplaceTempView("na_all_bu_transaction_full_table_hierarchy_validation")


qw_df = sqlContext.sql("""
SELECT
  *
FROM
  na_all_bu_transaction_full_table_hierarchy_validation
WHERE
  division_desc == "QUEBEC OUEST"      
""")

qw_df.createOrReplaceTempView("qw_df")

# COMMAND ----------

display(sqlContext.sql("""
SELECT
  division_desc,
  sys_environment_name,
  min(business_date),
  max(business_date)
FROM
  na_all_bu_transaction_full_table_hierarchy_validation
GROUP BY
  division_desc,
  sys_environment_name
"""))                 


# COMMAND ----------

# DBTITLE 1,Untitled
##Mapping for Canadian Categories
display(sqlContext.sql("""
select
  *
from
  (
    select
      category_desc,
      count (distinct product_key) as pk_count
    FROM
      na_all_bu_transaction_full_table_hierarchy_validation
    where
      division_desc in (
        "Western Division",
        "Central Division",
        "QUEBEC EST - ATLANTIQUE",
        "QUEBEC OUEST"
      )
    group by
      category_desc
  ) a
  LEFT JOIN localized_pricing.acc_cat b ON a.category_desc == b.category_desc
"""))  




# COMMAND ----------

# DBTITLE 1,Zone map validation? Do we need to add more?
# zone_map## you can look at the zone map for an earlier refresh. You will need to update it for the latest refresh.
# zone_map = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').\
#                           load(bu_zone_map)
# # display(zone_map)

# COMMAND ----------

# #categories  that were zoned
# display(zone_map[['category_desc']].distinct())

# COMMAND ----------

# DBTITLE 1,WIP - Validation that all inscope Y items have a item to site zone mapping 
# Look above
