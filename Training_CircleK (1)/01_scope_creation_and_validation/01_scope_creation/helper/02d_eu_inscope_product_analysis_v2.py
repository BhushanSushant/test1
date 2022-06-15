# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Creator: Taru Vaid, Jagruti Joshi, Sterling Fluharty
# Date Created: 14/12/2020
# Modified by: Sanjo Jose
# Date Modified: 12/08/2021
# Modifications: Phase 4 Improvements, Test run for Phase 4 BUs, Converted widgets to parameters, Other Minor modifications

# Description: This notebook enhances the scope filters created by accenture.
# The filters used are as below:
# 1	Business Unit
# 2	Start Date
# 3	End Date
# 4	Prop. Stores Selling
# 5	Recent Days With No Sales
# 6	Ratio for Dying Series	
# 7	Ratio for Slow Start	
# 8	Min Prop. Weeks Sold
# 9	Consc.Gap in Sales(days)	
# 10 Min Qtrs of Sales History	
# 11 Dept Sales % Cutoff

#Input file : 
# 1. Transaction data : phase3_extensions.{bu}_txn_data_{refresh}
# 2. Long term /short term exclusion details for the previous refresh (check with manager/accenture)
# 3. Inscope file from the previous refresh 

# Output file: {bu}_item_scoping_data_{refresh}.xlsx
# (to be shared with manager & CM to finalize scope for this refresh)

# Recommendations:
# 1. Please use atleast 449 days of the data for this code to flag items correctly. Its best to use 1.5 or more yrs of data if possible

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import necessary libraries

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
from dateutil.relativedelta import relativedelta
from datetime import *
import datetime
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from operator import add
from functools import reduce
import databricks.koalas as ks
import pandas as pd
import openpyxl
from shutil import move

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
spark.conf.set("spark.databricks.io.cache.enabled", "false")

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read widget values

# COMMAND ----------

business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

db = dbutils.widgets.get("db")

phase = dbutils.widgets.get("phase")
wave = dbutils.widgets.get("wave")

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

percent_of_stores = dbutils.widgets.get("percent_of_stores")
days_since_last_sold = dbutils.widgets.get("days_since_last_sold")
dying_series_ratio = dbutils.widgets.get("dying_series_ratio")
slow_start_ratio = dbutils.widgets.get("slow_start_ratio")
proportion_weeks_sold = dbutils.widgets.get("proportion_weeks_sold")
week_gap_allowed = dbutils.widgets.get("week_gap_allowed")
min_qtr = dbutils.widgets.get("min_qtr")
cumulative_sales_perc = dbutils.widgets.get("cumulative_sales_perc")
tolerance_qtrs = dbutils.widgets.get("tolerance_qtrs")
tolerance_last_yr = dbutils.widgets.get("tolerance_last_yr")
file_directory = dbutils.widgets.get("file_directory")

if business_units in {'Ireland', 'Sweden'}:
  long_short_exclusions = "/{0}/Elasticity_Modelling/{1}/{2}_Repo/Input/Circle_K_Scope_Determination_IE_BU_CM_Inputs_vUpload_dbfs.csv".format(phase, wave,                                                                                                                                 bu_code)
  ean = spark.sql("""SELECT trn_item_sys_id FROM (SELECT product_key as trn_item_sys_id, upc AS trn_barcode, modelling_level, last_update, 
                     ROW_NUMBER() OVER (PARTITION BY upc ORDER BY last_update DESC) AS latest 
                     FROM circlek_db.lp_dashboard_items_scope 
                     WHERE bu = '{}' 
                     AND modelling_level == 'EAN') a WHERE latest = 1""".format(business_units)) 
  simplified_coffee = '/{0}/Elasticity_Modelling/{1}/{2}_Repo/Input/simplified_coffee_jde.csv'.format(phase, wave, bu_code)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create temporary table rel_transactions

# COMMAND ----------

# Filtering the Relevant Data and Creating a Temporary Table 'rel_transactions'
sqlContext.sql("""
                SELECT site_bu,item_category_group,item_category,item_subcategory,item_name,item_manufacturer,item_brand,trn_promotion_id
                      ,REPLACE(LTRIM(REPLACE(trn_item_sys_id,'0',' ')),' ','0') as trn_item_sys_id
                      ,REPLACE(LTRIM(REPLACE(trn_barcode,'0',' ')),' ','0') as trn_barcode
                      ,REPLACE(LTRIM(REPLACE(trn_site_sys_id,'0',' ')),' ','0') as trn_site_sys_id
                      ,trn_transaction_date
                      ,trn_cost
                      ,trn_item_unit_price
                      ,trn_sold_quantity
                      ,trn_gross_amount
                      ,(trn_gross_amount-trn_vat_amount) as trn_net_amount
                FROM {0}.{1}_txn_data_{2}
                WHERE trn_cost > 0
                      AND trn_sold_quantity > 0
                      AND trn_gross_amount > 0
                      AND trn_item_unit_price > 0.1
                      """.format(db, bu_code.lower(), wave.lower())).createOrReplaceTempView("rel_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Modify rel_transactions with EAN switch

# COMMAND ----------

# Where category is "Snuff" in Sweden OR trn_item_sys_id in BU reviewed "EAN level items" list, make the trn_item_sys_id same as trn_barcode
if business_units == "Ireland" or business_units == "Sweden":
  ean_level_items_list = map(str, ean['trn_item_sys_id'].unique().tolist())
  ean_level_items_string = ",".join(ean_level_items_list)

  sqlContext.sql("""
    SELECT *
          ,CASE WHEN trn_item_sys_id in ({})
                THEN trn_barcode
                ELSE trn_item_sys_id 
                END AS trn_item_sys_id_main 
    FROM rel_transactions
    """.format(ean_level_items_string)).createOrReplaceTempView("rel_transactions")  
else:
  if business_units == "Sweden":
    sqlContext.sql("""
    SELECT *
    ,CASE WHEN item_category in ('Snuff')
                THEN trn_barcode 
                ELSE trn_item_sys_id 
                END AS trn_item_sys_id_main 
    FROM rel_transactions
                 """).createOrReplaceTempView("rel_transactions")    
  else:
    sqlContext.sql("""
    SELECT *, trn_item_sys_id as trn_item_sys_id_main
    FROM rel_transactions
                 """).createOrReplaceTempView("rel_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Change simplified coffee non test items to test items for Ireland

# COMMAND ----------

# stich sales history for simplified coffee JDEs
if business_units == "Ireland":
  simplified_coffee = spark.read.csv(simplified_coffee, header=True,inferSchema=True)
  simplified_coffee.createOrReplaceTempView("simplified_coffee")
  sqlContext.sql("""select a.test_trn_item_sys_id, a.non_test_trn_item_sys_id, b.item_name, b.item_manufacturer from simplified_coffee a left join 
  (select trn_item_sys_id, item_name, item_manufacturer from rel_transactions group by trn_item_sys_id, item_name, item_manufacturer) b on a.test_trn_item_sys_id = b.trn_item_sys_id""").createOrReplaceTempView("simplified_coffee_item_names_and_manufacturerers")
  sqlContext.sql("""select a.site_bu, a.item_category_group, a.item_category, a.item_subcategory, 
  CASE WHEN b.item_name is null THEN a.item_name else b.item_name end as item_name, 
  CASE WHEN b.item_manufacturer is null THEN a.item_manufacturer else b.item_manufacturer end as item_manufacturer, 
  a.item_brand, a.trn_promotion_id, a.trn_item_sys_id, a.trn_barcode, a.trn_site_sys_id, a.trn_transaction_date, a.trn_sold_quantity, a.trn_gross_amount, a.trn_net_amount, a.trn_cost, a.trn_item_unit_price,  
  CASE WHEN b.test_trn_item_sys_id is null THEN a.trn_item_sys_id_main ELSE b.test_trn_item_sys_id END as trn_item_sys_id_main 
  from rel_transactions a left join simplified_coffee_item_names_and_manufacturerers b on a.trn_item_sys_id = b.non_test_trn_item_sys_id""").createOrReplaceTempView("rel_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Correct item desc and item manufacturer

# COMMAND ----------

sqlContext.sql(""" SELECT trn_item_sys_id_main, item_name, max(trn_transaction_date) as max_date FROM rel_transactions GROUP BY trn_item_sys_id_main, item_name  """).createOrReplaceTempView("item_name_product_key_mapping")
sqlContext.sql(""" SELECT trn_item_sys_id_main, item_manufacturer, max(trn_transaction_date) as max_date FROM rel_transactions GROUP BY trn_item_sys_id_main, item_manufacturer  """).createOrReplaceTempView("item_manufacturer_product_key_mapping")
sqlContext.sql(""" SELECT trn_item_sys_id_main, item_subcategory, max(trn_transaction_date) as max_date FROM rel_transactions GROUP BY trn_item_sys_id_main, item_subcategory  """).createOrReplaceTempView("item_subcategory_product_key_mapping")

sqlContext.sql(""" SELECT trn_item_sys_id_main, item_name FROM ( select ROW_NUMBER() OVER(PARTITION BY trn_item_sys_id_main ORDER BY max_date DESC) as dups, * from item_name_product_key_mapping ) a where a.dups=1 """).createOrReplaceTempView('single_item_name_product_key_mapping')
sqlContext.sql(""" SELECT trn_item_sys_id_main, item_manufacturer FROM ( select ROW_NUMBER() OVER(PARTITION BY trn_item_sys_id_main ORDER BY max_date DESC) as dups, * from item_manufacturer_product_key_mapping ) a where a.dups=1 """).createOrReplaceTempView('single_item_manufacturer_product_key_mapping')
sqlContext.sql(""" SELECT trn_item_sys_id_main, item_subcategory FROM ( select ROW_NUMBER() OVER(PARTITION BY trn_item_sys_id_main ORDER BY max_date DESC) as dups, * from item_subcategory_product_key_mapping ) a where a.dups=1 """).createOrReplaceTempView('single_item_subcategory_product_key_mapping')

sqlContext.sql(""" SELECT a.site_bu, a.item_category_group, a.item_category, a.item_brand, a.trn_promotion_id, a.trn_item_sys_id, a.trn_barcode, a.trn_site_sys_id, a.trn_transaction_date, a.trn_sold_quantity, a.trn_gross_amount, a.trn_net_amount, a.trn_item_sys_id_main, b.item_name, c.item_manufacturer, d.item_subcategory from rel_transactions a 
join single_item_name_product_key_mapping b on a.trn_item_sys_id_main = b.trn_item_sys_id_main
join single_item_manufacturer_product_key_mapping c on a.trn_item_sys_id_main = c.trn_item_sys_id_main
join single_item_subcategory_product_key_mapping d on a.trn_item_sys_id_main = d.trn_item_sys_id_main""").createOrReplaceTempView('rel_transactions')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate item sales share to identify top 20% items with 80% revenue

# COMMAND ----------

# Rolling up the Data at Item Level
rolled_up_scoping_data = sqlContext.sql("""
                   SELECT site_bu,item_category_group,item_category,item_subcategory,item_name,item_manufacturer,item_brand,trn_item_sys_id_main,
                          SUM(trn_sold_quantity) as sales_quantity,
                          SUM(trn_gross_amount) as sales_amount,
                          SUM(trn_net_amount) as net_sales_amount
                   FROM rel_transactions
                   GROUP BY site_bu,item_category_group,item_category,item_subcategory,item_name,item_manufacturer,item_brand,trn_item_sys_id_main
                                    """)

# Rolling up the Data at Item Level for Promotions
rolled_up_promo_data = sqlContext.sql("""
                                       SELECT site_bu, item_category_group, trn_item_sys_id_main,
                                              SUM(trn_gross_amount) as promo_sales_amount,
                                              SUM(trn_net_amount) as promo_net_sales_amount
                                       FROM rel_transactions
                                       WHERE trn_promotion_id != -1
                                       GROUP BY site_bu, item_category_group, trn_item_sys_id_main
                                    """)

# COMMAND ----------

# Category Group Level Total Sales
group_level_sales = rolled_up_scoping_data.\
                      select("site_bu", "item_category_group", "sales_amount").\
                      groupBy("site_bu", "item_category_group").\
                      agg(sf.sum("sales_amount").alias("group_sales_amount"))

# Creating the Cumulative Sales Percentage Column for at Department Level
item_sales_share = rolled_up_scoping_data.\
                      join(group_level_sales, ["site_bu", "item_category_group"], "left").\
                      join(rolled_up_promo_data, ["site_bu", "item_category_group", "trn_item_sys_id_main"], "left").\
                      select("site_bu", "item_category_group", "item_category", "item_subcategory", "item_name", "item_manufacturer", "item_brand", "group_sales_amount",
                             sf.col("trn_item_sys_id_main").alias("trn_item_sys_id"), "sales_quantity", "sales_amount",
                             (sf.col("sales_amount")/sf.col("group_sales_amount")).alias("sales_share"), "net_sales_amount",
                             "promo_sales_amount", "promo_net_sales_amount").\
                      filter(sf.col('trn_item_sys_id').isNotNull()).\
                      withColumn("rank", sf.row_number().over(Window.partitionBy("item_category_group").\
                                                              orderBy(sf.col("sales_amount").desc()))).\
                      withColumn("cumulative_sales_perc", sf.sum("sales_share").over(Window.partitionBy("item_category_group").\
                                                                                    orderBy(sf.col("sales_amount").desc()))).\
                      orderBy(['item_category_group','rank'], ascending=[1,1])

# COMMAND ----------

item_sales_share_new = item_sales_share.withColumn("cum_sales_flag", \
                                         sf.when((col("cumulative_sales_perc")-col("sales_share")) <= cumulative_sales_perc, \
                                                     "within top " + str(float(cumulative_sales_perc)*100) + "% of sales").\
                                         otherwise("not in the top " + str(float(cumulative_sales_perc)*100) + "% of sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flag discontined/dying items by various logics

# COMMAND ----------

## Weekly Tables
# Filtering the Relevant Data and Creating a Temporary Table 'transaction_data'
sqlContext.sql("""SELECT site_bu, item_category_group, item_category, item_subcategory, item_name, item_manufacturer, item_brand, trn_item_sys_id_main, trn_transaction_date, trn_sold_quantity, trn_gross_amount FROM rel_transactions""").createOrReplaceTempView("transaction_data")

# Defining the Grouping Columns for Rolling the Data from Daily to Weekly Level
grouping_cols=["site_bu", "item_category_group", "item_category", "item_subcategory", "item_name", "item_manufacturer", "item_brand", "trn_item_sys_id_main", "week_start_date"]

# Rolling up the Data at Item x Week Level
master_BU_cat_site_item_sales = sqlContext.sql("select * from transaction_data").\
                      withColumn("week_no",sf.dayofweek("trn_transaction_date")).\
                      withColumn("adj_factor", sf.col("week_no") - 4).\
                      withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
                      withColumn("week_start_date", sf.expr("date_sub(trn_transaction_date, adj_factor)")).\
                      groupBy(*grouping_cols).\
                      agg(sf.sum("trn_sold_quantity").alias("qty"),
                          sf.sum("trn_gross_amount").alias("rev"))

## Quarterly Tables
master_weekly = master_BU_cat_site_item_sales.\
                filter((sf.col("qty") > 0) & (sf.col("rev") > 0)).\
                groupBy(['site_bu','trn_item_sys_id_main','week_start_date']).\
                agg(sf.sum("qty").alias("qty"),
                    sf.sum("rev").alias("rev")).\
                withColumn("row_num",sf.row_number().over(Window.partitionBy(['site_bu','trn_item_sys_id_main']).\
                                                          orderBy(sf.desc("week_start_date")))).\
                withColumn("qtr_start", sf.col("row_num") % 12).\
                withColumn("qtr_start_ind",sf.when(sf.col("qtr_start") == 1,1).otherwise(0)).\
                withColumn('quarter', sf.sum('qtr_start_ind').\
                           over((Window.partitionBy(['site_bu','trn_item_sys_id_main']).orderBy(sf.desc('week_start_date')).\
                                 rangeBetween(Window.unboundedPreceding, 0)))).\
                orderBy(['site_bu','trn_item_sys_id_main','week_start_date'], ascending=True)

# COMMAND ----------

## Logic for discontinued
# 1. Latest week vs latest day in its sale history if there is gap of more than 30 days
master_weekly_gap = master_weekly.\
                    withColumn("max_date", sf.max("week_start_date").over(Window.partitionBy(['site_bu','trn_item_sys_id_main']))).\
                    withColumn("latest_week",sf.lit(end_date)).\
                    withColumn("latest_week", sf.to_date(col("latest_week"),"yyyy-MM-dd")).\
                    withColumn("gap_from_latest", sf.datediff(col("latest_week"),col("max_date"))).\
                    withColumn('latest_date_diff_flag',sf.when((col("gap_from_latest")) > days_since_last_sold, "stopped_selling").otherwise("none")).\
                    withColumn("lag_date",
                               sf.lag(sf.col("week_start_date"),1).\
                               over(Window.partitionBy(['site_bu','trn_item_sys_id_main']).orderBy("week_start_date"))).\
                    withColumn("cons_date_diff",sf.datediff(col("week_start_date"),col("lag_date"))).\
                    withColumn("max_cons_diff",
                               sf.max("cons_date_diff").over(Window.partitionBy(['site_bu','trn_item_sys_id_main']))).\
                    withColumn('consecutive_flag',sf.when((col("max_cons_diff")) > week_gap_allowed, "missing_consecutive_weeks").otherwise("none"))

#add extra flag to not check consecutive cases
#where max max gap is 14 - no need to check - simply by changing params
#where the max gap is happenning prior to last year we excuse that
master_weekly_gap_check_consecutive = master_weekly_gap.\
                                      filter((col("consecutive_flag") == 'missing_consecutive_weeks') & ((col("cons_date_diff")) > week_gap_allowed)).\
                                      groupBy(['site_bu','trn_item_sys_id_main']).\
                                      agg(sf.min("row_num").alias("week_num_from_end")).\
                                      withColumn("consec_update",sf.when(col("week_num_from_end") > 52, "none").otherwise("delete")).\
                                      filter(col("consec_update") == "none").drop("week_num_from_end") 

# COMMAND ----------

# 2. If the last qtr vs previous qtr is more than 100% decline and also latest qtr vs last year same qtr needs to be not a similar decline [to take care of seasonality]
# 3. if there are not consecutive week's in ones sales flags as inconsistent series
master_dying_down_qtr = master_weekly_gap.\
                    groupBy(['site_bu','trn_item_sys_id_main','quarter']).\
                    agg(sf.mean("qty").alias("quarterly_qty")).\
                    withColumn("ly_qtr_qty", 
                               sf.lead(sf.col("quarterly_qty"),4).over(Window.partitionBy(['site_bu','trn_item_sys_id_main']).\
                                                                       orderBy("quarter"))).\
                    withColumn("lag_qtr_qty", 
                               sf.lead(sf.col("quarterly_qty"),1).over(Window.partitionBy(['site_bu','trn_item_sys_id_main']).\
                                                                       orderBy("quarter"))).\
                    orderBy(['site_bu','trn_item_sys_id_main','quarter'], ascending=True).\
                    withColumn("max_qtr", sf.max(sf.col("quarter")).over(Window.partitionBy(['site_bu','trn_item_sys_id_main']))).\
                    filter(col("quarter") == 1).\
                    withColumn("lag_ratio", col("quarterly_qty")/col("lag_qtr_qty")).\
                    withColumn("ly_ratio", col("quarterly_qty")/col("ly_qtr_qty")).\
                    withColumn("discont_flag", sf.when((col("lag_ratio") <= tolerance_qtrs) 
                                                       & ((col("ly_ratio") <= tolerance_qtrs) 
                                                          & (col("ly_ratio") < tolerance_last_yr)), 'discontinued').otherwise('none')).\
                    withColumn("data_enough_flag", sf.when((col("max_qtr") < min_qtr), 'not sold enough').otherwise('none'))

# 4. Also based on moving average and last year same qtr case - if there are holes in the series for stockout - not checked for this - too high level - consecutive gap good proxy

# COMMAND ----------

#get the unique cases where there is issue and merge them to create flag
master_discontinued_flag = master_weekly_gap.\
                           join(master_dying_down_qtr.\
                                select(['site_bu','trn_item_sys_id_main','quarter','discont_flag','data_enough_flag']),
                                ['site_bu','trn_item_sys_id_main','quarter'],'left').\
                           select(['site_bu','trn_item_sys_id_main','max_date','latest_week','latest_date_diff_flag',
                                   'consecutive_flag','discont_flag','data_enough_flag']).distinct().\
                           dropna().\
                           join(master_weekly_gap_check_consecutive,['site_bu','trn_item_sys_id_main'],'left').\
                           withColumn("trn_item_sys_id", sf.col("trn_item_sys_id_main")).\
                           withColumn("consecutive_flag", sf.when(col("consec_update").isNull(),
                                                                  col("consecutive_flag")).otherwise(col("consec_update"))).\
                           drop("consec_update").\
                           filter(~(((col("latest_date_diff_flag") == 'none')) & ((col("consecutive_flag") == 'none')) 
                                    & ((col("discont_flag") == 'none')) & ((col("data_enough_flag") == 'none')))).\
                           withColumn("check", sf.lit("Y")).\
                           withColumn("Check_Final", sf.when((col("latest_date_diff_flag") == 'none') & (col("data_enough_flag") == 'none')
                                                             & (col("discont_flag") == 'discontinued'), "Item Possibly Discontinued: large decline in sales").
                                      otherwise(sf.when((col("latest_date_diff_flag") == 'none') & (col("data_enough_flag") == 'none'),
                                                        "Unfit for Modelling: large gaps in sales").otherwise("Do not Check"))).\
                           withColumn("Comment", sf.when((col("latest_date_diff_flag") == 'stopped_selling'), "Item Discontinued: no recent sales").\
                                      otherwise(sf.when(~((col("Check_Final") == 'Do not Check')), col("Check_Final")).\
                                      otherwise(sf.when((col("data_enough_flag") == 'not sold enough'), "Unfit for Modelling: insufficient sales history").\
                                      otherwise("Unfit for Modelling: Intermittent Data Missing"))))

#get the unique cases where there is issue and merge them to create flag
discontinued = master_discontinued_flag.select("trn_item_sys_id").rdd.flatMap(lambda x: x).collect()

master_discontinued_flag_opposite = master_weekly_gap.select(['site_bu','trn_item_sys_id_main','max_date','latest_week', \
                                                               'latest_date_diff_flag', 'consecutive_flag']).\
                                                      filter(~sf.col("trn_item_sys_id_main").isin(discontinued)).\
                                                      withColumn("Comment", sf.lit("goodstuff")).distinct().\
                                                      withColumn("trn_item_sys_id", sf.col("trn_item_sys_id_main"))

if item_sales_share_new.count()-master_discontinued_flag.count()-master_discontinued_flag_opposite.count() == 0:
  print('ok')
else:
  raise ValueError('rows in output table not matching rows in input table')

# COMMAND ----------

output1_1= item_sales_share_new.join(master_discontinued_flag,['site_bu','trn_item_sys_id'],'inner')
output1_1 = output1_1.select("site_bu","trn_item_sys_id","item_category","item_category_group",\
                            "item_subcategory","item_name", "item_manufacturer", "item_brand","sales_quantity","sales_amount",\
                            "group_sales_amount","sales_share","promo_sales_amount","rank","cumulative_sales_perc",\
                            "cum_sales_flag","max_date","latest_week","latest_date_diff_flag","consecutive_flag",\
                            "discont_flag","data_enough_flag","check","Check_Final","Comment")

output1_2= item_sales_share_new.join(master_discontinued_flag_opposite,['site_bu','trn_item_sys_id'],'inner').\
                                withColumn("discont_flag", sf.lit("none")).\
                                withColumn("data_enough_flag", sf.lit("none")).\
                                withColumn("check", sf.lit("N")).\
                                withColumn("Check_Final", sf.lit("goodstuff"))

second_df = output1_2.select("site_bu","trn_item_sys_id","item_category","item_category_group",\
                            "item_subcategory","item_name", "item_manufacturer", "item_brand","sales_quantity","sales_amount",\
                            "group_sales_amount","sales_share","promo_sales_amount","rank","cumulative_sales_perc",\
                            "cum_sales_flag","max_date","latest_week","latest_date_diff_flag","consecutive_flag",\
                            "discont_flag","data_enough_flag","check","Check_Final","Comment")
output1 = output1_1.union(second_df)

if output1.count()- item_sales_share_new.count() == 0:
  print('ok')
else:
  raise ValueError('rows in output table not matching rows in input table')

# COMMAND ----------

# getting the weekly sales data of the flagged items
weekly_charts_flagged_items = master_BU_cat_site_item_sales.\
                                  join(master_discontinued_flag, ['site_bu','trn_item_sys_id_main'], "left").\
                                  filter(sf.col("check") == "Y").\
                                  drop("latest_week" ,"check").cache()

# COMMAND ----------

# dying & slow start series
transactions2 = sqlContext.sql("""SELECT site_bu,trn_transaction_date,trn_site_sys_id,trn_item_sys_id,\
                               item_category_group,item_category,item_subcategory,\
                               sum(trn_sold_quantity) as quantity_sold,sum(trn_gross_amount) as revenue from rel_transactions\
                               group by site_bu,trn_transaction_date,trn_site_sys_id,trn_item_sys_id,\
                               item_category_group,item_category,item_subcategory""")

year_flag= transactions2.selectExpr("trn_item_sys_id","item_category","item_subcategory", "trn_transaction_date"  , "year(trn_transaction_date) as year" )
year_flag=year_flag.withColumn("week",weekofyear(year_flag.trn_transaction_date))

# roll up dates to item-level
# define latest_sales, earliest_sales, days_sold, weeks_sold
various_dates= year_flag.groupBy("trn_item_sys_id","item_category","item_subcategory").\
                          agg(sf.min("trn_transaction_date").alias('earliest_sales'), \
                              sf.max("trn_transaction_date").alias('latest_sales'), \
                              sf.countDistinct("trn_transaction_date").alias('days_sold'), \
                              sf.countDistinct("week","year").alias('weeks_sold'))

# join items with various dates
dates_joined= transactions2.join(various_dates, ["trn_item_sys_id","item_category","item_subcategory"], "left")

# roll up stores and dates to item-level
total_stores_table=dates_joined.agg(sf.countDistinct("trn_site_sys_id").alias('total_stores')).withColumn('for_join',sf.lit(1))

sufficient_stores_1= dates_joined.groupBy("trn_item_sys_id","item_category","item_subcategory","latest_sales", "earliest_sales","days_sold", "weeks_sold").\
                                  agg(sf.countDistinct("trn_site_sys_id").alias('stores_selling_this_item')).\
                                  withColumn('for_join',sf.lit(1))

sufficient_stores_2=sufficient_stores_1.join(total_stores_table,sufficient_stores_1["for_join"]==total_stores_table["for_join"],'cross').drop('for_join')
sufficient_stores_2=sufficient_stores_2.withColumn("percent_stores_sold",col("stores_selling_this_item")/col("total_stores"))

# roll up stores to item-day-level
item_day=dates_joined.join(sufficient_stores_2,["trn_item_sys_id",\
                                                 "item_category",\
                                                 "item_subcategory",\
                                                 "latest_sales",\
                                                 "earliest_sales",\
                                                 "days_sold",\
                                                 "weeks_sold"],'left').groupby("trn_item_sys_id",\
                                                                               "trn_transaction_date",\
                                                                               "item_category",\
                                                                               "item_subcategory",\
                                                                               "latest_sales",\
                                                                               "earliest_sales",\
                                                                               "days_sold",\
                                                                               "weeks_sold",\
                                                                               "stores_selling_this_item",\
                                                                               "total_stores",\
                                                                               "percent_stores_sold").agg(sf.sum("quantity_sold").alias("sum_units_sold"),\
                                                                                                         sf.sum("revenue").alias("sum_revenue"))

# COMMAND ----------

# create 4 date flags
# defines 64 weeks ago to 52 weeks ago as previous_12_weeks
# defines 12 weeks ago to today as recent_12_weeks
# defines the initial 12 weeks an item was sold as first_12_weeks
# defines the 52 to 64 weeks after an item was first sold as later_12_weeks
date_flags= item_day.select("trn_transaction_date",\
                              "trn_item_sys_id",\
                              "item_category",\
                              "item_subcategory",\
                              "sum_units_sold",\
                              "sum_revenue",\
                              "latest_sales",\
                              "earliest_sales",\
                              "days_sold",\
                              "weeks_sold",\
                              "stores_selling_this_item",\
                              "total_stores",\
                              "percent_stores_sold")\
.withColumn("start_date",sf.lit(start_date))\
.withColumn("end_date",sf.lit(end_date))\
.withColumn("start_date",to_date(col('start_date'),'yyyy-MM-dd'))\
.withColumn("end_date",to_date(col('end_date'),'yyyy-MM-dd'))\
.withColumn('previous_12_weeks',\
            when(col('trn_transaction_date')>date_sub(col('end_date'),449),\
                 when(col('trn_transaction_date')<date_sub(col('end_date'),365),1).otherwise(0)).\
            otherwise(0))\
.withColumn('recent_12_weeks',\
            when(col('trn_transaction_date')>date_sub(col('end_date'),84),1).otherwise(0))\
.withColumn('first_12_weeks',\
            when(col('trn_transaction_date')>col('start_date'),\
                 when(col('trn_transaction_date')<date_add(col('start_date'),84),1).otherwise(0)).\
            otherwise(0))\
.withColumn('later_12_weeks',\
            when(col('trn_transaction_date')>date_add(col('start_date'),365),\
                 when(col('trn_transaction_date')<date_add(col('start_date'),449),1).otherwise(0)).\
            otherwise(0))

# COMMAND ----------

# create 3 new columns based on dates and 2 ratios from 4 date flags
# defines most recent day an item was sold as latest_sales
# defines the earliest day an item was sold as earliest_sales
# defines the number of days on which an item was sold as days_sold
# defines the number of weeks on which an item was sold as weeks_sold
# defines the total days between first day an item was sold to the last day it was sold as sales_length
# defines the percentage of days sold out of total days for sale as days_sold_percent
# defines the percentage of weeks sold out of total days for sale as weeks_sold_percent
# defines volume a year and 12 weeks ago divided by volume 12 weeks ago to today as dying_ratio
# defines volume the initial 12 weeks an item was sold divided by volume the same 12 weeks a year later as slow_start_ratio
ratios_df_1= date_flags.select("trn_item_sys_id",\
                               "item_category",\
                               "item_subcategory",\
                               "latest_sales",\
                               "earliest_sales",\
                               "days_sold",\
                               "weeks_sold",\
                               "percent_stores_sold",\
                               "sum_revenue",\
                              "sum_units_sold",\
                              "recent_12_weeks",\
                              "previous_12_weeks",\
                              "first_12_weeks",\
                              "later_12_weeks",
                              "start_date",
                              "end_date")\
.withColumn('drn',col('sum_units_sold')*col('recent_12_weeks'))\
.withColumn('drd',col('sum_units_sold')*col('previous_12_weeks'))\
.withColumn('ssn',col('sum_units_sold')*col('first_12_weeks'))\
.withColumn('ssd',col('sum_units_sold')*col('later_12_weeks'))\
.withColumn('days_since_last_sale',datediff(col('end_date'),col('latest_sales')))\
.withColumn('sales_length',datediff(col('latest_sales'), col('earliest_sales')))\
.withColumn('days_sold_percent',col('days_sold')/(datediff(col('end_date'),col('start_date'))+1))\
.withColumn('weeks_sold_percent',col('weeks_sold')/(((datediff(col('end_date'),col('start_date'))+1)/7)+2))    
            
#drn = dying ratio numerator
#drd = dying ration denominator
#ssn = slow start series numerator
#ssr = slow start series denominator

# COMMAND ----------

ratios_df_2=ratios_df_1.groupby("trn_item_sys_id",\
                               "item_category",\
                               "item_subcategory",\
                               "latest_sales",\
                               "earliest_sales",\
                               "days_sold",\
                               "weeks_sold",\
                               "percent_stores_sold")\
.agg(sf.sum('sum_revenue').alias('sum_revenue')\
     ,sf.max('days_since_last_sale').alias('days_since_last_sale')\
     ,sf.sum('drn').alias('drn')\
     ,sf.sum('drd').alias('drd')\
     ,sf.sum('ssn').alias('ssn')\
     ,sf.sum('ssd').alias('ssd')\
     ,sf.max("sales_length").alias('sales_length')\
     ,sf.max("days_sold_percent").alias('days_sold_percent')\
     ,sf.max("weeks_sold_percent").alias('weeks_sold_percent'))\
.withColumn("dying_ratio",col('drn')/col('drd'))\
.withColumn("slow_start_ratio",col('ssn')/col('ssd'))

# COMMAND ----------

# create 5 flags based on 5 ratios
# define items that last sold in 30 days or less as sold_in_last_30_days
# define items whose 12-week volume dropped 75% or more in the last year as dying_last_12_months
# define items whose 12-week volume increased at least fourfold in the first year as slow_start_first_12_months
# define items with less than 52 weeks of sales history as sold_for_less_than_a_year
# define items that were sold less than 25% of the time as sold_less_than_quarter_of_days
class_flags = ratios_df_2\
.withColumn("sold_recently",when(col("days_since_last_sale")<=days_since_last_sold,1).otherwise(0))\
.withColumn("dying_last_12_months",when(col("dying_ratio")<dying_series_ratio,1).otherwise(0))\
.withColumn("slow_start_first_12_months",when(col("slow_start_ratio")<slow_start_ratio,1).otherwise(0))\
.withColumn("sold_few_years",when((col("sales_length")*4/365)<min_qtr,1).otherwise(0))\
.withColumn("sold_few_weeks",when(col("weeks_sold_percent")<proportion_weeks_sold,1).otherwise(0))\
.withColumn("sold_few_stores",when(col("percent_stores_sold")<percent_of_stores,1).otherwise(0))

# COMMAND ----------

eligible = class_flags.withColumn("Comments2",when((col("sales_length")*4/365)<min_qtr,"Unfit for Modelling: insufficient sales history").\
                       otherwise(when(col("dying_ratio")<dying_series_ratio,"Unfit for Modelling : dying series").\
                       otherwise(when(col("slow_start_ratio")<slow_start_ratio,"Unfit for Modelling : slow start series").\
                       otherwise(when(col("weeks_sold_percent")<proportion_weeks_sold,"Unfit for Modelling: % of weeks sold is low").\
                       otherwise(when(col("percent_stores_sold")<percent_of_stores,"Unfit for Modelling:sold in very few stores").\
                       otherwise("goodstuff"))))))

# COMMAND ----------

output2= output1.join(eligible.drop(col('item_category')).drop(col('item_subcategory')),['trn_item_sys_id'],'left')

# COMMAND ----------

output3 = output2.withColumn("comments_trn",when(col('latest_date_diff_flag')=="stopped_selling","Item Discontinued: no recent sales").\
                otherwise(when((col("sales_length")*4/365)<min_qtr,"Unfit for Modelling: insufficient sales history").\
                otherwise(when(col("dying_ratio")<dying_series_ratio,"Unfit for Modelling : dying series ").\
                otherwise(when(col("slow_start_ratio")<slow_start_ratio,"Unfit for Modelling : slow start series ").\
                otherwise(when(col("weeks_sold_percent")<proportion_weeks_sold,"Unfit for Modelling: % of weeks sold is low").\
                otherwise(when(col('consecutive_flag')=='missing_consecutive_weeks',"Unfit for modelling: missing consecutive weeks").\
                otherwise(when(col("percent_stores_sold")<percent_of_stores,"Investigate: Item sold in very few stores").\
                otherwise("goodstuff"))))))))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Incorporate Long term - Short Term exclusions 

# COMMAND ----------

#user input
#Read in the file containing Long term & short term exclusions : usually located in LP proram teams/bu
# For Phase 4 BUs testing, This is a dummy file currently with same metadata as we do not have long term & short term exclusion file for Phase 4 BUs
if business_units == "Ireland":
  exclusions = spark.read.csv(long_short_exclusions, header=True,inferSchema=True).\
  withColumnRenamed("is.excluded","lt_excl_prev_refresh").\
  withColumnRenamed("is.excluded.short.term","st_excl_prev_refresh").\
  withColumnRenamed("is.discountinued","discont_prev_refresh")
  output4 = output3.join(exclusions.select(['trn_item_sys_id','lt_excl_prev_refresh','discont_prev_refresh','st_excl_prev_refresh','Comment']).\
                  withColumnRenamed("Comment", "comments_exclusions"),['trn_item_sys_id'],'left')
else:
  output4 = output3.withColumn("lt_excl_prev_refresh", sf.lit('N')).\
  withColumn("discont_prev_refresh", sf.lit('N')).\
  withColumn("st_excl_prev_refresh", sf.lit('N')).\
  withColumn("comments_exclusions", sf.lit('Not excluded'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Incorporate inscope file from the last refresh

# COMMAND ----------

scoped_items = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
scoped_items = scoped_items.filter(col("BU") == business_units)
scoped_items = scoped_items.withColumn('product_key_updated', when(col('modelling_level')=='EAN', col('UPC')).otherwise(col('product_key')))
scoped_items = scoped_items.groupBy('product_key_updated', 'item_number', 'UPC', 'Scope', 'department_name', 'category_name', 'item_name', 'price_family', 'sell_unit_qty', 'wave', 'date_live', 'BU').agg(sf.max('last_update'))
scoped_items = scoped_items.drop('max(last_update)').withColumnRenamed("product_key_updated","trn_item_sys_id").withColumnRenamed("Scope","inscope_prev_refresh")

# COMMAND ----------

output5 = output4.join(scoped_items.select(['trn_item_sys_id','inscope_prev_refresh']),['trn_item_sys_id'],'left')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create and save final output

# COMMAND ----------

output6=output5.select(["trn_item_sys_id",\
                        "site_bu",\
                        "item_category",\
                        "item_category_group",\
                        "item_subcategory",\
                        "item_name",\
                        "item_manufacturer",\
                        "item_brand",\
                        "sales_quantity",\
                        "sales_amount",\
                        "group_sales_amount",\
                        "sales_share",\
                        "promo_sales_amount",\
                        "rank",\
                        "max_date",\
                        "latest_week",\
                        "latest_date_diff_flag",\
                        "consecutive_flag",\
                        "discont_flag",\
                        "data_enough_flag",\
                        "Comment",\
                        "latest_sales",\
                        "earliest_sales",\
                        "days_sold",\
                        "weeks_sold",\
                        "percent_stores_sold",\
                        "sum_revenue",\
                        "days_since_last_sale",\
                        "drn",\
                        "drd",\
                        "ssn",\
                        "ssd",\
                        "sales_length",\
                        "days_sold_percent",\
                        "weeks_sold_percent",\
                        "dying_ratio",\
                        "slow_start_ratio",\
                        "sold_recently",\
                        "dying_last_12_months",\
                        "slow_start_first_12_months",\
                        "sold_few_years",\
                        "sold_few_weeks",\
                        "sold_few_stores",\
                        "lt_excl_prev_refresh",\
                        "discont_prev_refresh",\
                        "st_excl_prev_refresh",\
                        "comments_exclusions",\
                        "inscope_prev_refresh",\
                        "cumulative_sales_perc",\
                        "cum_sales_flag",\
                        "comments_trn"]).\
withColumnRenamed("Comment","comments_initial").\
withColumn("final_recommendation", when(col('lt_excl_prev_refresh')=='Y','LT exclusion item in last refresh').\
                                   otherwise(when(col('st_excl_prev_refresh')=='Y','ST exclusion item in last refresh').\
                                   otherwise(col('comments_trn'))))

if output6.count()-output2.count() == 0:
  print('ok')
else:
  raise ValueError('rows in output table not matching rows in input table')

# COMMAND ----------

with pd.ExcelWriter('output.xlsx') as writer:
    output6.toPandas().to_excel(writer, sheet_name='For Review', index=False)
move('output.xlsx', '/dbfs/{0}/Elasticity_Modelling/{1}/{2}_Repo/Input/{3}_item_scoping_data_{4}.xlsx'.format(phase, wave, bu_code, bu_code.lower(), wave.lower()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate output xlsx file

# COMMAND ----------

df = spark.createDataFrame(pd.read_excel('/dbfs/{0}/Elasticity_Modelling/{1}/{2}_Repo/Input/{3}_item_scoping_data_{4}.xlsx'.format(phase, wave, bu_code, bu_code.lower(), wave.lower()), sheet_name = 'For Review')) 
if df.count()-output6.count() == 0:
  print('ok')
else :
  raise ValueError('output not written out properly')
