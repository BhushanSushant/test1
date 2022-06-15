# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Creator: Taru Vaid, Jagruti Joshi, Sterling Fluharty
# Date Created: 14/12/2020
# Date Updated: 29/12/2020

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
# 1. Transaction data : phase3_extensions.master_merged_ads_may2021_refresh
# 2. Long term /short term exclusion details for the previous refresh (check with manager/accenture)
# 3. Inscope file for the previous refresh 

# Output file: {bu}_item_scoping_data_final.csv
#(to be shared with manager & CM to finalize scope for this refresh)

# Recommendations:
# 1. Please use atleast 449 days of the data for this code to flag items correctly. Its best to use 1.5 or more yrs of data if possible

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Functions

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
#from functools import reducea
import databricks.koalas as ks
import pandas as pd

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
spark.conf.set("spark.databricks.io.cache.enabled", "false")

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# Define parameters used in notebook
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

# Define cutoff metrics - Converted widgets to parameters
percent_of_stores = dbutils.widgets.get("percent_of_stores")
days_since_last_sold = dbutils.widgets.get("days_since_last_sold")
dying_series_ratio = dbutils.widgets.get("dying_series_ratio")
slow_start_ratio = dbutils.widgets.get("slow_start_ratio")
proportion_weeks_sold = dbutils.widgets.get("proportion_weeks_sold")
week_gap_allowed = dbutils.widgets.get("week_gap_allowed")
min_qtr = dbutils.widgets.get("min_qtr")
cumulative_sales_perc= dbutils.widgets.get("cumulative_sales_perc")
tolerance_qtrs= dbutils.widgets.get("tolerance_qtrs")
tolerance_last_yr = dbutils.widgets.get("tolerance_last_yr")
#inscope_file_last_refresh = dbutils.widgets.get("inscope_file_last_refresh")

# Define the database for tables and directory paths for input & output files
db = dbutils.widgets.get("db")
file_directory = dbutils.widgets.get("file_directory")
wave = dbutils.widgets.get("refresh")

# Define the input files
# Long term & Short term exclusions file
long_short_exclusions = "/{0}/{1}/{2}_Repo/Input/CE_Exclusion_June2021_Refresh.csv".format(file_directory, wave, bu_code)
long_short_exclusions
#long_short_exclusions = '/dbfs/Module_testing/Elasticity_Modelling/testing/CE_Repo/Input/CE_Exclusion_June2021_Refresh.csv' #currently, dummy created for phase 4 testing   ??????????????????????????????

# /dbfs/Module_testing/Elasticity_Modelling/testing/CE_Repo/Input/CE_Exclusion_June2021_Refresh.csv
# Circle_K_Scope_Determination_{2}_BU_CM_Inputs_vUpload_dbfs.csv
# inscope file for the last refresh
# inscope_file_last_refresh = '/dbfs/{0}/{1}/Final_Inscope_All_BU.csv'.format(file_directory, wave) #Use for Phase 3
#inscope_file_last_refresh = dbutils.widgets.get("inscope_file_last_refresh")

# Scope file from Niel/ Frances
#inscope_file_new = dbutils.widgets.get("inscope_file_new")#'/{0}/{1}/{2}_Repo/Modelling_Inputs/{2}_Scope_List_{1}.csv'.format(file_directory, wave, bu_code) #currently, dummy created for phase 4 testing

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Widgets

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

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
# dbutils.widgets.text("02.Start Date", "2019-03-01")
# dbutils.widgets.text("03.End Date", "2020-05-31")

# # define widgets for additional parameters that also apply filters to the list of items
# dbutils.widgets.text("04.Prop. Stores Selling", ".65")
# dbutils.widgets.text("05.Recent Days With No Sales", "30") ##how many days of gap from latest week of data allowed
# dbutils.widgets.text("06.Ratio for Dying Series", "0.25")
# dbutils.widgets.text("07.Ratio for Slow Start", "0.25")
# dbutils.widgets.text("08.Min Prop. Weeks Sold", "0.98")
# dbutils.widgets.text("09.Consc.Gap in Sales(days)", "14") #how many days of gap allowed b/w consecutive weekly data points
# # dbutils.widgets.text("10.Qtr to Qtr Sales Decline", "0.5") # qtr to qtr what % decpreciation allowed 
# dbutils.widgets.text("10.Min Qtrs of Sales History", "4") # no of qtrs it should atleast sell
# dbutils.widgets.text('11.Dept Sales Cutoff',"0.8")  ## top 80%


# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading the Widget Parameters

# COMMAND ----------

# #date ranges
# start_date = getArgument('02.Start Date')
# end_date = getArgument('03.End Date')

# #Apply BU 

# business_units = dbutils.widgets.get("01.Business Unit")
# business_units

# #get bu 2 character code
# bu_abb_ds = sqlContext.createDataFrame(bu_abb, ['bu', 'bu_abb']).filter('bu = "{}"'.format(business_units))
# bu_code = bu_abb_ds.collect()[0][1]

# print("Start Date:", start_date, "; End Date:", end_date, "; BU_Abb:", bu_code, "; Business Unit:", business_units)

# COMMAND ----------

# # declare variables for those additional parameters
# percent_of_stores = getArgument('04.Prop. Stores Selling')
# days_since_last_sold = getArgument('05.Recent Days With No Sales')#how many days of gap from latest week of data allowed
# dying_series_ratio = getArgument('06.Ratio for Dying Series')
# slow_start_ratio = getArgument('07.Ratio for Slow Start')
# proportion_weeks_sold = getArgument('08.Min Prop. Weeks Sold')

# ##accetnures paramters
# week_gap_allowed = getArgument('09.Consc.Gap in Sales(days)')
# # tolerance_qtrs = getArgument('10.Qtr to Qtr Sales Decline')

# min_qtr = getArgument('10.Min Qtrs of Sales History')  
# cumulative_sales_perc= getArgument('11.Dept Sales Cutoff')  

# tolerance_qtrs=0.5
# tolerance_last_yr = 1 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rolling Up the Transaction Table to Create the Scoping Raw Data

# COMMAND ----------

# Set up the categories that we know won't be in scope so we can filter them out
drop_categories_nacs= "'Lottery/Gaming (Commissions/Fee Income Only)','Store Services (Other Income - Fee-based Only) Not a merchandise category','Publications'"

drop_categories = "'300-INSTANT LOTTERY SALES', '203-PREPAID MC', '237-MONEY ORDER', '224-CHARITY - UCP', '201-PREPAID INTERNET CARDS', '200-WIRELESS & PIN', '301-LOTTERY MACHINE', '202-3RD PARTY GIFT CARDS', '218-CIRCLE K GIFT CARD', '236-ONLINE LOTTO SALES', '209-POSTAGE STAMPS', '204-BUS PASSES', '240-CK MEMBERS DISCOUNT', '229-CW - CASH ACCEPTOR', '215-DEBIT CASH BACK', 'Hardees', 'Subway'"

# This one is a weird rogue item_description that is in several environments but has no category so it was missed with the above
item_drop = "'Money Order'"

transactions_query = """
                        SELECT MT.*,
                        product.item_desc as item_name,
                          product.nacs_category_desc,
                          product.department_desc,
                          product.category_desc,
                          product.sub_category_desc,
                          product.manufacturer_desc,
                          product.brand_desc,
                          product.sell_unit_qty,
                          product.package_desc,
                          product.item_number,
                          product.private_brand_desc,
                          product.size_unit_of_measure,
                          product.UPC_Sell_Unit_Desc,
                          site.division_id,
                          site.site_number_corporate,
                          site.site_state_id,
                          CASE WHEN MT.promotion_description = "Regular Price" THEN 0 ELSE 1 end as Promo_Flag,
                          CASE WHEN division_id=3100 THEN "3100 - Grand Canyon Division"  end as BU
                        FROM dl_localized_pricing_all_bu.merchandise_item_transactions as MT
                        LEFT JOIN dl_localized_pricing_all_bu.site
                        ON MT.site_env_key == site.site_env_key
                        LEFT JOIN dl_localized_pricing_all_bu.product
                        ON MT.prod_item_env_key == product.product_item_env_key
                          WHERE division_id=3100 AND business_date BETWEEN '{0}' and '{1}'
                          AND sales_amount > 0 
                          AND quantity_sold > 0
                           AND MT.sys_environment_name = 'Tempe' AND nacs_category_desc NOT IN ({2}) AND category_desc NOT IN ({3}) AND item_desc != {4}
                    """.format(start_date,end_date,drop_categories_nacs, drop_categories, item_drop)


#filtering transactions for inscope analysis
########################################## Insert {0}.{1}_master_merged_ads_{2} instead of table hard coded below
if business_units!="3100 - Grand Canyon Division":
  transactions = sqlContext.sql(""" SELECT * FROM {0}.{1}_master_merged_ads_{2} 
                                    WHERE BU = '{3}'
                                    AND business_date >= '{4}' AND business_date <= '{5}'
                                    AND sales_amount > 0
                                    AND quantity_sold > 0
                                    AND tempe_product_key is not null                                 
                                """.format(db, bu_code.lower(), wave.lower(), business_units, start_date, end_date))
  print("business unit is not GR")
else :
  transactions = sqlContext.sql(transactions_query).\
                            withColumnRenamed("product_key", "tempe_product_key").\
                            withColumnRenamed("item_description", "item_desc").\
                            withColumnRenamed("prod_item_env_key", "tempe_prod_item_env_key")
  print("business unit is  GR")

transactions.createOrReplaceTempView("transactions")

#make sure the {db}.{bu}_master_merged_ads_{wave} has been updated before running this file

# COMMAND ----------

#validation check : if using master merged, make sure its updated before you run this
display(sqlContext.sql("select min(business_date),max(business_date)  from transactions"))

# COMMAND ----------

# %sql
# -- watchout for high revenue categories which sell in limited stores.
# select a.*,b.Acc_Cat from
#     (select category_desc, 
#             count(distinct site_number_corporate) sites_selling_this_category,
#             (select count(distinct site_number_corporate) from transactions) total_sites  
#     from transactions  
#     group by category_desc 
#     order by count(distinct site_number_corporate)) a
#     left join (select * 
#               from localized_pricing.acc_cat) b
# on a.category_desc==b.category_desc
# where Acc_Cat not in ("Out of Scope")

# COMMAND ----------

##item desc correction
sqlContext.sql(""" Select tempe_product_key, item_desc, max(business_date) as max_date 
                   FROM transactions 
                   GROUP BY tempe_product_key, item_desc 
                  """).createOrReplaceTempView("item_name_product_key_mapping")

sqlContext.sql(""" SELECT tempe_product_key, item_desc 
                   FROM ( select ROW_NUMBER() OVER(PARTITION BY tempe_product_key ORDER BY max_date DESC) as dups, * from item_name_product_key_mapping ) a 
                   where a.dups=1 
                  """).createOrReplaceTempView('single_item_name_product_key_mapping')

transactions_fixed = sqlContext.sql(""" select a.*, b.item_desc item_desc2 
                                        from transactions a 
                                        join single_item_name_product_key_mapping b 
                                        on a.tempe_product_key = b.tempe_product_key""")

transactions_fixed.createOrReplaceTempView("transactions_fixed")

# COMMAND ----------

# transactions.count() - transactions_fixed.count() 

# COMMAND ----------

# Filtering the Relevant Data
# Pull transactions for selected business unit for selected dates in widgets and Rolling up the Data at Item Level

rolled_up_scoping_data = sqlContext.sql("""
                               SELECT BU, department_desc, category_desc, sub_category_desc, brand_desc, package_desc, size_unit_of_measure
                                      ,upc, sell_unit_qty, item_desc2 as item_desc
                                      ,manufacturer_desc, tempe_prod_item_env_key, tempe_product_key
                                      ,SUM(quantity_sold) as sales_quantity
                                      ,SUM(sales_amount) as sales_amount
                               FROM transactions_fixed
                               GROUP BY BU, department_desc, category_desc, sub_category_desc, brand_desc, package_desc, size_unit_of_measure
                                        ,upc, sell_unit_qty, item_desc2
                                        ,manufacturer_desc, tempe_prod_item_env_key, tempe_product_key
                                      """.format(bu_code.lower(),business_units,start_date,end_date))

rolled_up_promo_data = sqlContext.sql("""
                               SELECT BU, department_desc, tempe_prod_item_env_key, tempe_product_key
                                      ,SUM(sales_amount) as promo_sales_amount
                               FROM transactions_fixed
                               WHERE promotion_id != 0
                               GROUP BY BU, department_desc, tempe_prod_item_env_key, tempe_product_key
                                      """)

# COMMAND ----------

# Category Group Level Total Sales
group_level_sales = rolled_up_scoping_data.\
                      select("BU", "department_desc", "sales_amount").\
                      groupBy("BU", "department_desc").\
                      agg(sf.sum("sales_amount").alias("dept_sales_amount"))

# Creating the Cumulative Sales Percentage Column for at Department Level
item_sales_share = rolled_up_scoping_data.\
                      join(group_level_sales, ["BU", "department_desc"], "left").\
                      join(rolled_up_promo_data, ["BU", "department_desc",  "tempe_prod_item_env_key", "tempe_product_key"], "left").\
                      select("BU", "department_desc", "category_desc", "sub_category_desc", "item_desc", "manufacturer_desc", "brand_desc", 
                              "package_desc", "size_unit_of_measure", "upc", "sell_unit_qty", "tempe_prod_item_env_key", "tempe_product_key", 
                              "sales_quantity", "sales_amount", "dept_sales_amount",
                              (sf.col("sales_amount")/sf.col("dept_sales_amount")).alias("sales_share"), "promo_sales_amount").\
                      withColumn("rank", sf.row_number().over(Window.partitionBy("department_desc").\
                                                              orderBy(sf.col("sales_amount").desc()))).\
                      withColumn("cumulative_sales_perc", sf.sum("sales_share").over(Window.partitionBy("department_desc").\
                                                                                    orderBy(sf.col("sales_amount").desc()))).\
                      orderBy(['department_desc','rank'], ascending=[1,1])

# COMMAND ----------

item_sales_share_new = item_sales_share.join(sqlContext.sql("select distinct category_desc, Acc_Cat from localized_pricing.acc_cat"), \
                                             ["category_desc"], "left").\
                                        withColumn("cum_sales_flag",sf.when((col("cumulative_sales_perc")-col("sales_share")) <= cumulative_sales_perc, \
                                                                       "within top " + str(float(cumulative_sales_perc)*100) + "% of sales").\
                                                                       otherwise("not in the top " + str(float(cumulative_sales_perc)*100) + "% of sales"))

# display(item_sales_share_new)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales Discontinuation Flag for the Top 80% Items in each of the Category Group

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Weekly Tables for each tempe_prod_item_env_key

# COMMAND ----------

# Filtering the Relevant Data and Creating a Temporary Table 'transaction_data'
sqlContext.sql("""
      SELECT BU, department_desc, category_desc, sub_category_desc, item_desc2 as item_desc, manufacturer_desc, tempe_prod_item_env_key
                                      ,tempe_product_key, business_date
                                      ,quantity_sold, sales_amount
                               FROM transactions_fixed  """).createOrReplaceTempView("transaction_data")

# Defining the Grouping Columns for Rolling the Data from Daily to Weekly Level
grouping_cols=["BU", "department_desc", "category_desc", "sub_category_desc", "item_desc", "manufacturer_desc", 
               "tempe_prod_item_env_key", "tempe_product_key", "week_start_date"]

merged_table = "transaction_data"

# Rolling up the Data at Item x Week Level
master_BU_cat_site_item_sales = sqlContext.sql("select * from {}".format(merged_table)).\
                      withColumn("week_no",sf.dayofweek("business_date")).\
                      withColumn("adj_factor", sf.col("week_no") - 4).\
                      withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
                      withColumn("week_start_date", sf.expr("date_sub(business_date, adj_factor)")).\
                      groupBy(*grouping_cols).\
                      agg(sf.sum("quantity_sold").alias("qty"),
                          sf.sum("sales_amount").alias("rev"))

# display(master_BU_cat_site_item_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Flagging the Items for Discontinuity on the Basis of Various Logics Defined Below 

# COMMAND ----------

# create quarter flagging
master_weekly = master_BU_cat_site_item_sales.\
                filter((sf.col("qty") > 0) & (sf.col("rev") > 0)).\
                groupBy(['BU','tempe_prod_item_env_key','tempe_product_key','week_start_date']).\
                agg(sf.sum("qty").alias("qty"),
                    sf.sum("rev").alias("rev")).\
                withColumn("row_num",sf.row_number().over(Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']).\
                                                          orderBy(sf.desc("week_start_date")))).\
                withColumn("qtr_start", sf.col("row_num") % 12).\
                withColumn("qtr_start_ind",sf.when(sf.col("qtr_start") == 1,1).otherwise(0)).\
                withColumn('quarter', sf.sum('qtr_start_ind').\
                           over((Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']).orderBy(sf.desc('week_start_date')).\
                                 rangeBetween(Window.unboundedPreceding, 0)))).\
                orderBy(['BU','tempe_prod_item_env_key','tempe_product_key','week_start_date'], ascending=True)

# COMMAND ----------

#now logic for discontinued -
# 1. Latest week vs latest day in its sale history if there is gap of more than 30 days
# 2. If the last qtr vs previous qtr is more than 100% decline and also latest qtr vs last year same qtr needs to be not a similar decline [to take care of seasonality]
# 3. if there are not consecutive week's in ones sales flags as inconsistent series
# 4. Also based on moving average and last year same qtr case - if there are holes in the series for stockout - not checked for this - too high level - consecutive gap good proxy

master_weekly_gap = master_weekly.\
                    withColumn("max_date", sf.max("week_start_date").over(Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']))).\
                    withColumn("latest_week",sf.lit(end_date)).\
                    withColumn("latest_week", sf.to_date(col("latest_week"),"yyyy-MM-dd")).\
                    withColumn("gap_from_latest", sf.datediff(col("latest_week"),col("max_date"))).\
                    withColumn('latest_date_diff_flag',sf.when((col("gap_from_latest")) > days_since_last_sold, "stopped_selling").otherwise("none")).\
                    withColumn("lag_date",
                               sf.lag(sf.col("week_start_date"),1).\
                               over(Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']).orderBy("week_start_date"))).\
                    withColumn("cons_date_diff",sf.datediff(col("week_start_date"),col("lag_date"))).\
                    withColumn("max_cons_diff",
                               sf.max("cons_date_diff").over(Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']))).\
                    withColumn('consecutive_flag',sf.when((col("max_cons_diff")) > week_gap_allowed, "missing_consecutive_weeks").otherwise("none"))

#add extra flag to not check consecutive cases
#where max max gap is 14 - no need to check - simply by changing params
#where the max gap is happenning prior to last year we excuse that
master_weekly_gap_check_consecutive = master_weekly_gap.\
                                      filter((col("consecutive_flag") == 'missing_consecutive_weeks') & ((col("cons_date_diff")) > week_gap_allowed)).\
                                      groupBy(['BU','tempe_prod_item_env_key','tempe_product_key']).\
                                      agg(sf.min("row_num").alias("week_num_from_end")).\
                                      withColumn("consec_update",sf.when(col("week_num_from_end") > 52, "none").otherwise("delete")).\
                                      filter(col("consec_update") == "none").drop("week_num_from_end")

# COMMAND ----------

# 2. If the last qtr vs previous qtr is more than 100% decline and also latest qtr vs last year same qtr needs to be not a similar decline [to take care of seasonality]
# 3.if atleast not sold for 3 qtrs cant be modelled flag

master_dying_down_qtr = master_weekly_gap.\
                    groupBy(['BU','tempe_prod_item_env_key','tempe_product_key','quarter']).\
                    agg(sf.mean("qty").alias("quarterly_qty")).\
                    withColumn("ly_qtr_qty", 
                               sf.lead(sf.col("quarterly_qty"),4).over(Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']).\
                                                                       orderBy("quarter"))).\
                    withColumn("lag_qtr_qty", 
                               sf.lead(sf.col("quarterly_qty"),1).over(Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']).\
                                                                       orderBy("quarter"))).\
                    orderBy(['BU','tempe_prod_item_env_key','tempe_product_key','quarter'], ascending=True).\
                    withColumn("max_qtr", sf.max(sf.col("quarter")).over(Window.partitionBy(['BU','tempe_prod_item_env_key','tempe_product_key']))).\
                    filter(col("quarter") == 1).\
                    withColumn("lag_ratio", col("quarterly_qty")/col("lag_qtr_qty")).\
                    withColumn("ly_ratio", col("quarterly_qty")/col("ly_qtr_qty")).\
                    withColumn("discont_flag", sf.when((col("lag_ratio") <= tolerance_qtrs) 
                                                       & ((col("ly_ratio") <= tolerance_qtrs) 
                                                          & (col("ly_ratio") < tolerance_last_yr)), 'discontinued').otherwise('none')).\
                    withColumn("data_enough_flag", sf.when((col("max_qtr") < min_qtr), 'not sold enough').otherwise('none'))

# COMMAND ----------

#get the unique cases where there is issue and merge them to create flag
master_discontinued_flag = master_weekly_gap.\
                           join(master_dying_down_qtr.\
                                select(['BU','tempe_prod_item_env_key','tempe_product_key','quarter','discont_flag','data_enough_flag']),
                                ['BU','tempe_prod_item_env_key','tempe_product_key','quarter'],'left').\
                           select(['BU','tempe_prod_item_env_key','tempe_product_key','max_date','latest_week','latest_date_diff_flag',
                                   'consecutive_flag','discont_flag','data_enough_flag']).distinct().\
                           dropna().\
                           join(master_weekly_gap_check_consecutive,['BU','tempe_prod_item_env_key','tempe_product_key'],'left').\
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

# COMMAND ----------

#get the unique cases where there is issue and merge them to create flag
discontinued = master_discontinued_flag.select("tempe_product_key").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

master_discontinued_flag_opposite = master_weekly_gap.select(['BU','tempe_prod_item_env_key','tempe_product_key','max_date','latest_week',\
                                                              'latest_date_diff_flag', 'consecutive_flag']).\
                                                        filter(~sf.col("tempe_product_key").isin(discontinued)).\
                                                        withColumn("Comment", sf.lit("goodstuff")).distinct()

# COMMAND ----------

# item_sales_share_new.count()-master_discontinued_flag.count()-master_discontinued_flag_opposite.count() #should be 0

# COMMAND ----------

output1_1= item_sales_share_new.join(master_discontinued_flag, ['BU','tempe_prod_item_env_key','tempe_product_key'], 'inner')
# display(output1_1)

# COMMAND ----------

output1_2= item_sales_share_new.join(master_discontinued_flag_opposite,['BU','tempe_prod_item_env_key','tempe_product_key'],'inner').\
                                withColumn("discont_flag", sf.lit("none")).\
                                withColumn("data_enough_flag", sf.lit("none")).\
                                withColumn("check", sf.lit("N")).\
                                withColumn("Check_Final", sf.lit("goodstuff"))

# COMMAND ----------

second_df = output1_2.select("BU","tempe_prod_item_env_key","tempe_product_key","category_desc","department_desc",\
                            "sub_category_desc","item_desc","manufacturer_desc","brand_desc","package_desc",\
                            "size_unit_of_measure","upc","sell_unit_qty","sales_quantity","sales_amount",\
                            "dept_sales_amount","sales_share","promo_sales_amount","rank","cumulative_sales_perc",\
                            "Acc_Cat","cum_sales_flag","max_date","latest_week","latest_date_diff_flag","consecutive_flag",\
                            "discont_flag","data_enough_flag","check","Check_Final","Comment")
output1 = output1_1.union(second_df)

# COMMAND ----------

# output1.count()- item_sales_share_new.count() #valdiation :should be 0 

# COMMAND ----------

# Getting the Weekly Sales Data of the Flagged Items
weekly_charts_flagged_items = master_BU_cat_site_item_sales.\
                                  join(master_discontinued_flag, ['BU','tempe_prod_item_env_key','tempe_product_key'], "left").\
                                  filter(sf.col("check") == "Y").\
                                  drop("latest_week" ,"check").cache()

weekly_charts_flagged_items.toPandas().\
                            to_csv('/dbfs/{0}/{1}/{2}_Repo/Input/{3}_flagged_items_weekly_sales.csv'.format(file_directory, wave, bu_code, bu_code.lower()), index=False)
#/dbfs/Module_testing/Elasticity_Modelling/testing/CE_Repo/Input

# /dbfs/{0}/{1}/{2}_Repo/Input/{3}_flagged_items_weekly_sales.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dying & Slow Start Series

# COMMAND ----------

transactions2 = sqlContext.sql("""
                               SELECT BU,business_date,site_number_corporate,tempe_product_key,\
                               department_desc,nacs_category_desc,category_desc,sub_category_desc,\
                               sum(quantity_sold) as quantity_sold,sum(sales_amount) as revenue \
                               from transactions_fixed \
                               group by BU,business_date, site_number_corporate, tempe_product_key, \
                               department_desc,nacs_category_desc,category_desc,sub_category_desc """)

# COMMAND ----------

year_flag = transactions2.selectExpr("tempe_product_key", "category_desc", "sub_category_desc", "business_date", "year(business_date) as year" )
year_flag = year_flag.withColumn("week", weekofyear(year_flag.business_date))

# COMMAND ----------

# roll up dates to item-level
# define latest_sales, earliest_sales, days_sold, weeks_sold
various_dates = year_flag.groupBy("tempe_product_key","category_desc","sub_category_desc").\
                         agg(sf.min("business_date").alias('earliest_sales'), \
                             sf.max("business_date").alias('latest_sales'), \
                             sf.countDistinct("business_date").alias('days_sold'), \
                             sf.countDistinct("week","year").alias('weeks_sold'))

# join items with various dates
dates_joined = transactions2.join(various_dates, ["tempe_product_key","category_desc","sub_category_desc"], "left")

# COMMAND ----------

# roll up stores and dates to item-level
total_stores_table = dates_joined.agg(sf.countDistinct("site_number_corporate").alias('total_stores')).\
                                withColumn('for_join',sf.lit(1))

sufficient_stores_1= dates_joined.groupBy("tempe_product_key","category_desc","sub_category_desc","latest_sales", "earliest_sales","days_sold", "weeks_sold").\
                                  agg(sf.countDistinct("site_number_corporate").alias('stores_selling_this_item')).\
                                  withColumn('for_join',sf.lit(1))

sufficient_stores_2=sufficient_stores_1.join(total_stores_table,sufficient_stores_1["for_join"]==total_stores_table["for_join"],'cross').drop('for_join')
sufficient_stores_2=sufficient_stores_2.withColumn("percent_stores_sold",col("stores_selling_this_item")/col("total_stores"))

# COMMAND ----------

# roll up stores to item-day-level
item_day=dates_joined.join(sufficient_stores_2,["tempe_product_key",\
                                                 "category_desc",\
                                                 "sub_category_desc",\
                                                 "latest_sales",\
                                                 "earliest_sales",\
                                                 "days_sold",\
                                                 "weeks_sold"],'left').groupby("tempe_product_key",\
                                                                               "business_date",\
                                                                               "category_desc",\
                                                                               "sub_category_desc",\
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

date_flags= item_day.select("business_date",\
                              "tempe_product_key",\
                              "category_desc",\
                              "sub_category_desc",\
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
            when(col('business_date')>date_sub(col('end_date'),449),\
                 when(col('business_date')<date_sub(col('end_date'),365),1).otherwise(0)).\
            otherwise(0))\
.withColumn('recent_12_weeks',\
            when(col('business_date')>date_sub(col('end_date'),84),1).otherwise(0))\
.withColumn('first_12_weeks',\
            when(col('business_date')>col('start_date'),\
                 when(col('business_date')<date_add(col('start_date'),84),1).otherwise(0)).\
            otherwise(0))\
.withColumn('later_12_weeks',\
            when(col('business_date')>date_add(col('start_date'),365),\
                 when(col('business_date')<date_add(col('start_date'),449),1).otherwise(0)).\
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
ratios_df_1= date_flags.select("tempe_product_key",\
                               "category_desc",\
                               "sub_category_desc",\
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

ratios_df_2=ratios_df_1.groupby("tempe_product_key",\
                               "category_desc",\
                               "sub_category_desc",\
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
class_flags=ratios_df_2\
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

output2= output1.join(eligible.drop(col('category_desc')).drop(col('sub_category_desc')),['tempe_product_key'],'left')

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

output3.toPandas().\
        to_csv('/dbfs/{0}/{1}/{2}_Repo/Input/{3}_item_scoping_data_initial.csv'.format(file_directory, wave, bu_code, bu_code.lower()), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incorporating Long term - Short Term exclusions 

# COMMAND ----------

#user input
#Read in the file containing Long term & short term exclusions : usually located in LP proram teams/bu

# For Phase 4 BUs testing, This is a dummy file currently with same metadata as we do not have long term & short term exclusion file for Phase 4 BUs
exclusions = spark.read.csv(long_short_exclusions, header=True,inferSchema=True).\
                          withColumnRenamed("is.excluded","lt_excl_prev_refresh").\
                          withColumnRenamed("is.excluded.short.term","st_excl_prev_refresh").\
                         withColumnRenamed("is.discountinued","discont_prev_refresh")

# COMMAND ----------

output4 = output3.join(exclusions.select(['BU','tempe_product_key','lt_excl_prev_refresh','discont_prev_refresh','st_excl_prev_refresh','Exclusion Comments']).\
                 withColumnRenamed("Exclusion Comments", "comments_exclusions"),['BU','tempe_product_key'],'left')

# COMMAND ----------

# output4.count()- output3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Incorporating the inscope file form the last refresh

# COMMAND ----------

#scope file in dbfs
#get final inscope itmes (check if this is the correct inscope file for the last refresh, else update it for your BU) 
#picking this file from dbfs, later we incorporate the file given by Neil & Frances
 
scoped_items = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
scoped_items = scoped_items.filter(col("BU") == business_units).withColumn('Scope',sf.lit('Y'))
scoped_items = scoped_items.groupBy("BU",'product_key', 'Scope').agg(sf.max('last_update'))
scoped_items = scoped_items.drop('max(last_update)').withColumnRenamed("product_key","tempe_product_key").withColumnRenamed("Scope","inscope_prev_refresh")

#scoped_items = pd.read_csv(inscope_file_last_refresh)
#scoped_items = spark.createDataFrame(scoped_items)
#scoped_items = scoped_items.filter(col("BU") == business_units).withColumn('inscope_prev_refresh',sf.lit('Y'))
display(scoped_items)

# COMMAND ----------

#user input 
#scope file from Neil/Frances
##scope file from frances( might need some validation)

# For Phase 4 BUs testing, This is a dummy file currently with same metadata
# scoped_items_new = pd.read_csv(inscope_file_new)
# scoped_items_new = spark.createDataFrame(scoped_items_new)

#scoped_items_new = spark.read.csv(inscope_file_new, header=True,inferSchema=True)

#scoped_items_new.createOrReplaceTempView("scoped_items_new")

scoped_items_new = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
scoped_items_new = scoped_items_new.filter(col("BU") == business_units).withColumn('Scope',sf.lit('Y'))
scoped_items_new = scoped_items_new.groupBy("BU",'product_key', 'Scope').agg(sf.max('last_update'))
scoped_items_new = scoped_items_new.drop('max(last_update)').withColumnRenamed("product_key","tempe_product_key")

# display(scoped_items_new)

# COMMAND ----------

# DBTITLE 1,Verify new data in master_merged_ads
# Do this on ADS not product***
## Identify items which don't match with tempe_product_key in master_merged_ads
# Change master_merged_ads according to your BU and Refresh

# display(sqlContext.sql("""select product_key, Scope
#                           from scoped_items_new
#                           where product_key not in
#                               (select distinct tempe_product_key
#                               from {0}.{1}_master_merged_ads_{2}
#                               where tempe_product_key is not null)
#                          """.format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

############## Matching with Product_key for unmatched items
# Matching product_key from Inscope with Product_key from master_merged_ads
# Replace old_product_key with new_product_key in Scope file used for Scoped_items_new
# Change master_merged_ads according to your BU and Refresh

# display(sqlContext.sql("""SELECT Distinct product_key as old_product_key, tempe_product_key as new_product_key
#                           FROM {0}.{1}_master_merged_ads_{2}
#                           WHERE product_key IN
#                                 (SELECT product_key
#                                  FROM 
#                                     (select product_key, Scope 
#                                     from scoped_items_new 
#                                     where product_key not in 
#                                           (select distinct tempe_product_key 
#                                           from {0}.{1}_master_merged_ads_{2} 
#                                           where tempe_product_key is not null)
#                                     )
#                                   WHERE product_key in
#                                       (select distinct product_key
#                                       from {0}.{1}_master_merged_ads_{2}
#                                       where product_key is not null)
#                                   )""".format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

####################### Items not even matched by Product_keys
# Change master_merged_ads according to your BU and Refresh
# The sum of this output and the output for 'Matching with Product_key for unmatched items' should be equal to total unmatched on Tempe_product_key

# display(sqlContext.sql("""select * 
#                           from scoped_items_new 
#                           where product_key not in
#                                 (select distinct product_key
#                                  from {0}.{1}_master_merged_ads_{2}
#                                  where product_key is not null)
#                        """.format(db, bu_code.lower(), wave.lower())))

# COMMAND ----------

# %sql
# -- Checking for InScope products in above table based on the Item Name
# -- Replace the old_product_key with this manually

# SELECT
#   s.division_id,
#   MT.product_key,
#   min(MT.business_date),
#   max(MT.business_date),
#   sum(MT.sales_amount)
# FROM dl_localized_pricing_all_bu.merchandise_item_transactions as MT
#   JOIN dl_localized_pricing_all_bu.site s ON MT.site_env_key == s.site_env_key
#   JOIN dl_localized_pricing_all_bu.product p ON MT.prod_item_env_key == p.product_item_env_key
# where ITEM_DESC like "%LIFEWATER 700ML%"
# GROUP BY
#   s.division_id,
#   MT.product_key;

# COMMAND ----------

output5 = output4.join(scoped_items.select(['tempe_product_key','inscope_prev_refresh']),['tempe_product_key'],'left').\
                  join(scoped_items_new.select(['tempe_product_key','Scope']).\
                                         withColumnRenamed('Scope','inscope_prev_refresh_updated'), \
                       ['tempe_product_key'],'left')

#                                          withColumnRenamed('product_key','tempe_product_key').\


# COMMAND ----------

# output5.count() - output4.count() ##should be 0

# COMMAND ----------

output6=output5.select(["tempe_product_key",\
"BU",\
"tempe_prod_item_env_key",\
"category_desc",\
"department_desc",\
"sub_category_desc",\
"item_desc",\
"manufacturer_desc",\
"brand_desc",\
"package_desc",\
"size_unit_of_measure",\
"upc",\
"sell_unit_qty",\
"sales_quantity",\
"sales_amount",\
"dept_sales_amount",\
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
"inscope_prev_refresh_updated","Acc_Cat",\
"cumulative_sales_perc",\
"cum_sales_flag",\
"comments_trn"]).\
withColumnRenamed("Comment","comments_initial").\
withColumn("final_recommendation",when(col('lt_excl_prev_refresh')=='Y','LT exclusion item in last refresh').\
                                  otherwise(when(col('st_excl_prev_refresh')=='Y','ST exclusion item in last refresh').\
                                  otherwise(when(col('Acc_Cat')=='Out of Scope','Usually Out of Scope for LP').\
                                  otherwise(col('comments_trn')))))

# COMMAND ----------

##update the path
output6.toPandas().to_csv('/dbfs/{0}/{1}/{2}_Repo/Input/{3}_item_scoping_data_final.csv'.format(file_directory, wave, bu_code, bu_code.lower()), index=False)

# COMMAND ----------

# output6.count()- output2.count()

# COMMAND ----------

#validation
df = pd.read_csv("/dbfs/{0}/{1}/{2}_Repo/Input/{3}_item_scoping_data_final.csv".format(file_directory, wave, bu_code, bu_code.lower()))
df= spark.createDataFrame(df)
a = df.count () - output6.count()
if (a>0):
  print('Error, Output not written out properly')
else :
  print("all good")
