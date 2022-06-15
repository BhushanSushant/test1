# Databricks notebook source
# MAGIC %md
# MAGIC #### Import necessary libraries

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, mean as _mean, stddev as _stddev
from functools import reduce
from operator import add

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read widget values

# COMMAND ----------

business_units = dbutils.widgets.get("business_unit") 
bu_code = dbutils.widgets.get("bu_code")
if bu_code=='SW': bu_code='SE'

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

transaction_table = dbutils.widgets.get("transaction_table")
item_table = dbutils.widgets.get("item_table")
station_table = dbutils.widgets.get("station_table")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pull transactions data, items data and sites data

# COMMAND ----------

# remove leading zeros from trn_site_sys_id, trn_barcode, trn_site_sys_id
transactions_table = sqlContext.sql("""
   SELECT REPLACE(LTRIM(REPLACE(trn_item_sys_id,'0',' ')),' ','0') as trn_item_sys_id
        ,REPLACE(LTRIM(REPLACE(trn_barcode,'0',' ')),' ','0') as trn_barcode
        ,REPLACE(LTRIM(REPLACE(trn_site_sys_id,'0',' ')),' ','0') as trn_site_sys_id
        ,trn_transaction_time
        ,trn_transaction_date
        ,trn_payment_mask
        ,trn_transaction_id
        ,trn_loy_unique_account_id
        ,trn_promotion_sys_id
        ,trn_country_sys_id
        ,trn_cost
        ,trn_loy_flag
        ,trn_loy_extra_member_flag
        ,trn_campaign_transaction_sys_id
        ,trn_campaign_sold_quantity
        ,trn_campaign_net_sales_usd
        ,trn_campaign_net_sales
        ,trn_item_number
        ,trn_last_cost_price
        ,trn_item_text
        ,trn_site_number
        ,trn_sold_quantity
        ,trn_gross_amount
        ,trn_item_unit_price
        ,trn_total_discount
        ,trn_promotion_id
        ,trn_vat_amount
        ,trn_local_item_flag
        ,trn_average_cost_price
        ,trn_uom
        ,trn_country_cd                           
  FROM {0}""".format(transaction_table))

items_table = sqlContext.sql("""
  SELECT REPLACE(LTRIM(REPLACE(sys_id,'0',' ')),' ','0') AS sys_id
        ,REPLACE(LTRIM(REPLACE(item_number,'0',' ')),' ','0') AS item_number
        ,item_name
        ,item_segment_code
        ,item_segment
        ,item_subcategory_code
        ,item_subcategory
        ,item_category_code
        ,item_category
        ,item_category_group_code
        ,item_category_group
        ,item_brand_code
        ,item_brand
        ,item_manufacturer_code
        ,item_manufacturer
         
  FROM {0}
  WHERE item_line_of_business == 'Shop'""".format(item_table))

stations_table = sqlContext.sql("""
  SELECT REPLACE(LTRIM(REPLACE(sys_id,'0',' ')),' ','0') AS sys_id
        ,site_number
        ,site_name 
        ,site_bu
        ,site_bu_name
  FROM {0}
  WHERE site_bu_name == '{1}'""".format(station_table, business_units))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter transactions data between specified dates

# COMMAND ----------

merchandise_item_transactions = transactions_table.filter(col('trn_transaction_date') >= start_date).filter(col('trn_transaction_date') <= end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check for columns with null values in filtered transactions

# COMMAND ----------

display(merchandise_item_transactions.select([count(when(col(c).isNull(), c)).alias(c) for c in merchandise_item_transactions.columns]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create master data (transactions + items + sites)

# COMMAND ----------

master_data = merchandise_item_transactions.join(items_table.withColumnRenamed('sys_id', 'trn_item_sys_id'),
                                                 'trn_item_sys_id').join(stations_table.withColumnRenamed('sys_id', 'trn_site_sys_id'), 'trn_site_sys_id', 'inner')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check # of txns, items and sites

# COMMAND ----------

master_data.count()

# COMMAND ----------

display(master_data.select([countDistinct(c) for c in ("trn_item_sys_id", "trn_site_sys_id")]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check summary stats for key numerical columns in master data

# COMMAND ----------

master_data.describe(['trn_gross_amount', 'trn_sold_quantity']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Investigate high amount transactions (> mu + 3sigma)

# COMMAND ----------

df_stats = master_data.select(_mean(col('trn_gross_amount')).alias('mean'),_stddev(col('trn_gross_amount')).alias('std')).collect()
mean = float(df_stats[0]['mean'])
std = df_stats[0]['std']

count_high_sales= master_data.filter(col('trn_gross_amount') > (mean + std*3)).count()
nrow_master_data = master_data.count()
high_sales = (count_high_sales/nrow_master_data)*100
print("{}% of transactions are having revenue >${} with {} rows of {}".format(high_sales,(mean + std*3), count_high_sales,nrow_master_data))

# COMMAND ----------

master_data.filter(col('trn_gross_amount') > (mean + std*3)).groupby('item_name','item_category').count().orderBy(desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Investigate low amount transactions (revenue = 0.01)

# COMMAND ----------

count_low_sales= master_data.filter(col('trn_gross_amount') == 0.01).count()
low_sales = (count_low_sales/nrow_master_data)*100
print("{}% of transactions are having revenue of $0.01 with {} rows of {}".format(low_sales, count_low_sales,nrow_master_data))

# COMMAND ----------

master_data.filter(col('trn_gross_amount') == 0.01).groupby('item_name','item_category').count().orderBy(desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Investigate negative revenue transactions (revenue < 0)

# COMMAND ----------

count_neg_sales= master_data.filter(col('trn_gross_amount') < 0).count()
neg_sales = (count_neg_sales/nrow_master_data)*100
print("{}% of transactions are having negative revenue with {} rows of {}".format(neg_sales, count_neg_sales,nrow_master_data))

# COMMAND ----------

master_data.filter(col('trn_gross_amount') < 0).groupby('item_name','item_category').count().orderBy(desc("count")).show()

# COMMAND ----------

#### Investigate negative gross margin cases at site x month level
# summary_site =  master_data.\
#                 withColumn("Year",year(col("trn_transaction_date"))).\
#                 withColumn("Month",month(col("trn_transaction_date"))).\
#                 groupBy(['trn_site_sys_id', 'site_number', 'site_name','Month', 'Year']).\
#                 agg(sum('trn_sold_quantity').alias('Quantity'),
#                     sum('trn_gross_amount').alias('Revenue'),
#                     sum('trn_cost').alias('Cost'),
#                     sum('trn_vat_amount').alias('Vat')).\
#                 withColumn("Gross_Margin",(col("Revenue") - col("Cost") - col("Vat")))

# Negative_Margin = summary_site.\
#     filter(col("Gross_Margin")<0)

# x = Negative_Margin
# if x.count() == 0:
#   print('ok')
# else:
#   display(x)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visually inspect time series of revenue vs quantity sold

# COMMAND ----------

x1= master_data.select('trn_transaction_date','trn_sold_quantity','trn_gross_amount', 'site_bu')
x2= x1.groupBy('trn_transaction_date', 'site_bu').agg(sum('trn_sold_quantity'),sum('trn_gross_amount')).withColumnRenamed('sum(trn_sold_quantity)','quantity').withColumnRenamed('sum(trn_gross_amount)','revenue').orderBy('trn_transaction_date')
display(x2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Investigate high price variation items and subcategories

# COMMAND ----------

# calculate mean price at item x site x day level
check_item_price = master_data.\
    filter((col("trn_promotion_sys_id") ==-1) & (col("trn_gross_amount") > 0) & (col("trn_sold_quantity")>0)).\
    withColumn("revenue_cross_price", ((col('trn_item_unit_price'))*(col("trn_gross_amount")))).\
    groupBy('site_number','site_name','trn_item_sys_id','item_name', 'item_subcategory','item_category','item_category_group','trn_transaction_date').\
    agg(min('trn_item_unit_price').alias("min_price"),
        max('trn_item_unit_price').alias("max_price"),
        (sum('revenue_cross_price')/sum('trn_gross_amount')).alias('weighted_price'),
        sum('trn_sold_quantity').alias('Quantity'),
        sum('trn_gross_amount').alias('Revenue')).\
    withColumn("mean_price",(col('min_price')+col('max_price')+col('weighted_price'))/3)

cols = check_item_price.columns[10:12]
sd = sqrt(reduce(add, ((col(x) - col("mean_price")) ** 2 for x in cols)) / (len(cols) - 1))

check_item_price_2 = check_item_price.withColumn("std_price",sd)
check_item_price_2 = check_item_price_2.withColumn("covariance_of_price",(col("std_price")/col("mean_price"))*100)
check_item_price_2 = check_item_price_2.withColumn("flag_for_covar",when((col("covariance_of_price") >1), '1').otherwise(0))

window = Window.partitionBy("item_subcategory","item_category","item_category_group").orderBy(check_item_price_2['covariance_of_price'].desc())
check_item_price_3 = check_item_price_2.withColumn("rank_based_on_covar",rank().over(window))

check_item_price_3_flagged = check_item_price_3.filter(col('Flag_For_covar')=='1').\
groupBy('item_category_group','item_category','item_subcategory','item_name','trn_item_sys_id').\
agg(countDistinct(col('trn_item_sys_id')).alias("Count_items_Impact"),
    sum(col('Revenue')).alias("Revenue_Impact"),
    sum(col('Quantity')).alias("Quantity_Impact"))

item_day_wise_covariation = check_item_price_3.\
    groupBy('item_category_group','item_category','item_subcategory','item_name','trn_item_sys_id').\
    agg(countDistinct(col('trn_item_sys_id')).alias("Count_items_Total"),
        sum(col('Revenue')).alias("Revenue_Total"),
        sum(col('Quantity')).alias("Quantity_Total")).\
    join(check_item_price_3_flagged,
         ['item_category_group','item_category','item_subcategory','item_name','trn_item_sys_id'],'left')

# COMMAND ----------

x = item_day_wise_covariation
if x.count() == 0:
  print('ok')
else:
  display(x)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if any trn_item_sys_id exists in multiple subcategories

# COMMAND ----------

x = master_data.\
             groupBy(['trn_item_sys_id']).\
             agg(countDistinct(col('item_subcategory')).alias("Count_dup")).filter(col("Count_dup") > 1)
if x.count() == 0:
  print('ok')
else:
  raise ValueError('some items exist in multiple subcategories, investigate using next cmd')

# COMMAND ----------

# uncomment if cmd 32 gives an error
# display(x)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if any trn_item_sys_id exists in multiple categories

# COMMAND ----------

x = master_data.\
             groupBy(['trn_item_sys_id']).\
             agg(countDistinct(col('item_category')).alias("Count_dup")).filter(col("Count_dup") > 1)
if x.count() == 0:
  print('ok')
else:
  raise ValueError('some items exist in multiple categories, investigate using next cmd')

# COMMAND ----------

# uncomment if cmd 35 gives an error
# display(x)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check if any trn_site_sys_id exists in multiple countries/BUs

# COMMAND ----------

x = master_data.\
             groupBy(['trn_site_sys_id']).\
             agg(countDistinct('site_bu').alias("Count_dup")).filter(col("Count_dup") > 1)
if x.count() == 0:
  print('ok')
else:
  raise ValueError('some sites exist in multiple BUs, investigate using next cmd')

# COMMAND ----------

# uncomment if cmd 38 gives an error
# display(x)
