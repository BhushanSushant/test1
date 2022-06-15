# Databricks notebook source
# MAGIC %md
# MAGIC #### Import functions

# COMMAND ----------

!pip install xlsxwriter

# COMMAND ----------

from pyspark.sql.functions import *
import xlsxwriter
from pyspark.sql import *
from dateutil.relativedelta import relativedelta
from datetime import *
import datetime
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from operator import add
from pyspark.sql.types import StringType,BooleanType,DateType, IntegerType, FloatType
import databricks.koalas as ks
import pandas as pd, numpy as np
import glob
import xlrd

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
spark.conf.set("spark.databricks.io.cache.enabled", "false")

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

spark.conf.set("spark.sql.broadcastTimeout",  36000)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read inputs

# COMMAND ----------

phase = dbutils.widgets.get("phase")
wave = dbutils.widgets.get("wave")
pre_wave = dbutils.widgets.get("base_directory_spark")+"/"

business_units = dbutils.widgets.get("business_units")
bu_code = dbutils.widgets.get("bu_code")
bu = bu_code + '_Repo'

bu_test_control = dbutils.widgets.get("bu_test_control")
csc_file = dbutils.widgets.get("csc_file")

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

db = dbutils.widgets.get('db')

effective_date = dbutils.widgets.get('effective_date')

scope_m2_path = bu_code.lower() + '_item_scoping_data_final.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read scope from last refresh

# COMMAND ----------

scoped_items = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope")
scoped_items = scoped_items.filter(sf.col("BU") == business_units)#.withColumn('Scope',sf.lit('Y'))
scoped_items = scoped_items.groupBy("BU",'product_key', 'Scope', 'price_family').agg(sf.max('last_update'))
scoped_items = scoped_items.drop('max(last_update)').withColumnRenamed("product_key","tempe_product_key")

#### Change Transaction_type_key according to following environment
#transaction_type_key_ENV = ["1||Columbus",\
#                            "10||Columbus",\
#                            "2||Holiday",\
#                            "6||Holiday",\
#                            "2||Laval",\
#                            "3||Laval",\
#                            "3||San_Antonio",\
#                            "4||San_Antonio",\
#                            "5||Sanford",\
#                            "8||Sanford",\
#                            "1||Tempe",\
#                            "6||Tempe",\
#                            "2||Toronto",\
#                            "3||Toronto"]

if bu_code in ['CE','WD']:
  df = scoped_items.join(spark.sql("""SELECT product_key as tempe_product_key,
         max(business_date) latest_sales_date
  FROM dl_localized_pricing_all_bu.merchandise_item_transactions txn
  INNER JOIN circlek_db.grouped_store_list store
  ON txn.site_number = store.site_id
  WHERE txn.business_date BETWEEN '{1}' AND '{2}'
  AND store.business_unit = '{0}'
  AND promotion_id = 0
  AND sys_environment_name = 'Toronto'
  AND transaction_type_key IN (2,3)
  GROUP BY product_key""".format(bu_test_control, start_date, end_date)),'tempe_product_key','left')

elif bu_code in ['QW', 'QE']:
  df = scoped_items.join(spark.sql("""SELECT product_key as tempe_product_key,
         max(business_date) latest_sales_date
  FROM dl_localized_pricing_all_bu.merchandise_item_transactions txn
  INNER JOIN circlek_db.grouped_store_list store
  ON txn.site_number = store.site_id
  WHERE txn.business_date BETWEEN '{1}' AND '{2}'
  AND store.business_unit = '{0}'
  AND promotion_id = 0
  AND sys_environment_name = 'Laval'
  AND transaction_type_key IN (2,3)
  GROUP BY product_key""".format(bu_test_control, start_date, end_date)),'tempe_product_key','left')   
  
else:
    df = scoped_items.join(spark.sql("""SELECT product_key as tempe_product_key,
         max(business_date) latest_sales_date
  FROM dl_localized_pricing_all_bu.merchandise_item_transactions txn
  INNER JOIN circlek_db.grouped_store_list store
  ON txn.site_number = store.site_id
  WHERE txn.business_date BETWEEN '{1}' AND '{2}'
  AND store.business_unit = '{0}'
  AND promotion_id = 0
  AND sys_environment_name = 'Tempe'
  AND transaction_type_key IN (1,6)
  GROUP BY product_key""".format(bu_test_control, start_date, end_date)),'tempe_product_key','left')

# COMMAND ----------

if bu_code in ['QW', 'QE']:
    df1 = df.join(sqlContext.sql("""SELECT product_key as tempe_product_key, upc, category_desc as category, sub_category_desc as sub_category, item_desc, size_unit_of_measure, sell_unit_qty  FROM dl_localized_pricing.product WHERE environment_name = 'Laval'"""),'tempe_product_key','left')

elif bu_code in ['CE', 'WD']:
    df1 = df.join(sqlContext.sql("""SELECT product_key as tempe_product_key, upc, category_desc as category, sub_category_desc as sub_category, item_desc, size_unit_of_measure, sell_unit_qty  FROM dl_localized_pricing.product WHERE environment_name = 'Toronto'"""),'tempe_product_key','left')
    
else:
    df1 = df.join(sqlContext.sql("""SELECT product_key as tempe_product_key, upc, category_desc as category, sub_category_desc as sub_category, item_desc, size_unit_of_measure, sell_unit_qty  FROM dl_localized_pricing.product WHERE environment_name = 'Tempe'"""),'tempe_product_key','left')
    
#display(df1)   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read/clean/join CSC file

# COMMAND ----------

# create csc clean column
df1.createOrReplaceTempView('df_scope')
df_csc_1 = pd.read_excel(csc_file)
df_csc_1['csc_clean'] = np.where(((df_csc_1['Category.Special.Classification'] == df_csc_1['sub_category_desc']) | (df_csc_1['Category.Special.Classification'].isna())| (df_csc_1['Category.Special.Classification'].str.contains('No_CSC'))), 
                                 'No_CSC', df_csc_1['Category.Special.Classification'])
spark.createDataFrame(df_csc_1).createOrReplaceTempView('csc_1')
df_csc_2 = df_csc_1[['csc_clean', 'Price Family']].drop_duplicates().dropna()

# Flag price family in multiple cscs
df_pf_multi_csc = df_csc_2.groupby(['Price Family']).agg(uni_csc = ('csc_clean','nunique')).reset_index()

# create column to flag pf that span multiple csc
df_pf_multi_csc['multi_csc_flag'] = np.where(df_pf_multi_csc['uni_csc']>1, 'Y', 'N')

# join pf csc flag to csc file
df_csc_1_f = df_csc_1.merge(df_pf_multi_csc, on = ['Price Family'], how='left')

spark.createDataFrame(df_csc_1_f).createOrReplaceTempView('csc')

# join csc to scope file: join with pk if flagged, else with pf
df2 = spark.sql('''
select distinct d.*, csc_clean
from df_scope d
left join csc c
ON (c.multi_csc_flag = 'Y' AND d.tempe_product_key = c.product_key) 
OR (c.multi_csc_flag = 'N' AND d.price_family = c.`Price Family`)
''').fillna('No_CSC', subset=['csc_clean']).withColumnRenamed('csc_clean','Category.Special.Classification')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read price and cost data

# COMMAND ----------

business_unit = business_units

# COMMAND ----------

# MAGIC %sql
# MAGIC Refresh table circlek_db.grouped_store_list;
# MAGIC Refresh table circlek_db.lp_dashboard_items_scope;

# COMMAND ----------

# MAGIC %run "./04_scope_item_price_cost"

# COMMAND ----------

price_cost_out = get_price_cost_rollup(get_pdi_price(effective_date, get_env(bu_test_control), business_unit, get_scope(business_unit), get_sites(bu_test_control)), get_pdi_cost(effective_date, get_env(bu_test_control), business_unit, get_scope(business_unit), get_sites(bu_test_control)), get_cluster_info(bu_test_control))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Join price and cost data

# COMMAND ----------

combined_df = df2.join(price_cost_out, ['tempe_product_key'], 'left')

# COMMAND ----------

#display(combined_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Add new columns for template

# COMMAND ----------

combined_df = combined_df.withColumn('New LP Scope', sf.lit('')).withColumn('New upc', sf.lit('')).withColumn('New price_family', sf.lit('')).withColumn('New csc', sf.lit('')).drop('division_desc')


# COMMAND ----------

if bu_code == 'GR':
  combined_df = combined_df.withColumn('sell_unit_qty', combined_df.sell_unit_qty.cast(IntegerType()))\
                           .withColumn('retail_price_1', combined_df.retail_price_1.cast(FloatType()))\
                           .withColumn('retail_price_2', combined_df.retail_price_2.cast(FloatType()))\
                           .withColumn('retail_price_3', combined_df.retail_price_3.cast(FloatType()))\
                           .withColumn('retail_price_4', combined_df.retail_price_4.cast(FloatType()))\
                           .withColumn('retail_price_5', combined_df.retail_price_5.cast(FloatType()))\
                           .withColumn('retail_price_6', combined_df.retail_price_6.cast(FloatType()))\
                           .withColumn('total_cost_1', combined_df.total_cost_1.cast(FloatType()))\
                           .withColumn('total_cost_2', combined_df.total_cost_2.cast(FloatType()))\
                           .withColumn('total_cost_3', combined_df.total_cost_3.cast(FloatType()))\
                           .withColumn('total_cost_4', combined_df.total_cost_4.cast(FloatType()))\
                           .withColumn('total_cost_5', combined_df.total_cost_5.cast(FloatType()))\
                           .withColumn('total_cost_6', combined_df.total_cost_6.cast(FloatType()))
else:
  combined_df = combined_df.withColumn('sell_unit_qty', combined_df.sell_unit_qty.cast(IntegerType()))\
                           .withColumn('retail_price_1', combined_df.retail_price_1.cast(FloatType()))\
                           .withColumn('retail_price_2', combined_df.retail_price_2.cast(FloatType()))\
                           .withColumn('retail_price_3', combined_df.retail_price_3.cast(FloatType()))\
                           .withColumn('retail_price_4', combined_df.retail_price_4.cast(FloatType()))\
                           .withColumn('retail_price_5', combined_df.retail_price_5.cast(FloatType()))\
                           .withColumn('total_cost_1', combined_df.total_cost_1.cast(FloatType()))\
                           .withColumn('total_cost_2', combined_df.total_cost_2.cast(FloatType()))\
                           .withColumn('total_cost_3', combined_df.total_cost_3.cast(FloatType()))\
                           .withColumn('total_cost_4', combined_df.total_cost_4.cast(FloatType()))\
                           .withColumn('total_cost_5', combined_df.total_cost_5.cast(FloatType()))
                  

if bu_code != 'GR':
  combined_df = combined_df.drop('retail_price_6', 'total_cost_6', 'retail_price_null', 'total_cost_null')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read/join scoping analysis output

# COMMAND ----------

scope_from_m2 = pd.read_csv('/dbfs'+pre_wave+wave+'/'+bu+'/Input/'+scope_m2_path)
scope_from_m2 = spark.createDataFrame(scope_from_m2)
scope_final_left = combined_df.join(scope_from_m2.drop('BU','upc','item_desc','size_unit_of_measure'), ['tempe_product_key', 'sell_unit_qty'], how='left')

# COMMAND ----------

'/dbfs'+pre_wave+wave+'/'+bu+'/Input/'+scope_m2_path

# COMMAND ----------

if bu_code == 'GR':
  scope_final = scope_final_left.select("BU","category","sub_category","item_desc","upc",
                                      "New upc", "tempe_product_key","New LP Scope",
                                      "price_family", "New price_family","size_unit_of_measure",
                                      "sell_unit_qty", "`Category.Special.Classification`","New csc",
                                      "retail_price_1","retail_price_2","retail_price_3","retail_price_4",
                                      "retail_price_5","retail_price_6","total_cost_1","total_cost_2",
                                      "total_cost_3","total_cost_4","total_cost_5","total_cost_6",
                                      "department_desc","manufacturer_desc","brand_desc","package_desc",
                                      "sales_quantity","sales_amount","dept_sales_amount","latest_sales_date", "latest_date_diff_flag",
                                      "consecutive_flag","discont_flag","data_enough_flag","earliest_sales",
                                      "percent_stores_sold","days_since_last_sale","days_sold_percent","weeks_sold_percent",
                                      "dying_ratio","slow_start_ratio","sold_recently","sold_few_weeks",
                                      "sold_few_stores","cum_sales_flag","final_recommendation")
else:
  scope_final = scope_final_left.select("BU","category","sub_category","item_desc","upc",
                                      "New upc", "tempe_product_key","New LP Scope",
                                      "price_family", "New price_family","size_unit_of_measure","sell_unit_qty", 
                                      "`Category.Special.Classification`","New csc",
                                      "retail_price_1","retail_price_2","retail_price_3","retail_price_4",
                                      "retail_price_5","total_cost_1","total_cost_2",
                                      "total_cost_3","total_cost_4","total_cost_5",
                                      "department_desc","manufacturer_desc","brand_desc","package_desc",
                                      "sales_quantity","sales_amount","dept_sales_amount","latest_sales_date","latest_date_diff_flag",
                                      "consecutive_flag","discont_flag","data_enough_flag","earliest_sales",
                                      "percent_stores_sold","days_since_last_sale","days_sold_percent","weeks_sold_percent",
                                      "dying_ratio","slow_start_ratio","sold_recently","sold_few_weeks",
                                      "sold_few_stores","cum_sales_flag","final_recommendation")

# COMMAND ----------

## Adjustment on formatting

scope_final = scope_final.na.fill("Item Discontinued: transactions not found for last 2 years", ["final_recommendation"])
if bu_code == 'GR':
  scope_final = scope_final.withColumn("retail_price_1", round(col("retail_price_1"), 2))\
                         .withColumn("retail_price_2", round(col("retail_price_2"), 2))\
                         .withColumn("retail_price_3", round(col("retail_price_3"), 2))\
                         .withColumn("retail_price_4", round(col("retail_price_4"), 2))\
                         .withColumn("retail_price_5", round(col("retail_price_5"), 2))\
                         .withColumn("retail_price_6", round(col("retail_price_6"), 2))\
                         .withColumn("total_cost_1", round(col("total_cost_1"), 2))\
                         .withColumn("total_cost_2", round(col("total_cost_2"), 2))\
                         .withColumn("total_cost_3", round(col("total_cost_3"), 2))\
                         .withColumn("total_cost_4", round(col("total_cost_4"), 2))\
                         .withColumn("total_cost_5", round(col("total_cost_5"), 2))\
                         .withColumn("total_cost_6", round(col("total_cost_6"), 2))\
                         .withColumn("sales_amount", round(col("sales_amount"), 0))\
                         .withColumn("dept_sales_amount", round(col("dept_sales_amount"), 0))\
                         .withColumn("latest_sales_date",  date_format(col("latest_sales_date"),"yyyy-MM-dd"))\
                         .withColumn("dying_ratio", round(col("dying_ratio"), 2))\
                         .withColumn("slow_start_ratio", round(col("slow_start_ratio"), 2))\
                         .withColumn("percent_stores_sold",concat(round(col("percent_stores_sold")*100,0),lit("%")))\
                         .withColumn("days_sold_percent",concat(round(col("days_sold_percent")*100,0),lit("%")))\
                         .withColumn("weeks_sold_percent",concat(round(col("weeks_sold_percent")*100,0),lit("%")))
else:
  scope_final = scope_final.withColumn("retail_price_1", round(col("retail_price_1"), 2))\
                         .withColumn("retail_price_2", round(col("retail_price_2"), 2))\
                         .withColumn("retail_price_3", round(col("retail_price_3"), 2))\
                         .withColumn("retail_price_4", round(col("retail_price_4"), 2))\
                         .withColumn("retail_price_5", round(col("retail_price_5"), 2))\
                         .withColumn("total_cost_1", round(col("total_cost_1"), 2))\
                         .withColumn("total_cost_2", round(col("total_cost_2"), 2))\
                         .withColumn("total_cost_3", round(col("total_cost_3"), 2))\
                         .withColumn("total_cost_4", round(col("total_cost_4"), 2))\
                         .withColumn("total_cost_5", round(col("total_cost_5"), 2))\
                         .withColumn("sales_amount", round(col("sales_amount"), 0))\
                         .withColumn("dept_sales_amount", round(col("dept_sales_amount"), 0))\
                         .withColumn("latest_sales_date",  date_format(col("latest_sales_date"),"yyyy-MM-dd"))\
                         .withColumn("dying_ratio", round(col("dying_ratio"), 2))\
                         .withColumn("slow_start_ratio", round(col("slow_start_ratio"), 2))\
                         .withColumn("percent_stores_sold",concat(round(col("percent_stores_sold")*100,0),lit("%")))\
                         .withColumn("days_sold_percent",concat(round(col("days_sold_percent")*100,0),lit("%")))\
                         .withColumn("weeks_sold_percent",concat(round(col("weeks_sold_percent")*100,0),lit("%")))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create recommendation column

# COMMAND ----------

#split the scope_final df into 2 parts - 'No Family'(no_family_df) and remaining price families
no_family_df = scope_final.filter(scope_final.price_family == "No_Family")
family_df = scope_final.filter(scope_final.price_family != "No_Family")

# COMMAND ----------

#Adding new column 'BU_Recom' based on the 'final_recommendation'.
condition = when(col("final_recommendation") == 'goodstuff', "").otherwise("Review PF:no good stuff")
no_family_df = no_family_df.withColumn("BU_Recom", condition)

#Add the final column 'BU_Recom' for all the remaining price families having at least 1 goodstuff item in it
pivotDF = family_df.groupBy("price_family").pivot("final_recommendation").count()
condition = when(col("goodstuff") > 0, "").otherwise("Review PF:no good stuff")
pivotDF = pivotDF.withColumn("BU_Recom", condition)
goodstuff_df = pivotDF.select("price_family","BU_Recom")

# COMMAND ----------

final = family_df.join(goodstuff_df,family_df.price_family ==  goodstuff_df.price_family,"inner").drop(goodstuff_df.price_family)
if bu_code == 'GR':
  final_df = final.select("BU","category","sub_category","item_desc","upc",
                                      "New upc", "tempe_product_key","New LP Scope",
                                      "price_family", "New price_family","size_unit_of_measure",
                                      "sell_unit_qty", "`Category.Special.Classification`","New csc",
                                      "retail_price_1","retail_price_2","retail_price_3","retail_price_4",
                                      "retail_price_5","retail_price_6","total_cost_1","total_cost_2",
                                      "total_cost_3","total_cost_4","total_cost_5","total_cost_6",
                                      "department_desc","manufacturer_desc","brand_desc","package_desc",
                                      "sales_quantity","sales_amount","dept_sales_amount","latest_sales_date", "latest_date_diff_flag",
                                      "consecutive_flag","discont_flag","data_enough_flag","earliest_sales",
                                      "percent_stores_sold","days_since_last_sale","days_sold_percent","weeks_sold_percent",
                                      "dying_ratio","slow_start_ratio","sold_recently","sold_few_weeks",
                                      "sold_few_stores","cum_sales_flag","final_recommendation","BU_Recom")
else:
  final_df = final.select("BU","category","sub_category","item_desc","upc",
                                      "New upc", "tempe_product_key","New LP Scope",
                                      "price_family", "New price_family","size_unit_of_measure","sell_unit_qty", 
                                      "`Category.Special.Classification`","New csc",
                                      "retail_price_1","retail_price_2","retail_price_3","retail_price_4",
                                      "retail_price_5","total_cost_1","total_cost_2",
                                      "total_cost_3","total_cost_4","total_cost_5",
                                      "department_desc","manufacturer_desc","brand_desc","package_desc",
                                      "sales_quantity","sales_amount","dept_sales_amount","latest_sales_date","latest_date_diff_flag",
                                      "consecutive_flag","discont_flag","data_enough_flag","earliest_sales",
                                      "percent_stores_sold","days_since_last_sale","days_sold_percent","weeks_sold_percent",
                                      "dying_ratio","slow_start_ratio","sold_recently","sold_few_weeks",
                                      "sold_few_stores","cum_sales_flag","final_recommendation","BU_Recom")

final_scope_df = no_family_df.union(final_df)
display(final_scope_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate output

# COMMAND ----------

# ## Saving the final scope table

dbutils.fs.rm("/dbfs"+pre_wave+"{0}/{1}_Repo/Input/{2}_final_scope_Apr22_Test_refresh.xlsx".format(wave,bu_code, bu_code.lower()))
writer = pd.ExcelWriter("Scope_to_review.xlsx", engine='xlsxwriter')
final_scope_df.toPandas().to_excel(writer, sheet_name='Final Scope', index = False)
writer.save()

# COMMAND ----------

# # Saving the final table

from shutil import move
move("Scope_to_review.xlsx", "/dbfs"+pre_wave+"{0}/{1}_Repo/Input/{2}_final_scope_Apr22_Test_refresh.xlsx".format(wave,bu_code, bu_code.lower()))

# COMMAND ----------


