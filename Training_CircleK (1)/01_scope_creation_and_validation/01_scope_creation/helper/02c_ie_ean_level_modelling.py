# Databricks notebook source
# MAGIC %md
# MAGIC #### Import necessary libraries

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, when, mean, lag, round, abs
from pyspark.sql.window import Window
from pyspark.sql.types import *
from shutil import move
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read widget values

# COMMAND ----------

phase = dbutils.widgets.get("phase")
wave = dbutils.widgets.get("wave")

business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

db = dbutils.widgets.get("db")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pull transactions data from pre LP period

# COMMAND ----------

transactions_pre_LP = sqlContext.sql("""
                SELECT site_bu,item_category_group,item_category,item_subcategory,item_name,item_manufacturer,item_brand,trn_promotion_id
                      ,REPLACE(LTRIM(REPLACE(trn_item_sys_id,'0',' ')),' ','0') as trn_item_sys_id
                      ,REPLACE(LTRIM(REPLACE(trn_barcode,'0',' ')),' ','0') as trn_barcode
                      ,REPLACE(LTRIM(REPLACE(trn_site_sys_id,'0',' ')),' ','0') as trn_site_sys_id
                      ,trn_item_number
                      ,trn_transaction_date
                      ,trn_sold_quantity
                      ,trn_gross_amount
                      ,(trn_gross_amount-trn_vat_amount) as trn_net_amount
                      ,trn_cost
                      ,trn_item_unit_price
                      ,item_line_of_business
                      ,site_brand
                      ,site_format
                      ,site_status
                FROM {0}.{1}_txn_data_{2}
                WHERE trn_transaction_date >= '{3}' AND trn_transaction_date <= '{4}'
                      AND trn_cost > 0
                      AND trn_sold_quantity > 0
                      AND trn_gross_amount > 0
                      AND trn_item_unit_price > 0.1
                      AND item_line_of_business = 'Shop'
                      AND site_brand = 'Circle K'
                      AND site_format = 'Full Service Station'
                      AND site_status = 'Active'
                      """.format(db, bu_code.lower(), wave.lower(),'2020-09-01','2020-09-30'))
#.createOrReplaceTempView('transactions_pre_LP')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify multiple barcode items

# COMMAND ----------

barcode_count = transactions_pre_LP.groupBy('trn_item_sys_id').agg(countDistinct("trn_barcode").alias('barcode_count'))
transactions_w_barcode_count = transactions_pre_LP.join(barcode_count,['trn_item_sys_id'],'inner')
transactions_multi_barcodes = transactions_w_barcode_count.filter(col('barcode_count')>1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate avg price

# COMMAND ----------

transactions_multi_barcodes = transactions_multi_barcodes.groupBy('trn_item_sys_id', 'item_category', 'item_subcategory', 'item_name', 'barcode_count', 'trn_barcode').agg(mean("trn_item_unit_price").alias('avg_trn_item_unit_price'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate difference between avg price across barcodes

# COMMAND ----------

transactions_multi_barcodes = transactions_multi_barcodes.withColumn("lag_avg_trn_item_unit_price", lag(col("avg_trn_item_unit_price")).over(Window.partitionBy("trn_item_sys_id").orderBy("trn_barcode")))
transactions_multi_barcodes = transactions_multi_barcodes.withColumn("price_diff", when(col("lag_avg_trn_item_unit_price").isNull(),0).otherwise( col("avg_trn_item_unit_price")-col("lag_avg_trn_item_unit_price")))
transactions_multi_barcodes = transactions_multi_barcodes.withColumn("price_diff_perc", when(col("lag_avg_trn_item_unit_price").isNull(),0).otherwise( 1- col("lag_avg_trn_item_unit_price")/col("avg_trn_item_unit_price")*100))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flag items with significant percentage price difference across barcodes

# COMMAND ----------

transactions_multi_barcodes_flagged = transactions_multi_barcodes.filter((abs(col('price_diff_perc'))>=0.05))
trn_item_sys_id_flagged = transactions_multi_barcodes.select("trn_item_sys_id").distinct().rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate ean level modelling items list to share with BU

# COMMAND ----------

output = transactions_multi_barcodes.filter(col("trn_item_sys_id").isin(trn_item_sys_id_flagged))
output = output.\
withColumn("trn_item_sys_id", output["trn_item_sys_id"].cast(IntegerType())).\
withColumn("trn_barcode", output["trn_barcode"].cast(LongType())).\
withColumn("avg_trn_item_unit_price", output["avg_trn_item_unit_price"].cast(FloatType())).\
withColumn("price_diff", round(output["price_diff"], 2).cast(FloatType())).\
withColumn("price_diff_perc", round(output["price_diff_perc"], 2).cast(FloatType())).\
select('trn_item_sys_id', 'item_name', 'item_category', 'barcode_count', 'trn_barcode', 'avg_trn_item_unit_price', 'price_diff', 'price_diff_perc')


with pd.ExcelWriter('output.xlsx') as writer:
    output.toPandas().to_excel(writer, sheet_name='For Review', index=False)
move('output.xlsx', '/dbfs/{}/Elasticity_Modelling/{}/'.format(phase, wave) + bu_code + '_Repo/Input/' + '{}_ean_level_modelling_{}.xlsx'.format(bu_code.lower(), wave.lower()))
