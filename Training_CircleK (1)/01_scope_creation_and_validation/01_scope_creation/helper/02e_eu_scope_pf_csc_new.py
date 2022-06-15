# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import *
from dateutil.relativedelta import relativedelta
import datetime
import pandas as pd
import numpy as np
import glob
import xlrd

# COMMAND ----------

# MAGIC %md ## Function Inputs

# COMMAND ----------

START_DATE = dbutils.widgets.get("start_date")
END_DATE = dbutils.widgets.get("end_date")
TRANSACTION_YEARS = [str(i) for i in list(range(int(START_DATE.split('-')[0]), int(END_DATE.split('-')[0])+1, 1))]
BU = dbutils.widgets.get("bu")
BU_CODE = dbutils.widgets.get("bu_code")
wave = dbutils.widgets.get("wave")
pre_wave = dbutils.widgets.get("file_directory")
csc_path = dbutils.widgets.get("eu_csc_path")

# manual input
# START_DATE = '2020-04-01'
# END_DATE = '2022-03-29'
# TRANSACTION_YEARS = ['2020', '2021', '2022']
# BU = 'Ireland'
# BU_CODE = 'IE'
# wave = "Apr22_Test_Refresh"
# pre_wave = "/Phase3_extensions/Elasticity_Modelling/"
# base_directory = 'Phase3_extensions/Optimization'
# last_refresh = 'JUNE2021_Refresh'
# csc_path = '/dbfs/' + base_directory + '/' + last_refresh + '/' + 'inputs/'+BU_CODE.lower()+'/' + BU_CODE.lower() + '_optimization_master_input_file_final_updated.xlsx'
# print(csc_path)

# COMMAND ----------

# Read product analysis output from Module 2: items with category, subcategory, sales qty, sales amount, check flags, and final recommendation

#scope_from_m2 = pd.read_csv('/dbfs'+pre_wave+'Nov21_Refresh/'+bu+'/Input/'+scope_m2_path)    ######## Used existing file from last refresh here. Need to be updated. 
scope_m2_path = BU_CODE.lower() + '_item_scoping_data_'+ wave.lower() + '.xlsx'
scope_from_m2 = pd.read_excel('/dbfs/' + pre_wave + '/' + wave+'/'+ BU_CODE + '_Repo'+'/Input/' + scope_m2_path)
# '/dbfs/Phase3_extensions/Elasticity_ModellingApr22_Test_Refresh/IE_Repo/Input/ie_item_scoping_data_apr22_test_refresh.xlsx' now

scope_from_m2 = spark.createDataFrame(scope_from_m2)

# COMMAND ----------

def pandas_to_spark(pandas_df):
    def equivalent_type(f):
        if f == 'datetime64[ns]': return TimestampType()
        elif f == 'int64': return LongType()
        elif f == 'int32': return IntegerType()
        elif f == 'float64': return FloatType()
        else: return StringType()
    def define_structure(string, format_type):
        try: typo = equivalent_type(format_type)
        except: typo = StringType()
        return StructField(string, typo)

    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

# COMMAND ----------

# MAGIC %md ## Product Scope and Store to Cluster Mapping

# COMMAND ----------

###  Scope and store list
def get_scope(business_unit, modelling_level):
  product_identifier = 'product_key' if modelling_level == 'Sys ID' else 'upc'
  col_filter = '!=' if modelling_level == 'Sys ID' else '='
  scope = spark.sql("""SELECT {0} 
                       FROM (SELECT {0}, last_update, 
                                    RANK() OVER (PARTITION BY {0} 
                                                 ORDER BY last_update DESC) AS latest 
                             FROM circlek_db.lp_dashboard_items_scope 
                             WHERE bu = '{1}' AND
                                   modelling_level {2} 'EAN') a
                       WHERE latest = 1""".format(product_identifier,
                                                  business_unit, 
                                                  col_filter))
  #product_scope = [x[0] for x in scope.select("product_key").collect()]
  return scope.select(product_identifier).distinct()

def get_sites(bu_test_control):
  sites = spark.sql("""SELECT site_id AS trn_site_number
                       FROM circlek_db.grouped_store_list 
                       WHERE business_unit = '{}' AND 
                             group = 'Test' """.format(bu_test_control))

  #sites_scope = [x[0] for x in sites.select("site_id").collect()]
  return sites.select('trn_site_number').distinct()

def get_site_cluster(bu_test_control):
  site_info = spark.sql("""SELECT site_id AS trn_site_number, Cluster 
                           FROM circlek_db.grouped_store_list 
                           WHERE business_unit = '{}' AND 
                                 group = 'Test' """.format(bu_test_control)).distinct()
  return site_info

# COMMAND ----------

# MAGIC %md ### Add in Salad and Sandwiches to the scope list which were dropped in May refresh (SW only)

# COMMAND ----------

# sw_scope_jan_refresh = pd.read_excel('/dbfs/SE_Repo/Price_Refresh_JAN2021/Optimization/Inputs/Hierarchy_w_Price_Family.xlsx')
# sw_scope_jan_refresh = sw_scope_jan_refresh.loc[sw_scope_jan_refresh['Category_Name'].isin(['Salads', 'Sandwiches']), ]
# extra_scope_prod_key = sw_scope_jan_refresh.rename(mapper={'Sys_ID': 'product_key'}, axis='columns')['product_key']
# # there are no ean level product in those two categories
# extra_scope_ean_level = sw_scope_jan_refresh.loc[sw_scope_jan_refresh['EAN'] == sw_scope_jan_refresh['Sys_ID_Join'], ].rename(mapper={'EAN': 'upc'}, axis='columns')['upc']
# extra_scope_sys_id_level = sw_scope_jan_refresh.loc[sw_scope_jan_refresh['EAN'] != sw_scope_jan_refresh['Sys_ID_Join'], ].rename(mapper={'Sys_ID': 'product_key'}, axis='columns')['product_key']

# extra_scope_sys_id_level_spark_df = pandas_to_spark(extra_scope_sys_id_level.to_frame())
# extra_scope_sys_id_level_spark_df.createOrReplaceTempView("extra_scope_sys_id_level")

# sw_scope_jan_refresh_spark_df = pandas_to_spark(sw_scope_jan_refresh)
# sw_scope_jan_refresh_spark_df.createOrReplaceTempView("sw_extra_scope_jan_refresh")

# COMMAND ----------

# sw_scope_jan_refresh = pd.read_excel('/dbfs/SE_Repo/Price_Refresh_JAN2021/Optimization/Inputs/Hierarchy_w_Price_Family.xlsx')
# # sw_scope_jan_refresh = sw_scope_jan_refresh.loc[sw_scope_jan_refresh['Category_Name'].isin(['Salads', 'Sandwiches']), ]
# sw_scope_jan_refresh

# COMMAND ----------

# MAGIC %md ### load scope file

# COMMAND ----------

sys_id_level_product_scope = get_scope(BU, 'Sys ID')
# sys_id_level_product_scope = sys_id_level_product_scope.unionAll(extra_scope_sys_id_level_spark_df)
EAN_level_product_scope = get_scope(BU, 'EAN')
test_sites_to_cluster = get_site_cluster(BU)

sys_id_level_product_scope.createOrReplaceTempView("sys_id_level_product_scope")
EAN_level_product_scope.createOrReplaceTempView("EAN_level_product_scope")
test_sites_to_cluster.createOrReplaceTempView("test_sites_to_cluster")

# store to cluster mapping alternative while the main test sites table is in defect
# test_sites_to_cluster = pd.read_csv('/dbfs/SE_Repo/Price_Refresh_MAY2021/Modelling_Inputs/store_cluster_map_may_2021.csv')
# test_sites_to_cluster = pandas_to_spark(test_sites_to_cluster).withColumnRenamed('Cluster_Final', 'Cluster')
test_sites_to_cluster.createOrReplaceTempView("test_sites_to_cluster")

# COMMAND ----------

display(EAN_level_product_scope)

# COMMAND ----------

display(sys_id_level_product_scope)

# COMMAND ----------

# sys_id_level_product_scope.count()
# EAN_level_product_scope.count()
sys_id_level_product_scope.count() + EAN_level_product_scope.count()

# COMMAND ----------

# MAGIC %md ## Use the mode unit_price, cost, and vat_rate from transaction data on the last day an item was sold in each test store 

# COMMAND ----------

# MAGIC %md ### sys_id level modelling

# COMMAND ----------

# join with transaction data, limit to last day when a site * item was sold
product_transaction_data = spark.sql("""
SELECT t.trn_transaction_date,
       s.Cluster,
       p.product_key,
       t.trn_item_unit_price,
       t.trn_cost / t.trn_sold_quantity AS unit_cost,
       t.trn_vat_rate,
       t.trn_gross_amount,
       DENSE_RANK() OVER (PARTITION BY p.product_key, s.Cluster 
                          ORDER BY t.trn_transaction_date DESC) AS date_order 
FROM dw_eu_rep_pos.pos_transactions_f t
INNER JOIN 
sys_id_level_product_scope p
ON 
t.trn_item_sys_id = p.product_key
INNER JOIN test_sites_to_cluster s
ON
t.trn_site_number = s.trn_site_number
WHERE t.trn_transaction_year IN ({0}) AND
      t.trn_country_cd = '{1}' AND
      t.trn_transaction_date >= '{2}' AND t.trn_transaction_date <= '{3}' AND
      t.trn_item_unit_price > 0 AND
      t.trn_cost > 0
      --t.trn_promotion_id = -1
ORDER BY p.product_key, s.Cluster, t.trn_transaction_date""".format(', '.join(y for y in TRANSACTION_YEARS),
                                                                    BU_CODE,
                                                                    START_DATE,
                                                                    END_DATE)).filter(F.col('date_order') == 1)

product_transaction_data.createOrReplaceTempView('product_transaction_data')

# COMMAND ----------

display(product_transaction_data)

# COMMAND ----------

spark.sql("""SELECT SUM(trn_gross_amount) FROM product_transaction_data""").show()

# COMMAND ----------

# mode of price, unit cost and vat_rate on latest day
def get_mode_on_latest_transaction_date(measurement):
  assert measurement in {'trn_item_unit_price', 'unit_cost', 'trn_vat_rate'}, 'Wrong measurement column'
  measurement_mode_latest_day = spark.sql("""
  WITH transaction_w_rank AS 
  (SELECT product_key,
          Cluster,
          {0},
          DENSE_RANK() OVER (PARTITION BY Cluster, product_key
                             ORDER BY COUNT(*) DESC) AS {0}_rank,
          ROW_NUMBER() OVER (PARTITION BY Cluster, product_key
                             ORDER BY COUNT(*) DESC) AS {0}_rnum
   FROM product_transaction_data
   GROUP BY product_key, Cluster, {0})
   SELECT product_key,
          Cluster, 
          {0} AS {0}_mode
   FROM transaction_w_rank
   WHERE {0}_rank = 1 AND
         {0}_rnum = 1
  """.format(measurement))
  return measurement_mode_latest_day

get_mode_on_latest_transaction_date('trn_item_unit_price').createOrReplaceTempView('unit_price_mode_tbl')
get_mode_on_latest_transaction_date('unit_cost').createOrReplaceTempView('unit_cost_mode_tbl')
get_mode_on_latest_transaction_date('trn_vat_rate').createOrReplaceTempView('trn_vat_rate_tbl')

product_mode_cost_price_vat_sys_id = spark.sql("""
SELECT p.product_key,
       p.Cluster,
       trn_item_unit_price_mode AS unit_price_mode,
       unit_cost_mode,
       trn_vat_rate_mode AS vat_rate_mode
FROM unit_price_mode_tbl p
     JOIN 
     unit_cost_mode_tbl c
     ON p.product_key = c.product_key AND
        p.Cluster = c.Cluster
     JOIN 
     trn_vat_rate_tbl v
     ON p.product_key = v.product_key AND
        p.Cluster = v.Cluster""")

product_mode_cost_price_vat_sys_id.createOrReplaceTempView('product_mode_cost_price_vat_sys_id')
display(product_mode_cost_price_vat_sys_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from unit_price_mode_tbl

# COMMAND ----------

# calculate average - deprecated
# product_average_cost_price_vat_sys_id = spark.sql("""
# SELECT product_key,
#        Cluster,
#        ROUND(AVG(unit_cost), 2) AS avg_unit_cost,
#        ROUND(AVG(trn_item_unit_price), 2) AS avg_unit_price,
#        ROUND(AVG(trn_vat_rate)) AS avg_vat_rate,
#        trn_transaction_date AS latest_transaction_date,
#        COUNT(*) AS transaction_cnt
# FROM product_transaction_data
# GROUP BY trn_transaction_date, Cluster, product_key""")

# display(product_average_cost_price_vat_sys_id)

# COMMAND ----------

# MAGIC %md ### EAN level modelling

# COMMAND ----------

# join with transaction data, limit to last day when a site * item was sold
product_transaction_data_ean = spark.sql("""
SELECT t.trn_transaction_date,
       s.Cluster,
       t.trn_item_sys_id AS product_key,
       p.upc,
       t.trn_item_unit_price,
       t.trn_cost / t.trn_sold_quantity AS unit_cost,
       t.trn_vat_rate,
       DENSE_RANK() OVER (PARTITION BY p.upc, s.Cluster 
                          ORDER BY t.trn_transaction_date DESC) AS date_order 
FROM dw_eu_rep_pos.pos_transactions_f t
INNER JOIN 
EAN_level_product_scope p
ON 
REPLACE(LTRIM(REPLACE(t.trn_barcode, '0', ' ')), ' ', '0') = p.upc
INNER JOIN test_sites_to_cluster s
ON
t.trn_site_number = s.trn_site_number
WHERE t.trn_transaction_year IN ({0}) AND
      t.trn_country_cd = '{1}' AND
      t.trn_transaction_date >= '{2}' AND t.trn_transaction_date <= '{3}' AND
      t.trn_item_unit_price > 0 AND
      t.trn_cost > 0
      --t.trn_promotion_id = -1
ORDER BY p.upc, s.Cluster, t.trn_transaction_date""".format(', '.join(y for y in TRANSACTION_YEARS),
                                                            BU_CODE,
                                                            START_DATE,
                                                            END_DATE)).filter(F.col('date_order') == 1)

product_transaction_data_ean.createOrReplaceTempView('product_transaction_data_ean')

# COMMAND ----------

display(product_transaction_data_ean)

# COMMAND ----------

# mode of price, unit cost and vat_rate on latest day
def get_ean_mode_on_latest_transaction_date(measurement):
  assert measurement in {'trn_item_unit_price', 'unit_cost', 'trn_vat_rate'}, 'Wrong measurement column'
  measurement_mode_latest_day = spark.sql("""
  WITH transaction_w_rank AS 
  (SELECT upc,
          Cluster,
          {0},
          DENSE_RANK() OVER (PARTITION BY Cluster, upc
                             ORDER BY COUNT(*) DESC) AS {0}_rank,
          ROW_NUMBER() OVER (PARTITION BY Cluster, upc
                             ORDER BY COUNT(*) DESC) AS {0}_rnum
   FROM product_transaction_data_ean
   GROUP BY upc, Cluster, {0})
   SELECT upc,
          Cluster, 
          {0} AS {0}_mode
   FROM transaction_w_rank
   WHERE {0}_rank = 1 AND
         {0}_rnum = 1
  """.format(measurement))
  return measurement_mode_latest_day

get_ean_mode_on_latest_transaction_date('trn_item_unit_price').createOrReplaceTempView('unit_price_mode_tbl')
get_ean_mode_on_latest_transaction_date('unit_cost').createOrReplaceTempView('unit_cost_mode_tbl')
get_ean_mode_on_latest_transaction_date('trn_vat_rate').createOrReplaceTempView('trn_vat_rate_tbl')

product_mode_cost_price_vat_ean = spark.sql("""
SELECT p.upc,
       p.Cluster,
       trn_item_unit_price_mode AS unit_price_mode,
       unit_cost_mode,
       trn_vat_rate_mode AS vat_rate_mode
FROM unit_price_mode_tbl p
     JOIN 
     unit_cost_mode_tbl c
     ON p.upc = c.upc AND
        p.Cluster = c.Cluster
     JOIN 
     trn_vat_rate_tbl v
     ON p.upc = v.upc AND
        p.Cluster = v.Cluster""")

product_mode_cost_price_vat_ean.createOrReplaceTempView('product_mode_cost_price_vat_ean')
display(product_mode_cost_price_vat_ean)

# COMMAND ----------

# MAGIC %md ### get max date and sum of revenue from transaction data

# COMMAND ----------

max_date_total_revenue_sys_id = spark.sql("""
SELECT p.product_key,
       MAX(t.trn_transaction_date) AS latest_sales_date,
       SUM(t.trn_gross_amount) AS total_sales_amount
FROM dw_eu_rep_pos.pos_transactions_f t
INNER JOIN 
sys_id_level_product_scope p
ON 
t.trn_item_sys_id = p.product_key
INNER JOIN test_sites_to_cluster s
ON
t.trn_site_number = s.trn_site_number
WHERE t.trn_transaction_year IN ({0}) AND
      t.trn_country_cd = '{1}' AND
      t.trn_transaction_date >= '{2}' AND t.trn_transaction_date <= '{3}' AND
      t.trn_item_unit_price > 0 AND
      t.trn_cost > 0
      --t.trn_promotion_id = -1
GROUP BY p.product_key""".format(', '.join(y for y in TRANSACTION_YEARS),
                         BU_CODE,
                         START_DATE,
                         END_DATE))

max_date_total_revenue_ean = spark.sql("""
SELECT p.upc,
       MAX(t.trn_transaction_date) AS latest_sales_date,
       SUM(t.trn_gross_amount) AS total_sales_amount
FROM dw_eu_rep_pos.pos_transactions_f t
INNER JOIN 
EAN_level_product_scope p
ON 
REPLACE(LTRIM(REPLACE(t.trn_barcode, '0', ' ')), ' ', '0') = p.upc
INNER JOIN test_sites_to_cluster s
ON
t.trn_site_number = s.trn_site_number
WHERE t.trn_transaction_year IN ({0}) AND
      t.trn_country_cd = '{1}' AND
      t.trn_transaction_date >= '{2}' AND t.trn_transaction_date <= '{3}' AND
      t.trn_item_unit_price > 0 AND
      t.trn_cost > 0
      --t.trn_promotion_id = -1
GROUP BY p.upc""".format(', '.join(y for y in TRANSACTION_YEARS),
                         BU_CODE,
                         START_DATE,
                         END_DATE))

max_date_total_revenue_sys_id.createOrReplaceTempView('max_date_total_revenue_sys_id')
max_date_total_revenue_ean.createOrReplaceTempView('max_date_total_revenue_ean')

# COMMAND ----------

# display(max_date_total_revenue_sys_id)
# display(max_date_total_revenue_ean)

# COMMAND ----------

# MAGIC %md ## get product hierarchy from item table

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC FROM circlek_db.lp_dashboard_items_scope

# COMMAND ----------

prod_hier = spark.sql("""
WITH scope AS
((SELECT DISTINCT bu, product_key
FROM
(SELECT bu, product_key,
        RANK() OVER (PARTITION BY product_key ORDER BY last_update DESC) AS latest
 FROM circlek_db.lp_dashboard_items_scope
 WHERE bu = 'Ireland')
WHERE latest = 1)

-- UNION

-- (SELECT * FROM extra_scope_sys_id_level)
)

SELECT s.bu, s.product_key, i.item_number, i.item_name, i.item_subcategory, i.item_category
FROM dw_eu_common.item_d i
INNER JOIN 
scope s
ON 
i.sys_id = s.product_key
""")


prod_hier.createOrReplaceTempView('prod_hier')
# display(prod_hier)

# COMMAND ----------

# get CSC from opti master input (for SE)
# opti_input = '/dbfs/SE_Repo/Optimization/Inputs/Hierarchy_map.xlsx'
# pd.read_excel(opti_input)
# csc = spark.createDataFrame(pd.read_excel(opti_input)[['Sys ID Join', 'Category Special Classification']].dropna()).distinct()
# #            .withColumnRenamed('Sys ID Join', 'product_key')
# csc_extra = sw_scope_jan_refresh_spark_df.select(['Sys_ID_Join', 'Category_Special_Classification'])
# csc = csc.unionAll(csc_extra)
# csc.createOrReplaceTempView('csc')
# display(csc)

# COMMAND ----------

# MAGIC %md ## Create Output

# COMMAND ----------

# skeleton, add BU columns
output_skl = spark.sql("""
WITH scope AS
     (SELECT product_key, upc, price_family, Scope, modelling_level
      FROM (SELECT product_key, upc, price_family, Scope, modelling_level, 
                   RANK() OVER (PARTITION BY product_key ORDER BY last_update DESC) AS latest
            FROM circlek_db.lp_dashboard_items_scope
            WHERE bu = 'Ireland' AND
                  modelling_level != 'EAN') sys_id_level
      WHERE latest = 1 
      UNION 
      SELECT product_key, upc, price_family, Scope, modelling_level
      FROM (SELECT product_key, upc, price_family, Scope, modelling_level, 
                   RANK() OVER (PARTITION BY upc ORDER BY last_update DESC) AS latest
            FROM circlek_db.lp_dashboard_items_scope
            WHERE bu = 'Ireland' AND
                  modelling_level = 'EAN') EAN_level
      WHERE latest = 1
--       UNION
--       SELECT Sys_ID AS product_key, EAN AS upc, Price_Family AS price_family, Scope, 'Sys ID' AS modelling_level
--       FROM sw_extra_scope_jan_refresh
     )
SELECT s.*, c.* FROM 
scope s
CROSS JOIN
(SELECT DISTINCT Cluster
FROM test_sites_to_cluster) c
/*(SELECT DISTINCT Cluster
FROM circlek_db.grouped_store_list 
WHERE business_unit = 'Sweden' AND 
      group = 'Test') c*/
ORDER BY product_key, Cluster
""")

output_skl.createOrReplaceTempView('output_skl')
display(output_skl)

# COMMAND ----------

output_skl.distinct().count()

# COMMAND ----------

# join price cost vat to skeleton
output_measure = spark.sql("""
SELECT o.*,
       COALESCE(s.unit_price_mode, e.unit_price_mode) AS unit_price_mode,
       COALESCE(s.unit_cost_mode, e.unit_cost_mode) AS unit_cost_mode,
       COALESCE(s.vat_rate_mode, e.vat_rate_mode) AS vat_rate_mode
FROM output_skl o
LEFT JOIN
product_mode_cost_price_vat_sys_id s
ON o.product_key = s.product_key AND
   o.Cluster = s.Cluster
LEFT JOIN
product_mode_cost_price_vat_ean e
ON o.upc = e.upc AND
   o.Cluster = e.Cluster""")

output_measure = output_measure.withColumn('Cluster', F.col('Cluster').cast('integer'))

# display(output_measure)

# COMMAND ----------

# MAGIC %md ### Pivot Output (long format to wide format for unit price, cost, vat rate columns)

# COMMAND ----------

output_pivot = output_measure.groupBy(['product_key', 'upc', 'Scope', 'price_family', 'modelling_level'])\
                             .pivot('Cluster').agg(F.first('unit_price_mode').alias('unit_price'),
                                                   F.first('unit_cost_mode').alias('unit_cost'), 
                                                   F.first('vat_rate_mode').alias('vat_rate'))

output_pivot = output_pivot.select([F.col(c).name(c[2:] + '_cluster_' + c[0]) if c[:1].isdigit()
                                                            else F.col(c) for c in output_pivot.columns])\
                           .orderBy(output_pivot.product_key.asc())

output_pivot.createOrReplaceTempView('output_pivot')
# display(output_pivot)

# COMMAND ----------

# MAGIC %md ### Add max_date, total rev, prod hier to output

# COMMAND ----------

# display(spark.sql('''select * from prod_hier'''))

# COMMAND ----------

output = spark.sql("""
SELECT ph.bu, o.*,
       ph.item_number, ph.item_name, ph.item_subcategory, ph.item_category,
--        csc.`Category Special Classification`,
       COALESCE(ms.latest_sales_date, me.latest_sales_date) AS latest_sales_date,
       COALESCE(ms.total_sales_amount, me.total_sales_amount) AS total_sales_amount
FROM
output_pivot o
LEFT JOIN 
max_date_total_revenue_sys_id ms
ON o.product_key = ms.product_key
LEFT JOIN 
max_date_total_revenue_ean me
ON o.upc = me.upc
LEFT JOIN 
prod_hier ph
ON o.product_key = ph.product_key
-- LEFT JOIN 
-- csc 
-- ON (o.modelling_level = 'EAN' AND o.upc = csc.product_key) 
-- OR (o.modelling_level = 'Sys ID' AND o.product_key = csc.product_key)
""")
df_scope = output.toPandas()
output.createOrReplaceTempView('df_scope')
scope_cnt = len(df_scope)
# display(df_scope)

# COMMAND ----------



# COMMAND ----------

# print(output_skl.count() / 4, ' : ', output_measure.count() / 4, ' : ', output_pivot.count(), ' : ', output.count() )

# output_skl.count() / 5 == output_measure.count() / 5 == output_pivot.count() == output.count()

# COMMAND ----------

# MAGIC %md ## Bring in CSC

# COMMAND ----------

# Load scope file that contains csc
df_csc_1 = pd.read_excel(csc_path, sheet_name='Bound_Rules_Other_Mappings')
# df_csc_1.createOrReplaceTempView('csc')
# display(df_csc_1)

# COMMAND ----------

# MAGIC %md ### Create csc_clean

# COMMAND ----------

# create csc clean column
df_csc_1['csc_clean'] = np.where(((df_csc_1['Category Special Classification'] == df_csc_1['sub_category_desc']) | df_csc_1['Category Special Classification'].isna()), 
                                 'No_CSC', df_csc_1['Category Special Classification'])
df_csc_2 = df_csc_1[['csc_clean', 'Price Family']].drop_duplicates().dropna() 

# display(df_csc_2)

# COMMAND ----------

# MAGIC %md ### Flag pf span multiple csc

# COMMAND ----------

# Flag price family in multiple cscs
df_pf_multi_csc = df_csc_2.groupby(['Price Family']).agg(uni_csc = ('csc_clean','nunique')).reset_index()
# create column to flag pf that span multiple csc
df_pf_multi_csc['multi_csc_flag'] = np.where(df_pf_multi_csc['uni_csc']>1, 'Y', 'N')
# df_pf_multi_csc

# join pf csc flag to csc file
df_csc_1_f = df_csc_1.merge(df_pf_multi_csc, on = ['Price Family'], how='left')
# display(df_csc_1_f)

# COMMAND ----------

# MAGIC %md ### Join CSC to scope table

# COMMAND ----------

df_csc_1_f = spark.createDataFrame(df_csc_1_f)
df_csc_1_f.createOrReplaceTempView('csc')
# display(df_csc_1_f)

# COMMAND ----------

df_scope_final = spark.sql('''
select distinct d.*, multi_csc_flag, csc_clean
from df_scope d
left join csc c
ON (c.multi_csc_flag = 'Y' AND d.product_key = c.product_key) 
OR (c.multi_csc_flag = 'N' AND d.price_family = c.`Price Family`)
''').fillna('No_CSC', subset=['csc_clean'])
# display(df_scope_final)

# COMMAND ----------

assert scope_cnt == df_scope_final.count()

# COMMAND ----------

# from shutil import move
# output_pd_df = df_scope_final.select("*").toPandas()
# file_name = BU_CODE + '_scope_pf_csc_v3.xlsx'
# with pd.ExcelWriter(file_name) as writer:
#     output_pd_df.to_excel(writer, sheet_name='scope_pf_csc', index=False)
    
# move(file_name, '/dbfs/Phase3_extensions/Elasticity_Modelling/Apr22_Test_Refresh/IE_Repo/Input/')

# COMMAND ----------

# import pandas as pd
# pd.read_excel('/dbfs/Phase3_extensions/Elasticity_Modelling/Apr22_Test_Refresh/IE_Repo/Input/IE_scope_pf_csc_v3.xlsx')

# COMMAND ----------

# MAGIC %md ## Merging tables

# COMMAND ----------

# scope_from_creation = df_scope_final

# COMMAND ----------

# use left join instead of inner join
scope_final_left = df_scope_final.join(scope_from_m2.drop('upc','item_desc','size_unit_of_measure', 'item_subcategory', 'item_category', 'item_name'), df_scope_final.product_key == scope_from_m2.trn_item_sys_id, how='left')
# display(scope_final_left)

# COMMAND ----------

# MAGIC %md ### add/rename columns
# MAGIC 
# MAGIC item desc - item name\
# MAGIC department desc - item_category_group \
# MAGIC dept_sales_amount - group_sales_amount  \
# MAGIC brand desc - item brand\
# MAGIC package desc, size_unit_measure, sell_unit_qty- not in eu\

# COMMAND ----------

# create empty input columns and rename columns
scope_final_left = scope_final_left.withColumn('New upc', lit(None).cast(StringType()))\
  .withColumn('New LP Scope', lit(None).cast(StringType()))\
  .withColumn('New price_family', lit(None).cast(StringType()))\
  .withColumn('New csc', lit(None).cast(StringType()))\
  .withColumn('package_desc', lit(None).cast(StringType()))\
  .withColumn('size_unit_of_measure', lit(None).cast(StringType()))\
  .withColumn('sell_unit_qty', lit(None).cast(StringType()))\
  .withColumnRenamed("product_key","tempe_product_key")\
  .withColumnRenamed("unit_price_cluster_1","retail_price_1")\
  .withColumnRenamed("unit_price_cluster_2","retail_price_2")\
  .withColumnRenamed("unit_price_cluster_3","retail_price_3")\
  .withColumnRenamed("unit_price_cluster_4","retail_price_4")\
  .withColumnRenamed("unit_cost_cluster_1","total_cost_1")\
  .withColumnRenamed("unit_cost_cluster_2","total_cost_2")\
  .withColumnRenamed("unit_cost_cluster_3","total_cost_3")\
  .withColumnRenamed("unit_cost_cluster_4","total_cost_4")\
  .withColumnRenamed("item_category_group","department_desc")\
  .withColumnRenamed("item_manufacturer","manufacturer_desc")\
  .withColumnRenamed("item_brand","brand_desc")\
  .withColumnRenamed("group_sales_amount","dept_sales_amount")\
  .withColumnRenamed("bu","BU")\
  .withColumnRenamed("item_category","category")\
  .withColumnRenamed("item_subcategory","sub_category")\
  .withColumnRenamed("item_name","item_desc")\
  .withColumnRenamed("csc_clean","Category.Special.Classification")


# COMMAND ----------

scope_final = scope_final_left.select(
'BU', 
'category',
'sub_category',
'item_desc',
'upc',
'New upc',
'tempe_product_key',
'New LP Scope',
'price_family',
'New price_family',
'size_unit_of_measure',
'sell_unit_qty',
'`Category.Special.Classification`',
'New csc',
'retail_price_1',
'retail_price_2',
'retail_price_3',
'retail_price_4',
'total_cost_1',
'total_cost_2',
'total_cost_3',
'total_cost_4',
'department_desc',
'manufacturer_desc',
'brand_desc',
'package_desc',
'sales_quantity',
'sales_amount',
'dept_sales_amount',
'latest_sales_date',
'latest_date_diff_flag',
'consecutive_flag',
'discont_flag',
'data_enough_flag',
# 'multi_csc_flag',
'earliest_sales',
'percent_stores_sold',
'days_since_last_sale',
'days_sold_percent',
'weeks_sold_percent',
'dying_ratio',
'slow_start_ratio',
'sold_recently',
'sold_few_weeks',
'sold_few_stores',
'cum_sales_flag',
'final_recommendation'
)


# COMMAND ----------

assert scope_cnt == scope_final.count()

# COMMAND ----------

# ## Saving the final scope table
dbutils.fs.rm("/Phase3_extensions/Elasticity_Modelling/{2}/{0}_Repo/Input/{1}_final_scope_{2}.xlsx".format(BU_CODE, BU_CODE.lower(), wave))
writer = pd.ExcelWriter("Scope_to_review.xlsx", engine='xlsxwriter')
scope_final.toPandas().to_excel(writer, sheet_name='Final Scope', index = False)

# COMMAND ----------

# Saving the final table
from shutil import move
move("Scope_to_review.xlsx", "/dbfs/Phase3_extensions/Elasticity_Modelling/{2}/{0}_Repo/Input/{1}_final_scope_{2}.xlsx".format(BU_CODE, BU_CODE.lower(), wave))
