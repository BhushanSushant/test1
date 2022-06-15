# Databricks notebook source
## Read the libs
import datetime
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import *
from datetime import *
from pyspark.sql.window import Window

import pandas as pd
import numpy as np
from datetime import timedelta
import datetime as dt


# COMMAND ----------

def get_dates(PDI_filter_date, end_date, days):
  
  PDI_start_date = PDI_filter_date
  PDI_end_date = (pd.to_datetime(PDI_start_date) + pd.DateOffset(days = 0)).strftime("%Y-%m-%d")
  Txn_end_date = end_date
  Txn_start_date = (pd.to_datetime(Txn_end_date) - pd.DateOffset(days = int(pd.to_numeric(((days).split(" "))[0])))).strftime("%Y-%m-%d")
  
  return PDI_start_date, PDI_end_date, Txn_start_date, Txn_end_date

# COMMAND ----------

def get_scope(business_unit):
  
  ## Inscope items being fetched for the BU based on the latest Pkey on the updated date
  scope = spark.sql("""Select product_key from 
                               (Select product_key, last_update, rank() over(partition by product_key order by last_update desc) as latest 
                                  From circlek_db.lp_dashboard_items_scope 
                                  Where bu = '{}'
                                    and Scope = 'Y'
                                ) a
                                Where latest =  1""".format(business_unit))
  
  ## Same logic for the price_family of items 
  price_family = spark.sql("""Select product_key as tempe_product_key, price_family from 
                               (Select product_key, price_family, last_update, rank() over(partition by product_key order by last_update desc) as latest 
                                  From circlek_db.lp_dashboard_items_scope 
                                  Where bu = '{}'
                                    and Scope = 'Y'
                                ) a
                                Where latest =  1""".format(business_unit))

  return scope, price_family

# COMMAND ----------

def get_sites(business_unit):
  
  ## Fetches only test sites from the grouped store list for a given BU
  sites = spark.sql("""Select site_id from circlek_db.grouped_store_list 
                            Where business_unit = '{}' 
                            and test_launch_date IS NOT NULL
                            """.format(business_unit))
    
  # Test/Control and Cluster map 
  site_map = spark.sql("""Select site_id, group, Cluster from circlek_db.grouped_store_list 
                                 Where business_unit = '{}' 
                                 and test_launch_date IS NOT NULL
                                 """.format(business_unit))

  
  return sites, site_map

# COMMAND ----------

# ## Check if the skeleton already exists within the last 3 months
# def skeleton_check(end_date, bu_abb):
#   skel_chk = 1
#   skel_chk_month = end_date[5:7]
#   skel_chk_yr = end_date[:4]
#   skel_chk_months = [skel_chk_month, int(skel_chk_month) - 1, int(skel_chk_month) - 2]
#   skel_chk_bu = bu_abb

#   tbl_names = sqlContext.tableNames("circlek_db") ## GENERIC LOCATION FOR ALL NA BU TXN SKELETONS

#   for each in skel_chk_months:
#     if skel_chk_bu + "_optimization_price_cost_refresh_skel_" + str(skel_chk_yr) + "_" + str(each) in tbl_names:
#       print("Skeleton available within last 3 months.")
#       skel_chk = 0
#       break
#     else:
#       print("")
#   if skel_chk == 1:
#     print("Skeleton not available, will be created now.")
#   return skel_chk


# COMMAND ----------

def get_transactions(start_date, business_unit, bu_abb, env, view, product, site):
  
  ## If it is report view - the dataframe will be saved as a normal skeleton in default DB. Refresh skeleton will be saved in the Circle K db
  ## Same month's subsequent runs will overwrite the Hive tables as directed by month name in the skeleton name
  
  if view == 'report':
      save_name = bu_abb + "_optimization_price_cost_skel_" + start_date[:4] + "_" + start_date[5:7]
  else:
      save_name = bu_abb + "_optimization_price_cost_refresh_skel_" + start_date[:4] + "_" + start_date[5:7]

  base_txn = spark.sql("""
  Select 
  distinct 
    latest_trn_date
  , txn_tbl.product_key
  , upc
  , item_description as item_desc
  , product_tbl.category_desc
  , product_tbl.sub_category_desc
  , product_tbl.sell_unit_qty
  , txn_tbl.site_number
  , txn_tbl.sys_environment_name
  , site_number_corporate
  , division_desc
  , site_state_id

  From 
      (
      Select  max(business_date) as latest_trn_date
      , product_key
      ,upc
      ,item_description
      ,site_number
      , sys_environment_name
      From 
        dl_localized_pricing_all_bu.merchandise_item_transactions
      Where sys_environment_name = '{2}'
      Group By
        2,3,4,5,6
      ) txn_tbl

  join 
  ( Select 
      site_number_corporate
      , site_state_id
      , division_desc
      , sys_environment_name
   From
     dl_localized_pricing_all_bu.site
      Where sys_environment_name = '{2}') site_tbl

  on txn_tbl.site_number = site_tbl.site_number_corporate
  and txn_tbl.sys_environment_name = site_tbl.sys_environment_name
  
  join 
    ( Select 
         product_key
         , CASE WHEN environment_name = 'Toronto' THEN department_desc
                ELSE category_desc
           END AS category_desc
         , sub_category_desc
         , sell_unit_qty
         , environment_name
       
     From dl_localized_pricing_all_bu.product
     Where environment_name = '{2}' ) product_tbl

  on txn_tbl.product_key = product_tbl.product_key
  and txn_tbl.sys_environment_name = product_tbl.environment_name

  Where latest_trn_date >= '{0}'
    and division_desc = '{1}'
    and txn_tbl.sys_environment_name = '{2}'

  """.format(start_date, business_unit, env))

  ## Refresh view of skeleton is filtered before being written to the DB
  if view == "report":
      save_name = bu_abb + "_optimization_price_cost_skel_" + start_date[:4] + "_" + start_date[5:7]
      base_txn.write.mode("overwrite").saveAsTable("default." + save_name)
      #print(save_name)
      
  else:
      save_name = bu_abb + "_optimization_price_cost_refresh_skel_" + start_date[:4] + "_" + start_date[5:7]+ "_mock"
      product_list, site_list = get_lp_scope(product,site)
      base_txn = base_txn.filter(col('product_key').isin(product_list)).filter(col('site_number').isin(site_list))
      base_txn.write.mode("overwrite").saveAsTable("circlek_db." + save_name)
      
  txn_skeleton = spark.sql("Select * From circlek_db.{}".format(save_name))

  return txn_skeleton

# COMMAND ----------

## Get items and sites for global function calls
def get_lp_scope(product, site):
  return product, site


# COMMAND ----------

# def chk_skel_to_del(start_date, bu_abb):
#   #start_date = '2021-08-04'
#   del_lst = []
  
#   for each in sqlContext.tableNames("circlek_db"):
#     dup_bu = each[:2]
#     dup_mth = each[-2:]
#     if dup_bu == bu_abb and int(dup_mth) < int(start_date[5:7]):
#       del_lst.append(each)
#     else:
#       None
#   return del_lst
  

# COMMAND ----------

def get_pdi_price(PDI_start_date, PDI_end_date, business_unit, env):
  
  ## The PDI data is filtered around the selected date (effective end and start dates). Currently both are same, however, end dates can be little longer in the future to accomodate any future changes
  
  price_tbl = spark.sql("""
                        SELECT
                          vw_irp.*,
                          all_bu_p.product_key as all_bu_product_key,
                          all_bu_s.division_desc as all_bu_division_desc,
                          all_bu_s.site_number_corporate

                        FROM dl_edw_na.vw_item_retail_prices vw_irp 

                        JOIN dl_localized_pricing_all_bu.site all_bu_s 
                          ON vw_irp.site_number = all_bu_s.site_number 
                            AND vw_irp.environment_name = all_bu_s.sys_environment_name
                        JOIN dl_localized_pricing_all_bu.product all_bu_p 
                          ON vw_irp.item_number = all_bu_p.item_number
                            AND vw_irp.environment_name = all_bu_p.environment_name
                            AND vw_irp.package_quantity = all_bu_p.sell_unit_qty
                        WHERE
                            effective_start_date <= '{0}'
                            AND effective_end_date >= '{1}'
                            AND all_bu_s.division_desc = '{2}'
                            AND vw_irp.environment_name = '{3}'

                        """.format(PDI_start_date, PDI_end_date, business_unit, env))\
                           .select('environment_name', 'item_number', 'site_number', 'gross_price', 'net_price', 'all_bu_product_key',
                                  'all_bu_division_desc', 'site_number_corporate', 'effective_start_date', 'effective_end_date')\
                           .withColumnRenamed('all_bu_product_key','tempe_product_key')\
                           .withColumnRenamed('all_bu_division_desc','division_desc')

  return price_tbl

# COMMAND ----------

def get_price_imputed(not_missing_price, missing_price):
  
  ## Create Modes and different levels - in case of ties, pick the value sorted as row 1 
  group1_cols = ['tempe_product_key', 'state_cluster', 'price_zone', 'gross_price']
  group1_price = not_missing_price.filter(col('state_cluster').isNotNull()).filter(col('price_zone').isNotNull()).filter(col('trn_site_sys_id').isNotNull()).\
                 groupby(group1_cols).agg(count('trn_site_sys_id').alias('rows')).orderBy(group1_cols).\
                 withColumnRenamed('gross_price', 'gross_price_one')

  group2_cols = ['tempe_product_key', 'Cluster', 'gross_price']
  group2_price = not_missing_price.filter(col('Cluster').isNotNull()).filter(col('trn_site_sys_id').isNotNull()).\
                 groupby(group2_cols).agg(count('trn_site_sys_id').alias('rows')).orderBy(group2_cols).\
                 withColumnRenamed('gross_price', 'gross_price_two')

  group3_cols = ['tempe_product_key', 'gross_price']
  group3_price = not_missing_price.filter(col('trn_site_sys_id').isNotNull()).\
                 groupby(group3_cols).agg(count('trn_site_sys_id').alias('rows')).orderBy(group3_cols).\
                 withColumnRenamed('gross_price', 'gross_price_three')

  # Create window
  win1 = Window.partitionBy(group1_cols[:-1]).orderBy(desc('rows'))

  win2 = Window.partitionBy(group2_cols[:-1]).orderBy(desc('rows'))

  win3 = Window.partitionBy(group3_cols[:-1]).orderBy(desc('rows'))


  ## Rank by count of rows and by latest effective date
  p_one = group1_price.withColumn('rank_one', dense_rank().over(win1))
  p_two = group2_price.withColumn('rank_two', dense_rank().over(win2))
  p_three = group3_price.withColumn('rank_three', dense_rank().over(win3))

  ## Apply MODE in three levels based on available data and resolving ties

  mode_imputed = missing_price.join(p_one.filter(col('rank_one')==1), ['tempe_product_key', 'state_cluster', 'price_zone'], 'left').select(missing_price["*"], p_one.gross_price_one).\
                join(p_two.filter(col('rank_two')==1), ['tempe_product_key', 'Cluster'], 'left').select(missing_price["*"], "gross_price_one", p_two.gross_price_two).\
                join(p_three.filter(col('rank_three')==1), ['tempe_product_key'], 'left').select(missing_price["*"], "gross_price_one", "gross_price_two", p_three.gross_price_three)

  mode_imputed_price = mode_imputed.withColumn('gross_price', when(col("gross_price_one").isNotNull(), col("gross_price_one")).\
                                         when(col("gross_price_one").isNull() & col("gross_price_two").isNotNull(), col("gross_price_two")).\
                                         otherwise(col("gross_price_three")))

  mode_imputed_final_price = mode_imputed_price.withColumn("rows", row_number().over(Window.partitionBy(['tempe_product_key', 'trn_site_sys_id']).orderBy(['tempe_product_key', 'trn_site_sys_id', 'gross_price']))).filter(col('rows')==1).\
  drop("gross_price_one").\
  drop("gross_price_two").\
  drop("gross_price_three").\
  drop("rows").\
  distinct()

  ## APPEND 

  final_price = not_missing_price.unionByName(mode_imputed_final_price)

  return final_price
  

# COMMAND ----------

def get_pdi_cost(PDI_start_date, PDI_end_date, business_unit, env):
  
    ## The PDI data is filtered around the selected date (effective end and start dates). Currently both are same, however, end dates can be little longer in the future to accomodate any future changes
  

  cost_tbl = spark.sql("""
                      SELECT
                        vw_cost.*,
                        all_bu_p.product_key as all_bu_product_key,
                        all_bu_s.division_desc as all_bu_division_desc,
                        all_bu_s.site_number_corporate

                      FROM dl_edw_na.vw_item_costs vw_cost 

                      JOIN dl_localized_pricing_all_bu.site all_bu_s 
                        ON vw_cost.site_number = all_bu_s.site_number 
                          AND vw_cost.environment_name = all_bu_s.sys_environment_name
                      JOIN dl_localized_pricing_all_bu.product all_bu_p 
                        ON vw_cost.item_number = all_bu_p.item_number
                          AND vw_cost.environment_name = all_bu_p.environment_name
                          AND vw_cost.package_quantity = all_bu_p.sell_unit_qty
                      WHERE
                          effective_start_date <= '{0}'
                          AND effective_end_date >= '{1}'
                          AND all_bu_s.division_desc = '{2}'
                          AND cost_is_promo_flag = 'false'
                          AND vw_cost.environment_name = '{3}' 
                    """.format(PDI_start_date, PDI_end_date, business_unit, env))\
                       .select('environment_name', 'item_number', 'site_number', 'total_cost', 'unit_cost', 'all_bu_product_key', 'site_number_corporate',
                               'effective_start_date', 'effective_end_date', 'vendor_id', 'source_cost_zone_key')\
                       .withColumnRenamed('all_bu_product_key','tempe_product_key')\
                       .withColumnRenamed('site_number_corporate','trn_site_sys_id')\
                       .withColumnRenamed('effective_start_date','cost_start_date')\
                       .withColumnRenamed('effective_end_date','cost_end_date').distinct()

  
  return cost_tbl
 

# COMMAND ----------

def get_cost_imputed(not_missing_cost, missing_cost):

  ## Find MODE at item x state level - in case of ties, pick the value sorted as row 1 
  group1_cols = ['tempe_product_key', 'site_state_id', 'total_cost']
  group1_cost = not_missing_cost.filter(col('site_state_id').isNotNull()).filter(col('trn_site_sys_id').isNotNull()).\
                 groupby(group1_cols).agg(count('trn_site_sys_id').alias('rows')).orderBy(group1_cols).\
                 withColumnRenamed('total_cost', 'total_cost_one')

  ## Find MODE at item level

  group2_cols = ['tempe_product_key', 'total_cost']
  group2_cost = not_missing_cost.filter(col('trn_site_sys_id').isNotNull()).\
                 groupby(group2_cols).agg(count('trn_site_sys_id').alias('rows')).orderBy(group2_cols).\
                 withColumnRenamed('total_cost', 'total_cost_two')

  # Create window

  cwin1 = Window.partitionBy(group1_cols[:-1]).orderBy(desc('rows'))
  cwin2 = Window.partitionBy(group2_cols[:-1]).orderBy(desc('rows'))

  ## Rank by count of rows and by latest effective date
  c_one = group1_cost.withColumn('rank_one', dense_rank().over(cwin1))
  c_two = group2_cost.withColumn('rank_two', dense_rank().over(cwin2))


  ## Apply MODE

  mode_imputed_c = missing_cost.join(c_one.filter(col('rank_one')==1), ['tempe_product_key', 'site_state_id'], 'left').select(missing_cost["*"], c_one.total_cost_one).\
                join(c_two.filter(col('rank_two')==1), ['tempe_product_key'], 'left').select(missing_cost["*"], "total_cost_one", c_two.total_cost_two)


  ## Select the correct cost

  mode_imputed_cost = mode_imputed_c.withColumn('total_cost', when(col("total_cost_one").isNotNull(), col("total_cost_one")).\
                                                otherwise(col("total_cost_two")))

  mode_imputed_final_cost = mode_imputed_cost.withColumn("rows", row_number().over(Window.partitionBy(['tempe_product_key', 'trn_site_sys_id']).orderBy(['tempe_product_key', 'trn_site_sys_id', 'total_cost']))).filter(col('rows')==1).\
  drop("total_cost_one").\
  drop("total_cost_two").\
  drop("rows").\
  distinct()  

  ## Union

  final_cost = not_missing_cost.unionByName(mode_imputed_final_cost)

  return final_cost

# COMMAND ----------

## Combine item purchase with site table to get site numbers. Then combine with LP product and site tables. Finally add vendor.
def vendor_purchase_info(business_unit, env):

  ## Get vendor details for the item and site 
  vendor_info = spark.sql("""
                            Select  ip.business_date, ip.source_system_key, ip.vendor_key, ip.item_key, ip.unit_cost, ip.qty_received,
                                    ip.inv_units_purchased, ip.extended_cost, ip.extended_discount, ip.extended_retail,
                                    s.site_number, 
                                    case when substr(ip.source_detail_key, 9,7) == 'Toronto' then substr(ip.source_detail_key, 9, 7)
                                         else substr(ip.source_detail_key, 9,5)
                                    end as sys_environment_name,
                                    ss.division_desc,
                                    p.item_number, p.product_key, p.item_desc, p.upc, p.sell_unit_qty ,
                                    v.vendor_id, v.deactivated

                            From 
                            dl_edw_na.item_purchases ip

                            Join dl_edw_na.site s 
                            on s.site_key = ip.site_key
                            and s.source_system_key = ip.source_system_key

                            Join dl_localized_pricing_all_bu.site ss
                            on ss.site_number = s.site_number
                            and sys_environment_name = ss.sys_environment_name

                            Join dl_edw_na.item i
                            on i.source_system_key = ip.source_system_key
                            and i.item_key = ip.item_key

                            Join dl_localized_pricing_all_bu.product p
                            on sys_environment_name = p.environment_name
                            and i.item_number = p.item_number

                            Join dl_edw_na.vendor v
                            on v.source_system_key = ip.source_system_key
                            and v.vendor_key = ip.vendor_key

                            Where ip.source_detail_key like '%{1}%'
                            and ip.business_date > '2021-01-01'
                            and ip.qty_received > 0
                            and ss.division_desc = '{0}'
                            and v.deactivated != 'Y'
                            and p.upc != 0
                            """.format(business_unit, env))
  
  return  vendor_info

# COMMAND ----------

def get_vendor(db):
  ## Combined with vendor_purchase_info, this returns the vendor_id that should be used for filtering cost data later
  
  df1 = db.groupby(['product_key', 'site_number', 'vendor_id']).agg(sum('qty_received').alias('tot_qty'))
  win = Window.partitionBy('product_key', 'site_number', 'vendor_id').orderBy(desc('tot_qty'))
  vendor_sel_skel = df1.withColumn('rank', rank().over(win)).filter(col('rank') == 1)
  
  return vendor_sel_skel.select('product_key', 'site_number', 'vendor_id', 'tot_qty')
    
  

# COMMAND ----------

def get_validations(db_dir, bu, refresh):
  
  ## Setup columns needed for validations. These will show up in the final excel validation outputs
  
  col_order_2 = ['division_desc', 'tempe_product_key', 'upc', 'category_desc', 'sub_category_desc', 'price_family', 'item_desc', 'sell_unit_qty', 'site_number', 'site_state_id', 'Cluster', 'price_zone', 'group', 'gross_price', 'gross_price_update', 'total_cost', 'total_cost_update', 'abs_margin', 'perc_margin'] 

  price_cost_initial = spark.sql("Select * from {}.{}_opti_price_cost_{}_initial".format(db_dir, bu, refresh.lower()))\
                          .withColumn('gross_price_update', lit(None).cast(FloatType())).withColumn('total_cost_update', lit(None).cast(FloatType()))\
                          .select(col_order_2)
  
  return price_cost_initial


# COMMAND ----------

def get_validations_ext(db_dir, bu, refresh):
  
  ## Setup columns needed for validations. These will show up in the final excel validation outputs
  
  col_order_2 = ['division_desc', 'tempe_product_key', 'upc', 'category_desc', 'sub_category_desc', 'price_family', 'item_desc', 'sell_unit_qty', 'site_number', 'site_state_id', 'Cluster', 'price_zone', 'group', 'gross_price', 'gross_price_update', 'total_cost', 'total_cost_update', 'abs_margin', 'perc_margin'] 

  price_cost_initial = spark.sql("Select * from {}.{}_opti_price_cost_{}_initial_extension_validations".format(db_dir, bu, refresh.lower()))\
                          .withColumn('gross_price_update', lit(None).cast(FloatType())).withColumn('total_cost_update', lit(None).cast(FloatType()))\
                          .select(col_order_2)
  
  return price_cost_initial

# COMMAND ----------

def get_validation_flags(tbl):

  ## Flags are created here - based on the flags, the data will be pivoted for category summary or written as it is into separate sheets
  final_table_w_valid = tbl.withColumn('issue_flag', 
                 when((col('gross_price').isNull()) | (col('gross_price')==0) | (col('gross_price')<0), '1-Price Issue').
                 when((col('total_cost').isNull())  | (col('total_cost')==0) | (col('total_cost')<0), '2-Cost Issue').
                 when(((col('gross_price').isNotNull()) | (col('gross_price') >= 0)) & ((col('total_cost').isNull()) | (col('total_cost') >= 0)) & (col('perc_margin') < 0), 
                      lit('3-Negative Margin')).
                 when(((col('gross_price').isNotNull()) | (col('gross_price') >= 0)) & ((col('total_cost').isNull()) | (col('total_cost') >= 0)) & (col('perc_margin') >= 0) & (col('perc_margin') <= 0.05 ),
                      lit('4-Low Margin (Under 5%)')).
                 when(((col('gross_price').isNotNull()) | (col('gross_price') >= 0)) & ((col('total_cost').isNull()) | (col('total_cost') >= 0)) & (col('perc_margin') > 0.8), 
                      lit('5-High Margin (Over 80%)')).
                 otherwise('Fine')).\
                 withColumn('Cluster', price_cost_initial.Cluster.cast(IntegerType())).\
                 withColumn('gross_price', price_cost_initial.gross_price.cast(FloatType())).\
                 withColumn('total_cost', price_cost_initial.total_cost.cast(FloatType())).\
                 withColumn('abs_margin', price_cost_initial.abs_margin.cast(FloatType())).\
                 withColumn('perc_margin', price_cost_initial.perc_margin.cast(FloatType()))
  
  price_val = final_table_w_valid.filter(col('issue_flag') == '1-Price Issue').drop('total_cost_update')
  cost_val = final_table_w_valid.filter(col('issue_flag') == '2-Cost Issue').drop('gross_price_update')
  margin_val = final_table_w_valid.filter(~(col('issue_flag').isin(['Fine', '1-Price Issue','2-Cost Issue'])))
  
  return final_table_w_valid, price_val, cost_val, margin_val



# COMMAND ----------

def write_validation_output(tbl2, tbl3, tbl4, tbl5):
  
  # Writes the validation flagged data into an excel as separate sheets
  # Validate if there are any changes being made to column ordering and sheet names 
  
  writer = pd.ExcelWriter("opti_price_cost_validations.xlsx", engine='xlsxwriter')
  
  cat_summary = tbl5.groupby(['category_desc']).pivot('issue_flag').agg(count("*")).fillna(0) 

  cat_summary.toPandas().to_excel(writer, sheet_name = 'category_summary', index = False)
  tbl2.toPandas().to_excel(writer, sheet_name = 'price_issue', index = False)
  tbl3.toPandas().to_excel(writer, sheet_name = 'cost_issue', index = False)
  tbl4.toPandas().to_excel(writer, sheet_name = 'margin_issue', index = False)
  tbl5.toPandas().to_excel(writer, sheet_name = 'all_data', index = False)
  
  ## Helps to navigate no data errors file reading the file back
  p_c_temp = [' ', 0, 0, ' ', ' ', ' ', ' ', 0, 0, ' ', 0, ' ', 0.0, 0.0, 0.0, 0.0, 0.0, ' ']
  m_a_temp = [' ', 0, 0, ' ', ' ', ' ', ' ', 0, 0, ' ', 0, ' ', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ' ']
  format_1 = writer.book.add_format({'num_format': '0.00'})
  

   # Sheet 1
  sheet1 = writer.sheets['category_summary']
  sheet1.ignore_errors({'number_stored_as_text': 'A:G'})
  sheet1.set_column('A:G', 35)
  sheet1.freeze_panes(1, 1)
  sheet1.hide_gridlines(1)

  # Sheet 2
  sheet2 = writer.sheets['price_issue']
  sheet2.ignore_errors({'number_stored_as_text': 'A:S'})
  sheet2.set_column('A:A', 35)
  sheet2.set_column('D:G', 35)
  sheet2.set_column('S:S', 35)
  sheet2.set_column('O:O', 18)
  sheet2.set_column('B:C', 18)
  sheet2.set_column('H:J', 18)
  sheet2.set_column('K:R', 12)
  sheet2.set_column('N1:N10000', None, format_1)
  sheet2.set_column('P1:R10000', None, format_1)
  sheet2.freeze_panes(1, 0)
  sheet2.hide_gridlines(1)
  if tbl2.count() == 0:
    sheet2.write_row(1, 0, p_c_temp)

  # Sheet 3
  sheet3 = writer.sheets['cost_issue']
  sheet3.ignore_errors({'number_stored_as_text': 'A:S'})
  sheet3.set_column('A:A', 35)
  sheet3.set_column('D:G', 35)
  sheet3.set_column('S:S', 35)
  sheet3.set_column('P:P', 18)
  sheet3.set_column('B:C', 18)
  sheet3.set_column('H:J', 18)
  sheet3.set_column('K:R', 12)
  sheet3.set_column('N1:O10000', None, format_1)
  sheet3.set_column('Q1:R10000', None, format_1)  
  sheet3.freeze_panes(1, 0)
  sheet3.hide_gridlines(1)
  if tbl3.count() == 0:
    sheet3.write_row(1, 0, p_c_temp)

  # Sheet 4
  sheet4 = writer.sheets['margin_issue']
  sheet4.ignore_errors({'number_stored_as_text': 'A:T'})
  sheet4.set_column('A:A', 35)
  sheet4.set_column('D:G', 35)
  sheet4.set_column('T:T', 35)
  sheet4.set_column('O:O', 18)
  sheet4.set_column('Q:Q', 18)
  sheet4.set_column('B:C', 18)
  sheet4.set_column('H:J', 18)
  sheet4.set_column('K:S', 12)
  sheet4.set_column('N1:N10000', None, format_1)
  sheet4.set_column('P1:P10000', None, format_1)
  sheet4.set_column('R1:S10000', None, format_1)
  sheet4.freeze_panes(1, 0)
  sheet4.hide_gridlines(1)
  if tbl4.count() == 0:
    writer.sheets['margin_issue'].write_row(1, 0, m_a_temp)

  #Sheet 5
  sheet5 = writer.sheets['all_data']
  sheet5.ignore_errors({'number_stored_as_text': 'A:T'})
  sheet5.set_column('A:A', 35)
  sheet5.set_column('D:G', 35)
  sheet5.set_column('T:T', 35)
  sheet5.set_column('O:O', 18)
  sheet5.set_column('Q:Q', 18)
  sheet5.set_column('B:C', 18)
  sheet5.set_column('H:J', 18)
  sheet5.set_column('K:S', 12)
  sheet5.set_column('N1:N10000', None, format_1)
  sheet5.set_column('P1:P10000', None, format_1)
  sheet5.set_column('R1:S10000', None, format_1)
  sheet5.freeze_panes(1, 0)
  sheet5.hide_gridlines(1)


  return writer, cat_summary
  

# COMMAND ----------

def write_validation_output_ext(tbl2, tbl3, tbl4, tbl5):
  
  from shutil import move

  # Writes the validation flagged data into an excel as separate sheets
  # Validate if there are any changes being made to column ordering and sheet names 
  #os.chdir("/dbfs/Phase4_extensions/Optimization/Nov21_Refresh/inputs/qe/")
  tmp_path = 'opti_price_cost_validations_ext.xlsx'
  #actual_path= "/dbfs/Phase4_extensions/Optimization/Nov21_Refresh/inputs/qe/opti_price_cost_validations_ext.xlsx"
  #writer = ExcelWriter(tmp_path)

  writer = pd.ExcelWriter(tmp_path, engine='xlsxwriter')
   
  
  cat_summary = tbl5.groupby(['category_desc']).pivot('issue_flag').agg(count("*")).fillna(0) 

  cat_summary.toPandas().to_excel(writer, sheet_name = 'category_summary', index = False)
  
  tbl2.toPandas().to_excel(writer, sheet_name = 'price_issue', index = False)
  tbl3.toPandas().to_excel(writer, sheet_name = 'cost_issue', index = False)
  tbl4.toPandas().to_excel(writer, sheet_name = 'margin_issue', index = False)
  tbl5.toPandas().to_excel(writer, sheet_name = 'all_data', index = False)
  
  ## Helps to navigate no data errors file reading the file back
  p_c_temp = [' ', 0, 0, ' ', ' ', ' ', ' ', 0, 0, ' ', 0, ' ', 0.0, 0.0, 0.0, 0.0, 0.0, ' ']
  m_a_temp = [' ', 0, 0, ' ', ' ', ' ', ' ', 0, 0, ' ', 0, ' ', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ' ']
  format_1 = writer.book.add_format({'num_format': '0.00'})
  

   # Sheet 1
  sheet1 = writer.sheets['category_summary']
  sheet1.ignore_errors({'number_stored_as_text': 'A:G'})
  sheet1.set_column('A:G', 35)
  sheet1.freeze_panes(1, 1)
  sheet1.hide_gridlines(1)

  # Sheet 2
  sheet2 = writer.sheets['price_issue']
  sheet2.ignore_errors({'number_stored_as_text': 'A:S'})
  sheet2.set_column('A:A', 35)
  sheet2.set_column('D:G', 35)
  sheet2.set_column('S:S', 35)
  sheet2.set_column('O:O', 18)
  sheet2.set_column('B:C', 18)
  sheet2.set_column('H:J', 18)
  sheet2.set_column('K:R', 12)
  sheet2.set_column('N1:N10000', None, format_1)
  sheet2.set_column('P1:R10000', None, format_1)
  sheet2.freeze_panes(1, 0)
  sheet2.hide_gridlines(1)
  if tbl2.count() == 0:
    sheet2.write_row(1, 0, p_c_temp)

  # Sheet 3
  sheet3 = writer.sheets['cost_issue']
  sheet3.ignore_errors({'number_stored_as_text': 'A:S'})
  sheet3.set_column('A:A', 35)
  sheet3.set_column('D:G', 35)
  sheet3.set_column('S:S', 35)
  sheet3.set_column('P:P', 18)
  sheet3.set_column('B:C', 18)
  sheet3.set_column('H:J', 18)
  sheet3.set_column('K:R', 12)
  sheet3.set_column('N1:O10000', None, format_1)
  sheet3.set_column('Q1:R10000', None, format_1)  
  sheet3.freeze_panes(1, 0)
  sheet3.hide_gridlines(1)
  if tbl3.count() == 0:
    sheet3.write_row(1, 0, p_c_temp)

  # Sheet 4
  sheet4 = writer.sheets['margin_issue']
  sheet4.ignore_errors({'number_stored_as_text': 'A:T'})
  sheet4.set_column('A:A', 35)
  sheet4.set_column('D:G', 35)
  sheet4.set_column('T:T', 35)
  sheet4.set_column('O:O', 18)
  sheet4.set_column('Q:Q', 18)
  sheet4.set_column('B:C', 18)
  sheet4.set_column('H:J', 18)
  sheet4.set_column('K:S', 12)
  sheet4.set_column('N1:N10000', None, format_1)
  sheet4.set_column('P1:P10000', None, format_1)
  sheet4.set_column('R1:S10000', None, format_1)
  sheet4.freeze_panes(1, 0)
  sheet4.hide_gridlines(1)
  if tbl4.count() == 0:
    writer.sheets['margin_issue'].write_row(1, 0, m_a_temp)

  #Sheet 5
  sheet5 = writer.sheets['all_data']
  sheet5.ignore_errors({'number_stored_as_text': 'A:T'})
  sheet5.set_column('A:A', 35)
  sheet5.set_column('D:G', 35)
  sheet5.set_column('T:T', 35)
  sheet5.set_column('O:O', 18)
  sheet5.set_column('Q:Q', 18)
  sheet5.set_column('B:C', 18)
  sheet5.set_column('H:J', 18)
  sheet5.set_column('K:S', 12)
  sheet5.set_column('N1:N10000', None, format_1)
  sheet5.set_column('P1:P10000', None, format_1)
  sheet5.set_column('R1:S10000', None, format_1)
  sheet5.freeze_panes(1, 0)
  sheet5.hide_gridlines(1)
  #writer.save() 
  writer.save()
  move("opti_price_cost_validations_ext.xlsx", "/dbfs/Phase4_extensions/Optimization/Nov21_Refresh/inputs/gl/gl_opti_price_cost_validations_ext.xlsx")
  #shutil.move(tmp_path, actual_path)
  return writer, cat_summary
  

# COMMAND ----------

def price_cost_corrections(tbl, tbl2, tbl3, tbl4):
  
  ## Convert to spark DF - Done separately to avoid accidental errors due to missing data or wrong column types
  pc_correction_price = spark.createDataFrame(tbl)
  pc_correction_cost = spark.createDataFrame(tbl2)
  pc_correction_margin = spark.createDataFrame(tbl3).withColumnRenamed('gross_price_update', 'gp_marg').withColumnRenamed('total_cost_update', 'tc_marg')
  pc_correction_all_data = spark.createDataFrame(tbl4).withColumnRenamed('gross_price_update', 'gp_all').withColumnRenamed('total_cost_update', 'tc_all')

  ## Merge all changes into final price and cost columns 
  price_cost_correction_1 = price_cost_initial.join(pc_correction_price, ['tempe_product_key', 'site_number'], 'left')\
                            .select(price_cost_initial["*"], pc_correction_price["gross_price_update"]).distinct()
  price_cost_correction_2 = price_cost_correction_1.join(pc_correction_cost, ['tempe_product_key', 'site_number'], 'left')\
                            .select(price_cost_correction_1["*"], pc_correction_cost["total_cost_update"]).distinct()
  price_cost_correction_3 = price_cost_correction_2.join(pc_correction_margin, ['tempe_product_key', 'site_number'], 'left')\
                            .select(price_cost_correction_2["*"],pc_correction_margin["gp_marg"], pc_correction_margin["tc_marg"]).distinct()
  price_cost_correction_4 = price_cost_correction_3.join(pc_correction_all_data, ['tempe_product_key', 'site_number'], 'left')\
                            .select(price_cost_correction_3["*"], pc_correction_all_data["gp_all"], pc_correction_all_data["tc_all"]).distinct()

  price_cost_corrected   = price_cost_correction_4\
                          .withColumn('gross_price', when((col('gross_price_update').isNotNull()) & ((col('gp_all') == col('gross_price'))|                                                               (col('gp_all').isNull()) | (col('gp_all') == col('gross_price_update'))), col('gross_price_update'))
                          .when((col('gross_price_update').isNull()) & (col('gp_all') != col('gross_price')),col('gp_all'))
                          .otherwise(col('gross_price')))\
                          .withColumn('gross_price', when((col('gp_marg').isNotNull()) & ((col('gp_all') == col('gross_price')) | (col('gp_all').isNull()) | (col('gp_all') ==                                col('gp_marg'))), col('gp_marg'))
                          .when((col('gp_marg').isNull()) & (col('gp_all') != col('gross_price')), col('gp_all'))
                          .otherwise(col('gross_price')))\
                          .withColumn('total_cost', when((col('total_cost_update').isNotNull()) & ((col('tc_all') == col('total_cost')) | (col('tc_all').isNull()) |                                       (col('tc_all') == col('total_cost_update'))), col('total_cost_update'))
                          .when((col('total_cost_update').isNull()) & (col('tc_all') != col('total_cost')), col('tc_all')).otherwise(col('total_cost')))\
                          .withColumn('total_cost', when((col('tc_marg').isNotNull()) & ((col('tc_all') == col('total_cost'))| (col('tc_all').isNull() ) | (col('tc_all') ==                                    col('tc_marg'))), col('tc_marg'))
                          .when((col('tc_marg').isNull()) & (col('tc_all') != col('total_cost')), col('tc_all'))
                          .otherwise(col('total_cost')))
  
  return price_cost_corrected

# COMMAND ----------

def get_correct_margins(tb1):
  tb1= tb1.withColumn("abs_margin", round((col('gross_price') - col('total_cost')),2))\
                        .withColumn('perc_margin', round((1-col('total_cost')/col('gross_price')),2))
  tb1= tb1.withColumn("unit_cost", round((col('total_cost')/col('sell_unit_qty')),2))
  return tb1

# COMMAND ----------

def check_duplicate(tb1, business_unit):
  
  scope= sqlContext.sql("SELECT *, 1 as dummy, product_key as tempe_product_key,item_name as item_desc FROM circlek_db.lp_dashboard_items_scope WHERE                                    bu='{}'".format(business_unit))
                        

  windowSpec = Window.partitionBy('key').orderBy('gross_price','total_cost')
  windowSpec_1 = Window.partitionBy('key').orderBy(col('cost_start_date').desc())
  windowSpec_2 = Window.partitionBy('key').orderBy(col('effective_start_date').desc())

  price_cost_corrected_scope = tb1.join(scope.select('tempe_product_key','item_name'),['tempe_product_key'])\
                               .drop('item_desc')\
                               .withColumnRenamed('item_name', 'item_desc')\
                               .distinct()\
                               .withColumn('key',concat_ws('_',tb1.tempe_product_key,tb1.site_number))\
                               .withColumn('rownb_cost', row_number().over(windowSpec_1))\
                               .withColumn('rownb_price', row_number().over(windowSpec_2))

  price_cost_corrected_scope =price_cost_corrected_scope.filter(price_cost_corrected_scope.rownb_cost == 1)\
                              .filter(price_cost_corrected_scope.rownb_price == 1)

  price_cost_corrected_scope = price_cost_corrected_scope.withColumn('rownb', row_number().over(windowSpec))\

  duplicate_price_cost = price_cost_corrected_scope.filter((price_cost_corrected_scope.rownb >1))
  initial_count = tb1.select('tempe_product_key','site_number').distinct().count()
  initial_count_price_cost = tb1.select('tempe_product_key','site_number','gross_price','total_cost').distinct().count()
  final_count = price_cost_corrected_scope.count()

  if initial_count == final_count and duplicate_price_cost.count() == 0:
    print('All good! No duplicate item x site combination found!')
  else: 
     raise Exception("duplicate price or cost for same item x site combination") 
  return price_cost_corrected_scope

# COMMAND ----------

def get_flagged_items(tb1):
  tb2 = tb1.filter((col('gross_price') <= 0 ) | (col('gross_price').isNull()) | (col("total_cost") <=0) | (col('total_cost').isNull()) | (col("abs_margin") < 0 ))
  return tb2 
