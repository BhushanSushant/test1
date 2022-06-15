# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ###### Setup 

# COMMAND ----------

### Libraries 

import datetime
from pyspark.sql import *
from pyspark.sql import functions as sf
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StringType,BooleanType,DateType, IntegerType
from datetime import *

import pandas as pd
import numpy as np
from datetime import timedelta
import datetime as dt

# COMMAND ----------


# #This is to help know the abbrevations
# Business_Units = ["1400 - Florida Division",
#                   "1600 - Coastal Carolina Division",
#                   "1700 - Southeast Division",
#                   "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division",
#                   "2800 - Texas Division",
#                   "3100 - Grand Canyon Division",
#                   "QUEBEC OUEST",
#                   "Central Division"
#                   ]

# BU_test_control = ["Central Canada", "Coastal Carolinas", "Florida", "Grand Canyon", "Gulf Coast", "Quebec West", "Rocky Mountain", "Southeast", "Sweden", "Texas"]


# BU_Abbreviation = ["fl","cc","se","rm","gc","tx","gr","qw","ce"]

# dbutils.widgets.dropdown("Business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("BU_abbreviation", "gr", BU_Abbreviation)
# dbutils.widgets.dropdown("BU_test_control", "Grand Canyon", BU_test_control)

# business_unit = dbutils.widgets.get("Business_unit")
# bu = dbutils.widgets.get("BU_abbreviation")
# bu_test_control = dbutils.widgets.get("BU_test_control")

# COMMAND ----------

# MAGIC %sql
# MAGIC Refresh table circlek_db.grouped_store_list;
# MAGIC Refresh table circlek_db.lp_dashboard_items_scope;

# COMMAND ----------

###  Scope and store list
def get_scope(busines_unit):
  scope = spark.sql("""Select product_key from 
                             (Select product_key, last_update, rank() over(partition by product_key order by last_update desc) as latest 
                                From circlek_db.lp_dashboard_items_scope 
                                Where bu = '{}'
                              ) a
                              Where latest =  1""".format(business_unit))
  product_scope = [x[0] for x in scope.select("product_key").collect()]
  return product_scope

def get_sites(bu_test_control):
  sites = spark.sql("""Select site_id from circlek_db.grouped_store_list 
                              Where business_unit = '{}' 
                              and group = 'Test' """.format(bu_test_control))

  sites_scope = [x[0] for x in sites.select("site_id").collect()]
  return sites_scope


def get_cluster_info(bu_test_control):
  site_info = spark.sql("""Select site_id, Cluster from circlek_db.grouped_store_list 
                              Where business_unit = '{}' 
                              and group = 'Test' """.format(bu_test_control))
  
  return site_info

# COMMAND ----------

#scope.count()

# COMMAND ----------

#sites.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###### Price Data 

# COMMAND ----------

#effective_date = "2021-10-01"

# COMMAND ----------

### PDI connections - latest price as of 10/1

## GETTING THE RIGHT ENVIRONMENT FOR EACH BU: 

def get_env(bu):
  env = "Tempe"
  if bu == "Quebec West":
    env = "Laval"
  elif bu == "Central Canada":
    env = "Toronto"

  return env

## Fetch PDI prices + filter for items x sites from latest txns.
def get_pdi_price(effective_date, env, business_unit, product_scope, sites_scope):
    price_table = spark.sql("""
    SELECT

      all_bu_p.product_key as tempe_product_key,
      all_bu_p.item_number as item_number,
      all_bu_p.category_desc as category_desc,
      all_bu_p.sub_category_desc as sub_category_desc,
      all_bu_p.item_desc as item_desc,
      all_bu_s.division_desc as division_desc,
      all_bu_s.site_number_corporate,
      all_bu_s.site_number,
      vw_irp.gross_price,
      vw_irp.net_price,
      vw_irp.environment_name,
      all_bu_s.site_state_id

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
        AND effective_end_date >= '{0}'
        AND division_desc = '{1}'

    """.format(effective_date, business_unit))\
    .filter(sf.col('environment_name') == env)

    price_table = price_table.filter(sf.col('tempe_product_key').isin(product_scope)).filter(sf.col('site_number').isin(sites_scope))
    return price_table


# COMMAND ----------

#display(price_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ###### Cost Data

# COMMAND ----------


## Fetch PDI costs + filter for items x sites from scoped lists

def get_pdi_cost(effective_date, env, business_unit, product_scope, sites_scope):
    cost_table = spark.sql("""
    SELECT

      all_bu_p.product_key as tempe_product_key,
      all_bu_p.item_number as item_number,
      all_bu_p.category_desc as category_desc,
      all_bu_p.sub_category_desc as sub_category_desc,
      all_bu_p.item_desc as item_desc,
      all_bu_s.division_desc as division_desc,
      all_bu_s.site_number_corporate,
      all_bu_s.site_number,
      vw_ic.total_cost,
      vw_ic.unit_cost,
      vw_ic.environment_name,
      all_bu_s.site_state_id

    FROM dl_edw_na.vw_item_costs vw_ic 

    JOIN dl_localized_pricing_all_bu.site all_bu_s 
      ON vw_ic.site_number = all_bu_s.site_number 
        AND vw_ic.environment_name = all_bu_s.sys_environment_name
    JOIN dl_localized_pricing_all_bu.product all_bu_p 
      ON vw_ic.item_number = all_bu_p.item_number
        AND vw_ic.environment_name = all_bu_p.environment_name
        AND vw_ic.package_quantity = all_bu_p.sell_unit_qty
    WHERE
        effective_start_date <= '{0}'
        AND effective_end_date >= '{0}'
        AND division_desc = '{1}'
        AND cost_is_promo_flag != 'true'
    """.format(effective_date, business_unit))\
    .filter(sf.col('environment_name') == env)
    
    cost_table = cost_table.filter(sf.col('tempe_product_key').isin(product_scope)).filter(sf.col('site_number').isin(sites_scope))
    return cost_table
    

# COMMAND ----------

#display(cost_table)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Roll ups 

# COMMAND ----------

#price_cost = price_table.join(cost_table, ['tempe_product_key', 'site_number'], 'left').select(price_table["*"], "total_cost", "unit_cost")

# COMMAND ----------

def get_price_cost_rollup(price_table, cost_table, site_info):

  price_cost = price_table.join(cost_table, ['tempe_product_key', 'site_number'], 'left').select(price_table["*"], "total_cost", "unit_cost")
  cluster_price_cost =  price_cost.join(site_info, [site_info.site_id == price_cost.site_number], 'left')
  cluster_price_cost =  cluster_price_cost.withColumn('Cluster', cluster_price_cost.Cluster.cast(IntegerType()))

  win = Window.partitionBy(['division_desc', 'tempe_product_key', 'Cluster']).orderBy(sf.desc('count'))
  rolled_price = cluster_price_cost.filter(sf.col('gross_price').isNotNull()).\
                                  filter(sf.col('total_cost').isNotNull()).\
                                  groupBy(['division_desc','tempe_product_key', 'Cluster','gross_price']).count().\
                                  withColumn('rank', sf.rank().over(win)).filter(sf.col('rank') == 1).drop('rank').\
                                  groupBy(['division_desc','tempe_product_key']).pivot('Cluster').avg('gross_price')

  rolled_price = rolled_price.select([sf.col(c).alias("retail_price_" + c) for c in rolled_price.columns])
  
  rolled_cost = cluster_price_cost.filter(sf.col('gross_price').isNotNull()).\
                                  filter(sf.col('total_cost').isNotNull()).\
                                  groupBy(['division_desc','tempe_product_key', 'Cluster','total_cost']).count().\
                                  withColumn('rank', sf.rank().over(win)).filter(sf.col('rank') == 1).drop('rank').\
                                  groupBy(['division_desc','tempe_product_key']).pivot('Cluster')\
                                                                 .agg(sf.avg('total_cost'))
  rolled_cost = rolled_cost.select([sf.col(c).alias("total_cost_" + c) for c in rolled_cost.columns])
  rolled_price_cost = rolled_price.join(rolled_cost, [rolled_price.retail_price_division_desc == rolled_cost.total_cost_division_desc, 
                                                      rolled_price.retail_price_tempe_product_key == rolled_cost.total_cost_tempe_product_key]).\
                                  drop('total_cost_division_desc').drop('total_cost_tempe_product_key').\
                                  withColumnRenamed('retail_price_division_desc', 'division_desc').\
                                  withColumnRenamed('retail_price_tempe_product_key', 'tempe_product_key')


  return  rolled_price_cost


# COMMAND ----------

def val(data):
  final_out =rolled_price_cost.withColumn('margin', round(1 - sf.col('avg_total_cost')/sf.col('avg_retail_price'),2)).\
                               withColumn('flag', when(sf.col('margin') < 0, sf.lit('negative_margin'))
                                                       .when((sf.col('margin') >0) & (sf.col('margin') < 0.05), sf.lit('low_margin'))
                                                       .when(sf.col('margin') > 0.80, sf.lit('high margin')))
  return final_out

# COMMAND ----------

# # # # ## Test Run BUs

# bu_test_control = 'Quebec West' # 'Grand Canyon'
# business_unit =  'QUEBEC OUEST'  # '1400 - Florida Division' #'3100 - Grand Canyon Division'
# effective_date = '2021-10-01'

# COMMAND ----------

# output = get_price_cost_rollup(get_pdi_price(effective_date, get_env(bu_test_control), business_unit, get_scope(business_unit), get_sites(bu_test_control)), get_pdi_cost(effective_date, get_env(bu_test_control), business_unit, get_scope(business_unit), get_sites(bu_test_control)), get_cluster_info(bu_test_control))


# COMMAND ----------

#display(output)

# COMMAND ----------

def dev_test(price_table, cost_table, site_info):

  price_cost = price_table.join(cost_table, ['tempe_product_key', 'site_number'], 'left').select(price_table["*"], "total_cost", "unit_cost")
  cluster_price_cost =  price_cost.join(site_info, [site_info.site_id == price_cost.site_number], 'left')
  
  win = Window.partitionBy(['division_desc', 'tempe_product_key', 'Cluster']).orderBy(sf.desc('count'))
  rolled_price = cluster_price_cost.filter(sf.col('gross_price').isNotNull()).\
                                  filter(sf.col('total_cost').isNotNull()).\
                                  groupBy(['division_desc','tempe_product_key', 'Cluster','gross_price']).count().\
                                  withColumn('rank', sf.rank().over(win)).filter(sf.col('rank') == 1).drop('rank').\
                                  groupBy(['division_desc','tempe_product_key']).pivot('Cluster').avg('gross_price').alias('retail')
  rolled_price = rolled_price.select([sf.col(c).alias("retail_price_" + c) for c in rolled_price.columns])
  rolled_cost = cluster_price_cost.filter(sf.col('gross_price').isNotNull()).\
                                  filter(sf.col('total_cost').isNotNull()).\
                                  groupBy(['division_desc','tempe_product_key', 'Cluster','total_cost']).count().\
                                  withColumn('rank', sf.rank().over(win)).filter(sf.col('rank') == 1).drop('rank').\
                                  groupBy(['division_desc','tempe_product_key']).pivot('Cluster')\
                                                                 .agg(sf.avg('total_cost'))
  rolled_cost = rolled_cost.select([sf.col(c).alias("total_cost_" + c) for c in rolled_cost.columns])
  rolled_price_cost = rolled_price.join(rolled_cost, [rolled_price.retail_price_division_desc == rolled_cost.total_cost_division_desc, 
                                                      rolled_price.retail_price_tempe_product_key == rolled_cost.total_cost_tempe_product_key]).\
                                  drop('total_cost_division_desc').drop('total_cost_tempe_product_key').\
                                  withColumnRenamed('retail_price_division_desc', 'division_desc').\
                                  withColumnRenamed('retail_price_tempe_product_key', 'tempe_product_key')
  
  
  return rolled_price_cost

