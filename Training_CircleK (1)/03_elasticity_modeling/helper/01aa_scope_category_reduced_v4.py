# Databricks notebook source
from datetime import *
import databricks.koalas as ks
import pandas as pd
import numpy as np

# COMMAND ----------

# Define parameters used in notebook
# business_units = '4200 - Great Lakes Division'
# bu_code = 'GL'
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'

# Define the database for tables and directory paths for input & output files
db = 'phase4_extensions'
base_directory = 'Phase4_extensions/Elasticity_Modelling'

# Modify for each new wave
# wave = 'Accenture_Refresh'
wave = dbutils.widgets.get("wave")

# Define the user
# user = 'sanjo'
user = dbutils.widgets.get("user")

# Define the category
# category_raw = '008-CANDY'
# category = '008_CANDY'
category = dbutils.widgets.get("category_name")

# Define computed variables
pre_wave= '/'+base_directory+'/'
bu = bu_code+'_Repo'

# DB table prefix
db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# COMMAND ----------

#user = ['prat','roopa','sterling','neil','taru','colby','kushal','logan','prakhar','neil','dayton','david','anuj','xiaofan','global_tech','jantest','aadarsh','jantest2','tushar']
# user = ['sanjo']

#category_name = ['002_CIGARETTES','003_OTHER_TOBACCO_PRODUCTS','004_BEER','005_WINE','006_LIQUOR','007_PACKAGED_BEVERAGES','008_CANDY','009_FLUID_MILK','010_OTHER_DAIRY_DELI_PRODUCT', '012_PCKGD_ICE_CREAM_NOVELTIES','013_FROZEN_FOODS','014_PACKAGED_BREAD','015_SALTY_SNACKS','016_PACKAGED_SWEET_SNACKS','017_ALTERNATIVE_SNACKS','019_EDIBLE_GROCERY','020_NON_EDIBLE_GROCERY','021_HEALTH_BEAUTY_CARE','022_GENERAL_MERCHANDISE','024_AUTOMOTIVE_PRODUCTS','028_ICE','030_HOT_DISPENSED_BEVERAGES',	'031_COLD_DISPENSED_BEVERAGES',	'032_FROZEN_DISPENSED_BEVERAGES','085_SOFT_DRINKS','089_FS_PREP_ON_SITE_OTHER','091_FS_ROLLERGRILL','092_FS_OTHER','094_BAKED_GOODS','095_SANDWICHES','503_SBT_PROPANE','504_SBT_GENERAL_MERCH','507_SBT_HBA']

#Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                  "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                  "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                 "Central Division","Western Division"]

#BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
#          ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
#          ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
#          ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# BU_abbr = {"1400 - Florida Division": 'FL',
#            "1600 - Coastal Carolina Division": 'CC',
#            "1700 - Southeast Division": "SE",
#            "1800 - Rocky Mountain Division": 'RM',
#            "1900 - Gulf Coast Division": "GC",
#            "2600 - West Coast Division": "WC",
#            "2800 - Texas Division": 'TX',
#            "2900 - South Atlantic Division": "SA",
#            "3100 - Grand Canyon Division": "GR",
#            "3800 - Northern Tier Division": "NT",
#            "4100 - Midwest Division": 'MW',
#            "4200 - Great Lakes Division": "GL",
#            "4300 - Heartland Division": 'HLD',
#            "QUEBEC OUEST": 'QW',
#            "QUEBEC EST - ATLANTIQUE": 'QE',
#            "Central Division": 'CE',
#            "Western Division": 'WC'
#           }

#wave = ["JAN2021_TestRefresh","Mar2021_Refresh_Group1"]
# wave = ["Accenture_Refresh"]

#dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])
# dbutils.widgets.dropdown("user", "sanjo", [str(x) for x in user])
#dbutils.widgets.dropdown("category_name", "013_FROZEN_FOODS", [str(x) for x in category_name])

#Define parameters used in notebook
# business_unit = '3100 - Grand Canyon Division'
# wave = 'JAN2021_TestRefresh'
# user = 'taru'
# category_name = '013_FROZEN_FOODS'
# pre_wave = '/Phase3_extensions/Elasticity_Modelling/'

#get bu 2 character code
#bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
#bu_abb = bu_abb_ds.collect()[0][1].lower()

# for key in BU_abbr:
#   if key != business_unit:
#     continue
#   bu = BU_abbr[key] + '_Repo'

# COMMAND ----------

# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD", "SW", "IE", "NO"]
# dbutils.widgets.dropdown("bu_code", "GL", bu_codes)

# COMMAND ----------

scope_category = ks.read_csv(pre_wave+wave+'/'+bu+'/Output/intermediate_files/scope_category_'+user+'_'+category+'.csv') #ps v9

# COMMAND ----------

# sf v4
reduced_ads = ks.read_csv(pre_wave+wave+'/'+bu+'/Modelling_Inputs/modelling_ads_cluster_reduced.csv') #ps v9

# COMMAND ----------

# sf v4
scope_cat_reduced = ks.sql(""" select * from {scope_category} where ITEMID in (select itemid from {reduced_ads})""")

# COMMAND ----------

# sf v4
reduced_scope_cat = scope_cat_reduced.to_pandas()

# COMMAND ----------

# sf v4
reduced_scope_cat.to_csv('/dbfs/'+pre_wave+wave+'/'+bu+'/Output/intermediate_files/scope_category_reduced_'+user+'_'+category+'.csv',index=False)

# spark.createDataFrame(reduced_scope_cat).write.mode('overwrite').saveAsTable('localized_pricing.'+db_tbl_prefix+'output_int_scope_category_reduced_'+user+'_'+category)
