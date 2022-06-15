# Databricks notebook source
# This notebook creates sheet 'scope_file' 
# This excel file is one of the input files for elasticity modelling
# Input to this notebook: modelling_ads_cluster_stateless.Rds
# Output: scope sheet for the BU having all inscope items for all categories

# Modified by: Sanjo Jose
# Date Modified: 26/08/2021
# Modifications: Test run for Phase 4 BUs, Converted widgets to parameters, Other Minor modifications

# The final version is modified by Noah Yang:
# Data Modified: 10/26/2021
# Modifications: The paramaters are changed from manual inputs to widgets

# COMMAND ----------

#!pip install xlsxwriter
# dbutils.library.installPyPI('xlrd','1.2.0')
# dbutils.library.restartPython()

#!pip3 install koalas #ps

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
from dateutil.relativedelta import relativedelta
from datetime import *
import datetime
import pandas as pd
import databricks.koalas as ks
import xlsxwriter
import xlrd

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
spark.conf.set("spark.databricks.io.cache.enabled", "false")
sqlContext.clearCache()

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# Define parameters used in notebook
# business_units = '4200 - Great Lakes Division'  # NY v202110
# bu_code = 'GL'  # NY v202110
business_units = dbutils.widgets.get("business_unit")  # NY v202110
bu_code = dbutils.widgets.get("bu_code")  # NY v202110

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
# wave = 'Accenture_Refresh'  # NY v202110
wave = dbutils.widgets.get("wave")  # NY v202110

# Old wave modelling input file path
old_wave_modelling_input_file = '/dbfs/Phase3_extensions/Elasticity_Modelling/JAN2021_TestRefresh/GR_Repo/Input/modelling_input_file.xlsx'

# DB table prefix
db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# COMMAND ----------

# MAGIC %r
# MAGIC # Define R libraries
# MAGIC library(tidyverse)
# MAGIC library(readxl)
# MAGIC library(parallel)
# MAGIC library(lubridate)
# MAGIC options(scipen = 999)
# MAGIC 
# MAGIC # Define R parameters used in notebook
# MAGIC # business_units <- '4200 - Great Lakes Division'  # NY v202110
# MAGIC # bu_code <- 'GL'  # NY v202110
# MAGIC business_units <- dbutils.widgets.get("business_unit")  # NY v202110
# MAGIC bu_code <- dbutils.widgets.get("bu_code")  # NY v202110
# MAGIC 
# MAGIC # Define Region
# MAGIC NA_BU <- c("FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD")
# MAGIC EU_BU <- c("IE", "SW", "NO", "DK", "PL")
# MAGIC 
# MAGIC if (bu_code %in% NA_BU){
# MAGIC   region <- 'NA'
# MAGIC   }
# MAGIC else if (bu_code %in% EU_BU){
# MAGIC   region <- 'EU'
# MAGIC   }
# MAGIC 
# MAGIC base_directory <- 'Phase4_extensions/Elasticity_Modelling'
# MAGIC # wave <- 'Accenture_Refresh'  # NY v202110
# MAGIC wave <- dbutils.widgets.get("wave")  # NY v202110

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# # sf
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                   "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                  "Central Division","Western Division"]

# BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
#           ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
#           ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
#           ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# # wave = ["JAN2021_TestRefresh","MAY2021_Refresh","JUNE2021_Refresh"] #ps v10
# wave = ['Accenture_Refresh', 'Nov21_Refresh']


# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])

# # get bu 2 character code
# bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# bu_abb = bu_abb_ds.collect()[0][1].lower()
# bu = bu_abb.upper()+'_Repo'
# wave=dbutils.widgets.get("wave")
# pre_wave = '/Phase4_extensions/Elasticity_Modelling/'

# COMMAND ----------

# %python
# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GL", bu_codes)

# COMMAND ----------

# %r
# library(tidyverse)
# library(readxl)
# library(parallel)
# #library(openxlsx)
# library(lubridate)
# options(scipen = 999)
# Business_Units = c("1400 - Florida Division","1600 - Coastal Carolina Division", "1700 - Southeast Division","1800 - Rocky Mountain Division","1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division",  "2900 - South Atlantic Division", "3100 - Grand Canyon Division",  "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division",  "Western Division")
# BU_abbr =tolower(c("FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WC"))

# bu_df =data.frame(Business_Units,BU_abbr)
# bu_abb = bu_df %>% filter(Business_Units %in% dbutils.widgets.get("business_unit")) %>% select(BU_abbr)
# bu_abb = bu_abb[1,1]
# bu <- paste(toupper(bu_abb), "_Repo", sep="")  # GC_repo #SE_repo #TC: automate based on short forms

# #other inputs

# wave <- dbutils.widgets.get("wave")
# pre_wave <- '/dbfs/Phase4_extensions/Elasticity_Modelling/'

# COMMAND ----------

# #sf
# print("bu: "+bu_code+" || wave: "+wave)

# COMMAND ----------

# MAGIC %r
# MAGIC # Unify the input # NY v202201
# MAGIC if (file.exists(paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_state_zone_new.Rds'))) {
# MAGIC   ads_file <- readRDS(paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_state_zone_new.Rds'))
# MAGIC   saveRDS(ads_file, paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_final.Rds'))
# MAGIC }
# MAGIC if (file.exists(paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_stateless_zone_new.Rds'))) {
# MAGIC   ads_file <- readRDS(paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_stateless_zone_new.Rds'))
# MAGIC   saveRDS(ads_file, paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_final.Rds'))
# MAGIC }
# MAGIC 
# MAGIC # sf v2 switched to final
# MAGIC ads_file <- readRDS(paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_final.Rds'))

# COMMAND ----------

# %r
# # sf v2 switched to final
# ads_file <- readRDS(paste0('/dbfs/Phase4_extensions/Elasticity_Modelling/Nov21_Refresh/QE_Repo/Modelling_Inputs/modelling_ads_cluster_stateless_zone_new.Rds'))

# COMMAND ----------

# MAGIC %r
# MAGIC # sf v2 switched to final
# MAGIC write.csv(ads_file, paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_final.csv'), row.names = FALSE)

# COMMAND ----------

# sf v2 switched to final
# ads_df = pd.read_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/'+'Modelling_Inputs/modelling_ads_cluster_final.csv')

ads_df = ks.read_csv('/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/'+'Modelling_Inputs/modelling_ads_cluster_final.csv')

# COMMAND ----------

# ads_df = pd.read_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/'+'Modelling_Inputs/modelling_ads_cluster_final.csv')
# tmp = ads_df.merge(product_map_df, how='left', left_on='itemid', right_on='product_key')
# tmp = tmp[tmp.category_desc.isin(['005-WINE','006-LIQUOR','004-BEER','085-SOFT DRINKS'])]
# tmp = tmp[['itemid','Cluster','weekdate','sales_qty','revenue','reg_price']]
# display(tmp)


# COMMAND ----------

# ads_reduced = ks.read_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/'+'Modelling_Inputs/modelling_ads_cluster_reduced.csv')

# COMMAND ----------

# sf v3 different thresholds
prcnt_zeroes = ads_df[ads_df['sales_qty']==0].shape[0]/ads_df.shape[0]
if prcnt_zeroes > 0.1:
  prcnt_wks_sold = 0.1
else:
  prcnt_wks_sold = 0.80

# COMMAND ----------

# sf v2 
non_zero_rows = ads_df[ads_df['revenue']!=0]

year_flag = ks.sql(""" select itemid
                           , Cluster
                           , weekdate
                           , year(weekdate) as year 
                               from {non_zero_rows} """)

year_flag['weekdate'] = ks.to_datetime(year_flag['weekdate'])
year_flag['week'] = year_flag['weekdate'].dt.week

various_dates = ks.sql(""" select itemid, Cluster
                                  
                                  , min(weekdate) as earliest_sales
                                  , max(weekdate) as latest_sales
                                  , count(distinct week, year) as weeks_sold
                                  
                                  from {year_flag} 
                                      group by itemid, Cluster """)

# COMMAND ----------

# sf v2 
weeks_sold_percent = ks.sql(""" select itemid
                                    , Cluster
                                    , weeks_sold / ((datediff(latest_sales,earliest_sales) / 7 ) + 1)  as weeks_sold_percent
                                    from {various_dates} """)

weak_sales = weeks_sold_percent[weeks_sold_percent['weeks_sold_percent']<prcnt_wks_sold] # sf v3 new variable

anti_join = ks.sql(""" SELECT A.*
                       FROM {non_zero_rows} A
                       LEFT JOIN {weak_sales} B 
                       ON (A.itemid = B.itemid) and (A.Cluster = B.Cluster)
                       WHERE B.itemid IS NULL and B.Cluster is null  """)

# COMMAND ----------

#display(weak_sales)

# COMMAND ----------

# sf v2 
zero_rows = ads_df[ads_df['revenue']==0]

zero_rows_reduced = ks.sql(""" SELECT distinct A.*
                                 FROM {zero_rows} A
                                 left JOIN {anti_join} B 
                                 ON (A.itemid = B.itemid) and (A.Cluster = B.Cluster)
                                 WHERE B.itemid IS not NULL and B.Cluster is not null """)

zeroes_returned = anti_join.append(zero_rows_reduced)
less_than_year = various_dates[various_dates['weeks_sold']<35]

anti_join_2 = ks.sql(""" SELECT A.*
                         FROM {zeroes_returned} A
                         LEFT JOIN {less_than_year} B 
                         ON (A.itemid = B.itemid) and (A.Cluster = B.Cluster)
                         WHERE B.itemid IS NULL and B.Cluster is null  """)

reduced_ads = anti_join_2.to_pandas()

# COMMAND ----------

#display(less_than_year)

# COMMAND ----------

# sf v2 from 01d notebook
reduced_ads.to_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/'+'Modelling_Inputs/modelling_ads_cluster_reduced.csv',index=False)

# COMMAND ----------

# MAGIC %r
# MAGIC # sf v2
# MAGIC ads_csv <- read.csv(paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_reduced.csv'))
# MAGIC ads_csv$weekdate=as.Date(ads_csv$weekdate,"%Y-%m-%d")

# COMMAND ----------

# MAGIC %r
# MAGIC # sf v2
# MAGIC saveRDS(ads_csv, file = paste0('/dbfs/',base_directory,'/',wave,'/',bu_code,'_Repo/','Modelling_Inputs/modelling_ads_cluster_reduced.Rds'))

# COMMAND ----------

# MAGIC %r
# MAGIC # sf v2 switched to new NA location
# MAGIC 
# MAGIC if (region=='NA'){
# MAGIC   product_map_file <- readRDS(paste0('/dbfs/',base_directory,'/',wave,'/product_map_na.Rds'))
# MAGIC }
# MAGIC if (region=='EU'){
# MAGIC   product_map_file <- readRDS(paste0('/dbfs/',base_directory,'/',wave,'/product_map_eu.Rds'))
# MAGIC }

# COMMAND ----------

# MAGIC %r
# MAGIC # sf v2 switched to new NA location
# MAGIC 
# MAGIC if (region=='NA'){
# MAGIC   write.csv(product_map_file, paste0('/dbfs/',base_directory,'/',wave,'/product_map_na.csv'),row.names = FALSE)
# MAGIC }
# MAGIC if (region=='EU'){
# MAGIC   write.csv(product_map_file, paste0('/dbfs/',base_directory,'/',wave,'/product_map_eu.csv'),row.names = FALSE)
# MAGIC }

# COMMAND ----------

# sf v2 switched to new NA location
#product_map_df = ks.read_csv(pre_wave+wave+'/product_map_na.csv')
# sf switched from koalas to pandas to resolve errors

if region=='NA':
  product_map_df = pd.read_csv('/dbfs/'+base_directory+'/'+wave+'/product_map_na.csv')
elif region=='EU':
  product_map_df = pd.read_csv('/dbfs/'+base_directory+'/'+wave+'/product_map_eu.csv')

# COMMAND ----------

if region=='NA':
  product_map_reduced = product_map_df[['department_desc','category_desc','sub_category_desc','size_unit_of_measure','item_desc','product_key']]
elif region=='EU':
  product_map_reduced = product_map_df[['item_category_group','item_category','item_subcategory','item_segment','item_name','sys_id']]

# COMMAND ----------

# sf v2
#inscope_items = list(reduced_ads['itemid'].unique())
# sf v3
full_ads = ads_df.to_pandas()
# full_ads = ads_df
inscope_items = list(full_ads['itemid'].unique())

# COMMAND ----------

# sf v3 - a method for determing the number of items in a category, which impacts time required to model, as well as the number of clusters in a category, which indicates whether the category is zoned or not
# if region=='NA':
#   ads_key_fields = full_ads[['itemid', 'Cluster']]
#   ads_key_dedup = ads_key_fields.drop_duplicates()
#   ads_prod_join = ads_key_dedup.merge(product_map_reduced, how='left', left_on='itemid', right_on='product_key')
#   ads_prod_count = ads_prod_join.groupby('category_desc').agg({"Cluster":  lambda x: x.nunique(), "itemid": lambda y: y.nunique()})
#   ads_prod_count = ads_prod_count.reset_index()
#   ads_prod_count.columns = ['category','cluster_count','item_count']
# elif region=='EU':
#   ads_key_fields = full_ads[['itemid', 'Cluster']]
#   ads_key_dedup = ads_key_fields.drop_duplicates()
#   ads_prod_join = ads_key_dedup.merge(product_map_reduced, how='left', left_on='itemid', right_on='sys_id')
#   ads_prod_count = ads_prod_join.groupby('item_category').agg({"Cluster":  lambda x: x.nunique(), "itemid": lambda y: y.nunique()})
#   ads_prod_count = ads_prod_count.reset_index()
#   ads_prod_count.columns = ['category','cluster_count','item_count']

# COMMAND ----------

if region=='NA':
  product_map_scope = product_map_reduced[product_map_reduced['product_key'].isin(inscope_items)]
elif region=='EU':
  product_map_scope = product_map_reduced[product_map_reduced['sys_id'].isin(inscope_items)]

# COMMAND ----------

product_map_copy = product_map_scope.copy(deep=True)

# COMMAND ----------

product_map_copy['status'] = 'N'

# COMMAND ----------

product_map_copy.columns = ['DEPARTMENT','CATEGORY','SUB_CATEGORY','SIZE_UNIT','ITEM_NAME','ITEMID','STATUS']

# COMMAND ----------

product_map_copy.to_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/'+'Input/scope_file.csv', index=False)

spark.createDataFrame(product_map_copy).write.mode('overwrite').saveAsTable("localized_pricing."+db_tbl_prefix+"input_scope_file")

# COMMAND ----------

# paste all the records of this downloaded file in the scope_file sheet of the modelling input excel template ; note that the other tabs like Signs, variables etc won't change
# make sure you call this tab scope_file

# Scope File Location : Phase3_extensions/Elasticity_Modelling/JAN2021_TestRefresh/TX_Repo/Input/scope_file.csv

# Modeling Input File Location : /dbfs/Phase3_extensions/Elasticity_Modelling/JAN2021_TestRefresh/TX_Repo/Input/modelling_input_file.xlsx

# CLI: dbfs cp modelling_input_file.xlsx dbfs:/Phase3_extensions/Elasticity_Modelling/JAN2021_TestRefresh/TX_Repo/Input/modelling_input_file.xlsx

# COMMAND ----------

sign_df = pd.read_excel(old_wave_modelling_input_file, sheet_name = "Signs")
baseline_variables_data_df = pd.read_excel(old_wave_modelling_input_file, sheet_name = "Variables")
mixed_variables_data_df = pd.read_excel(old_wave_modelling_input_file, sheet_name = "Mixed_Input")
customize_data_df = pd.read_excel(old_wave_modelling_input_file, sheet_name = "Customize_Data")
custom_run_variables_df = pd.read_excel(old_wave_modelling_input_file, sheet_name = "Variables_custom")

# COMMAND ----------

dbutils.fs.rm('/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/Input/modelling_input_file.xlsx')

writer3 = pd.ExcelWriter('modelling_input_file.xlsx', engine='xlsxwriter')
product_map_copy.to_excel(writer3, sheet_name='scope_file', index=False)
sign_df.to_excel(writer3, sheet_name='Signs', index=False)
baseline_variables_data_df.to_excel(writer3, sheet_name='Variables', index=False)
mixed_variables_data_df.to_excel(writer3, sheet_name='Mixed_Input', index=False)
customize_data_df.to_excel(writer3, sheet_name='Customize_Data', index=False)
custom_run_variables_df.to_excel(writer3, sheet_name='Variables_custom', index=False)
writer3.save()

# COMMAND ----------

from shutil import move
move('modelling_input_file.xlsx', '/dbfs/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/Input/')

# COMMAND ----------


