# Databricks notebook source
# DBTITLE 1,Metadata/Notebook Description
# MAGIC %md
# MAGIC #### Meta Data
# MAGIC 
# MAGIC **Creator: Accenture + Optimization Team **
# MAGIC  
# MAGIC  Reach out to Joshua / Taru  for questions & debugging
# MAGIC 
# MAGIC **Date Created: 05/25/2020**
# MAGIC 
# MAGIC **Date Updated: 11/17/2021**
# MAGIC  
# MAGIC The **Main Tasks** which are being executed in the this Notebook:
# MAGIC 
# MAGIC * Combine outputs from elasticity and price cost. Calcualte Base metrics and make this ready for ingestion into optimization
# MAGIC * This is a key input for optimization , hence make sure the validations built in check out
# MAGIC 
# MAGIC Key Changes
# MAGIC * Code simplified and converted to Python. No major change in the methodology.
# MAGIC * Some more testing for EU BUs is underway
# MAGIC * Zone file is being updated/ those changes are underway
# MAGIC 
# MAGIC  
# MAGIC **Output** from this notebook:
# MAGIC * Base metrics file & Test Stores Count

# COMMAND ----------

# DBTITLE 1,Configuration, Packages etc
# to read excel file 
!pip uninstall xlrd
!pip install xlrd==1.2.0
!pip install openpyxl

# To read R Data
!pip install pyreadr

# standard packages
import pandas as pd
import datetime
#import pyreadr
import pyspark.sql.functions as sf
from operator import add
from functools import reduce
from pyspark.sql.functions import when, sum, avg, col,concat,lit,row_number,broadcast,greatest,least,length
from pyspark.sql.window import Window

import xlrd

# check version of xlrd
print(xlrd.__version__)

# enabling delta caching to improve performance
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.app.name","localized_pricing")
spark.conf.set("spark.databricks.io.cache.enabled", "false")
sqlContext.clearCache()

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
sqlContext.setConf("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages("zip")
# MAGIC install.packages('tidyverse')
# MAGIC install.packages("dplyr") 
# MAGIC install.packages("rlang")
# MAGIC install.packages('openxlsx')
# MAGIC install.packages('nloptr')
# MAGIC install.packages('tidyverse')
# MAGIC install.packages('data.table')
# MAGIC install.packages('readxl')
# MAGIC install.packages('parallel')

# COMMAND ----------

# MAGIC %r
# MAGIC library(tidyverse)
# MAGIC library(data.table)
# MAGIC library(openxlsx)
# MAGIC library(nloptr)
# MAGIC library(readxl)
# MAGIC library(parallel)
# MAGIC options(scipen = 999)

# COMMAND ----------

# MAGIC %md
# MAGIC ### (1) Inputs

# COMMAND ----------

# DBTITLE 1,Widgets
##widgets
# Create widgets for business unit selection
dbutils.widgets.removeAll()

## TV : check the BU & abbreviations
Business_units = ["1400 - Florida Division",
                  "1600 - Coastal Carolina Division",
                  "1700 - Southeast Division",
                  "1800 - Rocky Mountain Division",
                  "1900 - Gulf Coast Division",
                  "2600 - West Coast Division",
                  "2800 - Texas Division",
                  "2900 - South Atlantic Division",
                  "3100 - Grand Canyon Division",
                  "3800 - Northern Tier Division",
                  "4100 - Midwest Division",
                  "4200 - Great Lakes Division",
                  "4300 - Heartland Division",                  
                  "Central Division",
                  "QUEBEC OUEST",
                  "QUEBEC EST - ATLANTIQUE",
                  "Western Division",
                  "Denmark",
                  "Ireland",
                  "Poland",
                  "Sweden",
                  "Norway"]

Geography = ["USA","Canada","Europe"]

BU_abb = [["1400 - Florida Division", 'FL'],
          ["1600 - Coastal Carolina Division", "CC"],
          ["1700 - Southeast Division", "SE"],
          ["1800 - Rocky Mountain Division", 'RM'],
          ["1900 - Gulf Coast Division", "GC"],
          ["2600 - West Coast Division", "WC"],
          ["2800 - Texas Division", 'TX'],
          ["2900 - South Atlantic Division", "SA"],
          ["3100 - Grand Canyon Division", "GR"],
          ["3800 - Northern Tier Division", "NT"],
          ["4100 - Midwest Division", 'MW'],
          ["4200 - Great Lakes Division", "GL"],
          ["4300 - Heartland Division", 'HD'],
          ["QUEBEC OUEST", 'QW'],
          ["QUEBEC EST - ATLANTIQUE", 'QE'],
          ["Central Division", 'CE'],
          ["Western Division", 'WD'],
          ["Denmark", 'DK'],
          ["Ireland", 'IE'],
          ["Poland", 'PL'],
          ["Sweden", 'SW'],
          ["Norway", 'NO']]

Refresh=['Mock_GL_Test']

extensions =["Phase4_extensions"]

run_list =["RUN_2_EXT","RUN_3_UPDT_PRICE_CST","RUN_1_MAIN"]

bu_test_control_values = ["South Atlantic","Western Canada","Quebec East","Rocky Mountain","Sweden","Denmark","Florida","Great Lakes","Russia","Lithuania","Midwest","Central Canada","Norway","Quebec West","Texas","Coastal Carolinas","Gulf Coast","Heartland","Poland","Estonia","Grand Canyon","Ireland","Latvia","Southeast","West Coast"]

dbutils.widgets.dropdown("01.Geography", "USA", Geography)
dbutils.widgets.dropdown("02.Business Unit", "1800 - Rocky Mountain Division", Business_units)
dbutils.widgets.dropdown("03.Refresh", "Mock_GL_Test", Refresh)
dbutils.widgets.dropdown("04.DBFS Location", "Phase4_extensions",extensions)
dbutils.widgets.text("05.Start Date", "2019-05-30")
dbutils.widgets.text("06.End Date", "2021-10-01")
dbutils.widgets.dropdown("07.BU Test Control File", "Great Lakes",bu_test_control_values)
dbutils.widgets.dropdown("08.Run", "RUN_1_MAIN",run_list) ## TV: LATEST FIX

# Reading the Widget Inputs
geography = dbutils.widgets.get('01.Geography')
business_unit = dbutils.widgets.get('02.Business Unit')
refresh = dbutils.widgets.get('03.Refresh')
dbfs_loc = dbutils.widgets.get('04.DBFS Location')
bu_test_control = dbutils.widgets.get('07.BU Test Control File')
run = dbutils.widgets.get('08.Run') ## TV: LATEST FIX

#get bu 2 character code
bu_abb_ds = sqlContext.createDataFrame(BU_abb, ['bu', 'bu_abb']).filter('bu = "{}"'.format(business_unit))
bu_code_upper = bu_abb_ds.collect()[0][1]
bu_code_lower=bu_code_upper.lower()

#date ranges 
#recommendation : use the dataprep date range
start_date = getArgument('05.Start Date')
end_date = getArgument('06.End Date')

print ("\033[1mUSER TO VALIDATE \n \033[0m")
print( "Geography: ",geography)
print("Business Unit:", business_unit,"; BU Code (Upper Case): ", bu_code_upper,"; BU Code (Lower Case): ",bu_code_lower)
print ("BU in the test control file :",bu_test_control)
print("Refresh name :",refresh)
print("DataPrep Dates || ","Start Date:", start_date, "; End Date:", end_date, ) ## same as dataprep
print("DBFS output location:", dbfs_loc )
print ("Run Details : ",run) ## TV: LATEST FIX

# COMMAND ----------

# DBTITLE 1,Additional Parameters
print ("\033[1mUSER TO VALIDATE \n \033[0m")

#Data Prep ADS
dataprep_ads_loc="/dbfs/"+ dbfs_loc+"/Elasticity_Modelling/"+refresh+"/" + bu_code_upper+ "_Repo/Modelling_Inputs/modelling_ads_cluster_final.Rds" #.RDS
dataprep_ads_loc= "/" + dbfs_loc+"/Elasticity_Modelling/"+refresh+"/" + bu_code_upper+ "_Repo/Modelling_Inputs/modelling_ads_cluster_final.csv" #CSV format # confirm with elasticity team

#Directories
elasticity_directory = "/dbfs/" + dbfs_loc +'/Elasticity_Modelling/'
optimization_directory = "/dbfs/" + dbfs_loc +'/Optimization/'

##Transaction table
if (geography in ['USA','Canada']):
  bu_daily_txn_table = "{}.{}_daily_txn_final_sweden_nomenclature_{}".format("phase4_extensions".lower(),bu_code_lower,refresh.lower()) 
  print("Daily Transaction Table Used :",bu_daily_txn_table)

#Price cost table name
 ## TV: LATEST FIX
if geography in ["Europe"]:
  price_cost_tbl = dbfs_loc + "." + bu_code_lower + "_item_cluster_mode_nov21_refresh_fixed"  
elif run in ("RUN_2_EXT","RUN_1_MAIN"):
  price_cost_tbl = "phase4_extensions" + "." + bu_code_lower + "_opti_price_cost_" + refresh + "_validated" 
elif run in ("RUN_3_UPDT_PRICE_CST"):
  price_cost_tbl = "phase4_extensions" + "." + bu_code_lower + "_opti_price_cost_" + refresh + "_validated_extension"
else:
  print ("Which run ?? ")
  
  
# Elasticity Outputs
final_elasticities_loc = "/" + dbfs_loc +'/Optimization/' + refresh + "/optimization_inputs/" + bu_code_upper+ "_Repo/Final_Elasticities_after_Imputation.csv"  
##baselines_collated_loc = elasticity_directory + refresh + "/" + bu_code_upper+ "_Repo/Inputs_for_elasticity_imputation/Baseline_Collated.xlsx" ## this is the file that usually read in

## Note: Below is an updated version of the file above: specifically created to fix a bug for the nov 2021 refresh. Please reach out to Prakhar or Taru if you have concerns. Please make sure to update this if you ever rerun elasticity.
baselines_collated_loc = elasticity_directory + refresh + "/" + bu_code_upper+ "_Repo/Inputs_for_elasticity_imputation/Baseline_Collated_all_items.xlsx" 


print ("DataPrep ADS : ",dataprep_ads_loc)
print ("Elasticity Directory : ",elasticity_directory)
print ("Optimization Directory : ",optimization_directory)
print ("Price Cost Table : ",price_cost_tbl)
print ("Baselines Collated (Elasticity Output) : ",baselines_collated_loc)
print ("Final Imputed Elasticities (Elasticity Output) : ",final_elasticities_loc)

# COMMAND ----------

# DBTITLE 1,Additional Parameters in R
# MAGIC %r
# MAGIC #final base metric output file name 
# MAGIC run= dbutils.widgets.get('08.Run')  ## TV: LATEST FIX
# MAGIC 
# MAGIC  ## TV: LATEST FIX
# MAGIC if (run %in% c("RUN_1_MAIN")) { base_metric_output_file ='base_metric_store_count.xlsx' }
# MAGIC else if (run %in% c("RUN_2_EXT")) { base_metric_output_file ='base_metric_store_count_ext.xlsx' } 
# MAGIC else if (run %in% c("RUN_3_UPDT_PRICE_CST")) { base_metric_output_file ='base_metric_store_count_w_new_pc.xlsx' }
# MAGIC else { print ("No output being written out") }
# MAGIC 
# MAGIC 
# MAGIC geography = dbutils.widgets.get('01.Geography')
# MAGIC business_unit = dbutils.widgets.get('02.Business Unit')
# MAGIC refresh= dbutils.widgets.get('03.Refresh')
# MAGIC dbfs_loc= dbutils.widgets.get('04.DBFS Location')
# MAGIC 
# MAGIC optimization_directory = paste0("/dbfs/" , dbfs_loc ,'/Optimization/')
# MAGIC 
# MAGIC Business_Units = c("1400 - Florida Division",
# MAGIC                   "1600 - Coastal Carolina Division",
# MAGIC                   "1700 - Southeast Division",
# MAGIC                   "1800 - Rocky Mountain Division",
# MAGIC                   "1900 - Gulf Coast Division",
# MAGIC                   "2600 - West Coast Division",
# MAGIC                   "2800 - Texas Division",
# MAGIC                   "2900 - South Atlantic Division",
# MAGIC                   "3100 - Grand Canyon Division",
# MAGIC                   "3800 - Northern Tier Division",
# MAGIC                   "4100 - Midwest Division",
# MAGIC                   "4200 - Great Lakes Division",
# MAGIC                   "4300 - Heartland Division",                  
# MAGIC                   "Central Division",
# MAGIC                   "QUEBEC OUEST",
# MAGIC                   "QUEBEC EST - ATLANTIQUE",
# MAGIC                   "Western Division",
# MAGIC                   "Denmark",
# MAGIC                   "Ireland",
# MAGIC                   "Poland",
# MAGIC                   "Sweden",
# MAGIC                   "Norway")
# MAGIC 
# MAGIC BU_abb = c('fl', "cc", "se","rm", "gc","wc", 'tx', "sa", "gr", "nt", 'mw', "gl", 'hd', "ce",'qw', 'qe', 'wd', 'dk', 'ie','pl', 'sw', 'no')
# MAGIC bu_map = data.frame(BU = Business_Units, BU_Abb = BU_abb)%>% mutate(BU = as.character(BU), BU_Abb = as.character(BU_Abb))
# MAGIC bu_code_lower = unique(bu_map$BU_Abb[bu_map$BU == business_unit])
# MAGIC # bu = unique(bu_map$BU_Abb[bu_map$BU == Business_Unit])
# MAGIC 
# MAGIC print (" USER TO VALIDATE")
# MAGIC print(paste0 (" Geography : ",geography))
# MAGIC print(paste0 (" BU Name : ",business_unit))
# MAGIC print(paste0 (" BU Abbreviation : ",bu_code_lower))
# MAGIC print(paste0 (" Refresh : ",refresh))
# MAGIC print(paste0 (" dbfs location : ",dbfs_loc))
# MAGIC print(paste0 (" Run : ",run)) ## TV: LATEST FIX
# MAGIC print(paste0 (" Output File location : ",base_metric_output_file)) ## TV: LATEST FIX

# COMMAND ----------

# MAGIC 
# MAGIC %r
# MAGIC BU_abb = c('fl', "cc", "se","rm", "gc","wc", 'tx', "sa", "gr", "nt", 'mw', "gl", 'hd', "ce",'qw', 'qe', 'wd', 'dk', 'ie','pl', 'sw', 'no')
# MAGIC bu_map = data.frame(BU = Business_Units, BU_Abb = BU_abb) %>% mutate(BU = as.character(BU), BU_Abb = as.character(BU_Abb))
# MAGIC bu_code_lower = unique(bu_map$BU_Abb[bu_map$BU == business_unit])
# MAGIC # bu = unique(bu_map$BU_Abb[bu_map$BU == Business_Unit])
# MAGIC 
# MAGIC print (" USER TO VALIDATE")
# MAGIC print(paste0 (" Geography : ",geography))
# MAGIC print(paste0 (" BU Name : ",business_unit))
# MAGIC print(paste0 (" BU Abbreviation : ",bu_code_lower))
# MAGIC print(paste0 (" Refresh : ",refresh))
# MAGIC print(paste0 (" dbfs location : ",dbfs_loc))

# COMMAND ----------

# DBTITLE 1,Multi State BUs + Categories Optimized at State level
############################################################# Note for User #############################################################
#Update multi_state_bu dataframe if your BU has more than 1 state. 
#Examine the "total_state_cats" list created at the end. This should be as your BU wants it this time around. If not then update it
#Refer the opti input templates, "State & Zone Level Cats" tab + Discuss with Manager
#########################################################################################################################################

print ("\033[1mUSER TO VALIDATE \n \033[0m")

if (geography in ['USA','Canada']):
  multi_state_BUs = ['1600 - Coastal Carolina Division',\
                     '1700 - Southeast Division',\
                     '1800 - Rocky Mountain Division',\
                     '1900 - Gulf Coast Division',\
                     '4200 - Great Lakes Division',\
                     'QUEBEC EST - ATLANTIQUE',\
                     'Western Division',\
                     '4300 - Heartland Division',\
                     '2600 - West Coast Division',\
                     '4100 - Midwest Division',\
                     '2900 - South Atlantic Division']

  ## State level cats from the last refresh
  state_cats = pd.read_excel('/dbfs/Phase3_extensions/Optimization/Accenture_Refresh/optimization_inputs/State_level_cats.xlsx')
  state_cats = spark.createDataFrame(state_cats).filter(col('state_flag') == 'Y').filter(col('BU') == business_unit)

  ## All zoned categories are modelled at a state level by default for the multi state BUs. Include any additonal categories that need modelling at a state level here:
  additional_state_categories = ['004-BEER','005-WINE']

  ##Combining the above two
  total_state_cats=state_cats.rdd.map(lambda state_cats: state_cats['category_desc']).collect()
  total_state_cats=total_state_cats+additional_state_categories

elif (geography in ['Europe']):
  total_state_cats=[]
  print ("No State Level/Zoned Categories for Europe based on prior refreshes ")

## Check if this is correct. If not, then uncomment the total_state_cats list below and update is as appropriate
# total_state_cats=['Category_desc1', 'Category_desc2',.....]

print("State Level Categories for your BU are : ", total_state_cats)

# COMMAND ----------

# DBTITLE 1,Scope
#specify the scope file name and column details
scoped_items = sqlContext.sql("select * from circlek_db.lp_dashboard_items_scope where BU=='{}' and Scope=='Y'".format(business_unit))
scoped_items= scoped_items.select(scoped_items.product_key.cast("integer")).distinct().rdd.flatMap(lambda x: x).collect()
print("Count of Inscope Items in BU", business_unit, "is", len(scoped_items))


# COMMAND ----------

# DBTITLE 1,EU : EAN Level Modelling
if geography in ['Europe']:
  scope_w_modelling_level = sqlContext.sql("select  product_key,upc ,modelling_level  from circlek_db.lp_dashboard_items_scope where BU=='{}' and Scope=='Y'".format(business_unit))\
                          .withColumn('tempe_product_key',sf.col('product_key').cast('integer'))
  
  scope_w_modelling_level = scope_w_modelling_level.withColumn('trn_barcode', scope_w_modelling_level.upc.cast('bigint'))\
  .withColumn('l1',length(col('trn_barcode'))).withColumn('l0',length(col('upc')))\
  .select('tempe_product_key','trn_barcode','modelling_level')

  display(scope_w_modelling_level)
else:
  print("code block not applicable for North American BUs")

# COMMAND ----------

# DBTITLE 1,Store List + Clusters + Test Control Mapping || TEST LAUNCH DATE FILTER UPDATE NEEDED FOR RUN1
## TV: LATEST FIX
#test control map table
if run=="RUN_1_MAIN":
  ##NOTE : Use all teh DATAPREP Filters i.e. update any filters to exclude the extension stores as applicable i.e.  test_launch_date <= '2020-11-09' / exclude_from_lp_price = 'N' etc.
  test_control_cluster_map = spark.sql("""Select site_id as trn_site_sys_id, Cluster as cluster, group as Type from circlek_db.grouped_store_list 
                                  Where business_unit = '{}' 
                                  and group = 'Test' """.format(bu_test_control)) 
else :
  ##NOTE: Allow extension stores to pass through . Do not update these filters
  test_control_cluster_map = spark.sql("""Select site_id as trn_site_sys_id, Cluster as cluster, group as Type from circlek_db.grouped_store_list 
                                  Where business_unit = '{}' 
                                  and test_launch_date IS NOT NULL """.format(bu_test_control))                       
                               

display(test_control_cluster_map)
#Note: Make sure the count of test stores is correct too
#Note: Make sure all test stores has a cluster assigned to them too i.e. no nulls in the cluster column

# COMMAND ----------

#Note : This command displays the Count of Nulls in every column # you should have no null values
#Note : Make sure all test stores has a cluster assigned to them too i.e. no nulls in the cluster column

test_control_cluster_map.select([sf.count(when(sf.isnan(c), c)).alias(c) for c in test_control_cluster_map.columns]).show()

# COMMAND ----------

# DBTITLE 1,Zones (Only applicable for USA/Canadian BUs) 
#Update /Review thefollowing
#1. zone_map : this data frame reads in the most updated zone file for you BU. Please check the file /filters and update as you deem fit
#2. all_zoned_categories : this lists ALL the categories with zones in your BU . Please check and update as you deem fit (irrespective of whether they elasticity was modelled at zone level or not)
#3. zoned_but_modelled_wo_zones : Include zoned categories which were optimized at cluster-zone level but modelled only at cluster level here (& not at a cluster - zone level).
#4. Central Canada/Western Division : Use departments instead of categories . Site ID might need a 3 million correction (Central Canada). Data scientist working on this BU please check/update accordingly
#5. cig_categories : this is not applicable for the Nov2021 refresh and can be left as is.

if (geography in ['Europe']):
   print("No Zones in Europe :) ") 
    
elif (geography in ['USA','Canada']):

  # Great Lakes 
  if (bu_code_upper in ('GL')):
    zone_map=pd.read_excel("/dbfs/Phase4_extensions/Optimization/Nov21_Refresh/gl_qe_zoning_file.xlsx").astype('str')
    zone_map = spark.createDataFrame(zone_map).filter(col('BU') == business_unit).distinct()
    all_zoned_categories=['004-BEER','005-WINE','085-SOFT DRINKS','BOISSONS GAZEUSES-C'] 
    zoned_but_modelled_wo_zones = []
    display(zone_map)
  
  # Quebec East
  elif (bu_code_upper in ('QE')):
    zone_map=pd.read_excel("/dbfs/Phase4_extensions/Optimization/Nov21_Refresh/gl_qe_zoning_file.xlsx").astype('str')
    zone_map = spark.createDataFrame(zone_map).filter(col('BU') == business_unit).distinct()
    all_zoned_categories=['004-BEER','005-WINE','085-SOFT DRINKS','BOISSONS GAZEUSES-C'] 
    zoned_but_modelled_wo_zones = []
    display(zone_map)
       
  # Western Division
  elif (bu_code_upper in ('WD')):
    zone_map=[]
    all_zoned_categories=[]  #Update departments here and not categories
    zoned_but_modelled_wo_zones = [] #Update departments here and not categories
    print("No Zones in WD :) ") 
    
  #Central Canada 
  elif (bu_code_upper == 'CE'):
    zone_map=[]
    all_zoned_categories=[]  #Update departments here and not categories
    zoned_but_modelled_wo_zones = []    #Update departments here and not categories
    print("Check if no zoned categories in scope for CE ") 
    
  # All other North American BUs
  else:
    zone_map =spark.sql("select * from circlek_db.phase3_beer_wine_zone where BU in ('{}')".format(business_unit)).select('tempe_product_key','trn_site_sys_id','price_zones').distinct()
    all_zoned_categories=['004-BEER','005-WINE','003-OTHER TOBACCO PRODUCTS','Tobacco (65)'] 
    zoned_but_modelled_wo_zones = ['003-OTHER TOBACCO PRODUCTS','Tobacco (65)','005-WINE']
    display(zone_map)
  
  
## Cigs is not in scope for the Nov 2021 refresh hence this can be skipped if you are working on the refresh
cig_categories= ['Cigarettes (60)',\
                 '002-CIGARETTES',\
                 'CIGARETTES-C',\
                 'Discount/vfm 20s (6002)',\
                 'Subgener/budget 20s (6003)',\
                 'Premium Cigarettes 20s (6001)']

# COMMAND ----------

# DBTITLE 1,Validation :To Identify categories modelled at a cluster zone level 
if(geography in ['USA','Canada']):
  #Read the csv copy of the rds instead
  ads_validation=spark.read.csv(dataprep_ads_loc,header=True,inferSchema=True)

  if(bu_code_upper in ['CE','WD']):
    env="Toronto"
    w_validation=Window.partitionBy('department_desc').orderBy('itemid')
    prod=sqlContext.sql("select distinct product_key as itemid,department_desc,environment_name from dl_localized_pricing_all_bu.product where environment_name=='{}' ".format(env))
  elif (bu_code_upper  in ['QW','QE']):
    env="Laval"
    w_validation=Window.partitionBy('category_desc').orderBy('itemid')
    prod=sqlContext.sql("select distinct product_key as itemid,category_desc,environment_name from dl_localized_pricing_all_bu.product where environment_name=='{}' ".format(env))
  else :
    env="Tempe"
    w_validation=Window.partitionBy('category_desc').orderBy('itemid')
    prod=sqlContext.sql("select distinct product_key as itemid,category_desc,environment_name from dl_localized_pricing_all_bu.product where environment_name=='{}' ".format(env))

  if (bu_code_upper not in ['GL','QE','WD','CE']):
    zone_v=zone_map.withColumn("zoned",sf.lit("yes")).withColumnRenamed("tempe_product_key","itemid")
    display(ads_validation\
            .select('itemid','Cluster')\
            .distinct()\
            .join(prod,['itemid'],'left')
            .join(zone_v,['itemid'],'left')\
            .withColumn("row",row_number().over(w_validation))\
            .filter(col("row")==1)\
            .withColumn('flag',length("Cluster"))\
            .withColumn('Modelling Level',when(col('flag')>1,"cluster zone level").otherwise("cluster level : modelled without zone"))\
           .filter((col("flag")>1) | (col("zoned")=="yes")))
  else:
    print(" GL,QE,WD and CE have a unique type of zoning. Please make sure you validate the zones for these BUs manually")
  
else:
  print("EU does not have zones")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (2) Cost & Transactions

# COMMAND ----------

# DBTITLE 1,Latest Price & Cost (TBU)
if(geography in ['USA','Canada']):
  price_cost = sqlContext.sql("""select distinct tempe_product_key,\
                                        trn_site_sys_id,\
                                        total_cost as cost,\
                                        gross_price,\
                                        net_price FROM {}""".format(price_cost_tbl)) 
     
elif (geography == 'Europe'):
  price_cost = sqlContext.sql("""select distinct sys_id_join as tempe_product_key,\
                                        cluster as cluster,\
                                        unit_cost_mode as cost,\
                                        unit_price_mode as reg_price,\
                                        vat_rate_mode as  vat_perc from {0}  """.format(price_cost_tbl))

display(price_cost)


# COMMAND ----------

display(price_cost.filter((col('tempe_product_key')==2481389) & (col('trn_site_sys_id')==4704005)))

# COMMAND ----------

# DBTITLE 1,EU Transactions Table
if (geography in ['Europe']):
  query_txt_txn = "select * from dw_eu_rep_pos.pos_transactions_f "
  query_txt_item = "select distinct sys_id as trn_item_sys_id, item_name, item_subcategory, item_category from dw_eu_common.item_d "
  query_txt_site = "select distinct sys_id as trn_site_sys_id, site_bu, site_bu_name,site_country, site_name, site_number  from dw_eu_common.site_d"
  
  eu_txn_table = sqlContext.sql(query_txt_txn).\
                        join(sqlContext.sql(query_txt_item),['trn_item_sys_id'],"left").\
                        join(sqlContext.sql(query_txt_site),['trn_site_sys_id'],"left").\
                        filter(sf.col('site_bu_name')==business_unit)
  
  eu_txn_table.createOrReplaceTempView('eu_txn_table')
  display(eu_txn_table)


# COMMAND ----------

# DBTITLE 1,Calendar
calendar_file = sqlContext.sql("select * from dl_localized_pricing_all_bu.calendar").\
                select(["calendar_date","day_of_week_name"]).distinct().\
                withColumn("WEEK_NUM",sf.dayofweek("calendar_date")).\
                withColumn("adj_factor", sf.col("WEEK_NUM") - 4).\
                withColumn("adj_factor", sf.when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
                withColumn("WEEKDATE", sf.expr("date_sub(calendar_date,adj_factor)")).\
                orderBy(col("calendar_date")).\
                withColumn("YEAR",sf.year(col("WEEKDATE"))).\
                withColumn("dummy",sf.when(col("adj_factor") == 0,1).otherwise(0)).\
                withColumn("WEEK_NUM",sf.sum(col('dummy')).
                           over((Window.partitionBy(['YEAR']).orderBy("WEEKDATE").rangeBetween(Window.unboundedPreceding, 0)))).\
                orderBy(col("calendar_date")).select('YEAR','WEEKDATE', 'WEEK_NUM').distinct()

display(calendar_file)

# COMMAND ----------

# DBTITLE 1,Master Item Store : 1. Create Skeleton
if(geography in ['USA','Canada']):
  if (bu_code_upper == 'CE'):
    master_item_store = sqlContext.sql("select BU,\
                                               tempe_product_key,\
                                               department_desc as category_desc,\
                                               (trn_site_sys_id + 3000000) as trn_site_sys_id,\
                                               site_state_id,\
                                               sum(trn_gross_amount) as sales_wt,\
                                               min(trn_transaction_date) as min_date,\
                                               max(trn_transaction_date) as max_date \
                                               from {0} \
                                               where trn_transaction_date >= '{1}' and trn_transaction_date <= '{2}' \
                                               group by BU, tempe_product_key, department_desc, trn_site_sys_id, site_state_id".format(bu_daily_txn_table, start_date, end_date))
    
  elif (bu_code_upper in ['WD']):
    master_item_store = sqlContext.sql("select BU,\
                                              tempe_product_key,\
                                              department_desc as category_desc,\
                                              trn_site_sys_id, \
                                              site_state_id,\
                                              sum(trn_gross_amount) as sales_wt,\
                                              min(trn_transaction_date) as min_date,\
                                              max(trn_transaction_date) as max_date \
                                              from {0} \
                                              where trn_transaction_date >= '{1}' and trn_transaction_date <= '{2}'\
                                              group by BU, tempe_product_key, department_desc, trn_site_sys_id, site_state_id".format(bu_daily_txn_table, start_date, end_date))
  else:
    master_item_store = sqlContext.sql("select BU,\
                                              tempe_product_key,\
                                              category_desc,\
                                              trn_site_sys_id,\
                                              site_state_id,\
                                              sum(trn_gross_amount) as sales_wt,\
                                              min(trn_transaction_date) as min_date,\
                                              max(trn_transaction_date) as max_date\
                                              from {0} \
                                              where trn_transaction_date >= '{1}' and trn_transaction_date <= '{2}' \
                                              group by BU, tempe_product_key, category_desc, trn_site_sys_id, site_state_id".format(bu_daily_txn_table, start_date, end_date))
    
  display(master_item_store)
  
if(geography in ['Europe']):
  master_item_store_sysid_level = sqlContext.sql("select site_bu_name as BU,\
                                                         REPLACE(LTRIM(REPLACE(trn_item_sys_id,'0',' ')),' ','0') as tempe_product_key,\
                                                         item_category as category_desc,\
                                                         site_number as  trn_site_sys_id,\
                                                         null as site_state_id,\
                                                         sum(trn_gross_amount) as sales_wt,\
                                                         min(trn_transaction_date) as min_date,\
                                                         max(trn_transaction_date) as max_date \
                                                         from eu_txn_table \
                                                         where trn_transaction_date >= '{}' and trn_transaction_date <= '{}'\
                                                         group by BU, trn_item_sys_id,item_category, site_number, site_state_id".format( start_date, end_date))

  master_item_store_ean_level  = sqlContext.sql("select site_bu_name as BU,\
                                                        REPLACE(LTRIM(REPLACE(trn_item_sys_id,'0',' ')),' ','0') as tempe_product_key,\
                                                        REPLACE(LTRIM(REPLACE(trn_barcode,'0',' ')),' ','0') as trn_barcode,\
                                                        item_category as category_desc,\
                                                        site_number as  trn_site_sys_id,\
                                                        null as site_state_id,\
                                                        sum(trn_gross_amount) as sales_wt,\
                                                        min(trn_transaction_date) as min_date,\
                                                        max(trn_transaction_date) as max_date\
                                                        from eu_txn_table \
                                                        where trn_transaction_date >= '{}' and trn_transaction_date <= '{}' \
                                                        group by BU, trn_item_sys_id,trn_barcode,item_category, site_number, site_state_id".format( start_date, end_date))
  
  #filtering for scope
  sysid_level_skeleton=master_item_store_sysid_level.join(scope_w_modelling_level\
                                                          .filter(~(col('modelling_level').isin(['EAN'])))\
                                                          .select('tempe_product_key')\
                                                          .withColumn('modelling_level',lit("Sys ID"))\
                                                          .distinct(),'tempe_product_key','inner')
  
  ean_level_skeleton=master_item_store_ean_level.join(scope_w_modelling_level\
                                                      .filter((col('modelling_level').isin(['EAN'])))\
                                                      .withColumn('modelling_level',lit("EAN"))\
                                                      .distinct(),['tempe_product_key','trn_barcode'],'inner')\
  .drop(col('tempe_product_key'))\
  .withColumnRenamed('trn_barcode','tempe_product_key')
  
  master_item_store = sysid_level_skeleton.select('BU','tempe_product_key','category_desc','trn_site_sys_id','site_state_id','sales_wt','min_date','max_date','modelling_level')\
                     .union(\
                            ean_level_skeleton.select('BU','tempe_product_key','category_desc','trn_site_sys_id','site_state_id','sales_wt','min_date','max_date','modelling_level'))
  
                                                
  display(master_item_store)                                              

# COMMAND ----------

#display(master_item_store.filter((col('tempe_product_key')==542907) & (col('trn_site_sys_id')==4704005)))

# COMMAND ----------

if(geography in ['Europe']):
  print ("\033[1mUSER TO VALIDATE \n \033[0m")
  c1 = sysid_level_skeleton.select('tempe_product_key').distinct().count()
  c2 = ean_level_skeleton.select('tempe_product_key').distinct().count()
  
  print(" Count of items modelled at Sys ID level : " ,c1)
  print(" Count of items modelled at EAN level :" ,c2)
  print(" Count of all primary items :" ,c1+c2)

# COMMAND ----------

# DBTITLE 1,Master Item Store : 2. Add Price, Cost, Zones & Clusters
if(geography in ['USA','Canada']):
  if (bu_code_upper in ['GL','QE']):
    master_item_store_na = master_item_store.\
                     filter(col('tempe_product_key').isin(*scoped_items)).\
                     join(test_control_cluster_map,['trn_site_sys_id'],'left').\
                     filter(col("cluster").isNotNull()).\
                     join(zone_map,['BU','trn_site_sys_id','category_desc'],'left').\
                     withColumn('zone_flag',sf.when(col('category_desc').isin(*all_zoned_categories) & (col("price_zones").isNull()), "drop").otherwise("keep")).\
                     filter(col("zone_flag") == "keep").\
                     join(price_cost,['tempe_product_key','trn_site_sys_id'], 'left').\
                     withColumn('reg_price', sf.when(col('category_desc').isin(cig_categories),sf.col("net_price")).otherwise(sf.col("gross_price"))).\
                     withColumn('cluster_zone',sf.when(col("price_zones").isNotNull(),sf.concat(col("cluster"),sf.lit("_"), col("price_zones"))).otherwise(col("cluster"))).\
                     withColumn('state_cluster_zone',sf.when(((col("category_desc").isin(*total_state_cats)) & (col("BU").isin(*multi_state_BUs))),\
                                                             sf.concat(col("site_state_id"),sf.lit("_"),col('cluster_zone')))\
                                .otherwise(col("cluster_zone"))).\
                     withColumn('site_state_id',sf.when(col("category_desc").isin(*total_state_cats),\
                                                        col("site_state_id"))\
                                .otherwise("")).\
                    withColumn('cluster_zone_map',sf.when(col('category_desc').isin(*zoned_but_modelled_wo_zones),\
                                                          col('cluster'))\
                               .otherwise(col('cluster_zone')))
  else:
      master_item_store_na = master_item_store.\
                     filter(col('tempe_product_key').isin(*scoped_items)).\
                     join(test_control_cluster_map,['trn_site_sys_id'],'left').\
                     filter(col("cluster").isNotNull()).\
                     join(zone_map,['trn_site_sys_id','tempe_product_key'],'left').\
                     withColumn('zone_flag',sf.when(col('category_desc').isin(*all_zoned_categories) & (col("price_zones").isNull()), "drop").otherwise("keep")).\
                     filter(col("zone_flag") == "keep").\
                     join(price_cost,['tempe_product_key','trn_site_sys_id'], 'left').\
                     withColumn('reg_price', sf.when(col('category_desc').isin(cig_categories),sf.col("net_price")).otherwise(sf.col("gross_price"))).\
                     withColumn('cluster_zone',sf.when(col("price_zones").isNotNull(),sf.concat(col("cluster"),sf.lit("_"), col("price_zones"))).otherwise(col("cluster"))).\
                     withColumn('state_cluster_zone',sf.when(((col("category_desc").isin(*total_state_cats)) & (col("BU").isin(*multi_state_BUs))),\
                                                             sf.concat(col("site_state_id"),sf.lit("_"),col('cluster_zone')))\
                                .otherwise(col("cluster_zone"))).\
                     withColumn('site_state_id',sf.when(col("category_desc").isin(*total_state_cats),\
                                                        col("site_state_id"))\
                                .otherwise("")).\
                    withColumn('cluster_zone_map',sf.when(col('category_desc').isin(*zoned_but_modelled_wo_zones),\
                                                          col('cluster'))\
                               .otherwise(col('cluster_zone')))
  
  display(master_item_store_na)
  
elif(geography in ['Europe']):
  #Price cost will be added in the next step
  master_item_store_eu = master_item_store.\
                     join(test_control_cluster_map,['trn_site_sys_id'],'left').\
                     filter(col("cluster").isNotNull()).\
                     withColumn('site_state_id',sf.when(col("category_desc").isin(*total_state_cats),\
                                                        col("site_state_id"))\
                                .otherwise(""))

  display(master_item_store_eu)

# COMMAND ----------

#display(master_item_store_na.filter((col('tempe_product_key')==542907) & (col('trn_site_sys_id')==4704005)))

# COMMAND ----------

#display(master_item_store_na.filter(col('gross_price').isNull()))

# COMMAND ----------

master_item_store_na_non_null=master_item_store_na.na.drop(subset=["gross_price"]) 
display(master_item_store_na_non_null)

# COMMAND ----------

#display(master_item_store_na_non_null.filter((col('tempe_product_key')==542907) & (col('trn_site_sys_id')==4704005)))

# COMMAND ----------

# DBTITLE 1,Validation : Ensure the Cluster Zone State Mappings are Accurate
## Make sure the clusters are created correctly

###### For North America ######
  # cluster_zone = should contain cluster with zone for all zoned categories
  # cluster_zone_map = should contain cluster with zone for all zoned categories , exception being zoned categories that are not modelled (elasticity) at a cluster zone level
  # state_cluster_zone= should contain state with cluster with zone for all categories 
  # Plus any other checks the DS would like to do

###### For Europe ######
  # Modelling Levels should be correct
  # Plus any other checks the DS would like to do


if(geography in ['USA','Canada']):
  display(master_item_store_na.select('category_desc','site_state_id','cluster','cluster_zone','cluster_zone_map','state_cluster_zone').distinct())
elif(geography in ['Europe']):
   display(master_item_store_eu.select('category_desc','site_state_id','cluster','modelling_level').distinct())

# COMMAND ----------

# DBTITLE 1,Validation : Ensure the Count of In scope Items is correct
if(geography in ['USA','Canada']):
  print("Total inscope items (primaries) : ", master_item_store_na.select('tempe_product_key').distinct().count())
elif(geography in ['Europe']):
  print("Total inscope items (primaries) : ", master_item_store_eu.select('tempe_product_key').distinct().count())
  
print("If this count is not correct, then stop and investigate  ")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (3) Store Counts

# COMMAND ----------

# DBTITLE 1,Master Item Store(NA) : 3. Roll it up to an Item x Cluster x Zone level
if(geography in ['USA','Canada']):
  final_out = master_item_store_na.\
            filter(col("reg_price").isNotNull()).\
            filter(col("cost").isNotNull()).\
            withColumn("sales_x_price", col("sales_wt") * col("reg_price")).\
            withColumn("sales_x_cost", col("sales_wt") * col("cost")).\
            groupBy(['BU','tempe_product_key','category_desc','site_state_id','cluster_zone','cluster_zone_map','state_cluster_zone']).\
            agg(sf.sum("sales_wt").alias("sales_wt"),
                sf.sum("sales_x_price").alias("sales_x_price"),
                sf.sum("sales_x_cost").alias("sales_x_cost"),
                sf.countDistinct("trn_site_sys_id").alias("Test_Stores")).\
           withColumn("reg_price", col("sales_x_price") / col("sales_wt")).\
           withColumn("cost", col("sales_x_cost") / col("sales_wt")).\
           withColumn('reg_price',sf.round('reg_price',2)).\
           withColumn('cost',sf.round('cost',2))
  base_metrics_df = final_out.\
                   withColumn('old_reg_price', col("reg_price")).\
                   select(['BU',\
                           'category_desc',\
                           'tempe_product_key',\
                           'cluster_zone',\
                           'cluster_zone_map',\
                           'state_cluster_zone'\
                           ,'site_state_id',\
                           'sales_wt',\
                           'cost',\
                           'reg_price',\
                           'old_reg_price'])
  test_stores_count = final_out.\
                    select(['site_state_id',\
                            'tempe_product_key',\
                            'state_cluster_zone',\
                            'Test_Stores']).\
                  withColumn("contract",sf.lit("Not Applicable"))
                    
if(geography in ['Europe']):
  final_out = master_item_store_eu.\
  groupBy(['BU','tempe_product_key','category_desc','site_state_id','cluster']).\
            agg(sf.sum("sales_wt").alias("sales_wt"),
                sf.countDistinct("trn_site_sys_id").alias("Test_Stores")).\
            join(price_cost,['tempe_product_key','cluster'], 'left').\
            filter(col("reg_price").isNotNull()).\
            filter(col("cost").isNotNull()).\
            withColumn('reg_price',sf.round('reg_price',2)).\
            withColumn('cost',sf.round('cost',2))
  
  base_metrics_df = final_out.\
                   withColumn('old_reg_price', col("reg_price")).\
                   select(['BU',\
                           'category_desc',\
                           'tempe_product_key',\
                           'cluster',\
                           'site_state_id',\
                           'sales_wt',\
                           'cost',\
                           'reg_price',\
                           'old_reg_price',
                           'vat_perc'])
  test_stores_count = final_out.\
                    select(['site_state_id',\
                            'tempe_product_key',\
                            'cluster',\
                            'Test_Stores']).\
                  withColumn("contract",sf.lit("Not Applicable"))
                    

display(base_metrics_df)

# COMMAND ----------

display(base_metrics_df.filter((col('tempe_product_key')==561229) & (col('cluster_zone')=='3_NB')))

# COMMAND ----------

## Each item is not sold in every store. This result shows you how my test stores every item is selling in
display(test_stores_count)

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count


base_metrics_cost_price = base_metrics_df\
                                        .withColumn('cost',when(col('cost').contains('None') |\
                                                                     col('cost').contains('NULL') |\
                                                                     (col('cost') == '' ) |\
                                                                     col('cost').isNull() |\
                                                                     isnan('cost'),0)\
                                                    .otherwise(col('cost')))\
                                       .withColumn('reg_price',when(col('reg_price').contains('None') |\
                                                                     col('reg_price').contains('NULL') |\
                                                                     (col('reg_price') == '' ) |\
                                                                     col('reg_price').isNull() |\
                                                                     isnan('reg_price'),0)\
                                                   .otherwise(col('reg_price')))\
                                       .withColumn('old_reg_price',when(col('old_reg_price').contains('None') |\
                                                                    col('old_reg_price').contains('NULL') |\
                                                                    (col('old_reg_price') == '' ) |\
                                                                    col('old_reg_price').isNull() |\
                                                                    isnan('old_reg_price'),0)\
                                                  .otherwise(col('old_reg_price')))\
                                      .withColumn('sales_wt',when(col('sales_wt').contains('None') |\
                                                                  col('sales_wt').contains('NULL') |\
                                                                  (col('sales_wt') == '' ) |\
                                                                  col('sales_wt').isNull() |\
                                                                  isnan('sales_wt'),0)\
                                                  .otherwise(col('sales_wt')))

if(geography in ['Europe']):
  base_metrics_cost_price = base_metrics_cost_price.withColumn('vat_perc',when(col('vat_perc').contains('None') |\
                                                                     col('vat_perc').contains('NULL') |\
                                                                     (col('vat_perc') == '' ) |\
                                                                     col('vat_perc').isNull() |\
                                                                     isnan('vat_perc'),0)\
                                                    .otherwise(col('vat_perc')))\



display(base_metrics_cost_price)

# COMMAND ----------

display(base_metrics_cost_price.filter((col('tempe_product_key')==561229) & (col('cluster_zone')=='3_NB')))

# COMMAND ----------

# DBTITLE 1,Validation : Check items with '0' cost , price,vat OR cases where cost > price --> Discuss these with the BU & update values accordingly
if (geography in ['USA','Canada']):
  check = base_metrics_cost_price.filter((col('reg_price')==0) | (col('cost')==0) | (col('cost') > col('reg_price') ))
elif (geography in ['Europe']):
  check = base_metrics_cost_price.filter((col('reg_price')==0) | (col('cost')==0) | (col('vat_perc')==0) | (col('cost') > col('reg_price') ))
  
display(check)

# COMMAND ----------

e=check.count()

if (e>0):
    print("Potential Cost Price Issues!!!!!!!!!!!! Discuss with team. All problematic cases filtered for in the check dataframe displayed above.")
else:
  print("Good to move on...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (4) Baselines

# COMMAND ----------

# DBTITLE 1,Baseline Collated
##read in the Baselines 
baseline_collated_1 = pd.read_excel(baselines_collated_loc).astype('str')
baseline_collated_1=spark.createDataFrame(baseline_collated_1)

#fixing datatypes
if (geography in ['USA','Canada']):
  baseline_collated_1=baseline_collated_1\
                    .withColumn('PRODUCT_KEY',sf.col('PRODUCT_KEY').cast('integer'))\
                    .withColumn('CLUSTER',sf.col('CLUSTER').cast("string"))\
                    .withColumn('UCM_BASELINE',sf.col('UCM_BASELINE').cast('float'))\
                    .withColumn('WEEKDATE',sf.to_date(col('WEEK_START_DATE'),"yyyy-MM-dd"))

elif (geography in ['Europe']):
  baseline_collated_1=baseline_collated_1\
                    .withColumn('PRODUCT_KEY',sf.col('TRN_ITEM_SYS_ID'))\
                    .withColumn('CLUSTER',sf.col('CLUSTER').cast("string"))\
                    .withColumn('UCM_BASELINE',sf.col('UCM_BASELINE').cast('float'))\
                    .withColumn('WEEKDATE',sf.to_date(col('WEEK_START_DATE'),"yyyy-MM-dd"))

#lets have a look
display(baseline_collated_1)

# COMMAND ----------

display(baseline_collated_1.filter((col('PRODUCT_KEY')==561229) & (col('CLUSTER')=='3_NB')))

# COMMAND ----------

# DBTITLE 1,Stop & Validate
##Creating a flag to check for bad data
baseline_collated_2= baseline_collated_1.withColumn('UCM_BASELINE_check',when(col('UCM_BASELINE').contains('None') |\
                                                                     col('UCM_BASELINE').contains('NULL') |\
                                                                     (col('UCM_BASELINE') == '' ) |\
                                                                     col('UCM_BASELINE').isNull() |\
                                                                     isnan('UCM_BASELINE'),lit('nulls in UCM Baseline Values'))\
                                                    .otherwise(lit("ok")))

baseline_nulls=baseline_collated_2.filter(col('UCM_BASELINE_check')=='nulls in UCM Baseline Values')

#Do not ignore these comments
if (baseline_nulls.count()==0):
  print ('No nulls in UCM Baseline values. Good to move ahead')
else:
  print('STOP : UCM Baseline has nulls - look at the check data frame to identify these cases. Discuss with team on how to handle these. Fix before proceeding with this code.')

# COMMAND ----------

# no result is good. If this command displays values then it means that the baselines have nulls and we will need to discuss these cases with the Manager and Elasticity team
display(baseline_nulls)

# COMMAND ----------

# DBTITLE 1,Creating the Base Metrics for Sales
## window specifications
windowSpec  = Window.partitionBy('PRODUCT_KEY','CLUSTER').orderBy('PRODUCT_KEY','CLUSTER')

## Creating the basemetrics for baseline sales
if (geography in ['USA','Canada']):
  baseline_collated_3=baseline_collated_2\
                                      .filter(col('UCM_BASELINE_check')=='ok')\
                                      .filter(col('PRODUCT_KEY').isin(*scoped_items))\
                                      .select('WEEKDATE','PRODUCT_KEY','CATEGORY','CLUSTER','UCM_BASELINE')\
                                      .join(calendar_file,["WEEKDATE"],'left')\
                                      .withColumn('YYYYWW',concat('YEAR',when(sf.length(col('WEEK_NUM'))==1,concat(sf.lit('0'),'WEEK_NUM')).otherwise(col('WEEK_NUM'))).cast('integer'))\
                                      .drop('WEEKDATE','WEEK_NUM','YEAR','CATEGORY')\
                                      .groupBy('PRODUCT_KEY','CLUSTER','YYYYWW')\
                                      .agg(sum('UCM_BASELINE').alias('UCM_BASELINE'))\
                                      .orderBy('PRODUCT_KEY','CLUSTER','YYYYWW')\
                                      .withColumn("LEAD_4",sf.lead("YYYYWW",4).over(windowSpec))\
                                      .filter(col('LEAD_4').isNull())\
                                      .groupBy('PRODUCT_KEY','CLUSTER')\
                                      .agg(avg(col('UCM_BASELINE')).alias('UCM_BASELINE'))
  
elif (geography in ['Europe']):
  baseline_collated_3=baseline_collated_2\
                                      .filter(col('UCM_BASELINE_check')=='ok')\
                                      .join(master_item_store_eu.select('tempe_product_key').distinct().withColumnRenamed('tempe_product_key','PRODUCT_KEY'),['PRODUCT_KEY'],'inner')\
                                      .select('WEEKDATE','PRODUCT_KEY','CATEGORY','CLUSTER','UCM_BASELINE')\
                                      .join(calendar_file,["WEEKDATE"],'left')\
                                      .withColumn('YYYYWW',concat('YEAR',when(sf.length(col('WEEK_NUM'))==1,concat(sf.lit('0'),'WEEK_NUM')).otherwise(col('WEEK_NUM'))).cast('integer'))\
                                      .drop('WEEKDATE','WEEK_NUM','YEAR','CATEGORY')\
                                      .groupBy('PRODUCT_KEY','CLUSTER','YYYYWW')\
                                      .agg(sum('UCM_BASELINE').alias('UCM_BASELINE'))\
                                      .orderBy('PRODUCT_KEY','CLUSTER','YYYYWW')\
                                      .withColumn("LEAD_4",sf.lead("YYYYWW",4).over(windowSpec))\
                                      .filter(col('LEAD_4').isNull())\
                                      .groupBy('PRODUCT_KEY','CLUSTER')\
                                      .agg(avg(col('UCM_BASELINE')).alias('UCM_BASELINE'))

display(baseline_collated_3)

# COMMAND ----------

c1 = baseline_collated_3.select('product_key').distinct().count() - baseline_collated_1.select('product_key').distinct().count() 

print ("Validation! :",c1 ,"(result should be 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (5) Vanished Items Correction

# COMMAND ----------

# DBTITLE 1,Reduced ADS Baseline Correction / Vanished Items
ads_0 = spark.read.csv(dataprep_ads_loc,header=True,inferSchema=True)

baselines_abr=baseline_collated_1\
                .select('PRODUCT_KEY','CLUSTER')\
                .distinct()\
                .withColumn('FLAG',sf.lit("BASELINES PRESENT"))

if (geography in ['USA','Canada']):
  ads_1=ads_0.withColumn('PRODUCT_KEY',sf.col('ITEMID').cast('integer'))\
           .withColumn('CLUSTER',sf.col('CLUSTER').cast("string"))\
           .withColumn('UCM_BASELINE',sf.col('sales_qty').cast("float"))\
           .withColumn('WEEKDATE',sf.to_date(col('weekdate'),"yyyy-MM-dd"))\
           .filter(col('PRODUCT_KEY').isin(*scoped_items))
  
elif (geography in ['Europe']):
  ads_1=ads_0.withColumn('PRODUCT_KEY',sf.col('ITEMID'))\
           .withColumn('CLUSTER',sf.col('CLUSTER').cast("string"))\
           .withColumn('UCM_BASELINE',sf.col('sales_qty').cast("float"))\
           .withColumn('WEEKDATE',sf.to_date(col('weekdate'),"yyyy-MM-dd"))\
           .join(master_item_store_eu.select('tempe_product_key').distinct().withColumnRenamed('tempe_product_key','PRODUCT_KEY'),['PRODUCT_KEY'],'inner')

ads_2=ads_1\
            .filter(col('revenue')>0)\
            .groupBy('PRODUCT_KEY','CLUSTER')\
            .agg(sf.max(col('WEEKDATE')).alias('LAST_TRN_DATE'))

ads_3=ads_1\
           .select('PRODUCT_KEY','CLUSTER','WEEKDATE','UCM_BASELINE')\
          .join(baselines_abr,['PRODUCT_KEY','CLUSTER'],'left')\
          .withColumn('FLAG',when(col('FLAG').isNull(),sf.lit("BASELINES ABSENT")).otherwise(col('FLAG')))\
          .filter(col('FLAG')=="BASELINES ABSENT")


vanished_items=ads_1\
          .groupBy('PRODUCT_KEY','CLUSTER')\
          .agg(sum(col('revenue')).alias('REVENUE'))\
          .join(ads_2,['PRODUCT_KEY','CLUSTER'])\
          .join(baselines_abr,['PRODUCT_KEY','CLUSTER'],'left')\
          .withColumn('FLAG',when(col('FLAG').isNull(),sf.lit("BASELINES ABSENT")).otherwise(col('FLAG')))\
          .filter(col('FLAG')=="BASELINES ABSENT")

# these are combinations that got lost because of bad sales. We will now add a correction to get them back
print("Item-Clusters without Baselines : ",vanished_items.count())   

# COMMAND ----------

## window specifications
windowSpec = Window.partitionBy('PRODUCT_KEY','CLUSTER').orderBy('PRODUCT_KEY','CLUSTER')

# Number of Latest Weeks to be considered for calculating the Baseline Sales of Item*Cluster
baseline_weeks = 12    

## Creating the basemetrics for baseline sales
baseline_collated_4=ads_3\
                        .select('WEEKDATE','PRODUCT_KEY','CLUSTER','UCM_BASELINE')\
                        .join(calendar_file,["WEEKDATE"],'left')\
                        .withColumn('YYYYWW',concat('YEAR',when(sf.length(col('WEEK_NUM'))==1,concat(sf.lit('0'),'WEEK_NUM')).otherwise(col('WEEK_NUM'))).cast('integer'))\
                        .drop('WEEKDATE','WEEK_NUM','YEAR','CATEGORY')\
                        .groupBy('PRODUCT_KEY','CLUSTER','YYYYWW')\
                        .agg(sum('UCM_BASELINE').alias('UCM_BASELINE'))\
                        .orderBy('PRODUCT_KEY','CLUSTER','YYYYWW')\
                        .withColumn("LEAD",sf.lead("YYYYWW",baseline_weeks).over(windowSpec))\
                        .filter(col('LEAD').isNull())\
                        .groupBy('PRODUCT_KEY','CLUSTER')\
                        .agg(avg(col('UCM_BASELINE')).alias('UCM_BASELINE'))

baseline_collated_5=baseline_collated_3\
                                      .select('PRODUCT_KEY','CLUSTER','UCM_BASELINE')\
                                      .withColumn('SOURCE',sf.lit("baselines_3"))\
                                      .union(baseline_collated_4\
                                             .select('PRODUCT_KEY','CLUSTER','UCM_BASELINE')\
                                             .withColumn('SOURCE',sf.lit("baselines_4")))
baseline_collated_6= baseline_collated_5\
                                        .withColumn('UCM_BASELINE_check',when(col('UCM_BASELINE').contains('None') |\
                                                                                 col('UCM_BASELINE').contains('NULL') |\
                                                                                 (col('UCM_BASELINE') == '' ) |\
                                                                                 col('UCM_BASELINE').isNull() |\
                                                                                 (col('UCM_BASELINE')==0) |\
                                                                                 isnan('UCM_BASELINE'),lit('impute'))\
                                                                .otherwise(lit("ok")))\
                                       .withColumn('UCM_BASELINE_FINAL',when(col('UCM_BASELINE_check')=='impute',1).otherwise(col('UCM_BASELINE')))

baseline_collated_7=baseline_collated_6\
                                      .select('PRODUCT_KEY','CLUSTER','UCM_BASELINE_FINAL')\
                                      .withColumnRenamed('UCM_BASELINE_FINAL','UCM_BASELINE')

# display(baseline_collated_7.filter(col('UCM_BASELINE_check')=='impute'))
display(baseline_collated_7)


# COMMAND ----------

e1=ads_2.count()-baseline_collated_7.count()
e2=baseline_collated_5.count()-baseline_collated_3.count()-baseline_collated_4.count()

if (e1>0):
    print ("Rows in the ADS and Baselines Collated file do not match !!!!!!!!!!!! Investigate, then discuss with team/SME")
elif (e2>0):
    print ("Counts of the Baseline files do not add up !!!!!!!!!!!! Investigate, then discuss with team/SME")
else:
  print("Good to move on...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (6) Final Elasticities

# COMMAND ----------

# DBTITLE 1,Adding Elasticity values 
imputed_elast = spark.read.csv(final_elasticities_loc,header=True,inferSchema=True)


if (geography in ['USA','Canada']):
    imputed_elast=imputed_elast\
                            .select('item_category','item_subcategory','item_name','UPC','PRODUCT_KEY','CLUSTER','FINAL_BP_ELASTICITY_adj')\
                            .withColumn('PRODUCT_KEY',sf.col('PRODUCT_KEY').cast('integer'))\
                            .withColumn('CLUSTER',sf.col('CLUSTER').cast("string"))\
                            .withColumnRenamed('FINAL_BP_ELASTICITY_adj','FINAL_BP_ELASTICITY')\
                            .filter(col('PRODUCT_KEY').isin(*scoped_items))
  
elif (geography in ['Europe']):
  imputed_elast=imputed_elast\
                            .withColumnRenamed('JDE_NUMBER','UPC')\
                            .withColumnRenamed('TRN_ITEM_SYS_ID','PRODUCT_KEY')   
  imputed_elast=imputed_elast\
                            .select('item_category','item_subcategory','item_name','UPC','PRODUCT_KEY','CLUSTER','FINAL_BP_ELASTICITY_adj')\
                            .withColumn('PRODUCT_KEY',sf.col('PRODUCT_KEY'))\
                            .withColumn('CLUSTER',sf.col('CLUSTER').cast("string"))\
                            .withColumnRenamed('FINAL_BP_ELASTICITY_adj','FINAL_BP_ELASTICITY')\
                            .join(master_item_store_eu.select('tempe_product_key').distinct().withColumnRenamed('tempe_product_key','PRODUCT_KEY'),['PRODUCT_KEY'],'inner')

e1 = imputed_elast.select('PRODUCT_KEY','CLUSTER').distinct().count()-imputed_elast.count() 
e2=len(scoped_items) - imputed_elast.select('PRODUCT_KEY').distinct().count()

if (e1>0):
    print ("Duplicates in the Elasticity File!!!!!!!!!!!! Discuss with team")
elif (e2>0):
    print ("Elasticity file does not contain all inscope items!!!!!!!!!!!! Discuss with team")
else:
  display(imputed_elast)


# COMMAND ----------

base_elast_ucm=imputed_elast.join(baseline_collated_7,['PRODUCT_KEY','CLUSTER'],'left')

display(base_elast_ucm)

# COMMAND ----------

e1=base_elast_ucm.filter(col('UCM_BASELINE').isNull()).count() #shud be 0
e2=base_elast_ucm.count()-imputed_elast.count() ##shud be 0
e3=base_elast_ucm.count()-baseline_collated_7.count()  ##shud be 0

print(e1,e2,e3)

if((e1>0) | (e2>0) | (e3>0)):
  print ("Something does not add up!!!!!!!!!!!! Investigate, then discuss with team/SME")
else:
  print("Good to move on...")
  

# COMMAND ----------

# DBTITLE 1,No Elasticity Values : lack of modelling
if (geography in ['USA','Canada']):  
  check_0=base_metrics_cost_price\
                                .select('tempe_product_key','cluster_zone_map')\
                                .withColumnRenamed('tempe_product_key','PRODUCT_KEY')\
                                .withColumnRenamed('cluster_zone_map','CLUSTER')\
                                .distinct()\
                                .join(base_elast_ucm\
                                      .select('PRODUCT_KEY','CLUSTER','FINAL_BP_ELASTICITY'),['PRODUCT_KEY','CLUSTER'],'left')\
                                .filter(col('FINAL_BP_ELASTICITY').isNull())

  check_1=check_0\
              .withColumnRenamed('PRODUCT_KEY','tempe_product_key')\
              .withColumnRenamed('CLUSTER','cluster_zone_map')\
              .withColumn('Flagged',sf.lit(1))

  check_2 = master_item_store_na\
            .join(check_1,['tempe_product_key','cluster_zone_map'],'left')\
            .filter(col("Flagged") == 1)\
            .groupBy(['BU','category_desc','tempe_product_key','cluster_zone_map'])\
            .agg(sf.countDistinct("trn_site_sys_id").alias("count_stores"),
                sf.sum("sales_wt").alias('revenue'),
                sf.min("min_date").alias("min_date"),
                sf.max("max_date").alias("max_date"))

elif (geography in ['Europe']):
    check_0=base_metrics_cost_price\
                                .select('tempe_product_key','cluster')\
                                .withColumnRenamed('tempe_product_key','PRODUCT_KEY')\
                                .withColumnRenamed('cluster','CLUSTER')\
                                .distinct()\
                                .join(base_elast_ucm\
                                      .select('PRODUCT_KEY','CLUSTER','FINAL_BP_ELASTICITY'),['PRODUCT_KEY','CLUSTER'],'left')\
                                .filter(col('FINAL_BP_ELASTICITY').isNull())

    check_1=check_0\
              .withColumnRenamed('PRODUCT_KEY','tempe_product_key')\
              .withColumnRenamed('CLUSTER','cluster')\
              .withColumn('Flagged',sf.lit(1))

    check_2 = master_item_store_eu\
              .join(check_1,['tempe_product_key','cluster'],'left')\
              .filter(col("Flagged") == 1)\
              .groupBy(['BU','category_desc','tempe_product_key','cluster'])\
              .agg(sf.countDistinct("trn_site_sys_id").alias("count_stores"),
                  sf.sum("sales_wt").alias('revenue'),
                  sf.min("min_date").alias("min_date"),
                  sf.max("max_date").alias("max_date"))

display(check_2)

# COMMAND ----------

e=check_0.count()

if(e > 0):
  print ("Why do we have nulls? Investigate & Discuss with SME")
else:
  print("Good to move on...")

# COMMAND ----------

if (geography in ['USA','Canada']): 
  df_1 = base_metrics_cost_price\
                            .withColumnRenamed('tempe_product_key','PRODUCT_KEY')\
                            .withColumnRenamed('Cluster_zone_map','CLUSTER')

  df_2=df_1.groupBy('PRODUCT_KEY','CLUSTER').agg(sum("sales_wt").alias("sales_div"))

  df_3 = test_stores_count.withColumnRenamed('tempe_product_key','PRODUCT_KEY').drop("site_state_id")

  df_4=base_elast_ucm\
  .join(df_1,['PRODUCT_KEY','CLUSTER'],'left')\
  .join(df_2,['PRODUCT_KEY','CLUSTER'],'left')\
  .join(df_3,['PRODUCT_KEY','state_cluster_zone'])\
  .withColumn("base_split_ratio",col("sales_wt")/col("sales_div"))\
  .withColumn("base_adj",col("UCM_BASELINE")*col("base_split_ratio"))\
  .withColumn("base_units",col("base_adj")/col("Test_Stores"))

  base_metrics_final=df_4\
                        .withColumn("BU",sf.lit(business_unit))\
                        .withColumn("contract",sf.lit("Not Applicable"))\
                        .select("BU",\
                                "item_category",\
                                "PRODUCT_KEY",\
                                "state_cluster_zone",\
                                "site_state_id",\
                                "base_units",\
                                "cost",\
                                "reg_price",\
                                "FINAL_BP_ELASTICITY",\
                                "old_reg_price",\
                               "contract")

elif (geography in ['Europe']): 
  df_1 = base_metrics_cost_price\
                            .withColumnRenamed('tempe_product_key','PRODUCT_KEY')\
                            .withColumnRenamed('cluster','CLUSTER')
  
  df_2=df_1.groupBy('PRODUCT_KEY','CLUSTER').agg(sum("sales_wt").alias("sales_div"))

  df_3 = test_stores_count.withColumnRenamed('tempe_product_key','PRODUCT_KEY').drop("site_state_id")

  df_4=base_elast_ucm\
  .join(df_1,['PRODUCT_KEY','CLUSTER'],'left')\
  .join(df_2,['PRODUCT_KEY','CLUSTER'],'left')\
  .join(df_3,['PRODUCT_KEY','cluster'])\
  .withColumn("base_split_ratio",col("sales_wt")/col("sales_div"))\
  .withColumn("base_adj",col("UCM_BASELINE")*col("base_split_ratio"))\
  .withColumn("base_units",col("base_adj")/col("Test_Stores"))

  base_metrics_final=df_4\
                        .withColumn("BU",sf.lit(business_unit))\
                        .withColumn("contract",sf.lit("Not Applicable"))\
                        .select("BU",\
                                "item_category",\
                                "PRODUCT_KEY",\
                                "cluster",\
                                "site_state_id",\
                                "base_units",\
                                "cost",\
                                "reg_price",\
                                "FINAL_BP_ELASTICITY",\
                                "old_reg_price",\
                                "vat_perc",\
                               "contract")

#creating sql views
base_metrics_final.createOrReplaceTempView("base_metrics_final")
test_stores_count.createOrReplaceTempView("test_stores_count")

display(base_metrics_final)

# COMMAND ----------

# DBTITLE 1,Validation : Ensure all item - clusters have values
if(geography in ['USA','Canada']):
  validation_skeleton_store_level=master_item_store_na.select('category_desc','tempe_product_key','trn_site_sys_id','max_date','state_cluster_zone').withColumnRenamed('tempe_product_key','PRODUCT_KEY').distinct()
  
  validation_skeleton_cluster_level=validation_skeleton_store_level.groupBy('category_desc','PRODUCT_KEY','state_cluster_zone').agg(sf.max("max_date").alias('max_date'))
  
  validation_base_metrics= base_metrics_final.select('PRODUCT_KEY','state_cluster_zone','base_units','cost','reg_price','FINAL_BP_ELASTICITY').distinct()
  
  join_store_level=validation_skeleton_store_level.join(validation_base_metrics,['PRODUCT_KEY','state_cluster_zone'],'left').withColumn('flag',\
                                                                                                          when((col('base_units').isNull()) |\
                                                                                                               (col('cost').isNull()) | \
                                                                                                               (col('reg_price').isNull()) |\
                                                                                                               (col('FINAL_BP_ELASTICITY').isNull()),lit("not good")).otherwise(lit("good")))
  
  join_cluster_level=validation_skeleton_cluster_level.join(validation_base_metrics,['PRODUCT_KEY','state_cluster_zone'],'left').withColumn('flag',\
                                                                                                          when((col('base_units').isNull()) |\
                                                                                                               (col('cost').isNull()) | \
                                                                                                               (col('reg_price').isNull()) |\
                                                                                                               (col('FINAL_BP_ELASTICITY').isNull()),lit("not good")).otherwise(lit("good")))
  
elif(geography in ['Europe']):
  validation_skeleton_store_level=master_item_store_eu.select('category_desc','tempe_product_key','trn_site_sys_id','max_date','cluster').distinct().withColumnRenamed('tempe_product_key','PRODUCT_KEY').distinct()
  
  validation_skeleton_cluster_level=validation_skeleton_store_level.groupBy('category_desc','PRODUCT_KEY','cluster').agg(sf.max("max_date").alias('max_date'))
  
  validation_base_metrics= base_metrics_final.select('PRODUCT_KEY','cluster','base_units','cost','reg_price','FINAL_BP_ELASTICITY').distinct()
  
  join_store_level=validation_skeleton_store_level.join(validation_base_metrics,['PRODUCT_KEY','cluster'],'left').withColumn('flag',\
                                                                                                          when((col('base_units').isNull()) |\
                                                                                                               (col('cost').isNull()) | \
                                                                                                               (col('reg_price').isNull()) |\
                                                                                                               (col('FINAL_BP_ELASTICITY').isNull()),lit("not good")).otherwise(lit("good")))
  
  join_cluster_level=validation_skeleton_cluster_level.join(validation_base_metrics,['PRODUCT_KEY','cluster'],'left').withColumn('flag',\
                                                                                                          when((col('base_units').isNull()) |\
                                                                                                               (col('cost').isNull()) | \
                                                                                                               (col('reg_price').isNull()) |\
                                                                                                               (col('FINAL_BP_ELASTICITY').isNull()),lit("not good")).otherwise(lit("good")))

missing_values_store_level = join_store_level.filter(col('flag')=="not good")
missing_values_cluster_level = join_cluster_level.filter(col('flag')=="not good")

if (missing_values_cluster_level.count()==0):
  print("all item clusters where a product sells have been accounted for. Anything now missing is because the item does not sell there. Good to proceed")
else :
  print("STOP : some item clusters are missing. Please check the missing_values_cluster_level dataframe  & missing_values_store_level dataframe and investigate")
  
# Hint: look at the max_date, if its over an year old then items would have dropped during data prep ADS creation for scanty sales. Discuss never the less.
# Hint: look at price cost, missing values there could be the result 

# COMMAND ----------

import os
os.chdir('/')

# COMMAND ----------

pwd

# COMMAND ----------

# DBTITLE 1,New Code Addition : 3rd Feb '22
!pip install xlsxwriter
import xlsxwriter

## TV: LATEST FIX
if run =="RUN_1_MAIN":
  filename = f'{bu_code_lower}_item_store_validations.xlsx'
elif run=="RUN_2_EXT":
  filename = f'{bu_code_lower}_item_store_validations_run2.xlsx'
else :
  filename = f'{bu_code_lower}_item_store_validations_run3.xlsx'
  


path = f'/dbfs/{dbfs_loc}/Optimization/{refresh}/inputs/{bu_code_lower}/{filename}'
writer = pd.ExcelWriter(filename, engine='xlsxwriter')
#move("opti_price_cost_validations_ext.xlsx", "/dbfs/Phase4_extensions/Optimization/Nov21_Refresh/inputs/qe/qe_opti_price_cost_validations_ext.xlsx")

if(geography in ['USA','Canada']):
  master_item_store_na_non_null.toPandas().to_excel(writer, sheet_name='master_item_store_na', index=False)
  
elif(geography in ['Europe']):
  master_item_store_eu.toPandas().to_excel(writer, sheet_name='master_item_store_eu', index=False)

missing_values_store_level.toPandas().to_excel(writer, sheet_name='missing_store_level', index=False)
missing_values_cluster_level.toPandas().to_excel(writer, sheet_name='missing_cluster_level', index=False)


writer.save()

from shutil import move

move(filename, path)


# COMMAND ----------

display(missing_values_cluster_level)

# COMMAND ----------

# MAGIC %md
# MAGIC ### (7) Output (R)

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages('zip')
# MAGIC install.packages('openxlsx')
# MAGIC library(openxlsx)
# MAGIC library(tidyverse)

# COMMAND ----------

# MAGIC %r
# MAGIC base_metrics_final <- as.data.frame(SparkR::collect(SparkR::sql("select * from base_metrics_final")))
# MAGIC test_stores_count <- as.data.frame(SparkR::collect(SparkR::sql("select * from test_stores_count")))

# COMMAND ----------

# MAGIC %r
# MAGIC #renaming columns
# MAGIC if (geography %in% c('USA','Canada')){
# MAGIC   base_metrics_final_col_renamed <- base_metrics_final %>% rename (category_desc = item_category,
# MAGIC                                                                  Sys.ID.Join=PRODUCT_KEY,
# MAGIC                                                                  Cluster=state_cluster_zone,
# MAGIC                                                                  Base.Units=base_units,
# MAGIC                                                                  Per.Unit.Cost=cost,
# MAGIC                                                                  Regular.Price=reg_price,
# MAGIC                                                                  Base.Price.Elasticity=FINAL_BP_ELASTICITY,
# MAGIC                                                                  Old.Regular.Price=old_reg_price)
# MAGIC   
# MAGIC   test_stores_count_col_renamed <- test_stores_count %>% rename (Sys.ID.Join=tempe_product_key, 
# MAGIC                                                                Cluster=state_cluster_zone)
# MAGIC   }
# MAGIC 
# MAGIC else if (geography=='Europe'){
# MAGIC base_metrics_final_col_renamed <- base_metrics_final %>% rename (category_desc = item_category,
# MAGIC                                                                  Sys.ID.Join=PRODUCT_KEY,
# MAGIC                                                                  Cluster=cluster,
# MAGIC                                                                  Base.Units=base_units,
# MAGIC                                                                  Per.Unit.Cost=cost,
# MAGIC                                                                  Regular.Price=reg_price,
# MAGIC                                                                  Base.Price.Elasticity=FINAL_BP_ELASTICITY,
# MAGIC                                                                  Old.Regular.Price=old_reg_price,
# MAGIC                                                                 `Vat%` = vat_perc)
# MAGIC   
# MAGIC     test_stores_count_col_renamed <- test_stores_count %>% rename (Sys.ID.Join=tempe_product_key, 
# MAGIC                                                                Cluster=cluster)
# MAGIC   }
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC display(base_metrics_final_col_renamed)

# COMMAND ----------

# MAGIC %r
# MAGIC ##writing out the file
# MAGIC sheets <- list("Base Metrics" = base_metrics_final_col_renamed,"Test Store Count" = test_stores_count_col_renamed)
# MAGIC invisible(file.remove(file.path(paste0(optimization_directory,refresh,"/","inputs/",bu_code_lower,"/",base_metric_output_file)),full.names = T))
# MAGIC openxlsx::write.xlsx(sheets,paste0(optimization_directory,refresh,"/","inputs/",bu_code_lower,"/",base_metric_output_file))

# COMMAND ----------

# DBTITLE 1,Function to Compare DataTypes
# MAGIC %r
# MAGIC compareColumns <- function(my_file, last_refresh_file) {
# MAGIC   commonNames <- names(my_file)[names(my_file) %in% names(last_refresh_file)]
# MAGIC   data.frame(Column = commonNames,
# MAGIC              my_file = sapply(my_file[,commonNames], class),
# MAGIC              last_refresh_file = sapply(last_refresh_file[,commonNames], class)) }

# COMMAND ----------

# DBTITLE 1,Data Type Comparison - Base Metrics
# MAGIC %r
# MAGIC #Comparing Datatypes for Base Metrics
# MAGIC 
# MAGIC my_file <- read.xlsx(paste0(optimization_directory,"/",refresh,"/","inputs/",bu_code_lower,"/",base_metric_output_file), sheet = 'Base Metrics')
# MAGIC 
# MAGIC ##Past waves file (no need to update these links)
# MAGIC if (geography %in% c('USA','Canada')){
# MAGIC   last_refresh_file <- read.xlsx("/dbfs/Phase_3/optimization/wave_1/inputs/cc/base_metric_store_count.xlsx", sheet = 'Base Metrics')
# MAGIC }
# MAGIC else if (geography=='Europe'){
# MAGIC   last_refresh_file <- read.xlsx("/dbfs/Phase_3/optimization/wave_1/inputs/ie/base_metric_store_count.xlsx", sheet = 'Base Metrics')  
# MAGIC   }
# MAGIC 
# MAGIC compareColumns(my_file, last_refresh_file)
# MAGIC 
# MAGIC # Note: Please make sure all datatypes match exactly. Else this will mess up joins and throw errors downstream

# COMMAND ----------

# DBTITLE 1,Data Type Comparison - Test Store Count
# MAGIC %r
# MAGIC #Comparing Datatypes for Test Store Count
# MAGIC my_file <- read.xlsx(paste0(optimization_directory,"/",refresh,"/","inputs/",bu_code_lower,"/",base_metric_output_file), sheet = 'Test Store Count')
# MAGIC 
# MAGIC last_refresh_file <- read.xlsx("/dbfs/Phase_3/optimization/wave_1/inputs/cc/base_metric_store_count.xlsx", sheet = 'Test Store Count')
# MAGIC 
# MAGIC ##Past waves file (no need to update these links)
# MAGIC if (geography %in% c('USA','Canada')){
# MAGIC   last_refresh_file <- read.xlsx("/dbfs/Phase_3/optimization/wave_1/inputs/cc/base_metric_store_count.xlsx", sheet = 'Test Store Count')
# MAGIC }
# MAGIC else if (geography=='Europe'){
# MAGIC   last_refresh_file <- read.xlsx("/dbfs/Phase_3/optimization/wave_1/inputs/ie/base_metric_store_count.xlsx", sheet = 'Test Store Count')  
# MAGIC   }
# MAGIC 
# MAGIC compareColumns(my_file, last_refresh_file)
# MAGIC 
# MAGIC # Note: Please make sure all datatypes match exactly. Else this will mess up joins and throw errors downstream

# COMMAND ----------

# MAGIC %r
# MAGIC #Quality Check
# MAGIC if(nrow(base_metrics_final_col_renamed%>% dplyr::filter((is.na(Cluster))|(is.na(Base.Units))
# MAGIC                                                |(is.infinite(Base.Units))|(Base.Units == 0) | (is.na(Regular.Price))|(Regular.Price == 0)|
# MAGIC                                                (is.na(Base.Price.Elasticity))|(Base.Price.Elasticity > -0.5) |(Base.Price.Elasticity < -2.5)|(Base.Price.Elasticity == 0)|
# MAGIC                                               (is.na(Per.Unit.Cost))|(Per.Unit.Cost == 0)|(Per.Unit.Cost > Regular.Price))) >0 ){
# MAGIC         display(base_metrics_final_col_renamed%>% dplyr::filter((is.na(Cluster))|(is.na(Base.Units))
# MAGIC                                                |(is.infinite(Base.Units))|(Base.Units == 0) | (is.na(Regular.Price))|(Regular.Price == 0)|
# MAGIC                                                (is.na(Base.Price.Elasticity))|(Base.Price.Elasticity > -0.5) |(Base.Price.Elasticity < -2.5)|(Base.Price.Elasticity == 0)|
# MAGIC                                               (is.na(Per.Unit.Cost))|(Per.Unit.Cost == 0)|(Per.Unit.Cost > Regular.Price)))
# MAGIC   
# MAGIC   }
# MAGIC else {print ("Looks good!")}

# COMMAND ----------


