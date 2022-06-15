# Databricks notebook source
# Modified by: Nikhil Soni/ Sanjo Jose
# Date Modified: 03/09/2021
# Modifications: Test run for Phase 4 BUs, Converted widgets to parameters & Other Minor modifications

# COMMAND ----------

#v5
# Data type of column 'CLUSTER' changed to string
# Updates in cmd 25,35,58
#TD

# COMMAND ----------

# import the necessary packages and prepare pyspark for use during this program
import pandas as pd
import glob
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from dateutil.relativedelta import relativedelta

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

user = "sanjo"
user = dbutils.widgets.get("user")

# Define category
category_name = "016_PACKAGED_SWEET_SNACKS" #"002_CIGARETTES"
category_name = dbutils.widgets.get("category_name")

# Define business unit
business_units = "3100 - Grand Canyon Division" #"4200 - Great Lakes Division"
business_units = dbutils.widgets.get("business_unit")

bu_code = "GR" #"GL"
bu_code = dbutils.widgets.get("bu_code")

# Define bu_repo
bu_repo = bu_code.upper()+'_Repo'

# Define wave
wave = "Accenture_Refresh"
wave = dbutils.widgets.get("wave")

base_directory = '/dbfs/Phase4_extensions/Elasticity_Modelling/'

# DB table prefix
db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'

if region=='NA':
  prod_id = 'PRODUCT_KEY' ## for EU BU: 'TRN_ITEM_SYS_ID' and for NA/CA BU: 'PRODUCT_KEY'
  prod_num = 'UPC' ## for EU BU: 'JDE_NUMBER' and for NA/CA BU: 'UPC'
  item_indt_1_prod = 'product_key'
  item_indt_2_subcat = 'sub_category_desc'
  product_map_file_name = 'product_map_na.csv'
elif region=='EU':
  prod_id = 'TRN_ITEM_SYS_ID' ## for EU BU: 'TRN_ITEM_SYS_ID' and for NA/CA BU: 'PRODUCT_KEY'
  prod_num = 'JDE_NUMBER' ## for EU BU: 'JDE_NUMBER' and for NA/CA BU: 'UPC'
  item_indt_1_prod = 'sys_id'
  item_indt_2_subcat = 'item_subcategory'
  product_map_file_name = 'product_map_eu.csv'

template_name = "elast_cons_reg_out_template.csv"
collation_intermediate_file_columns = [prod_id, prod_num, 'ITEM_DESCRIPTION', 'CATEGORY', 'CLUSTER', 'BP_ELASTICITY', 'ESTIMATES_MAPE', 'ESTIMATES_MAPE_FLAG']
collation_input_file_columns = [prod_id,'CLUSTER','VERSION']
elasticities_df_columns = [prod_id,'CLUSTER','VERSION','CATEGORY','ITEM_DESCRIPTION',prod_num,'MAPE','BP_ELASTICITY']
hierarchy_map_columns = [prod_id,'item_subcategory']
final_table_columns = ['BP_ELAST_OPTI','item_category','CLUSTER','ESTIMATES_MAPE','ESTIMATES_MAPE_FLAG','item_name',prod_id,prod_num,'VERSION','item_subcategory']
print("user: "+user+" || category: "+category_name+" || bu: "+bu_code+" || wave: "+wave)

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# # # # inputs for the user of this program
# # # username_during_modeling = 'Sterling'
# # # latest_version_number = 'SOFT_DRINKS_v3'
# # dbutils.widgets.dropdown("user", "dayton", [str(x) for x in ('prat','roopa','sterling','neil','taru','colby','kushal','logan','neil','dayton','jantest','tushar','prakhar')])

# dbutils.widgets.dropdown("user", "sanjo", [str(x) for x in ('sanjo', 'user')])

# # dbutils.widgets.dropdown("username_during_modeling", "jantest", [str(x) for x in ('prathamesh','roopa','sterling','neil','taru','colby','kushal','logan','neil','dayton','jantest','tushar','prakhar')])

# # dbutils.widgets.dropdown("latest_version_number", "v1", [str(x) for x in ('v0','v1','v2','v3')])

# dbutils.widgets.dropdown("category_raw", "005-WINE", [str(x) for x in ('002-CIGARETTES','003-OTHER TOBACCO PRODUCTS','004-BEER','005-WINE','006-LIQUOR','007-PACKAGED BEVERAGES','008-CANDY','009-FLUID MILK','010-OTHER DAIRY & DELI PRODUCT','012-PCKGD ICE CREAM/NOVELTIES','013-FROZEN FOODS','014-PACKAGED BREAD','015-SALTY SNACKS','016-PACKAGED SWEET SNACKS','017-ALTERNATIVE SNACKS','019-EDIBLE GROCERY','020-NON-EDIBLE GROCERY','021-HEALTH & BEAUTY CARE','022-GENERAL MERCHANDISE','024-AUTOMOTIVE PRODUCTS','028-ICE','030-HOT DISPENSED BEVERAGES','031-COLD DISPENSED BEVERAGES','032-FROZEN DISPENSED BEVERAGES','085-SOFT DRINKS','089-FS PREP-ON-SITE OTHER','091-FS ROLLERGRILL','092-FS OTHER','094-BAKED GOODS','095-SANDWICHES','503-SBT PROPANE','504-SBT GENERAL MERCH','507-SBT HBA')])

# dbutils.widgets.dropdown("category_name", "005_WINE", [str(x) for x in ('002_CIGARETTES','003_OTHER_TOBACCO_PRODUCTS','004_BEER','005_WINE','006_LIQUOR','007_PACKAGED_BEVERAGES','008_CANDY','009_FLUID_MILK','010_OTHER_DAIRY_DELI_PRODUCT', '012_PCKGD_ICE_CREAM_NOVELTIES','013_FROZEN_FOODS','014_PACKAGED_BREAD','015_SALTY_SNACKS','016_PACKAGED_SWEET_SNACKS','017_ALTERNATIVE_SNACKS','019_EDIBLE_GROCERY','020_NON_EDIBLE_GROCERY','021_HEALTH_BEAUTY_CARE','022_GENERAL_MERCHANDISE','024_AUTOMOTIVE_PRODUCTS','028_ICE','030_HOT_DISPENSED_BEVERAGES',	'031_COLD_DISPENSED_BEVERAGES',	'032_FROZEN_DISPENSED_BEVERAGES','085_SOFT_DRINKS','089_FS_PREP_ON_SITE_OTHER','091_FS_ROLLERGRILL','092_FS_OTHER','094_BAKED_GOODS','095_SANDWICHES','503_SBT_PROPANE','504_SBT_GENERAL_MERCH','507_SBT_HBA')])

# # dbutils.widgets.dropdown("business_unit","1400 - Florida Division", [str(x) for x in ("1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
# #                   "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
# #                   "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
# #                  "Central Division","Western Division")])

# # dbutils.widgets.dropdown("wave_number", "1", [str(x) for x in ("1","2","3","4","5","6")])

                         
# #sf
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                   "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                  "Central Division","Western Division"]

# # BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
# #           ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
# #           ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
# #           ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# # wave = ["JAN2021_TestRefresh","Mar2021_Refresh_Group1"]

# wave = ["Accenture_Refresh"]


# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])

# #get bu 2 character code
# # bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# # bu_abb = bu_abb_ds.collect()[0][1].lower()
# # bu = bu_abb.upper()+'_Repo'

# COMMAND ----------

# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GR", bu_codes)

# COMMAND ----------

# All files path
files_path = base_directory + wave +'/' + bu_repo + '/Output/final_output/' + user + '/**/' + template_name
# the path where the collation input file can be found
collation_input_file_path = base_directory + wave + '/' + bu_repo + '/Input/collation_input_' + user + '_' + category_name + '.csv' #ps v9
# product map path
if region=='NA':
  product_map_df_path = base_directory + wave + '/' + product_map_file_name
elif region=='EU':
  product_map_df_path = '/dbfs/Phase3_extensions/Elasticity_Modelling/' + wave + '/' + product_map_file_name
# Final df write path
final_table_write_path = base_directory + wave + '/' + bu_repo + '/Inputs_for_elasticity_imputation/Reviewed_elasticities_for_imputation_' + user + '_' + category_name + '.csv'
# All files location
files = glob.glob(files_path) #ps v9
# select from the files retrieved those that correspond to the format of the latest version number
files_revised = [path for path in files if category_name in path]

# COMMAND ----------

# Reading the files and creating empty dataframes
# open the collation input file that contains the version number with the lowest MAPE for each item-cluster combination
collation_input_file = pd.read_csv(collation_input_file_path)
# sf v3 open product map as pandas dataframe
product_map_df = pd.read_csv(product_map_df_path)#pg v10
# define the dataframe that will hold the final results
collation_intermediate_file = pd.DataFrame(columns=collation_intermediate_file_columns)
# iterate over the files for the version and add their rows to the dataframe, along with which version they came from
for path in files_revised:
  version = pd.read_csv(path)
  path_part = path.split('_v')[-1]
  version_number = path_part.split('/')[0]
  version['VERSION'] = version_number
  collation_intermediate_file = collation_intermediate_file.append(version)
# create an empty dataframe to hold the results
elasticities = pd.DataFrame(columns=elasticities_df_columns)

# COMMAND ----------

# get rid of unnecessary decimal points
collation_intermediate_file[prod_id] = pd.to_numeric(collation_intermediate_file[prod_id], downcast='integer')
collation_intermediate_file[prod_num] = pd.to_numeric(collation_intermediate_file[prod_num], downcast='integer')
collation_intermediate_file['CLUSTER'] = collation_intermediate_file['CLUSTER'].astype('str')

# COMMAND ----------

# rename the columns
collation_input_file.columns = collation_input_file_columns
# get rid of unnecessary decimal points
collation_input_file[prod_id] = pd.to_numeric(collation_input_file[prod_id], downcast='integer')
collation_input_file['CLUSTER'] = collation_input_file['CLUSTER'].astype(str)  #v5 Datatype of Cluster column changed to string
collation_input_file['VERSION'] = collation_input_file['VERSION'].apply(str)

# COMMAND ----------

# get a list of items
items = list(collation_input_file[prod_id].unique())
# add the extra columns to the collation input file, based on look-up by item, cluster, and version
for item in items:
  inter_item_slice = collation_intermediate_file[collation_intermediate_file[prod_id]==item]
  input_item_slice = collation_input_file[collation_input_file[prod_id]==item]
  clusters = list(input_item_slice.CLUSTER.unique())
  for cluster in clusters:
    inter_cluster_slice = inter_item_slice[inter_item_slice['CLUSTER']==cluster]
    input_cluster_slice = input_item_slice[input_item_slice['CLUSTER']==cluster]
    version = list(input_cluster_slice.VERSION.unique())[0]
    inter_version_slice = inter_cluster_slice[inter_cluster_slice['VERSION']==version]
    elasticities = elasticities.append(inter_version_slice) 

# COMMAND ----------

# display(elasticities)

# COMMAND ----------

#hierarchy map subset from product_map_df
hierarchy_map = product_map_df[[item_indt_1_prod,item_indt_2_subcat]]
# get a copy of the dataframe
hierarchy_map = hierarchy_map.copy(deep=True)
hierarchy_map.columns = [prod_id,'item_subcategory']
hierarchy_map[prod_id] = pd.to_numeric(hierarchy_map[prod_id])

# COMMAND ----------

# merge the elasticities dataframe with the key fields
final_table = pd.merge(elasticities,hierarchy_map,on=prod_id)
#final_table.shape
if final_table.empty:
  ft_columns = ['item_category','item_subcategory','item_name',prod_num,prod_id,'CLUSTER','BP_ELAST_OPTI']
  final_table = pd.DataFrame(columns=ft_columns)
else:
  final_table = final_table[["BP_ELASTICITY","CATEGORY","CLUSTER","ESTIMATES_MAPE","ESTIMATES_MAPE_FLAG","ITEM_DESCRIPTION",prod_id,prod_num,"VERSION","item_subcategory"]]
  # rename the columns to match the spelling in the file prepared by Accenture
  final_table.columns = final_table_columns
  # remove the unnecessary columns
  del final_table['VERSION']
  del final_table['ESTIMATES_MAPE']
  del final_table['ESTIMATES_MAPE_FLAG']
  # rearrange the order of the columns
  final_table = final_table[['item_category', 'item_subcategory', 'item_name', prod_num, prod_id, 'CLUSTER', 'BP_ELAST_OPTI']]

# COMMAND ----------

#PG:v6 to see if there are any duplicates
f = final_table[final_table.duplicated(subset=[prod_id,'CLUSTER'])]
if f.empty==False:
  raise Exception("Duplicates in the table, please raise to Prakhar")

# COMMAND ----------

final_table['CLUSTER'] = final_table['CLUSTER'].astype(str) #v5 ~ data type to string

# COMMAND ----------

# display(final_table)

# COMMAND ----------

#sf
final_table.to_csv(final_table_write_path,index=False) #ps v9

if not final_table.empty:
  spark.createDataFrame(final_table).write.mode('overwrite').saveAsTable('localized_pricing.'+db_tbl_prefix+'input_reviewed_elasticities_for_imputation_'+user+'_'+category_name)
