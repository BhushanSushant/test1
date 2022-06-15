# Databricks notebook source
#v2
# Cluster data type changed to string
#updates in cmd 13,14

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

#sf
base_directory = "/Phase4_extensions/Elasticity_Modelling/"

# business_units = "3100 - Grand Canyon Division"
# bu_code = "GR" 
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'

wave = "Accenture_Refresh"
wave = dbutils.widgets.get("wave")

bu_repo = bu_code + "_Repo"
uc_file_name = 'user_category_' + bu_code.lower() + '.csv'

if region=='NA':
  final_df_columns = ['item_category', 'item_subcategory', 'item_name', 'UPC', 'PRODUCT_KEY', 'CLUSTER', 'BP_ELAST_OPTI']
elif region=='EU':
  final_df_columns = ['item_category', 'item_subcategory', 'item_name', 'JDE_NUMBER', 'TRN_ITEM_SYS_ID', 'CLUSTER', 'BP_ELAST_OPTI']

reviewed_elasticites_for_imputation_suffix = "Reviewed_elasticities_for_imputation_"
reviewed_elasticities_write_name = "Reviewed_elasticities_for_imputation.csv"

# COMMAND ----------

uc_path = '/dbfs' + base_directory + wave + '/' + bu_repo + '/Input/' + uc_file_name
reviewed_elasticities_path = '/dbfs' + base_directory + wave + '/' + bu_repo + '/Inputs_for_elasticity_imputation/'
collated_reviewed_elasticities_write_path = '/dbfs' + base_directory + wave + '/' + bu_repo + '/Inputs_for_elasticity_imputation/' + reviewed_elasticities_write_name

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# #dbutils.widgets.dropdown("user", "dayton", [str(x) for x in ('prat','roopa','sterling','neil','taru','colby','kushal','logan','neil','dayton','jantest','tushar','prakhar')]) #ps v3

# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                   "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                  "Central Division","Western Division"]

# # BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
# #           ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
# #           ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
# #           ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# # wave = ["JAN2021_TestRefresh","MAY2021_Refresh","JUNE2021_Refresh"] #ps v3
# wave = ["Accenture_Refresh"]

# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", [str(x) for x in Business_Units])
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])

# #get bu 2 character code
# # bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# # bu_abb = bu_abb_ds.collect()[0][1].lower()
# # bu = bu_abb.upper()+'_Repo'

# COMMAND ----------

# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GR", bu_codes)

# COMMAND ----------

uc = pd.read_csv(uc_path) #sf v3
final_df = pd.DataFrame(columns=final_df_columns)
for a,b in uc.iterrows(): #sf v3
  # use this line for testing with wave 1
  df = pd.read_csv(reviewed_elasticities_path + reviewed_elasticites_for_imputation_suffix + b[0] + '_' + b[1] + '.csv') #sf v3
  # use this line for the refreshes
  # baseline_data_output = pd.read_excel(r"/dbfs/Phase3_extensions/Elasticity_Modelling/"+wave+"/"+bu+"/Output/final_output/"+pair[0]+"/Final_Combined_Result_"+bu_abb+"_"+pair[1]+".xlsx", sheet_name = "Baseline Data Output")
  #key_fields = baseline_data_output[['PRODUCT_KEY', 'CATEGORY', 'CLUSTER', 'WEEK_START_DATE', 'UCM_BASELINE']]
  #baseline_collated = baseline_collated.append(key_fields)
  final_df = final_df.append(df) #sf v3

# COMMAND ----------

final_df['CLUSTER'] = final_df['CLUSTER'].astype(str) #v2 data type to string

# COMMAND ----------

final_df.to_csv(collated_reviewed_elasticities_write_path, index = False) #ps v9
