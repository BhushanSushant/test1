# Databricks notebook source
#v2
# Cluster column changed to string
#cmd 10 updated
#td

# COMMAND ----------

# import the necessary packages and prepare pyspark for use during this program
import pandas as pd
import glob
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from dateutil.relativedelta import relativedelta
import xlrd
from shutil import move

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

#sf
base_directory = "/Phase4_extensions/Elasticity_Modelling/"

# business_units = "3100 - Grand Canyon Division" #"2800 - Texas Division"
# bu_code = "GR" #"TX"
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'

wave = "Accenture_Refresh" #"MAY2021_Refresh"
wave = dbutils.widgets.get("wave")

bu_repo = bu_code + "_Repo"
uc_file_name = 'user_category_' + bu_code.lower() + '.csv'

if region=='NA':
  baseline_collated_columns = ['PRODUCT_KEY', 'CATEGORY', 'CLUSTER', 'WEEK_START_DATE', 'UCM_BASELINE']
elif region=='EU':
  baseline_collated_columns =['TRN_ITEM_SYS_ID', 'CATEGORY', 'CLUSTER', 'WEEK_START_DATE', 'UCM_BASELINE']

final_combined_result_prefix = "Final_Combined_Result_"
baseline_data_sheet_name = "Baseline Data Output"
baseline_collated_file_name = "Baseline_Collated.xlsx"
baseline_collated_sheet_name = 'collated_baselines'

# COMMAND ----------

uc_path = '/dbfs' + base_directory + wave + '/' + bu_repo + '/Input/' + uc_file_name
baseline_data_output_path = '/dbfs' + base_directory + wave + "/" + bu_repo + "/Output/final_output/"
baseline_collated_path = '/dbfs' + base_directory + wave + "/" + bu_repo + "/Inputs_for_elasticity_imputation/"

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# #sf
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                   "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                  "Central Division","Western Division"]

# # BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
# #           ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
# #           ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
# #           ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]


# # wave_list = ["JAN2021_TestRefresh","MAY2021_Refresh","JUNE2021_Refresh"] #ps v4
# wave_list = ["Accenture_Refresh"]

# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave_list])

# #get bu 2 character code
# # bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# # bu_abb = bu_abb_ds.collect()[0][1].lower()
# # bu = bu_abb.upper()+'_Repo'
# # wave=dbutils.widgets.get("wave")
# # pre_wave = '/Phase3_extensions/Elasticity_Modelling/' #ps v9

# COMMAND ----------

# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GR", bu_codes)

# COMMAND ----------

uc = pd.read_csv(uc_path) #ps v4
baseline_collated = pd.DataFrame(columns=baseline_collated_columns)
for a,b in uc.iterrows(): #ps v4
  # use this line for testing with wave 1
  baseline_data_output = pd.read_excel(baseline_data_output_path + b[0] + "/" + final_combined_result_prefix + b[1] + ".xlsx", sheet_name = baseline_data_sheet_name) #ps v9
  # use this line for the refreshes
  # baseline_data_output = pd.read_excel(r"/dbfs/Phase3_extensions/Elasticity_Modelling/"+wave+"/"+bu_repo+"/Output/final_output/"+pair[0]+"/Final_Combined_Result_"+bu_abb+"_"+pair[1]+".xlsx", sheet_name = "Baseline Data Output")
  key_fields = baseline_data_output[baseline_collated_columns]
  baseline_collated = baseline_collated.append(key_fields)

# COMMAND ----------

baseline_collated['CLUSTER'] = baseline_collated['CLUSTER'].astype(str)   #v2 Cluster column data type changed to string
baseline_collated = baseline_collated.drop_duplicates()

# COMMAND ----------

dbutils.fs.rm(baseline_collated_path[5:] + baseline_collated_file_name)
writer3 = pd.ExcelWriter(baseline_collated_file_name, engine = 'xlsxwriter')
baseline_collated.to_excel(writer3, sheet_name = baseline_collated_sheet_name, index = False)
writer3.save()

# COMMAND ----------

move(baseline_collated_file_name, baseline_collated_path)
