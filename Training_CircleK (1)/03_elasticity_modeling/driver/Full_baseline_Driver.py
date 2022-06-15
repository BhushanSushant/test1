# Databricks notebook source
# import the necessary packages
import pandas as pd
import glob
#import glob2

import numpy as np
from datetime import datetime

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

#Define parameters

#specify the user here


#specify the wave here 
wave= "Mock_GL_Test"

#define the base directory here
base_directory = 'Phase4_extensions/Elasticity_Modelling'  #Change for Phase 4
pre_wave= '/dbfs/'+base_directory+'/'

#specfiy the BU code
business_units = "4200 - Great Lakes Division" #'4200 - Great Lakes Division'
bu_code = "GL" #"GL"

# DB table prefix
db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'


bu_repo = bu_code + "_Repo"

#Product identification columns
if region=='NA':
  prod_id = 'PRODUCT_KEY' ## for EU BU: 'TRN_ITEM_SYS_ID' and for NA/CA BU: 'PRODUCT_KEY'
  prod_num = 'UPC' ## for EU BU: 'JDE_NUMBER' and for NA/CA BU: 'UPC'
elif region=='EU':
  prod_id = 'TRN_ITEM_SYS_ID' ## for EU BU: 'TRN_ITEM_SYS_ID' and for NA/CA BU: 'PRODUCT_KEY'
  prod_num = 'JDE_NUMBER' ## for EU BU: 'JDE_NUMBER' and for NA/CA BU: 'UPC'


#column names for collation collation_intermediate_file
collation_intermediate_file_cols_cons_reg = [prod_id, prod_num, 'ITEM_DESCRIPTION', 'CATEGORY', 'CLUSTER', 'BP_ELASTICITY', 'ESTIMATES_MAPE', 'ESTIMATES_MAPE_FLAG']
collation_intermediate_file_cols_ucm = [prod_id, 'CLUSTER', 'MAPE_UCM', 'UCM_PREDICTION_FLAG', 'ROUGH_COEF', 'UCM_SMOOTH_FLAG', 'REGULAR_PRICE_FLAG', 'SALES_FLAG', 'IMPUTE_FLAG', 'ISSUE_COMMENT', 'VERSION']

#column names for collation collation_output_file
collation_output_file_cols = ['ITEMID','CLUSTER','VERSION']

# Define notebook path
# databricks_notebooks_path = "/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/" # NY v202110
databricks_notebooks_path = "../helper/" # NY v202110

uc_file_name = 'user_category_' + bu_code.lower() + '.csv'
uc_path = '/dbfs/' + base_directory +'/'+ wave + '/' + bu_repo + '/Input/' + uc_file_name

# COMMAND ----------

uc = pd.read_csv(uc_path)

# COMMAND ----------

def run_driver(wave,bu_code,uc,base_directory):
  
  for a,b in uc.iterrows():
    dbutils.notebook.run(databricks_notebooks_path+"Full_Baseline_function",1000,\
                       {"wave": wave, "bu_code":bu_code,"user":b[0],"category":b[1],"base_directory":base_directory })
    print('Full Baselines created for',bu_code,'category=',b[1],'ran with the user name',b[0])

# COMMAND ----------

run_driver(wave,bu_code,uc,base_directory)

# COMMAND ----------

if region=='NA':
  baseline_collated_columns = ['PRODUCT_KEY', 'CATEGORY', 'CLUSTER', 'WEEK_START_DATE', 'UCM_BASELINE']
elif region=='EU':
  baseline_collated_columns =['TRN_ITEM_SYS_ID', 'CATEGORY', 'CLUSTER', 'WEEK_START_DATE', 'UCM_BASELINE']

final_combined_result_prefix = "Final_Combined_Result_"
baseline_data_sheet_name = "Baseline Data Output"
baseline_collated_file_name = "Baseline_Collated_all_items.xlsx"
baseline_collated_sheet_name = 'collated_baselines'

# COMMAND ----------


baseline_data_output_path = '/dbfs/' + base_directory +"/"+ wave + "/" + bu_repo + "/Output/final_output/"
baseline_collated_path = '/dbfs/' + base_directory +"/" +wave + "/" + bu_repo + "/Inputs_for_elasticity_imputation/"

# COMMAND ----------


baseline_collated = pd.DataFrame(columns=baseline_collated_columns)
for a,b in uc.iterrows(): #ps v4
  # use this line for testing with wave 1
  baseline_data_output = pd.read_excel(baseline_data_output_path + b[0] + "/" + final_combined_result_prefix + b[1] + "_full_baseline.xlsx", sheet_name = baseline_data_sheet_name) #ps v9
  # use this line for the refreshes
  # baseline_data_output = pd.read_excel(r"/dbfs/Phase3_extensions/Elasticity_Modelling/"+wave+"/"+bu_repo+"/Output/final_output/"+pair[0]+"/Final_Combined_Result_"+bu_abb+"_"+pair[1]+".xlsx", sheet_name = "Baseline Data Output")
  key_fields = baseline_data_output[baseline_collated_columns]
  baseline_collated = baseline_collated.append(key_fields)

# COMMAND ----------

baseline_collated['CLUSTER'] = baseline_collated['CLUSTER'].astype(str)   #v2 Cluster column data type changed to string
baseline_collated = baseline_collated.drop_duplicates()

# COMMAND ----------

print(baseline_collated.groupby([prod_id,'CLUSTER']).ngroups)

# COMMAND ----------

dbutils.fs.rm(baseline_collated_path[5:] + baseline_collated_file_name)
writer3 = pd.ExcelWriter(baseline_collated_file_name, engine = 'xlsxwriter')
baseline_collated.to_excel(writer3, sheet_name = baseline_collated_sheet_name, index = False)
writer3.save()

# COMMAND ----------

move(baseline_collated_file_name, baseline_collated_path)
