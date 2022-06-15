# Databricks notebook source
# Modified by: Karan Kumar/ Sanjo Jose
# Date Modified: 02/09/2021
# Modifications: Modification of logic for best model selection at item x cluster, Test run for Phase 4 BUs, Converted widgets to parameters & Other Minor modifications

# COMMAND ----------

# import the necessary packages
import pandas as pd
import glob
#import glob2

import numpy as np
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

#Define parameters

#specify the user here
user = "sanjo"
user = dbutils.widgets.get("user")

#specify the category name for which the collatio input file needs to be created
# category_name="006_LIQUOR"
category_raw = "016-PACKAGED SWEET SNACKS" #"002-CIGARETTES" #'008-CANDY'
category = "016_PACKAGED_SWEET_SNACKS" #"002_CIGARETTES" #'008_CANDY'
category_raw = dbutils.widgets.get("category_raw")
category = dbutils.widgets.get("category_name")

#specify the wave here 
wave= "Accenture_Refresh"
wave = dbutils.widgets.get("wave")

#define the base directory here
base_directory = 'Phase4_extensions/Elasticity_Modelling'
pre_wave= '/dbfs/'+base_directory+'/'

#specfiy the BU code
business_units = "3100 - Grand Canyon Division" #'4200 - Great Lakes Division'
bu_code = "GR" #"GL"
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

# DB table prefix
db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'

zoned = 'no' #yes or no
zoned = dbutils.widgets.get("zoned")

# Derived variables
model_rerun_output_folder = bu_code.lower()+'_'+category+'_v9'

#Product identification columns
if region=='NA':
  prod_id = 'PRODUCT_KEY' ## for EU BU: 'TRN_ITEM_SYS_ID' and for NA/CA BU: 'PRODUCT_KEY'
  prod_num = 'UPC' ## for EU BU: 'JDE_NUMBER' and for NA/CA BU: 'UPC'
elif region=='EU':
  prod_id = 'TRN_ITEM_SYS_ID' ## for EU BU: 'TRN_ITEM_SYS_ID' and for NA/CA BU: 'PRODUCT_KEY'
  prod_num = 'JDE_NUMBER' ## for EU BU: 'JDE_NUMBER' and for NA/CA BU: 'UPC'
# prod_id = 'PRODUCT_KEY' ## for EU BU: 'TRN_ITEM_SYS_ID' and for NA/CA BU: 'PRODUCT_KEY'
# prod_num = 'UPC' ## for EU BU: 'JDE_NUMBER' and for NA/CA BU: 'UPC'

#column names for collation collation_intermediate_file
collation_intermediate_file_cols_cons_reg = [prod_id, prod_num, 'ITEM_DESCRIPTION', 'CATEGORY', 'CLUSTER', 'BP_ELASTICITY', 'ESTIMATES_MAPE', 'ESTIMATES_MAPE_FLAG']
collation_intermediate_file_cols_ucm = [prod_id, 'CLUSTER', 'MAPE_UCM', 'UCM_PREDICTION_FLAG', 'ROUGH_COEF', 'UCM_SMOOTH_FLAG', 'REGULAR_PRICE_FLAG', 'SALES_FLAG', 'IMPUTE_FLAG', 'ISSUE_COMMENT', 'VERSION']

#column names for collation collation_output_file
collation_output_file_cols = ['ITEMID','CLUSTER','VERSION']

# Define notebook path
# databricks_notebooks_path = "/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/" # NY v202110
databricks_notebooks_path = "./" # NY v202110

# COMMAND ----------

# v5 
# data type of the column CLUSTER is changed to str from int
# Updtaes in cmd 14 and 17
#TD
# #Run only if widgets has to be removed/ recreated
# dbutils.widgets.removeAll()

# COMMAND ----------

# # #sf
# # user = ['prat','roopa','sterling','neil','taru','colby','kushal','logan','prakhar','neil','dayton','david','anuj','xiaofan','global_tech','jantest','aadarsh','jantest2','tushar','rachael','alisa1','alisa','frances','jagruti','kushal1','darren']
# user = ['sanjo']

# category_name = ['002_CIGARETTES','003_OTHER_TOBACCO_PRODUCTS','004_BEER','005_WINE','006_LIQUOR','007_PACKAGED_BEVERAGES','008_CANDY','009_FLUID_MILK','010_OTHER_DAIRY_DELI_PRODUCT', '012_PCKGD_ICE_CREAM_NOVELTIES','013_FROZEN_FOODS','014_PACKAGED_BREAD','015_SALTY_SNACKS','016_PACKAGED_SWEET_SNACKS','017_ALTERNATIVE_SNACKS','019_EDIBLE_GROCERY','020_NON_EDIBLE_GROCERY','021_HEALTH_BEAUTY_CARE','022_GENERAL_MERCHANDISE','024_AUTOMOTIVE_PRODUCTS','028_ICE','030_HOT_DISPENSED_BEVERAGES',	'031_COLD_DISPENSED_BEVERAGES',	'032_FROZEN_DISPENSED_BEVERAGES','085_SOFT_DRINKS','089_FS_PREP_ON_SITE_OTHER','091_FS_ROLLERGRILL','092_FS_OTHER','094_BAKED_GOODS','095_SANDWICHES','503_SBT_PROPANE','504_SBT_GENERAL_MERCH','507_SBT_HBA']

# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                   "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                  "Central Division","Western Division"]

# # BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
# #           ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
# #           ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
# #           ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# # wave = ["JAN2021_TestRefresh","Mar2021_Refresh_Group1",'MAY2021_Refresh','JUNE_2021_Refresh']
# wave = ['Accenture_Refresh']

# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])
# dbutils.widgets.dropdown("user", "sanjo", [str(x) for x in user])
# dbutils.widgets.dropdown("category_name", "013_FROZEN_FOODS", [str(x) for x in category_name])

# # #get bu 2 character code
# # bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# # bu_abb = bu_abb_ds.collect()[0][1].lower()

# COMMAND ----------

# #sf
# bu = bu_abb.upper()+'_Repo'

# #sf
# user=dbutils.widgets.get("user")
# category_name=dbutils.widgets.get("category_name")
# wave=dbutils.widgets.get("wave")
# pre_wave= '/dbfs/Phase3_extensions/Elasticity_Modelling/' #ps v9

# COMMAND ----------

# %python
# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GL", bu_codes)

# category_raw = ['002-CIGARETTES','003-OTHER TOBACCO PRODUCTS','004-BEER','005-WINE','006-LIQUOR','007-PACKAGED BEVERAGES','008-CANDY','009-FLUID MILK','010-OTHER DAIRY & DELI PRODUCT','012-PCKGD ICE CREAM/NOVELTIES','013-FROZEN FOODS','014-PACKAGED BREAD','015-SALTY SNACKS','016-PACKAGED SWEET SNACKS','017-ALTERNATIVE SNACKS','019-EDIBLE GROCERY','020-NON-EDIBLE GROCERY','021-HEALTH & BEAUTY CARE','022-GENERAL MERCHANDISE','024-AUTOMOTIVE PRODUCTS','028-ICE','030-HOT DISPENSED BEVERAGES','031-COLD DISPENSED BEVERAGES','032-FROZEN DISPENSED BEVERAGES','085-SOFT DRINKS','089-FS PREP-ON-SITE OTHER','091-FS ROLLERGRILL','092-FS OTHER','094-BAKED GOODS','095-SANDWICHES','503-SBT PROPANE','504-SBT GENERAL MERCH','507-SBT HBA']
# dbutils.widgets.dropdown("category_raw", "013-FROZEN FOODS", [str(x) for x in category_raw])

# zoned = ['yes','no']
# dbutils.widgets.dropdown("zoned", "no", [str(x) for x in zoned])

# COMMAND ----------

# print("bu_Repo: "+bu_code+"_Repo"+" || wave: "+wave+" || user: "+user+" || category: "+category)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read all model outputs

# COMMAND ----------

# get the various version files saved under the username - UCM & cons reg
files_ucm = glob.glob(pre_wave+wave+'/'+bu_code+'_Repo'+'/Output/final_output/'+user+'/**/baselines_collated_template.csv')
files_cons_reg = glob.glob(pre_wave+wave+'/'+bu_code+'_Repo'+'/Output/final_output/'+user+'/**/elast_cons_reg_out_template.csv')

# COMMAND ----------

# select from the files retrieved those that correspond to the format of the latest version number - category under consideration

files_revised_ucm = [path for path in files_ucm if category in path]
files_revised_cons_reg = [path for path in files_cons_reg if category in path]

# COMMAND ----------

# iterate over the files for the version and add their rows to the dataframe, along with which version they came from

# UCM
collation_intermediate_file_ucm_full = pd.DataFrame(columns=collation_intermediate_file_cols_ucm)
for path in files_revised_ucm:
#   version = pd.read_csv(path) #PGv11 #NY202201
#   version = version[collation_intermediate_file_cols_ucm].drop_duplicates()
  version = pd.read_csv(path, encoding = "ISO-8859-1") #PGv11 #NY202201
  version['CLUSTER'] = version['CLUSTER'].astype(str)
  path_part = path.split('_v')[-1]
  version_number = path_part.split('/')[0]
  version['VERSION'] = version_number
  if version_number != '9':
    collation_intermediate_file_ucm_full = collation_intermediate_file_ucm_full.append(version)
  
# Kep only specific cols from UCM for choosing best model - data at item x cluster x version level
collation_intermediate_file_ucm = collation_intermediate_file_ucm_full[collation_intermediate_file_cols_ucm].drop_duplicates()

  
# cons reg
collation_intermediate_file_cons_reg = pd.DataFrame(columns=collation_intermediate_file_cols_cons_reg)
for path in files_revised_cons_reg:
#   version = pd.read_csv(path) #PGv11 #NY202201
  version = pd.read_csv(path, encoding = "ISO-8859-1") #PGv11 #NY202201
  version['CLUSTER'] = version['CLUSTER'].astype(str)
  version = version[collation_intermediate_file_cols_cons_reg].drop_duplicates()
  path_part = path.split('_v')[-1]
  version_number = path_part.split('/')[0]
  version['VERSION'] = version_number
  if version_number != '9':
    collation_intermediate_file_cons_reg = collation_intermediate_file_cons_reg.append(version)

# COMMAND ----------

# Get all model metrics togther
collation_intermediate_file = pd.merge(collation_intermediate_file_cons_reg, collation_intermediate_file_ucm, on=[prod_id, 'CLUSTER', 'VERSION'], how='left')

# COMMAND ----------

# get rid of unnecessary decimal points
collation_intermediate_file[prod_id] = pd.to_numeric(collation_intermediate_file[prod_id], downcast='integer')
collation_intermediate_file[prod_num] = pd.to_numeric(collation_intermediate_file[prod_num], downcast='integer')
collation_intermediate_file["CLUSTER"] = collation_intermediate_file.CLUSTER.astype(str) 

#PGv11
collation_intermediate_file["ROUGH_COEF"] = collation_intermediate_file.ROUGH_COEF.fillna(0.5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Best Model Selection

# COMMAND ----------

# Create flags to identify best version
df_selection = collation_intermediate_file.copy(deep=True)

# df_selection['item_cluster_key'] = df_selection[prod_id].astype(str) + '_' + df_selection['CLUSTER'].astype(str)
df_selection['ucm_mape_flag'] = np.where(df_selection['UCM_PREDICTION_FLAG'] == 'Good', 0, 1)
df_selection['ucm_roughness_flag'] = np.where(df_selection['UCM_SMOOTH_FLAG'] == 'Good', 0, 1)
df_selection['con_reg_mape_flag'] = np.where(df_selection['ESTIMATES_MAPE_FLAG'] == 'Good', 0, 1)
df_selection['extreme_elast_flag'] = np.where((df_selection['BP_ELASTICITY'] == -0.5) | (df_selection['BP_ELASTICITY'] == -2.5), 1, 0)
df_selection['sum_flags'] = df_selection['ucm_mape_flag'] + df_selection['ucm_roughness_flag']+ df_selection['con_reg_mape_flag'] + df_selection['extreme_elast_flag']
df_selection['avg_mape'] = df_selection[['ESTIMATES_MAPE', 'MAPE_UCM']].mean(axis=1)
# df_selection['selection_metric'] = df_selection['sum_flags'] + df_selection['avg_mape'] + df_selection['ROUGH_COEF'] #PGv11 #NY202201
df_selection['selection_metric'] = (1.02 * df_selection['sum_flags']) + df_selection['avg_mape'] + (1.04 * df_selection['ROUGH_COEF']) + (.01 * df_selection['VERSION'].astype(int))  #PGv11 #NY202201

# COMMAND ----------

# Choose the best model for each item x cluster combination - Min value of selection_metric
df_min = df_selection.groupby([prod_id, 'CLUSTER'], as_index=False)['selection_metric'].min()

df_selected = pd.merge(df_selection, df_min, on=[prod_id, 'CLUSTER', 'selection_metric'], how='inner')

# COMMAND ----------

# Validation Check 1: If there is only 1 final version selected for an item x cluster combination

df_best_count = df_selected.groupby([prod_id,'CLUSTER'])['VERSION'].agg(['count'])
df_best_dup_count = df_best_count[df_best_count['count']>1]
assert df_best_dup_count.empty

# COMMAND ----------

# Validation Check 2: If all the item x cluster has a best version selected

count_distinct_item_cluster = collation_intermediate_file.groupby([prod_id,'CLUSTER'],as_index=False).nunique()[prod_id].sum()
count_distinct_item_cluster_best = df_selected.groupby([prod_id,'CLUSTER'],as_index=False).nunique()[prod_id].sum()
assert count_distinct_item_cluster == count_distinct_item_cluster_best

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model Evaluation

# COMMAND ----------

# Check any item x cluster cases which has to go to Model Rerun or Elasticity Imputation - based on SALES_FLAG, REGULAR_PRICE_FLAG & IMPUTE_FLAG

# Identify all these item x clusters
df_selected_flagged = df_selected[(df_selected['SALES_FLAG'] != 'Good') | (df_selected['REGULAR_PRICE_FLAG'] != 'Good') | (df_selected['IMPUTE_FLAG'] != 'No Impute')]

# Define the cases based on Issue Comment where series has to be Truncated and Re-run models
truncate_cases = "Might consider truncation from beginning  as inconsitent starting sales|Might be massive YOY sales shift in data|Might want to Truncate Series due to very recent single price change"
# Define the cases based on Issue Comment which are to be considered directly for Imputation
impute_cases = "Might be too stockouts in the sales series spanning more than 6 weeks|unstable prices exist in series"

# Introduce the flag in data
df_selected_flagged['TRUNCATE_FLAG'] = np.where(df_selected_flagged['ISSUE_COMMENT'].str.contains(truncate_cases), 1, 0)
df_selected_flagged['IMPUTATION_FLAG'] = np.where((df_selected_flagged['IMPUTE_FLAG'] != 'No Impute')|(df_selected_flagged['ISSUE_COMMENT'].str.contains(impute_cases)), 1, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Truncate Series for specific cases

# COMMAND ----------

# For cases where TRUNCATE_FLAG = 1, Truncate data for latest 1 year at item x cluster level

df_selected_flagged_truncate = df_selected_flagged[(df_selected_flagged['TRUNCATE_FLAG']==1) & (df_selected_flagged['IMPUTATION_FLAG']==0)]
df_truncate = df_selected_flagged_truncate[[prod_id, 'CLUSTER', 'VERSION']]

# Get all the data from UCM
df_truncate_fulldata = pd.merge(collation_intermediate_file_ucm_full, df_truncate, on=[prod_id, 'CLUSTER', 'VERSION'], how='inner')
df_truncate_fulldata['WEEK_START_DATE'] = pd.to_datetime(df_truncate_fulldata['WEEK_START_DATE']).dt.date

if not df_truncate_fulldata.empty:
  # Get the max week at item x cluster level
  df_truncate_fulldata1 = df_truncate_fulldata.groupby([prod_id, 'CLUSTER']).agg(max_week=('WEEK_START_DATE', 'max')).reset_index()
  # Get the start week for latest 1 year data
  # df_truncate_fulldata1['max_week'] = pd.to_datetime(df_truncate_fulldata1['max']).dt.date
  df_truncate_fulldata1['start_week'] = df_truncate_fulldata1['max_week'] - pd.Timedelta(days=363)
  # df_truncate_fulldata1['start'] = df_truncate_fulldata1['start_week'].astype(str)
else:
  df_truncate_fulldata1 = df_truncate_fulldata[[prod_id, 'CLUSTER']]
  df_truncate_fulldata1['start_week'] = []
  df_truncate_fulldata1['max_week'] = []
  
df_truncate_fulldata2 = df_truncate_fulldata1[[prod_id, 'CLUSTER', 'start_week', 'max_week']]
df_truncate_fulldata3 = pd.merge(df_truncate_fulldata, df_truncate_fulldata2, on=[prod_id, 'CLUSTER'], how='inner')
df_truncated_data = df_truncate_fulldata3[(df_truncate_fulldata3['WEEK_START_DATE']>=df_truncate_fulldata3['start_week']) & (df_truncate_fulldata3['WEEK_START_DATE']<=df_truncate_fulldata3['max_week'])]

# COMMAND ----------

# Check if all these item x clusters have the right data to Re-run the models - Number of datapoints > 30, There should be a Regular price variation

# Number of datapoints > 30
df_truncated_data_counts = df_truncated_data.groupby([prod_id, 'CLUSTER']).agg(count_weeks=('WEEK_START_DATE', 'count')).reset_index()
df_truncated_data_counts_model = df_truncated_data_counts[df_truncated_data_counts['count_weeks']>30]
df_truncated_data_counts_impute = df_truncated_data_counts[df_truncated_data_counts['count_weeks']<=30]

# Regular price variation
df_truncated_data_reg_prices = df_truncated_data.groupby([prod_id, 'CLUSTER']).agg(count_reg_price=('REG_PRICE', 'nunique')).reset_index()
df_truncated_data_reg_prices_model = df_truncated_data_reg_prices[df_truncated_data_reg_prices['count_reg_price']>1]
df_truncated_data_reg_prices_impute = df_truncated_data_reg_prices[df_truncated_data_reg_prices['count_reg_price']==1]

if not df_truncated_data.empty:
  # Prepare final df for Re-running models
  df_re_model = pd.merge(df_truncated_data, df_truncated_data_counts_model, on=[prod_id, 'CLUSTER'], how='inner')
  df_re_model = pd.merge(df_re_model, df_truncated_data_reg_prices_model, on=[prod_id, 'CLUSTER'], how='inner')
  # df_re_model = df_re_model[[prod_id, 'CLUSTER', 'VERSION']]
else:
  df_re_model = df_truncated_data

# COMMAND ----------

# Update the Imputation flag for remaining set
if not df_selected_flagged.empty:
  df_selected_flagged2 = pd.merge(df_selected_flagged, df_truncated_data_counts_impute,on=[prod_id, 'CLUSTER'], how='left')
  df_selected_flagged2 = pd.merge(df_selected_flagged2, df_truncated_data_reg_prices_impute,on=[prod_id, 'CLUSTER'], how='left')
  df_selected_flagged2['IMPUTATION_FLAG'] = np.where(df_selected_flagged2['count_weeks'].notnull(), 1, df_selected_flagged2['IMPUTATION_FLAG'])
  df_selected_flagged2['IMPUTATION_FLAG'] = np.where(df_selected_flagged2['count_reg_price'].notnull(), 1, df_selected_flagged2['IMPUTATION_FLAG'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model Re-run with truncated series

# COMMAND ----------

# Get the input - to be used for model custom Re-run

# customize_data
df_customize_data = df_re_model[[prod_id, 'CLUSTER', 'start_week', 'max_week']].drop_duplicates()
df_customize_data['ITEMID'] = df_customize_data[prod_id]
df_customize_data['ACTION'] = 'TRUNCATE'
df_customize_data['START_DATE'] = pd.to_datetime(df_customize_data['start_week']).dt.strftime('%m/%d/%Y')
df_customize_data['END_DATE'] = pd.to_datetime(df_customize_data['max_week']).dt.strftime('%m/%d/%Y')
df_customize_data['REG_PRICE_FIX'] = ''
df_customize_data = df_customize_data[['ITEMID', 'CLUSTER', 'ACTION', 'START_DATE', 'END_DATE', 'REG_PRICE_FIX']]

# Get level_var column
versions = ['1','2','3','4','5','6','7','8']
level_vars = [0.15,0.12,0.1,0.08,0.05,0.03,0.01,0.008]
df_version_levelvar = pd.DataFrame({'VERSION': versions,'LEVEL_VAR': level_vars})

# custom_run_variables
df_custom_run_variables = df_re_model[[prod_id, 'CLUSTER', 'VERSION']].drop_duplicates()
df_custom_run_variables = pd.merge(df_custom_run_variables, df_version_levelvar, on='VERSION')
df_custom_run_variables['itemid'] = df_custom_run_variables[prod_id]
df_custom_run_variables['cluster'] = df_custom_run_variables['CLUSTER']
df_custom_run_variables['INDEPENDENT'] = 'DISCOUNT_PERC'
df_custom_run_variables['DEPENDENT'] = 'LN_SALES_QTY'
df_custom_run_variables = df_custom_run_variables[['itemid', 'cluster', 'INDEPENDENT', 'DEPENDENT', 'LEVEL_VAR']]

# COMMAND ----------

# display(df_customize_data)

# COMMAND ----------

# display(df_custom_run_variables)

# COMMAND ----------

# save these files to DBFS
# df_customize_data.to_csv(pre_wave+wave+'/'+bu_code+'_Repo'+'/Input/model_rerun_customize_data_'+user+'_'+category+'.csv',index=False)
# df_custom_run_variables.to_csv(pre_wave+wave+'/'+bu_code+'_Repo'+'/Input/model_rerun_custom_run_variables_'+user+'_'+category+'.csv',index=False)
if not df_re_model.empty:
  spark.createDataFrame(df_customize_data).write.mode('overwrite').saveAsTable('localized_pricing.'+db_tbl_prefix+'input_model_rerun_customize_data_'+user+'_'+category)
  spark.createDataFrame(df_custom_run_variables).write.mode('overwrite').saveAsTable('localized_pricing.'+db_tbl_prefix+'input_model_rerun_custom_run_variables_'+user+'_'+category)

# COMMAND ----------

# #Re-run models
if not df_re_model.empty:
  dbutils.notebook.run(databricks_notebooks_path+"Functions/elast_driver_code_model_rerun",1000,\
                         {"bu_code": bu_code, "business_unit": business_units, "category_name": category, "category_raw": category_raw, "user": user,"wave": wave, "zoned": zoned})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluate the rerun outputs

# COMMAND ----------

# get the model re-run output files saved under the username - UCM & cons reg
files_ucm_rerun = pre_wave+wave+'/'+bu_code+'_Repo'+'/Output/final_output/'+user+'/v'+model_rerun_output_folder+'/baselines_collated_template.csv'
files_cons_reg_rerun = pre_wave+wave+'/'+bu_code+'_Repo'+'/Output/final_output/'+user+'/v'+model_rerun_output_folder+'/elast_cons_reg_out_template.csv'

# COMMAND ----------

if not df_re_model.empty:
  # UCM
  collation_intermediate_file_ucm_rerun = pd.read_csv(files_ucm_rerun)
  collation_intermediate_file_ucm_rerun['CLUSTER'] = collation_intermediate_file_ucm_rerun['CLUSTER'].astype(str)
  collation_intermediate_file_ucm_rerun['VERSION'] = '9'
  collation_intermediate_file_ucm_rerun = collation_intermediate_file_ucm_rerun[collation_intermediate_file_cols_ucm].drop_duplicates()

  # cons reg
  collation_intermediate_file_cons_reg_rerun = pd.read_csv(files_cons_reg_rerun)
  collation_intermediate_file_cons_reg_rerun['CLUSTER'] = collation_intermediate_file_cons_reg_rerun['CLUSTER'].astype(str)
  collation_intermediate_file_cons_reg_rerun = collation_intermediate_file_cons_reg_rerun[collation_intermediate_file_cols_cons_reg].drop_duplicates()
  collation_intermediate_file_cons_reg_rerun['VERSION'] = '9'

  # Get all model metrics togther
  collation_intermediate_file_rerun = pd.merge(collation_intermediate_file_cons_reg_rerun, collation_intermediate_file_ucm_rerun, on=[prod_id, 'CLUSTER', 'VERSION'], how='left')

  # get rid of unnecessary decimal points
  collation_intermediate_file_rerun[prod_id] = pd.to_numeric(collation_intermediate_file_rerun[prod_id], downcast='integer')
  collation_intermediate_file_rerun[prod_num] = pd.to_numeric(collation_intermediate_file_rerun[prod_num], downcast='integer')
  collation_intermediate_file_rerun["CLUSTER"] = collation_intermediate_file_rerun.CLUSTER.astype(str) 

  # Final Output from Model rerun
  df_selection_rerun = collation_intermediate_file_rerun.copy(deep=True)

# COMMAND ----------

if not df_re_model.empty:
  # Check the item x cluster cases which has to go to Elasticity Imputation and for final output - based on SALES_FLAG, REGULAR_PRICE_FLAG & IMPUTE_FLAG

  # Identify all these item x clusters
  # df_selected_flagged_rerun = df_selection_rerun[(df_selection_rerun['SALES_FLAG'] != 'Good') | (df_selection_rerun['REGULAR_PRICE_FLAG'] != 'Good') | (df_selection_rerun['IMPUTE_FLAG'] != 'No Impute')]
  df_selected_flagged_rerun_impute = df_selection_rerun[df_selection_rerun['IMPUTE_FLAG'] != 'No Impute']

  # Check the cases which are good
  df_selected_flagged_rerun = df_selection_rerun[df_selection_rerun['IMPUTE_FLAG'] == 'No Impute']

# COMMAND ----------

# MAGIC %md
# MAGIC ###Impute cases

# COMMAND ----------

# Idenitfy the final set for Imputation from original run
if not df_selected_flagged.empty:
  df_selected_flagged_impute = df_selected_flagged2[df_selected_flagged2['IMPUTATION_FLAG']==1]
  df_selected_flagged_impute = df_selected_flagged_impute[[prod_id, 'CLUSTER', 'VERSION']]

  # Get the list of items for imputation from rerun
  if not df_re_model.empty:
    df_selected_flagged_rerun_impute = df_selected_flagged_rerun_impute[[prod_id, 'CLUSTER', 'VERSION']]
    # Append the results
    df_impute = df_selected_flagged_impute.append(df_selected_flagged_rerun_impute)
  else:
    df_impute = df_selected_flagged_impute.copy(deep=True)

  df_impute_final = df_impute[[prod_id, 'CLUSTER']]
  
else:
  df_impute_final = df_selected_flagged[[prod_id, 'CLUSTER']]

# COMMAND ----------

# save the final table to DBFS
#sf

if not df_impute_final.empty:
  df_impute.to_csv(pre_wave+wave+'/'+bu_code+'_Repo'+'/Input/impute_cases_after_model_'+user+'_'+category+'.csv',index=False)
  spark.createDataFrame(df_impute_final).write.mode('overwrite').saveAsTable('localized_pricing.'+db_tbl_prefix+'input_impute_cases_after_model_'+user+'_'+category)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Elasticities

# COMMAND ----------

# Idenitfy the final set which can be passed to next steps from original run
df_selected_final = df_selected[(df_selected['SALES_FLAG'] == 'Good') & (df_selected['REGULAR_PRICE_FLAG'] == 'Good') & (df_selected['IMPUTE_FLAG'] == 'No Impute')]
df_selected_final = df_selected_final[[prod_id, 'CLUSTER', 'VERSION']]

# Idenitfy the set which can be passed to next steps from the flagged set
df_selected_flagged_final = df_selected_flagged[(df_selected_flagged['TRUNCATE_FLAG']==0) & (df_selected_flagged['IMPUTATION_FLAG']==0)]
df_selected_flagged_final = df_selected_flagged_final[[prod_id, 'CLUSTER', 'VERSION']]

# Get the list of items for imputation from rerun
if not df_re_model.empty:
  df_selected_flagged_rerun = df_selected_flagged_rerun[[prod_id, 'CLUSTER', 'VERSION']]
  # Append the results
  df_final = df_selected_final.append(df_selected_flagged_final).append(df_selected_flagged_rerun)
else:
  df_final = df_selected_final.append(df_selected_flagged_final)

# COMMAND ----------

# Get output into required format
collation_output_file = df_final.copy(deep=True)

collation_output_file['ITEMID'] = collation_output_file[prod_id]
collation_output_file = collation_output_file[collation_output_file_cols]

# COMMAND ----------

# display(collation_output_file)

# COMMAND ----------

# save the final table to DBFS
#sf
collation_output_file.to_csv(pre_wave+wave+'/'+bu_code+'_Repo'+'/Input/collation_input_'+user+'_'+category+'.csv',index=False) #ps v9

if not collation_output_file.empty:
  spark.createDataFrame(collation_output_file).write.mode('overwrite').saveAsTable('localized_pricing.'+db_tbl_prefix+'input_collation_input_'+user+'_'+category)

# COMMAND ----------

# Review different cases basis the criterias used in this code

df_review_good = pd.DataFrame()
df_review_good['type'] = ['Good from model']
df_review_good['count'] = len(df_selected_final)

df_review_truncate = pd.DataFrame()
df_review_truncate['type'] = ['Truncated for model rerun']
df_review_truncate['count'] = len(df_custom_run_variables)

df_review_impute = pd.DataFrame()
df_review_impute['type'] = ['Imputation cases']
df_review_impute['count'] = len(df_impute_final)

df_review_total = pd.DataFrame()
df_review_total['type'] = ['Total']
df_review_total['count'] = len(df_selected)

df_review = df_review_good.append(df_review_truncate).append(df_review_impute).append(df_review_total)
df_review['percent_cases'] = df_review['count']/ len(df_selected)

# COMMAND ----------

# save the file for review by user
df_review.to_csv(pre_wave+wave+'/'+bu_code+'_Repo'+'/Inputs_for_elasticity_imputation/model_selection_review_'+user+'_'+category+'.csv',index=False)

# COMMAND ----------

# display(df_review)
