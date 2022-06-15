# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Creator: Steven Zhong, Jagruti Joshi, Anuj Rewale, Kushal Kapadia, Sai Kommisetty, Jing Lou, Binlun Feng
# Date Created: 08/24/2021
# Date Updated: 04/22/2022

# Description: Driver notebook for Data Prep Module 2
# Update 4/22/2022: Added scope template generation notebook and scope validation notebook as helper notebooks, removed scope column to move the primary/ secondary item definition within a price family from BU to DS team

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import libraries

# COMMAND ----------

# DBTITLE 0,Import library
from pyspark.sql.functions import *
from pyspark.sql import *
from dateutil.relativedelta import relativedelta
from datetime import *
import datetime
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
import databricks.koalas as ks
import pandas as pd
from operator import add
from functools import reduce
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import when, sum, avg, col, concat, lit,broadcast,greatest,least,countDistinct, mean,sqrt as _mean, stddev as _stddev
from dateutil.relativedelta import relativedelta
import sys

# COMMAND ----------

# MAGIC %md
# MAGIC #### Change parameters for current refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Modify following parameters - phase, wave, desired bu, start date, end date, effective date, csc file, new_upc_pk_file

# COMMAND ----------

# DBTITLE 0,Parameters (convert to widget in future work)
bu_codes = {"1400 - Florida Division": 'FL',
           "1600 - Coastal Carolina Division":"CC",
           "1700 - Southeast Division":"SE",
           "1800 - Rocky Mountain Division":'RM',
           "1900 - Gulf Coast Division":"GC",
           "2600 - West Coast Division":"WC",
           "2800 - Texas Division":'TX',
           "2900 - South Atlantic Division":"SA",
           "3100 - Grand Canyon Division":"GR",
           "3800 - Northern Tier Division":"NT",
           "4100 - Midwest Division":'MW',
           "4200 - Great Lakes Division":"GL",
           "4300 - Heartland Division":'HLD',
           "QUEBEC OUEST":'QW',
           "QUEBEC EST - ATLANTIQUE":'QE',
           "Central Division":'CE',
           "Western Division":'WC',
           "Norway":'NO',
           "Sweden":'SW',
           "Ireland":'IE',
           "Denmark":'DK'}

na_bu_codes = {"1400 - Florida Division": 'FL',
           "1600 - Coastal Carolina Division":"CC",
           "1700 - Southeast Division":"SE",
           "1800 - Rocky Mountain Division":'RM',
           "1900 - Gulf Coast Division":"GC",
           "2600 - West Coast Division":"WC",
           "2800 - Texas Division":'TX',
           "2900 - South Atlantic Division":"SA",
           "3100 - Grand Canyon Division":"GR",
           "3800 - Northern Tier Division":"NT",
           "4100 - Midwest Division":'MW',
           "4200 - Great Lakes Division":"GL",
           "4300 - Heartland Division":'HLD',
           "QUEBEC OUEST":'QW',
           "QUEBEC EST - ATLANTIQUE":'QE',
           "Central Division":'CE',
           "Western Division":'WC'}
           
eu_bu_codes = {k:v for k,v in bu_codes.items() if k not in na_bu_codes}

bu_test_control_dict = {"Russia": "Russia",
                        "Sweden": "Sweden",
                        "Central Division": "Central Canada",
                        "4100 - Midwest Division": "Midwest",
                        "2800 - Texas Division": "Texas",
                        "1900 - Gulf Coast Division": "Gulf Coast",
                        "3800 - Northern Tier Division": "3800 - Northern Tier Division",
                        "4300 - Heartland Division": "Heartland",
                        "4200 - Great Lakes Division": "Great Lakes",
                        "Lithuania": "Lithuania",
                        "Norway": "Norway",
                        "Western Division": "Western Canada",
                        "1800 - Rocky Mountain Division": "Rocky Mountain",
                        "Denmark": "Denmark",
                        "Ireland": "Ireland",
                        "1700 - Southeast Division": "Southeast",
                        "Estonia": "Estonia",
                        "3100 - Grand Canyon Division": "Grand Canyon",
                        "QUEBEC OUEST": "Quebec West",
                        "2900 - South Atlantic Division": "South Atlantic",
                        "Latvia": "Latvia",
                        "2600 - West Coast Division": "West Coast",
                        "QUEBEC EST - ATLANTIQUE": "Quebec East",
                        "1600 - Coastal Carolina Division": "Coastal Carolinas",
                        "1400 - Florida Division": "Florida",
                        "Poland": "Poland"}

## common parameters used across all notebooks
phase = 'Phase4_extensions'  ##### change this input #####
wave = 'Mock_GL_Test'  ##### change this input #####

desired_bu = '4200 - Great Lakes Division'  ##### change this input #####
bu_code = bu_codes[desired_bu]
bu_test_control = bu_test_control_dict[desired_bu] 

# USED ONLY FOR NA BUs
csc_file = '/dbfs/Phase4_extensions/Optimization/MAY2021_Refresh/inputs/gl/GL_optimization_master_input_file.xlsx'  ##### change this input #####

start_date = '2021-04-01'  ##### change this input #####
end_date = '2022-03-31'  ##### change this input #####
# transaction_years = ['2000', '2021', '2022'] ##### change this input for EU #####


db = 'phase4_extensions'

## parameters for 02 notebooks
base_directory_dbfs = "/dbfs/Phase4_extensions/Elasticity_Modelling" #dbfs directory
base_directory_spark = "/Phase4_extensions/Elasticity_Modelling" #Spark directory

## parameters for 03 NA notebook
#static file path location for the different issues, which have been identified in the hierarchy validation
# multi_to_promo.csv
# Problem_type_3.csv
# Problem_type_1_2.csv
new_upc_pk_file = '/dbfs/Phase3_extensions/Elasticity_Modelling/Nov21_Refresh/new_upc_pk.xlsx'
static_file_path = '/Phase_4/Data_Validation_All_BU/'
  
## parameters for 03 EU notebooks
transaction_table = 'dw_eu_rep_pos.pos_transactions_f'
item_table = 'dw_eu_common.item_d'
site_table = 'dw_eu_common.site_d'

## parameters for 04 notebooks
file_directory = 'Phase4_extensions/Elasticity_Modelling'
percent_of_stores = 0.65
days_since_last_sold = 30
dying_series_ratio = 0.25
slow_start_ratio = 0.25
proportion_weeks_sold = 0.98
week_gap_allowed = 14
min_qtr = 4 
cumulative_sales_perc= 0.8 
tolerance_qtrs=0.5
tolerance_last_yr = 1 

effective_date = "2022-03-31"  ##### change this input #####

## parameters for 04 EU scope_pf_csc notebooks
last_refresh = 'JUNE2021_Refresh'
eu_csc_path = '/dbfs/Phase3_extensions/Optimization/' + last_refresh + '/' + 'inputs/'+bu_code.lower()+'/' + bu_code.lower() + '_optimization_master_input_file_final_updated.xlsx'



# COMMAND ----------

# MAGIC %md
# MAGIC #### NA BU helper notebooks

# COMMAND ----------

# DBTITLE 0,NA BU Helper Notebooks
if desired_bu in na_bu_codes.keys():
  dbutils.notebook.run("../helper/02a_na_data_validation", 0, {"business_unit": "{}".format(desired_bu),\
                                                     "bu_code":"{}".format(bu_code),\
                                                     "start_date": "{}".format(start_date),\
                                                     "end_date": "{}".format(end_date),\
                                                     "wave": "{}".format(phase),\
                                                     "base_directory_dbfs":"{}".format(base_directory_dbfs),\
                                                     "base_directory_spark":"{}".format(base_directory_spark)})


# COMMAND ----------

if desired_bu in na_bu_codes.keys():
  if bu_code not in ("RM",'GC'):
    dbutils.notebook.run("../helper/02b_na_bus_business_hierarchy_validation_roll_up_except_rm_gc_v3", 0, {"business_unit": "{}".format(desired_bu),\
                                                     "bu_code":"{}".format(bu_code),\
                                                     "start_date": "{}".format(start_date),\
                                                     "end_date": "{}".format(end_date),\
                                                     "db": "{}".format(phase),\
                                                     "refresh": "{}".format(wave),\
                                                     "static_file_path": "{}".format(static_file_path),\
                                                     "new_upc_pk_file":"{}".format(new_upc_pk_file)})
  else:
    dbutils.notebook.run("../helper/02b_rm_gc_na_bus_business_hierarchy_validation_roll_up_v1", 0, {"business_unit": "{}".format(desired_bu),\
                                                     "bu_code":"{}".format(bu_code),\
                                                     "start_date": "{}".format(start_date),\
                                                     "end_date": "{}".format(end_date),\
                                                     "db": "{}".format(phase),\
                                                     "refresh": "{}".format(wave),\
                                                     "static_file_path": "{}".format(static_file_path),\
                                                     "new_upc_pk_file":"{}".format(new_upc_pk_file)})

# COMMAND ----------

if desired_bu in na_bu_codes.keys():
  dbutils.notebook.run("../helper/02c_na_inscope_product_analysis_v2", 0, {"business_unit": "{}".format(desired_bu),\
                                                     "bu_code":"{}".format(bu_code),\
                                                     "start_date": "{}".format(start_date),\
                                                     "end_date": "{}".format(end_date),\
                                                     "db": "{}".format(phase),\
                                                     "refresh": "{}".format(wave),\
                                                     "percent_of_stores":"{}".format(percent_of_stores),\
                                                     "days_since_last_sold":"{}".format(days_since_last_sold),\
                                                     "dying_series_ratio":"{}".format(dying_series_ratio),\
                                                     "slow_start_ratio":"{}".format(slow_start_ratio),\
                                                     "proportion_weeks_sold":"{}".format(proportion_weeks_sold),\
                                                     "week_gap_allowed":"{}".format(week_gap_allowed),\
                                                     "min_qtr":"{}".format(min_qtr),\
                                                     "cumulative_sales_perc":"{}".format(cumulative_sales_perc),\
                                                     "tolerance_qtrs":"{}".format(tolerance_qtrs),\
                                                     "tolerance_last_yr":"{}".format(tolerance_last_yr),\
                                                     "file_directory": "{}".format(file_directory)})

# COMMAND ----------

df = pd.read_csv('/dbfs/Phase4_extensions/Elasticity_Modelling/Mock_GL_Test/GL_Repo/Input/gl_item_scoping_data_final.csv')
df.head()

# COMMAND ----------

if desired_bu in na_bu_codes.keys():
  dbutils.notebook.run("../helper/02d_na_scope_pf_csc_new", 0, {"phase": "{}".format(phase),\
                                                     "wave":"{}".format(wave),\
                                                     "business_units": "{}".format(desired_bu),\
                                                     "bu_code": "{}".format(bu_code),\
                                                     "bu_test_control": "{}".format(bu_test_control),\
                                                     "csc_file": "{}".format(csc_file),\
                                                     "start_date": "{}".format(start_date),\
                                                     "end_date": "{}".format(end_date),\
                                                     "effective_date": "{}".format(effective_date),\
                                                     "db":"{}".format(db),\
                                                     "base_directory_spark": "{}".format(base_directory_spark)})

# COMMAND ----------

# MAGIC %md
# MAGIC #### EU BU helper notebooks

# COMMAND ----------

# DBTITLE 0,EU BU Helper Notebook
if desired_bu in eu_bu_codes.keys():
  dbutils.notebook.run("../helper/02a_eu_data_validation", 0,
                       {"business_unit": "{}".format(desired_bu),\
                        "bu_code":"{}".format(bu_code),\
                        "start_date": "{}".format(start_date),\
                        "end_date": "{}".format(end_date),\
                        "transaction_table":"{}".format(transaction_table),\
                        "item_table":"{}".format(item_table),\
                        "station_table":"{}".format(site_table)})

# COMMAND ----------

if desired_bu in eu_bu_codes.keys():  
  dbutils.notebook.run("../helper/02b_create_table", 0,
                       {"business_unit": "{}".format(desired_bu),\
                        "bu_code":"{}".format(bu_code),\
                        "start_date": "{}".format(start_date),\
                        "end_date": "{}".format(end_date),\
                        "db": "{}".format(db),\
                        "wave": "{}".format(wave),\
                        "transaction_table": "{}".format(transaction_table),\
                        "item_table": "{}".format(item_table),\
                        "station_table": "{}".format(site_table)})

# COMMAND ----------

if desired_bu in {'Ireland'}:
  dbutils.notebook.run("../helper/02c_ie_ean_level_modelling", 0,
                       {"business_unit": "{}".format(desired_bu),\
                        "bu_code":"{}".format(bu_code),\
                        "db": "{}".format(db),\
                        "phase": "{}".format(phase),\
                        "wave": "{}".format(wave)})

# COMMAND ----------

if desired_bu in eu_bu_codes.keys():    
  dbutils.notebook.run("../helper/02d_eu_inscope_product_analysis_v2", 0, {"business_unit": "{}".format(desired_bu),\
                                                                          "bu_code":"{}".format(bu_code),\
                                                                          "db": "{}".format(db),\
                                                                          "phase": "{}".format(phase),\
                                                                          "wave": "{}".format(wave),\
                                                                          "start_date": "{}".format(start_date),\
                                                                          "end_date": "{}".format(end_date),\
                                                                          "percent_of_stores":"{}".format(percent_of_stores),\
                                                                          "days_since_last_sold":"{}".format(days_since_last_sold),\
                                                                          "dying_series_ratio":"{}".format(dying_series_ratio),\
                                                                          "slow_start_ratio":"{}".format(slow_start_ratio),\
                                                                          "proportion_weeks_sold":"{}".format(proportion_weeks_sold),\
                                                                          "week_gap_allowed":"{}".format(week_gap_allowed),\
                                                                          "min_qtr":"{}".format(min_qtr),\
                                                                          "cumulative_sales_perc":"{}".format(cumulative_sales_perc),\
                                                                          "tolerance_qtrs":"{}".format(tolerance_qtrs),\
                                                                          "tolerance_last_yr":"{}".format(tolerance_last_yr),\
                                                                          "file_directory": "{}".format(file_directory)})

# COMMAND ----------

if desired_bu in eu_bu_codes.keys():    
  dbutils.notebook.run("../helper/02e_eu_scope_pf_csc_new", 0, {
                                                     "phase" : "{}".format(phase),\
                                                     "wave" : "{}".format(wave),\
                                                     "bu_code" : "{}".format(bu_code),\
                                                     "bu" : "{}".format(bu_test_control),\
                                                     "start_date" : "{}".format(start_date),\
                                                     "end_date" : "{}".format(end_date),\
                                                     "eu_csc_path" : "{}".format(eu_csc_path),\
                                                     "file_directory" : "{}".format(file_directory)
                                                     })

# COMMAND ----------

# MAGIC %md
# MAGIC #### Error catching for incorrect BU name

# COMMAND ----------

# DBTITLE 0,Error Catching
if (desired_bu not in na_bu_codes.keys()) and (desired_bu not in eu_bu_codes.keys()):
  print("THROW ERROR BAD BU")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Share scope template with BU

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 1. Download scope template from sharepoint/email
# MAGIC ###### 2. Download output of 04 scope_pf_csc_new helper notebook
# MAGIC ###### 3. Copy paste data starting from cell A2 of scope_wave to cell A5 of initial_scope_wave 
# MAGIC (DON'T OVERWRITE COLUMN H i.e. New LP Scope)
# MAGIC ###### 4. Hide columns AE:AU
# MAGIC ###### 5. Send this file to BU PEM/PA via email ccing your manager and Kevin/ Chris

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validate BU reviewed scope template

# COMMAND ----------

dbutils.notebook.run("../helper/scope_input_file_validation", 0, {"phase": "{}".format(phase),\
                                                     "wave":"{}".format(wave),\
                                                     "bu_code": "{}".format(bu_code),\
                                                     "BU": "{}".format(bu_test_control),\
                                                     "start_date": "{}".format(start_date),\
                                                     "end_date": "{}".format(end_date)})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify primary and secondary items for inscope price families

# COMMAND ----------

# MAGIC %md
# MAGIC ###### For price families with goodstuff items, goodstuff = primary and remaining items are secondary
# MAGIC ###### For price families without goodstuff items, 
# MAGIC ###### if # of flagged price families is low (<10), remove them 
# MAGIC ###### if # of flagged price families is high (>=10), the DS team can either relax the thresholds a bit and re-run the code before sending out the file to the BU OR can potentially use flagged items/ items with as primary if that item has the least critical data quality flag or that item is in top 80% sales

# COMMAND ----------

# MAGIC %md
# MAGIC #### Send finalized scope with primary and secondary items identified to Frances/ Noah via Microsoft Forms
# MAGIC #### Wait for scope approval before proceeding to Module 3
# MAGIC https://forms.office.com/Pages/ResponsePage.aspx?id=PB_aGVjZ6kSFDRlcFQJSXO9XSOFEjnxNvbTnP2CH5ClURE1VVDBDTk4xRkxPNlg0QkdRV0pIMlJDWCQlQCN0PWcu
