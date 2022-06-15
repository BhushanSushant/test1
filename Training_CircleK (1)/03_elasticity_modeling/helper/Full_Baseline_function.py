# Databricks notebook source
# MAGIC %r
# MAGIC library(tidyverse)
# MAGIC library(readxl)
# MAGIC library(parallel)
# MAGIC library(openxlsx)
# MAGIC options(scipen = 999)

# COMMAND ----------

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
# user = "feng"
user = dbutils.widgets.get("user")

#specify the category name for which the collatio input file needs to be created
# category_name="006_LIQUOR"
# category_raw = "016-PACKAGED SWEET SNACKS" #"002-CIGARETTES" #'008-CANDY'
# category = "017_ALTERNATIVE_SNACKS" #"002_CIGARETTES" #'008_CANDY'
# category_raw = dbutils.widgets.get("category_raw")
category = dbutils.widgets.get("category")

#specify the wave here 
# wave= "Nov21_Refresh"
wave = dbutils.widgets.get("wave")

#define the base directory here
base_directory = dbutils.widgets.get("base_directory")
pre_wave= '/dbfs/'+base_directory+'/'

#specfiy the BU code
# business_units = "1400 - Florida Division" #'4200 - Great Lakes Division'
# bu_code = "FL" #"GL"
# business_units = dbutils.widgets.get("business_unit")
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
# zoned = dbutils.widgets.get("zoned")

bu_repo = bu_code + "_Repo"

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
final_combined_result_prefix = "Final_Combined_Result_"
baseline_data_sheet_name = "Baseline Data Output"
# Define notebook path
# databricks_notebooks_path = "/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/" # NY v202110
databricks_notebooks_path = "./" # NY v202110

uc_file_name = 'user_category_' + bu_code.lower() + '.csv'


# COMMAND ----------

# MAGIC %r
# MAGIC #specify the user here
# MAGIC # user <- "feng" #"karan"
# MAGIC user <- dbutils.widgets.get("user")
# MAGIC 
# MAGIC #specify the category name for which the collatio input file needs to be created
# MAGIC # category<- "017_ALTERNATIVE_SNACKS" #"006_LIQUOR"
# MAGIC category <- dbutils.widgets.get("category")
# MAGIC 
# MAGIC #specify the wave here 
# MAGIC # wave <- "Nov21_Refresh"
# MAGIC wave <- dbutils.widgets.get("wave")
# MAGIC 
# MAGIC #specfiy the BU code
# MAGIC # business_units <- '1400 - Florida Division'
# MAGIC bu_code <- "FL" #"GL"
# MAGIC 
# MAGIC # business_units <- dbutils.widgets.get("business_unit")
# MAGIC bu_code <- dbutils.widgets.get("bu_code")
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
# MAGIC #Item Id column 
# MAGIC if (region=='NA'){
# MAGIC   prod_id <- "PRODUCT_KEY"
# MAGIC }
# MAGIC if (region=='EU'){
# MAGIC   prod_id <- "TRN_ITEM_SYS_ID"
# MAGIC }
# MAGIC 
# MAGIC #specify base_directory
# MAGIC base_directory = dbutils.widgets.get("base_directory")
# MAGIC 
# MAGIC #define the base directory here
# MAGIC pre_wave= paste0('/dbfs/',base_directory,'/')
# MAGIC 
# MAGIC print(paste0('bu_code: ',bu_code, ' || user: ',user,' || category: ', category))

# COMMAND ----------

# get the various version files saved under the username - UCM & cons reg
files_ucm = glob.glob(pre_wave+wave+'/'+bu_code+'_Repo'+'/Output/final_output/'+user+'/**/baselines_collated_template.csv')
files_cons_reg = glob.glob(pre_wave+wave+'/'+bu_code+'_Repo'+'/Output/final_output/'+user+'/**/elast_cons_reg_out_template.csv')

# COMMAND ----------

# select from the files retrieved those that correspond to the format of the latest version number - category under consideration

files_revised_ucm = [path for path in files_ucm if category in path]
files_revised_cons_reg = [path for path in files_cons_reg if category in path]
files_revised_ucm

# COMMAND ----------

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
  collation_intermediate_file_ucm_full = collation_intermediate_file_ucm_full.append(version)
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
  collation_intermediate_file_cons_reg = collation_intermediate_file_cons_reg.append(version)

# COMMAND ----------

# Get all model metrics togther
collation_intermediate_file = pd.merge(collation_intermediate_file_cons_reg, collation_intermediate_file_ucm, on=[prod_id, 'CLUSTER', 'VERSION'], how='left')

# COMMAND ----------

collation_intermediate_file_cons_reg

# COMMAND ----------

# get rid of unnecessary decimal points
collation_intermediate_file[prod_id] = pd.to_numeric(collation_intermediate_file[prod_id], downcast='integer')
collation_intermediate_file[prod_num] = pd.to_numeric(collation_intermediate_file[prod_num], downcast='integer')
collation_intermediate_file["CLUSTER"] = collation_intermediate_file.CLUSTER.astype(str) 

#PGv11
collation_intermediate_file["ROUGH_COEF"] = collation_intermediate_file.ROUGH_COEF.fillna(0.5)

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

count_distinct_item_cluster = collation_intermediate_file.groupby([prod_id,'CLUSTER']).ngroups
count_distinct_item_cluster_best = df_selected.groupby([prod_id,'CLUSTER']).ngroups
assert count_distinct_item_cluster == count_distinct_item_cluster_best

# COMMAND ----------

# display(collation_intermediate_file)


# COMMAND ----------

# display(df_selected)

# COMMAND ----------

df_selected_final = df_selected[[prod_id, 'CLUSTER', 'VERSION']]

# COMMAND ----------

df_selected_final.to_csv(pre_wave+wave+'/'+bu_code+'_Repo'+'/Input/collation_input_'+user+'_'+category+'_full_baseline.csv',index=False)

# COMMAND ----------

# MAGIC %r
# MAGIC #read the collation input
# MAGIC col_input <- read_csv(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Input/","collation_input_",user,"_",category,"_full_baseline.csv"))%>% #ps v9
# MAGIC            mutate(prod_id = as.numeric(prod_id),
# MAGIC                   CLUSTER = as.character(CLUSTER))
# MAGIC #PG v12
# MAGIC 
# MAGIC unique_vers <- unique(col_input$VERSION)
# MAGIC 
# MAGIC baselines <- c()
# MAGIC estimates <- c()
# MAGIC discounts <- c()
# MAGIC # sf v2 updated paths to all combined result
# MAGIC for(i in unique_vers){
# MAGIC    baseline <- read_excel(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/v",tolower(bu_code),"_",category,"_v",i,"/","All_Combined_Result_",category,".xlsx"), #ps v9
# MAGIC                           sheet = "Baseline Data Output", guess_max = 100000)%>%filter(!(is.na(get(prod_id))))%>%
# MAGIC                mutate(VSOURCE = i)%>%
# MAGIC                mutate(ESTIMATES_MAPE = as.character(ESTIMATES_MAPE))%>%
# MAGIC                mutate(WEEK_START_DATE = as.Date(WEEK_START_DATE))
# MAGIC    cols.char <- c("CLUSTER","UCM_PREDICTION_FLAG", "UCM_SMOOTH_FLAG", "REGULAR_PRICE_FLAG", "SALES_FLAG", "IMPUTE_FLAG", "IMPUTE_COMMENT", "ISSUE_COMMENT", "ESTIMATES_MAPE", "ESTIMATES_MAPE_FLAG", "OVERALL_ISSUE")
# MAGIC #    baseline[cols.char] <- sapply(baseline[cols.char],as.character)
# MAGIC    baseline <- dplyr::mutate_at(baseline, .vars = cols.char, .funs = as.character)
# MAGIC    
# MAGIC #    estimate[cols.char] <- sapply(estimate[cols.char],as.character)
# MAGIC    
# MAGIC #    discount[cols.char] <- sapply(discount[cols.char],as.character)
# MAGIC    
# MAGIC    baselines <- bind_rows(baselines,baseline)
# MAGIC    
# MAGIC }
# MAGIC baselines_filter <- baselines %>%
# MAGIC                    mutate(CLUSTER = as.character(CLUSTER))%>%left_join(col_input, c(prod_id,"CLUSTER"))%>%
# MAGIC                    filter(VERSION == VSOURCE)%>%select(-VSOURCE, -VERSION)%>%
# MAGIC                    arrange(!!as.name(prod_id),CLUSTER,WEEK_START_DATE)
# MAGIC 
# MAGIC 
# MAGIC all_combined <- list("Baseline Data Output" = baselines_filter)
# MAGIC invisible(file.remove(file.path(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/","Final_Combined_Result_",category,"_full_baseline.xlsx")),full.names = T)) #ps v9
# MAGIC write.xlsx(all_combined,paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/","Final_Combined_Result_",category,"_full_baseline.xlsx")) #ps v9

# COMMAND ----------

all = baseline_data_output = pd.read_excel(pre_wave+wave+'/'+bu_code+'_Repo'+'/Output/final_output/'+user+'/'+ final_combined_result_prefix + category + "_full_baseline.xlsx", sheet_name = baseline_data_sheet_name)

print(df_selected_final.groupby([prod_id,'CLUSTER']).ngroups)
print(all.groupby([prod_id,'CLUSTER']).ngroups)
assert df_selected_final.groupby([prod_id,'CLUSTER']).ngroups == all.groupby([prod_id,'CLUSTER']).ngroups

