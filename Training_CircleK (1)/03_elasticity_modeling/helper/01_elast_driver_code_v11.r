# Databricks notebook source
#####################
##### Meta Data #####
#####################

# Creator: Taru Vaid,prat,sterling
# Date Created: 01/08/2021
# Date Updated: 01/19/2021
# Description: 

# Modified by: Sanjo Jose
# Date Modified: 26/08/2021
# Modifications: Test run for Phase 4 BUs, Converted widgets to parameters, Automated run for multiple versions, Model Improvements, Other Minor 

# The final version is modified by Noah Yang:
# Data Modified: 10/26/2021
# Modifications: The paramaters are changed from manual inputs to widgets. The databricks_notebooks_path has been updated to a widget. All the changes are commented by "# NY v202110" 
# The top 2 and least 2 weeks dummies are revised in the cmd 361-371 and 394 in do_parallel.R_final, contributed by Frances Zhang

# COMMAND ----------

# MAGIC %md
# MAGIC #### Meta Data
# MAGIC 
# MAGIC **Creator: *Nayan Mallick* | Updated by: ** Sterling Fluharty, Prakhar Goyal, Prat Shinge, and Taru Vaid
# MAGIC 
# MAGIC **Date Created: 09/02/2020**
# MAGIC 
# MAGIC **Date Updated: 04/30/2021**
# MAGIC 
# MAGIC **Description: The Driver notebook to carry out elasticity modelling for  a BU x Category in American BUs**
# MAGIC 
# MAGIC **Run Order:**
# MAGIC    1. Before you model any categories for a BU, make sure the 0_create_modeling_input_na_v2 notebook has been run for the BU, since it creates the reduced ADS file that is used in this driver notebook for zoned categories.
# MAGIC    2. Select a Hi-Cap cluster that is not assigned to Accenture and attach your notebook to it.
# MAGIC    3. Make your selections in the above widgets. If your BU is in Group 1, select the May 2021 wave, else select the June 2021 wave for Group 2 BUs. Start with version 1 and a levelvar setting of 0.15.
# MAGIC    4. Each time you run the notebook for the same category and BU, select a different levelvar and version, ideally working your way down each list. The code will pick the best version automatically, after your iterations.
# MAGIC    5. Run cell 5 to load the libraries.
# MAGIC    6. Check if the user list in cmd 8 contains your name. If your name is missing, then add it and return to step 2.
# MAGIC    7. Run cells 8 through 10.
# MAGIC    8. If cmd 10 gives you an error, make sure you select the same category in the category_name (which contains underscores) and category_raw (which contains spaces and special characters) widgets and return to step 2.
# MAGIC    9. Run cmd 11.
# MAGIC    10. If cmd 11 gives you an error, select the next available version (paired with an appropriate levelvar setting) and return to step 2.
# MAGIC    11. Run cells 12 through 17.
# MAGIC    12. If cmd 17 gives you an error, then uncomment cmd 18, run cmd 18, and recomment cmd 18. Ctrl+A selects the entire cell and Ctrl+/ comments the entire cell.
# MAGIC    13. If cmd 18 reports a tibble with 0 x 7 dimensions, that means all of the items in the category were dropped, likely for having weak sales. Select another category and return to step 2.
# MAGIC    14. Run cells 21 through 35. Cmd 35 can take between 10 minutes and 2 hours to run, depending on the size of the category.
# MAGIC    15. If cmd 35 reports node errors, then click show cell for cmd 37 and cmd 38, uncomment cmd 37 and 38, keep cmd 37 set to 'yes', and run those two cells. The run time for cmd 38 is between a few minutes and a few hours.
# MAGIC    16. If cmd 38 reports "All Models Successfully Run" or "Improvement in # of models due to core conflict resolution", then recomment cmd 37 and 38 and click the minimize button for cmd 37 and cmd 38.
# MAGIC    17. If cmd 38 does not report success or improvement, switch to a different cluster and restart the notebook.
# MAGIC    18. If cmd 38 gave you a good report, run cmd 40 to 54. The run time for cmd 54 is a few minutes.
# MAGIC    19. If cmd 54 runs without any errors, run cmd 56 to 66. 
# MAGIC    20. After you finish running version 1, follow the instructions in cmd 57 and 58 to determine whether manual price fixes or truncations are needed and then make those changes. You will not need to repeat this step for later versions.
# MAGIC    21. Continue running the notebook from start to finish until you have created 8 versions using 8 different levelvar settings for your category and BU.
# MAGIC    22. After you finish modeling all of the categories for a specific BU, open notebook 03a and update the list that matches users to particular categories. Make sure no more than one person is matched to each category.
# MAGIC    23. Once the user-category mapping is complete, run notebook 03a, which handles the final steps of elasticity modeling, such as elasticity imputation.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Installation of Packages

# COMMAND ----------

# install.packages('tidyverse')
# install.packages('zoo')
# install.packages('openxlsx')
# install.packages('trustOptim')

# COMMAND ----------

library(tidyverse)
library(readxl)
library(parallel)
library(openxlsx)
library(lubridate)
library(trustOptim)
options(scipen = 999)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Widgets

# COMMAND ----------

# reset the widget
#  dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.get("bu_code")
# dbutils.widgets.get("business_unit")
# dbutils.widgets.get("category_name")
# dbutils.widgets.get("category_raw")
# dbutils.widgets.get("levelvar")
# dbutils.widgets.get("user")
# dbutils.widgets.get("version_num")
# dbutils.widgets.get("wave")
# dbutils.widgets.get("zoned")

# COMMAND ----------

# DBTITLE 1,Variables: Python
# MAGIC %python
# MAGIC # Define parameters used in notebook
# MAGIC # business_units = '3100 - Grand Canyon Division' #'4200 - Great Lakes Division'  # NY v202110
# MAGIC # bu_code = 'GR' #'GL'  # NY v202110
# MAGIC business_units = dbutils.widgets.get("business_unit")  # NY v202110
# MAGIC bu_code = dbutils.widgets.get("bu_code")  # NY v202110
# MAGIC 
# MAGIC # Define Region
# MAGIC NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# MAGIC EU_BU = ["IE", "SW", "NO", "DK", "PL"]
# MAGIC 
# MAGIC if bu_code in NA_BU:
# MAGIC   region = 'NA'
# MAGIC elif bu_code in EU_BU:
# MAGIC   region = 'EU'
# MAGIC 
# MAGIC # Modify for each new wave
# MAGIC # wave = 'Accenture_Refresh'  # NY v202110
# MAGIC wave = dbutils.widgets.get("wave")  # NY v202110
# MAGIC 
# MAGIC # Define the user
# MAGIC # user = 'sanjo'  # NY v202110
# MAGIC user = dbutils.widgets.get("user")  # NY v202110
# MAGIC 
# MAGIC # Define the category
# MAGIC # category_raw = "016-PACKAGED SWEET SNACKS" #"002-CIGARETTES" #'008-CANDY'  # NY v202110
# MAGIC # category = "016_PACKAGED_SWEET_SNACKS" #"002_CIGARETTES" #'008_CANDY'  # NY v202110
# MAGIC category_raw = dbutils.widgets.get("category_raw")  # NY v202110
# MAGIC category = dbutils.widgets.get("category_name")  # NY v202110
# MAGIC 
# MAGIC 
# MAGIC # Update for each run - Use version 9 for any Model Reruns - custom run
# MAGIC # version_num = '2' #1 # Increment from 1 to 8  # NY v202110
# MAGIC # levelvar = 0.12 #0.15 # Choose from [0.15,0.12,0.1,0.08,0.05,0.03,0.01,0.008]  # NY v202110
# MAGIC # Update for R variables as well
# MAGIC 
# MAGIC version_num = dbutils.widgets.get("versions")  # NY v202110
# MAGIC levelvar = float(dbutils.widgets.get("levelvar"))  # NY v202110
# MAGIC 
# MAGIC # zoned = 'no' # Choose whether 'yes' or 'no'  # NY v202110
# MAGIC zoned = dbutils.widgets.get("zoned")  # NY v202110
# MAGIC 
# MAGIC # Define the database for tables and directory paths for input & output files
# MAGIC db = 'phase4_extensions'
# MAGIC base_directory = 'Phase4_extensions/Elasticity_Modelling'
# MAGIC pre_wave= '/dbfs/'+base_directory+'/'
# MAGIC 
# MAGIC # DB table prefix
# MAGIC db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'
# MAGIC 
# MAGIC # Define computed variables
# MAGIC bu = bu_code+'_Repo'
# MAGIC version = bu_code.lower()+'_'+category+'_v'+version_num
# MAGIC dbfs_path = '/dbfs/'+base_directory+'/'+wave+'/'+bu+'/Modelling_Inputs/'
# MAGIC 
# MAGIC # Define notebook path
# MAGIC # databricks_notebooks_path = "/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/" # NY v202110
# MAGIC databricks_notebooks_path = './'  # NY v202110
# MAGIC 
# MAGIC # Define input files for modelling
# MAGIC mod_input_master_file = '/dbfs/'+base_directory+'/'+wave+'/'+bu+'/Input/modelling_input_file.xlsx'

# COMMAND ----------

# DBTITLE 1,Variables: R
# Define parameters used in notebook
# business_units <- '3100 - Grand Canyon Division' #'4200 - Great Lakes Division'  # NY v202110
# bu_code <- 'GR' #'GL'  # NY v202110
business_units <- dbutils.widgets.get("business_unit")  # NY v202110
bu_code <- dbutils.widgets.get("bu_code")  # NY v202110

# Define Region
NA_BU <- c("FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD")
EU_BU <- c("IE", "SW", "NO", "DK", "PL")

if (bu_code %in% NA_BU){
  region <- 'NA'
  }
else if (bu_code %in% EU_BU){
  region <- 'EU'
  }

# Modify for each new wave
# wave <- 'Accenture_Refresh'  # NY v202110
wave <- dbutils.widgets.get("wave")  # NY v202110

# Define the user who is running the driver
# user <- 'sanjo'
user <- dbutils.widgets.get("user")  # NY v202110

# Define the category
# category_raw <- "016-PACKAGED SWEET SNACKS" #"002-CIGARETTES" #'008-CANDY'  # NY v202110
# category <- "016_PACKAGED_SWEET_SNACKS" #"002_CIGARETTES" #'008_CANDY'  # NY v202110
category_raw <- dbutils.widgets.get("category_raw")  # NY v202110
category <- dbutils.widgets.get("category_name")  # NY v202110

# Update for each run
# version_num <- '2' # Increment from 1 to 8 - Use version 9 for any Model Reruns - custom run  # NY v202110
# levelvar <- 0.12 # Choose from [0.15,0.12,0.1,0.08,0.05,0.03,0.01,0.008]  # NY v202110

version_num <- dbutils.widgets.get("versions")  # NY v202110
levelvar <- as.numeric(dbutils.widgets.get("levelvar"))  # NY v202110

# zoned <- 'no' # Choose whether 'yes' or 'no'  # NY v202110
zoned <- dbutils.widgets.get("zoned")  # NY v202110

# Define run_type
run_type <- "initial"  #Use #custom_run #initial basis if we are doing a model re-run or initial run

# Define the database for tables and directory paths for input & output files
db <- 'phase4_extensions'
base_directory <- 'Phase4_extensions/Elasticity_Modelling'
pre_wave <- paste0('/dbfs/',base_directory,'/')

# DB table prefix
db_tbl_prefix <- paste0(tolower(bu_code),'_',tolower(wave),'_')

# Define computed variables
bu <- paste0(bu_code,'_Repo')
version <- paste0(tolower(bu_code),'_',category,'_v',version_num)
mod_inputs_path <- paste0('/dbfs/',base_directory,'/',wave,'/',bu,'/Modelling_Inputs/')

# # Define notebook path
# databricks_notebooks_path <- paste0("/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/")  # NY v202110
databricks_notebooks_path <- paste0('./')  # NY v202110

# Define input files for modelling
mod_input_master_file <- paste0('/dbfs/',base_directory,'/',wave,'/',bu,'/Input/modelling_input_file.xlsx')

# COMMAND ----------

# %python
# #TC
# # Creating lists for widgets

# # If you are using this notebook and your name isn't here add it
# user = ['prat','roopa','sterling','neil','taru','colby','kushal','logan','prakhar','neil','dayton','david','anuj','xiaofan','global_tech','jantest','aadarsh','jantest2','tushar','rachael','alisa1','alisa','frances','jagruti', 'kushal1', 'manisha']
# # user =['sanjo']

# #Category
# # sf v8 reordered
# category_raw = ['002-CIGARETTES','003-OTHER TOBACCO PRODUCTS','004-BEER','005-WINE','006-LIQUOR','007-PACKAGED BEVERAGES','008-CANDY','009-FLUID MILK','010-OTHER DAIRY & DELI PRODUCT','012-PCKGD ICE CREAM/NOVELTIES','013-FROZEN FOODS','014-PACKAGED BREAD','015-SALTY SNACKS','016-PACKAGED SWEET SNACKS','017-ALTERNATIVE SNACKS','019-EDIBLE GROCERY','020-NON-EDIBLE GROCERY','021-HEALTH & BEAUTY CARE','022-GENERAL MERCHANDISE','024-AUTOMOTIVE PRODUCTS','028-ICE','030-HOT DISPENSED BEVERAGES','031-COLD DISPENSED BEVERAGES','032-FROZEN DISPENSED BEVERAGES','085-SOFT DRINKS','089-FS PREP-ON-SITE OTHER','091-FS ROLLERGRILL','092-FS OTHER','094-BAKED GOODS','095-SANDWICHES','503-SBT PROPANE','504-SBT GENERAL MERCH','507-SBT HBA', 'GLACE', 'PAIN', 'NON-ALIMENTAIRE-C']

# # sf v8 reordered
# category_name = ['002_CIGARETTES', '003_OTHER_TOBACCO_PRODUCTS', '004_BEER', '005_WINE', '006_LIQUOR', '007_PACKAGED_BEVERAGES', '008_CANDY', '009_FLUID_MILK', '010_OTHER_DAIRY_DELI_PRODUCT', '012_PCKGD_ICE_CREAM_NOVELTIES', '013_FROZEN_FOODS', '014_PACKAGED_BREAD', '015_SALTY_SNACKS', '016_PACKAGED_SWEET_SNACKS', '017_ALTERNATIVE_SNACKS', '019_EDIBLE_GROCERY', '020_NON_EDIBLE_GROCERY', '021_HEALTH_BEAUTY_CARE', '022_GENERAL_MERCHANDISE', '024_AUTOMOTIVE_PRODUCTS', '028_ICE', '030_HOT_DISPENSED_BEVERAGES', '031_COLD_DISPENSED_BEVERAGES', '032_FROZEN_DISPENSED_BEVERAGES', '085_SOFT_DRINKS', '089_FS_PREP_ON_SITE_OTHER', '091_FS_ROLLERGRILL', '092_FS_OTHER', '094_BAKED_GOODS', '095_SANDWICHES', '503_SBT_PROPANE', '504_SBT_GENERAL_MERCH', '507_SBT_HBA','GLACE', 'PAIN', 'NON_ALIMENTAIRE_C']

# # business unit & wave/refresh selection
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division", "1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division", "2900 - South Atlantic Division", "3100 - Grand Canyon Division", "3800 - Northern Tier Division", "4100 - Midwest Division", "4200 - Great Lakes Division", "4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division", "Western Division"]

# BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"], ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"], ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'], ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WD']]

# # wave = ["JAN2021_TestRefresh","MAY2021_Refresh","JUNE2021_Refresh"] #ps v10
# wave = ['Accenture_Refresh', 'Nov21_Refresh']
# versions = [1,2,3,4,5,6,7,8] #ps #sf
# levelvars = [0.15,0.12,0.1,0.08,0.05,0.03,0.01,0.008] #sf
# zoned = ['yes','no'] # sf v11

# #TC:creating widgets

# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("category_name", "013_FROZEN_FOODS", [str(x) for x in category_name])
# dbutils.widgets.dropdown("category_raw", "013-FROZEN FOODS", [str(x) for x in category_raw])
# dbutils.widgets.dropdown("user", "manisha", [str(x) for x in user])
# dbutils.widgets.dropdown("levelvar", "0.15", [str(x) for x in levelvars]) #sf
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])
# dbutils.widgets.dropdown("version", "1", [str(x) for x in versions])
# dbutils.widgets.dropdown("zoned", "no", [str(x) for x in zoned]) # sf v11

# COMMAND ----------

# %python
# # Added version 9 for model reruns
# versions = [1,2,3,4,5,6,7,8,9]
# dbutils.widgets.dropdown("versions", "1", [str(x) for x in versions])

# COMMAND ----------

# %python
# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GL", bu_codes)

# COMMAND ----------

# DBTITLE 1,Variables: Py
# %python
# import pandas as pd
# #get bu 2 character code
# # bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# # bu_abb = bu_abb_ds.collect()[0][1].lower()
# wave = dbutils.widgets.get("wave")
# # bu = bu_abb_ds.collect()[0][1]+"_Repo"

# user = dbutils.widgets.get("user") #3 users were modelling at the same time different categories
# category = dbutils.widgets.get("category_name")   #for the sake of printing file name & reading same name as user input file
# category_raw = dbutils.widgets.get("category_raw")
# version_num = dbutils.widgets.get("version")
# version  =  bu_abb+"_"+dbutils.widgets.get("category_name")+"_v"+dbutils.widgets.get("version")  #ps  #to keep track of modelling version 
# levelvar = pd.to_numeric(dbutils.widgets.get("levelvar"), downcast='float')  #parameter used to control how much variance of dependent ucm should capture
# zoned = dbutils.widgets.get("zoned") # sf v11

# #paths
# pre_wave = "/dbfs/Phase4_extensions/Elasticity_Modelling/" #ps v9
# dbfs_path = pre_wave+wave+"/"+bu+"/Modelling_Inputs/"
# databricks_notebooks_path = "/Localized-Pricing/Phase4_extensions/Elasticity_Modelling/"+wave+"/"+bu+"/" #ps v9
# #sf
# databricks_perm_path = "/Localized-Pricing/Phase4_extensions/Elasticity_Modelling/JAN2021_TestRefresh/GR_Repo/" #ps v10
# mod_input_master_file = "/dbfs/Phase4_extensions/Elasticity_Modelling/"+wave+"/"+bu+"/Input/modelling_input_file.xlsx"
# #ps temp texas test:
# #mod_input_master_file = "/dbfs/Phase3_extensions/Elasticity_Modelling/"+wave+"/"+bu+"/Input/modelling_input_pg_tx_saltysnacks_v1.xlsx"
 
# print("BU_Abb:", bu_abb, "; Business Unit:", dbutils.widgets.get("business_unit"),";Category: ",dbutils.widgets.get("category_name"))
# # #ps

# COMMAND ----------

# MAGIC %python
# MAGIC #sf
# MAGIC #validations on category widgets
# MAGIC import sys
# MAGIC if category[:3] != category_raw[:3]:
# MAGIC   sys.exit("different categories selected in category_name and category_raw")                 

# COMMAND ----------

# MAGIC %python
# MAGIC # sf
# MAGIC # validation on version widget
# MAGIC import glob
# MAGIC version_paths = glob.glob('/dbfs/'+base_directory+'/'+wave+'/'+bu+'/Output/final_output/'+user+'/**/elast_cons_reg_out_template.csv') #ps v9
# MAGIC files_revised = [path for path in version_paths if category in path]
# MAGIC version_list = []
# MAGIC for path in files_revised:
# MAGIC   path_part = path.split('_v')[1]
# MAGIC   version_number = path_part.split('/')[0]
# MAGIC   version_list.append(version_number)
# MAGIC if version_num in version_list:
# MAGIC   sys.exit("version number already used")

# COMMAND ----------

# DBTITLE 1,Variables: R
# ##TC: inputs for R
# Business_Units = c("1400 - Florida Division","1600 - Coastal Carolina Division", "1700 - Southeast Division","1800 - Rocky Mountain Division","1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division",  "2900 - South Atlantic Division", "3100 - Grand Canyon Division",  "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division",  "Western Division")
# BU_abbr =tolower(c("FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"))

# bu_df =data.frame(Business_Units,BU_abbr)
# bu_abb = bu_df %>% filter(Business_Units %in% dbutils.widgets.get("business_unit")) %>% select(BU_abbr)
# bu_abb = bu_abb[1,1]
# bu <- paste(toupper(bu_abb), "_Repo", sep="")  # GC_repo #SE_repo #TC: automate based on short forms

# #other inputs
# user <- dbutils.widgets.get("user") #3 users were modelling at the same time different categories
# category <- dbutils.widgets.get("category_name")   #for the sake of printing file name & reading same name as user input file
# version  <-  paste0(bu_abb,"_",dbutils.widgets.get("category_name"),"_v",dbutils.widgets.get("version"))  #ps  #to keep track of modelling version 
# levelvar <- as.numeric(dbutils.widgets.get("levelvar"))  #parameter used to control how much variance of dependent ucm should capture

# wave <- dbutils.widgets.get("wave")
# #ps paths
# pre_wave <- "/dbfs/Phase4_extensions/Elasticity_Modelling/"
# mod_inputs <- paste0(pre_wave,wave,"/",bu,"/Modelling_Inputs/")
# databricks_notebooks_path <- paste0("/Localized-Pricing/Phase4_extensions/Elasticity_Modelling/",wave,"/",bu,"/") #ps v9
# #sf
# databricks_perm_path <- paste0("/Localized-Pricing/Phase4_extensions/Elasticity_Modelling/JAN2021_TestRefresh/GR_Repo/") #ps v10
# mod_input_master_file = paste0(pre_wave,wave,"/",bu,"/Input/modelling_input_file.xlsx") 
# #mod_input_master_file <- paste0(pre_wave,wave,"/",bu,"/Input/modelling_input_pg_tx_saltysnacks_v1.xlsx")
# #TC: add widget for version (v1,v2,v3,v4)
# #ps: added widget for version (v1,v2,v3,v4)

# COMMAND ----------

##################################################### USER INPUTS ########################################################################################

# run_type <- "initial"                  #custom_run #initial if we have changes after inital model qc - we call modified; if you want to run custom runs for
# # run_type <- dbutils.widgets.get("run_type") 

erase_old_output <- T                  #if we want to keep or replace output from old run for same version
remove_weeks <- c("2099-12-31")           #provision to remove incomplete  weeks especially at the beginning & end of history from modelling data

#BU Specific  Inputs
if (region=='NA'){
  item_indt_1 <- "PRODUCT_KEY"    #PRODUCT_KEY #TRN_ITEM_SYS_ID
  item_indt_2 <- "upc" #upc #item_number
  item_indt_3 <- "item_desc" #item_desc #item_name
  item_indt_4 <- "category_desc" #category_desc #item_category
  item_indt_2_temp <- "UPC" #UPC #JDE_NUMBER
  hierarchy_cols <- c( "nacs_category_desc", "sub_category_desc", "department_desc")
  item_indt_1_prod <- "product_key"
}
if (region=='EU'){
  item_indt_1 <- "TRN_ITEM_SYS_ID"    #PRODUCT_KEY #TRN_ITEM_SYS_ID
  item_indt_2 <- "item_number" #upc #item_number
  item_indt_3 <- "item_name" #item_desc #item_name
  item_indt_4 <- "item_category" #category_desc #item_category
  item_indt_2_temp <- "JDE_NUMBER" #UPC #JDE_NUMBER
  hierarchy_cols <- c("item_subcategory", "item_category_group")
  item_indt_1_prod <- "sys_id"
}

# item_indt_1 <- "PRODUCT_KEY"    #PRODUCT_KEY #TRN_ITEM_SYS_ID
# item_indt_2 <- "upc" #upc #item_number
# item_indt_3 <- "item_desc" #item_desc #item_name
# item_indt_4 <- "category_desc" #category_desc #item_category
# item_indt_2_temp <- "UPC" #UPC #JDE_NUMBER
# hierarchy_cols <- c( "nacs_category_desc", "sub_category_desc", "department_desc")
# item_indt_1_prod <- "product_key"

# sf v11
# zoned <- dbutils.widgets.get("zoned")

# sf v11
if (zoned == "yes") {
ADS_loc <-  "modelling_ads_cluster_reduced.Rds"
} else {
ADS_loc <-  "modelling_ads_cluster_final.Rds"
}

#ADS_loc <-  "modelling_ads_cluster_stateless.Rds" #"modelling_ads_cluster_state.Rds" 
#ADS_loc <-  "modelling_ads_cluster_final.Rds" #sf v6 test of special ADS created by Taru
#ADS_loc <-  "modelling_ads_cluster_reduced.Rds" # sf v8

print("Run Specs..")
print(paste0("BU:",business_units,"||","User:",user,"||version:",version,"||levelvar:",levelvar,"||Run Type:", run_type))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run STAGE 1 - Baseline Estimation

# COMMAND ----------

#TC
# Read scope from master modeling input, 
# filter items for category selected in widget, 
# change STATUS of all items of that category to Y

scope_file <- read_excel(mod_input_master_file, sheet = "scope_file")
scope_category = scope_file[scope_file['CATEGORY']==category_raw,]
scope_category$STATUS <- 'Y'
# sf v6 save scope_category as a csv
write_csv(scope_category,paste0(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/","scope_category_",user,"_",category,".csv")))
print(paste0('Total items in ', category,": ",nrow(scope_category)))

# COMMAND ----------

# MAGIC %python
# MAGIC # sf v11
# MAGIC # zoned = dbutils.widgets.get("zoned")
# MAGIC if zoned == 'yes':
# MAGIC   # sf v6 remove the item x cluster (and possibly x zone) combinations that have weak sales and produce a reduced ADS for modeling
# MAGIC   # sf v7 linked to v3 of the same notebook
# MAGIC   # sf v8 changed from drop weak sales notebook to scope category reduced notebook
# MAGIC   dbutils.notebook.run(databricks_notebooks_path+"01aa_scope_category_reduced_v4",1000,\
# MAGIC                        {"user": user,"category_name":category,"business_unit": business_units,"wave":wave, "bu_code":bu_code})

# COMMAND ----------

# sf v6 use the reduced ADS for modeling
# sf v8 commented out because file loaded above
#ADS_loc <-  "modelling_ads_cluster_reduced.Rds"
# sf v7 read in reduced scope category csv file, which is created in v2 of the 01d notebook
# sf v11
if (zoned == "yes") {
  scope_category <- read_csv(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/","scope_category_reduced_",user,"_",category,".csv"))
  #scope_category <- scope_category[1:15,]
#   display(scope_category)
} else {
# display(scope_category)
}
#items_to_drop <- c(1804751)
#scope_category <- scope_category[!scope_category$ITEMID %in% items_to_drop, ]

# COMMAND ----------

#sf v7 run this line if you get an error in cmd 17
#sf v7 if you get a tibble with 0 x 7 dimensions, that means all of the items were dropped by the 01d notebook
#scope_category

# COMMAND ----------

# MAGIC %md
# MAGIC # MODELLING

# COMMAND ----------

# MAGIC %md
# MAGIC ##### source codes

# COMMAND ----------

# MAGIC %run "./Functions/ucm_functions.R_final"

# COMMAND ----------

# MAGIC %run "./Functions/do_parallel.R_final"

# COMMAND ----------

# MAGIC %run "./Functions/core_functions.R_final"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User Defined Functions

# COMMAND ----------

# # category_check <- "017-ALTERNATIVE SNACKS"
# category_check <- dbutils.widgets.get("category_raw")
# category_check

# COMMAND ----------

# MAGIC %r
# MAGIC print("Specify Location to Modelling ADS...")
# MAGIC #Location to Modelling ADS
# MAGIC datasource <- paste0(mod_inputs_path,ADS_loc)
# MAGIC 
# MAGIC custom_data_func <- function(model_data,customize_data){
# MAGIC   #Enter code to make custom modifications to functions here
# MAGIC   #Enter new variables introduced here to non_baseline_vars
# MAGIC   
# MAGIC   customize_data_instance_main <- customize_data %>% filter(ITEMID %in% model_data$ITEMID & CLUSTER %in% model_data$STORE)
# MAGIC   
# MAGIC   #Truncating_Data
# MAGIC   if(nrow(customize_data_instance_main) > 0){
# MAGIC     actions <- unique(customize_data_instance_main$ACTION)
# MAGIC     for(i in actions){  
# MAGIC           customize_data_instance <- customize_data_instance_main%>%filter(ACTION == i)
# MAGIC           if(customize_data_instance$ACTION=="TRUNCATE"){
# MAGIC 
# MAGIC             if(!(is.na(customize_data_instance$START_DATE))){
# MAGIC                 model_data <- model_data %>% filter(WEEKDATE >= as.Date(customize_data_instance$START_DATE ,"%m/%d/%Y"))
# MAGIC             }
# MAGIC 
# MAGIC             if(!(is.na(customize_data_instance$END_DATE))){
# MAGIC                 model_data <- model_data %>% filter(WEEKDATE <= as.Date(customize_data_instance$END_DATE , "%m/%d/%Y")) 
# MAGIC             }
# MAGIC 
# MAGIC 
# MAGIC           }  #Fixing Reg Price
# MAGIC         if(grepl("PRICE_FIX",customize_data_instance$ACTION) == TRUE){
# MAGIC           model_data <- model_data %>%
# MAGIC                         mutate(REG_PRICE = ifelse(WEEKDATE >= as.Date(customize_data_instance$START_DATE ,"%m/%d/%Y") & WEEKDATE <=                       
# MAGIC                                                   as.Date(customize_data_instance$END_DATE , "%m/%d/%Y"), customize_data_instance$REG_PRICE_FIX, REG_PRICE))
# MAGIC 
# MAGIC           }
# MAGIC       }
# MAGIC   }
# MAGIC 
# MAGIC   return(model_data)
# MAGIC }
# MAGIC  
# MAGIC print("Custom Function for SKU level data registered...")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Decide Category Overall Seasonal Dummy & Check Remove Weeks as well

# COMMAND ----------

cat_sum <- c()
# sf v6 updated paths for product map and calendar file, based on changes that Taru's data prep group made
if(region=='NA'){
  product_map <- readRDS(paste0("/dbfs/",base_directory,'/',wave,"/product_map_na.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))
}
if(region=='EU'){
  product_map <- readRDS(paste0("/dbfs/",base_directory,'/',wave,"/product_map_eu.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))
}
# product_map <- readRDS(paste0("/dbfs/",base_directory,'/',wave,"/product_map_na.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod))) #ps v9
calendar_dat <- readRDS(paste0("/dbfs/",base_directory,'/',wave,"/calendar.Rds"))%>%   #ps v9
                  mutate(fiscal_year = year(week_start_date))%>%
                  select(fiscal_year,week_start_date, Week_No)%>%distinct()


cat_sum <- readRDS(paste0(datasource))%>%
          dplyr::left_join(product_map%>%dplyr::select(c(item_indt_1_prod,item_indt_4)),
                           c(setNames(item_indt_1_prod ,"itemid")))%>%
          left_join(calendar_dat,c("weekdate" = "week_start_date"))%>%
          filter(get(item_indt_4) == category_raw)%>%
#           mutate(cluster = parse_number(i))%>%
          group_by(Week_No,fiscal_year)%>%
          summarise(sales_qty = sum(sales_qty))%>%ungroup()#%>%
#           spread(fiscal_year,sales_qty)

# TC:
#writing out the catsum file
write_csv(cat_sum,paste0(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/","cat_sum_",user,"_",category,".csv")))

cat_sum[is.na(cat_sum)] <- 0
display(cat_sum)

# COMMAND ----------

# DBTITLE 1,Elasticity automation: Stage 1 #category level dummy creation
# MAGIC %python
# MAGIC #TC
# MAGIC #sf
# MAGIC dbutils.notebook.run(databricks_notebooks_path+"01a_create_category_dummies_v3",1000,\
# MAGIC                      {"user": user, "category_name":category, "business_unit": business_units, "wave": wave, "bu_code": bu_code })

# COMMAND ----------

#TC: New Code
#update path to pick the result automatically

cat_seasonal_dummy_num <- c()  
cat_indiv_week_dummy_num <- c()      
cat_indiv_month_dummy_num <- c()

cat_dummies_test <- as.data.frame(read_csv(paste0('/dbfs/',base_directory,'/',wave,'/',bu,'/Output/intermediate_files/',"cat_dummies_",user,"_",category,".csv")))
cat_seasonal_dummy_num<- cat_dummies_test$dummies

cat_seasonal_dummy_num

# COMMAND ----------

# MAGIC %md
# MAGIC ##Optional: Review Category leve dummy selection
# MAGIC #####if you want to add/remove dummies then uncomment the below code and make selection

# COMMAND ----------

##Manually selecting category level dummies
# To take care of it 
# cat_seasonal_dummy_num <- c(5,7,10,15,30,32,40,44,47,49)  #there needs to be atleast one weekly seasonal dummy for code to function
# cat_indiv_week_dummy_num <- c('48_2019')      #48_2019 # c('') #for selecting no dummies
# cat_indiv_month_dummy_num <- c('2_2019')

cat_seasonal_dummy <- paste0("WEEK_DUMMY_",cat_seasonal_dummy_num)
cat_indiv_week_dummy <- paste0("INDIV_DUMMY_",cat_indiv_week_dummy_num)
cat_indiv_month_dummy <- paste0("INDIV_MONTH_DUMMY_",cat_indiv_month_dummy_num)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Main Code

# COMMAND ----------

# MAGIC %r
# MAGIC #Data Read and Other Prep
# MAGIC 
# MAGIC log_loc_lvl1 = paste0('/dbfs/',base_directory,'/',wave,'/',bu,"/Log/lvl1_log/lvl1_log_",user,"_v",version,".txt")
# MAGIC log_loc_lvl2 = paste0('/dbfs/',base_directory,'/',wave,'/',bu,"/Log/lvl2_log/lvl2_log_",user,"_v",version,".txt")
# MAGIC nodal_log = paste0('/dbfs/',base_directory,'/',wave,'/',bu,"/Log/nodal_log/",user,"/v",version,"/")
# MAGIC 
# MAGIC #File writing Locations
# MAGIC 
# MAGIC if(erase_old_output == T ){
# MAGIC   print("Creating Folders for writing files...")
# MAGIC   generate_user_version_folders(user,version,bu,base_directory)
# MAGIC   
# MAGIC   #Clearing Old Logs
# MAGIC   
# MAGIC   # print("Clearing Old Logs...")
# MAGIC   close(file(paste0(paste0(log_loc_lvl1)), open="w" ) )
# MAGIC   close(file(paste0(paste0(log_loc_lvl2)), open="w" ) )
# MAGIC   invisible(do.call(file.remove, list(list.files(nodal_log, full.names = TRUE))))
# MAGIC }
# MAGIC 
# MAGIC 
# MAGIC pval = 1  #unused parameter but kept for sake of code sanity
# MAGIC non_baseline_vars = c(
# MAGIC   "DISCOUNT_PERC"
# MAGIC )
# MAGIC 
# MAGIC print("Inscope SKU selected from input....")
# MAGIC #Defining Scope from excel
# MAGIC scope <-  scope_category
# MAGIC 
# MAGIC print("Expected Signs registered from modelling input...")
# MAGIC #Recording Sign for variables
# MAGIC sign_file <- read_excel(mod_input_master_file, sheet = "Signs")
# MAGIC 
# MAGIC print("Independant Variables registered from modelling input...")
# MAGIC baseline_variables_data <- read_excel(mod_input_master_file,sheet = "Variables") 
# MAGIC mixed_variables_data <- read_excel(mod_input_master_file,sheet = "Mixed_Input")
# MAGIC if(run_type == "initial" ){
# MAGIC   customize_data <- read_excel(mod_input_master_file,sheet = "Customize_Data")
# MAGIC   custom_run_variables <- read_excel(mod_input_master_file, sheet = "Variables_custom")
# MAGIC }
# MAGIC 
# MAGIC if(run_type == "custom_run" ){
# MAGIC #   customize_data <- read_csv(customize_data_file)
# MAGIC #   custom_run_variables <- read_csv(custom_run_variables_file)
# MAGIC   customize_data <- as.data.frame(SparkR::collect(SparkR::sql(paste0('select * from localized_pricing.',db_tbl_prefix,'input_model_rerun_customize_data_',user,'_',category))))
# MAGIC   custom_run_variables <- as.data.frame(SparkR::collect(SparkR::sql(paste0('select * from localized_pricing.',db_tbl_prefix,'input_model_rerun_custom_run_variables_',user,'_',category))))
# MAGIC   
# MAGIC   # Update scope file
# MAGIC   updated_scope <- unique(custom_run_variables$itemid)
# MAGIC   scope <- scope%>%filter(ITEMID %in% updated_scope)
# MAGIC   }
# MAGIC     
# MAGIC print("Initiating Modelling...")

# COMMAND ----------

scope

# COMMAND ----------

#main Run
parallel_lvl1_run(
                  user = user,
                  version = version,
                  datasource = datasource,
                  closed_stores = c(),
                  sign_file = sign_file,
                  baseline_variables_data = baseline_variables_data,
                  mixed_variables_data = mixed_variables_data,
                  custom_run_variables = custom_run_variables,
                  custom_data_func = custom_data_func,
                  non_baseline_vars = non_baseline_vars,
                  support_functions_baseline = paste0("./Functions/ucm_functions.R_final"),
                  support_functions_mixed = paste0("./Functions/mixed_functions.R"),
                  scope = scope,
                  customize_data=customize_data,
                  pval = pval,
                  levelvar = levelvar,
                  lvl2_function = parallel_lvl2_run,
                  lvl_1_cores = 10,
                  lvl_2_cores = 10,
                  log_loc_lvl1 = log_loc_lvl1,
                  log_loc_lvl2 = log_loc_lvl2,
                  remove_weeks = remove_weeks,
                  run_type = run_type,
                  bu=bu,
                  cat_seasonal_dummy = cat_seasonal_dummy,
                  cat_indiv_week_dummy = cat_indiv_week_dummy,
                  cat_indiv_month_dummy = cat_indiv_month_dummy,
                  wave = wave,
                  base_directory = base_directory
  
)

#TC: node error

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Only Run if Model has Hit Error in cmd 34 (main model run)

# COMMAND ----------

# expecting_core_conflict <- "Yes" #  No if you are expecting core conflict

# COMMAND ----------


# if(length(list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/"))) > 0 & expecting_core_conflict == "Yes"){
#     initial_count <- length(list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")))
  
#     scope <-    scope_category # v6 read_excel(paste0("/dbfs/Phase3_extensions/Elasticity_Modelling/",wave,"/",bu,"/Input/modelling_input_",user,"_",category,".xlsx"), sheet = "scope_file")
#     scope_skus <- scope$ITEMID[scope$STATUS == "Y"]

#     #get all item x cluster combinations that exist in data
#     files <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Modelling_Inputs/",ADS_loc))%>%
#     filter(itemid %in% scope_skus)%>%
#     select(itemid,Cluster)%>%
#     rename(CLUSTER = Cluster)%>%
#     mutate(key = paste0(itemid,"_",CLUSTER))%>%
#     unique()
#   # sf v6 updated path location to NA version of product map
#     product_map <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/product_map_na.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))


#     if(run_type == "custom_run"){
#       files<- custom_run_variables %>%
#             select(itemid,cluster) %>%
#             rename(CLUSTER=cluster)%>%
#       mutate(key = paste0(itemid,"_",CLUSTER))%>%
#       unique()
#     }else{
#       files <- files
#     }
  
  
#    last_run_count <- initial_count
#    updated_count <- initial_count + 1
#    persistence <- 0
#    while((updated_count > last_run_count) | (persistence < 4)){
#       last_run_count <- length(list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")))
     
#       all_files <- list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/"))
#       #get all unique item ids present in the intermediate
#       tot_items <- sapply(all_files, function(x) {
#         x <- gsub(".Rds","",x)
#         strsplit(x,"\\_")})
#       tot_items <- as.numeric(unique(sapply(tot_items, function(x) tail(x,n=1))))

#       files_2 <- c("prediction","baseline_estimates","baselines")

#       for(each_file in files_2){
#         items <- list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/"),pattern=each_file)
#         final <- bind_rows(lapply(items, function(x){
#           readRDS(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/",x))%>%
#           mutate(ITEMID = as.numeric(ITEMID))
#         }))
#         write.csv(final,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/",each_file,"_collated.csv"),row.names=F)
#         if( each_file == "baselines"){
#           final2 <- final%>%
#                 dplyr::left_join(product_map%>%dplyr::select(c(item_indt_1_prod,item_indt_2,item_indt_3,hierarchy_cols,item_indt_4)),
#                                  c(setNames(item_indt_1_prod ,"ITEMID")))%>%  #lhs key should be to the right
#                   dplyr::mutate(REG_PRICE_OLD = REG_PRICE) %>%                               
#                 dplyr::select(c(ITEMID,item_indt_2,item_indt_3, item_indt_4, STORE, WEEKDATE, SALES_QTY, 
#                               REVENUE, REG_PRICE_OLD ,REG_PRICE, NET_PRICE, LN_SALES_QTY, LN_REG_PRICE,
#                                DISCOUNT, DISCOUNT_PERC,
#                                S_LEVEL, UCM_BASELINE, BASELINE_SALES, PREDICTED_SALES,PREDICTED_FINAL_REV, BASELINE_REV,MAPE_UCM, UCM_PREDICTION_FLAG,ROUGH_COEF,UCM_SMOOTH_FLAG, 
#                                 REGULAR_PRICE_FLAG,SALES_FLAG,IMPUTE_FLAG, 
#                                 IMPUTE_COMMENT,ISSUE_COMMENT ))%>%
#                 dplyr::mutate(UCM_PREDICTION_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",UCM_PREDICTION_FLAG),
#                              UCM_SMOOTH_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",UCM_SMOOTH_FLAG),
#                              REGULAR_PRICE_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",REGULAR_PRICE_FLAG),
#                              SALES_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",SALES_FLAG),
#                              ISSUE_COMMENT = ifelse(IMPUTE_FLAG == 'Impute',"None",ISSUE_COMMENT))%>%
#                 dplyr::rename(!!item_indt_1 := ITEMID,
#                               !!item_indt_2_temp := !!item_indt_2,
#                        ITEM_DESCRIPTION = !!item_indt_3,
#                        CATEGORY = !!item_indt_4,
#                        CLUSTER = STORE,
#                        WEEK_START_DATE = WEEKDATE,
#                        PREDICTED_SALES_BEF = PREDICTED_SALES,
#                        PREDICTED_SALES = PREDICTED_FINAL_REV)

#             write_csv(final2,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/",each_file,"_collated_template.csv"))
#         }
#       }
    

#     #now check which of these have been modelled from the bp_estimates_collated_template.csv output
#     run_models <- read.csv(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","baselines","_collated_template.csv"),)%>%
#        select(c(item_indt_1, CLUSTER))%>%unique()%>%
#        mutate(key = paste0(get(item_indt_1),"_",CLUSTER))%>%
#        select (-CLUSTER )

#     # now left join this with the skeleton to find which have not been modelled:
#     issues <- files%>%
#              left_join(run_models,c("key"))%>%
#              filter(is.na(get(item_indt_1)))

#     if(nrow(issues) == 0 ){
#       print("All Models Successfully Run")
#     }else{
#       rerun_items <- unique(issues$itemid)
#     }
    
#     scope <- scope%>% filter(ITEMID %in% rerun_items)
#     tryCatch({
#       parallel_lvl1_run(
#                       user = user,
#                       version = version,
#                       datasource = datasource,
#                       closed_stores = c(),
#                       sign_file = sign_file,
#                       baseline_variables_data = baseline_variables_data,
#                       mixed_variables_data = mixed_variables_data,
#                       custom_run_variables = custom_run_variables,
#                       custom_data_func = custom_data_func,
#                       non_baseline_vars = non_baseline_vars,
#                       support_functions_baseline = paste0("/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/Functions/ucm_functions.R_final"),
#                       support_functions_mixed = paste0("/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/Functions/mixed_functions.R"),
#                       scope = scope,
#                       customize_data=customize_data,
#                       pval = pval,
#                       levelvar = levelvar,
#                       lvl2_function = parallel_lvl2_run,
#                       lvl_1_cores = 10,
#                       lvl_2_cores = 5,
#                       log_loc_lvl1 = log_loc_lvl1,
#                       log_loc_lvl2 = log_loc_lvl2,
#                       run_type = run_type,
#                       remove_weeks = remove_weeks,
#                       bu_repo=bu_repo,
#                       cat_seasonal_dummy = cat_seasonal_dummy,
#                       cat_indiv_week_dummy = cat_indiv_week_dummy,
#                       cat_indiv_month_dummy = cat_indiv_month_dummy,
#                       wave = wave,
#                       base_directory = base_directory


#     )},
#       error = function(e){
#         print("some error in model happenned - retrying")
#         })
#  updated_count <- length(list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")))
                                           
#  if(updated_count == last_run_count){
#    persistence = persistence + 1
#  }else{
#    persistence = 999
#  }
                                            
# }
# }

# if(updated_count == initial_count){
#   print("No Improvement in # of models due to core conflict resolution - some other issue in model")
# }else{
#   print("Improvement in # of models due to core conflict resolution")
# # sf v6 removed print statement that said "run cmd 29 and 32 to check if all models have run!""
# } 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Combine Generated Output

# COMMAND ----------

# MAGIC %r
# MAGIC ########################################################################################
# MAGIC # sf v6 updated path link to NA version of product map
# MAGIC if (region=='NA'){
# MAGIC   product_map <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/product_map_na.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))
# MAGIC }
# MAGIC if(region=='EU'){
# MAGIC   product_map <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/product_map_eu.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))
# MAGIC }
# MAGIC 
# MAGIC # product_map <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/product_map_na.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod))) #ps v9
# MAGIC all_files <- list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/"))
# MAGIC 
# MAGIC 
# MAGIC #get all unique item ids present in the intermediate
# MAGIC tot_items <- sapply(all_files, function(x) {
# MAGIC   x <- gsub(".Rds","",x)
# MAGIC   strsplit(x,"\\_")})
# MAGIC tot_items <- as.numeric(unique(sapply(tot_items, function(x) tail(x,n=1))))
# MAGIC 
# MAGIC files <- c("prediction","baseline_estimates","baselines")
# MAGIC 
# MAGIC for(each_file in files){
# MAGIC   items <- list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/"),pattern=each_file)
# MAGIC   final <- bind_rows(lapply(items, function(x){
# MAGIC     readRDS(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/",x))%>%
# MAGIC     mutate(ITEMID = as.numeric(ITEMID))
# MAGIC   }))
# MAGIC   write.csv(final,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/",each_file,"_collated.csv"),row.names=F)
# MAGIC   if( each_file == "baselines"){
# MAGIC     final2 <- final%>%
# MAGIC           dplyr::left_join(product_map%>%dplyr::select(c(item_indt_1_prod,item_indt_2,item_indt_3,hierarchy_cols,item_indt_4)),
# MAGIC                            c(setNames(item_indt_1_prod ,"ITEMID")))%>%  #lhs key should be to the right
# MAGIC             dplyr::mutate(REG_PRICE_OLD = REG_PRICE) %>%                               
# MAGIC           dplyr::select(c(ITEMID,item_indt_2,item_indt_3, item_indt_4, STORE, WEEKDATE, SALES_QTY, 
# MAGIC                         REVENUE, REG_PRICE_OLD ,REG_PRICE, NET_PRICE, LN_SALES_QTY, LN_REG_PRICE,
# MAGIC                          DISCOUNT, DISCOUNT_PERC,
# MAGIC                          S_LEVEL, UCM_BASELINE, BASELINE_SALES, PREDICTED_SALES,PREDICTED_FINAL_REV, BASELINE_REV,MAPE_UCM, UCM_PREDICTION_FLAG,ROUGH_COEF,UCM_SMOOTH_FLAG, 
# MAGIC                           REGULAR_PRICE_FLAG,SALES_FLAG,IMPUTE_FLAG, 
# MAGIC                           IMPUTE_COMMENT,ISSUE_COMMENT ))%>%
# MAGIC           dplyr::mutate(UCM_PREDICTION_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",UCM_PREDICTION_FLAG),
# MAGIC                        UCM_SMOOTH_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",UCM_SMOOTH_FLAG),
# MAGIC                        REGULAR_PRICE_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",REGULAR_PRICE_FLAG),
# MAGIC                        SALES_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",SALES_FLAG),
# MAGIC                        ISSUE_COMMENT = ifelse(IMPUTE_FLAG == 'Impute',"None",ISSUE_COMMENT))%>%
# MAGIC           dplyr::rename(!!item_indt_1 := ITEMID,
# MAGIC                         !!item_indt_2_temp := !!item_indt_2,
# MAGIC                  ITEM_DESCRIPTION = !!item_indt_3,
# MAGIC                  CATEGORY = !!item_indt_4,
# MAGIC                  CLUSTER = STORE,
# MAGIC                  WEEK_START_DATE = WEEKDATE,
# MAGIC                  PREDICTED_SALES_BEF = PREDICTED_SALES,
# MAGIC                  PREDICTED_SALES = PREDICTED_FINAL_REV)
# MAGIC    
# MAGIC       write_csv(final2,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/",each_file,"_collated_template.csv"))
# MAGIC   }
# MAGIC }
# MAGIC # get the data output in a format that is general for templating
# MAGIC        

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Debugging and General Sanity Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Check which models have not run

# COMMAND ----------

# How many models have not run and list of those items
scope <-     scope_category
scope_skus <- scope$ITEMID[scope$STATUS == "Y"]

#get all item x cluster combinations that exist in data
files <- readRDS(paste0(mod_inputs_path,ADS_loc))%>%
filter(itemid %in% scope_skus)%>%
select(itemid,Cluster)%>%
rename(CLUSTER = Cluster)%>%
mutate(key = paste0(itemid,"_",CLUSTER))%>%
unique()

if(run_type == "custom_run"){
  files<- custom_run_variables %>%
        select(itemid,cluster) %>%
        rename(CLUSTER=cluster)%>%
  mutate(key = paste0(itemid,"_",CLUSTER))%>%
  unique()
}else{
  files <- files
}

#now check which of these have been modelled from the bp_estimates_collated_template.csv output
run_models <- read.csv(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","baselines","_collated_template.csv"),)%>%
   select(c(item_indt_1, CLUSTER))%>%unique()%>%
   mutate(key = paste0(get(item_indt_1),"_",CLUSTER))%>%
   select (-CLUSTER )

# now left join this with the skeleton to find which have not been modelled:
issues <- files%>%
         left_join(run_models,c("key"))%>%
         filter(is.na(get(item_indt_1)))

if(nrow(issues) == 0 ){
  print("All Models Successfully Run")
}else{
  print("some models have not run")
  print(paste0("The product key x clusters have not run \n",unique(issues$key)))
  rerun_items <- unique(issues$itemid)
  print(paste0("Try rerunning these items from the following rerun section \n",unique(rerun_items)))
}

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Read Error Messages

# COMMAND ----------

#main error
library(knitr)
output_error <- paste0('/dbfs/',base_directory,'/',wave,"/",bu,"/Log/model_failure_report/",user,"/v",version,"/output.txt")
error_file_time <- file.info(paste0('/dbfs/',base_directory,'/',wave,"/",bu,"/Log/model_failure_report/",user,"/v",version,"/output.txt"))$ctime

cat(paste0("Please check error time to validate if this file was created during your current run :","\n",error_file_time,"\n",file.show(output_error)))
cat(paste0("Current Time:","\n", Sys.time()))

# COMMAND ----------

#level 1 log
# lvl1_log <- paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/lvl1_log/lvl1_log_",user,"_v",version,".txt")
# file.show(lvl1_log)

# COMMAND ----------

#level 2 log
# lvl2_log <- paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/lvl2_log/lvl2_log_",user,"_v",version,".txt")
# file.show(lvl2_log)

# COMMAND ----------

#nodal log - to check for a particular items all cluster model runs' console output to check for errors
#first list all files with that itemid
# lvl2_nodal_files <- grep("635352", list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version)),value = T)
# lvl2_nodal_files


# COMMAND ----------

# #read the files
# file.show(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version,"/",lvl2_nodal_files[5]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run STAGE 2 - Constrained Elasticity Estimation

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### User Inputs

# COMMAND ----------

slope_type <- "extrapolated" # normal if you want continuous trend
# sf v8 updated for a new calendar year up refreshes, since the previous date would likely be at the start of the time series, rather than the middle
trend_inflex_date <- "2020-12-01"
#trend_inflex_date <- "2019-01-01" #if most significant price change is after this date then trend before most significant price change is extrapolated only 
#                                     #if slope_type <- "extrapolated" is selected
erase_old_output = T

# COMMAND ----------

# MAGIC %md
# MAGIC #### source STAGE 2 code

# COMMAND ----------

# MAGIC %run "./Functions/STG_2_Elasticity_via_Constrained_Regression_New"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Final Output

# COMMAND ----------

#check which cases have no price var

change_elasticity <- final2%>%group_by(.dots = c(item_indt_1,"CATEGORY","CLUSTER"))%>%summarise(sd_reg = sd(REG_PRICE))%>%ungroup()%>%filter(sd_reg == 0)%>%mutate(CLUSTER = as.character(CLUSTER))
final3 <- final2%>%dplyr::mutate(CLUSTER = as.character(CLUSTER))%>%
          dplyr::left_join(out_t%>%mutate(CLUSTER = as.character(CLUSTER))%>%select(item_indt_1,CLUSTER,BP_ELASTICITY,ESTIMATES_MAPE,ESTIMATES_MAPE_FLAG))%>%
          dplyr::mutate(IMPUTE_FLAG = ifelse(ESTIMATES_MAPE == -999, "Impute",IMPUTE_FLAG))%>%
          dplyr::mutate(IMPUTE_COMMENT = ifelse(ESTIMATES_MAPE == -999, paste0(IMPUTE_COMMENT," Estimates not reliable cause very low baseline (LN(1) = 0)"),IMPUTE_COMMENT))%>%
           dplyr::mutate(ESTIMATES_MAPE_FLAG = ifelse(IMPUTE_FLAG == 'Impute',"",ESTIMATES_MAPE_FLAG))%>%
          dplyr::mutate(OVERALL_ISSUE = ifelse((UCM_PREDICTION_FLAG == "Issue") | (UCM_SMOOTH_FLAG == "Issue") | (REGULAR_PRICE_FLAG == "Issue") | 
                                        (ESTIMATES_MAPE_FLAG == "Issue") | (SALES_FLAG == "Issue"),"Issue","Good"
                                       ))%>%
         dplyr::left_join(change_elasticity)%>%
         dplyr::mutate(BP_ELASTICITY = ifelse(is.na(sd_reg),BP_ELASTICITY, 0),
                      ESTIMATES_MAPE = ifelse(is.na(sd_reg),ESTIMATES_MAPE, ""),
                      ESTIMATES_MAPE_FLAG = ifelse(is.na(sd_reg),ESTIMATES_MAPE_FLAG, ""))%>%
         dplyr::select(-sd_reg)


out_t <- out_t %>%
         mutate(CLUSTER = as.character(CLUSTER))%>%
         dplyr::left_join(change_elasticity)%>%
         dplyr::mutate(BP_ELASTICITY = ifelse(is.na(sd_reg),BP_ELASTICITY, 0),
                      ESTIMATES_MAPE = ifelse(is.na(sd_reg),ESTIMATES_MAPE, ""),
                      ESTIMATES_MAPE_FLAG = ifelse(is.na(sd_reg),ESTIMATES_MAPE_FLAG, ""))%>%
         dplyr::select(-sd_reg)

discount_estimates <- read_csv(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","baseline_estimates_collated.csv"))%>%
                      filter(VARIABLE == "DISCOUNT_PERC")%>%rename(CLUSTER = STORE)

version_summary <- data.frame(TYPE = c("VERSION","LEVELVAR"),
                              VALUE = c(version, levelvar))
if(length(cat_seasonal_dummy) >0){
      Cat_seas_dummy_summary <- data.frame(TYPE = rep("CATEGORY_SEASONAL_DUMMY",length(cat_seasonal_dummy)),
                                     VALUE = paste0("WEEK_DUMMY_",cat_seasonal_dummy))
 }else{
     Cat_seas_dummy_summary <- data.frame()
}

if(length(cat_indiv_week_dummy) >0){
    Cat_corr_dummy  <- data.frame(TYPE = rep("CATEGORY_CORRECTION_DUMMY",length(cat_indiv_week_dummy)),
                                     VALUE = paste0("INDIV_DUMMY_",cat_indiv_week_dummy))
}else{
      Cat_corr_dummy <- data.frame()
}


if(length(cat_indiv_month_dummy) >0){
    Cat_corr_mnth_dummy  <- data.frame(TYPE = rep("CATEGORY_CORRECTION_DUMMY",length(cat_indiv_month_dummy)),
                                     VALUE = paste0("INDIV_DUMMY_",cat_indiv_month_dummy))
}else{
      Cat_corr_mnth_dummy <- data.frame()
}
version_all <- bind_rows(version_summary,Cat_seas_dummy_summary,Cat_corr_dummy,Cat_corr_mnth_dummy)

#need review Skeleton
rev_skel = final3%>%dplyr::select(
                    item_indt_1,
                    item_indt_2_temp,
                    ITEM_DESCRIPTION,
                    CATEGORY,
                    CLUSTER,
                    UCM_PREDICTION_FLAG,
                    UCM_SMOOTH_FLAG,
                    REGULAR_PRICE_FLAG,
                    SALES_FLAG,
                    IMPUTE_FLAG,
                    ESTIMATES_MAPE_FLAG,
                    OVERALL_ISSUE,
                    ISSUE_COMMENT,
                    IMPUTE_COMMENT,
                    MAPE_UCM,
                    ROUGH_COEF,
                    BP_ELASTICITY,
                    ESTIMATES_MAPE)%>%unique()%>%dplyr::arrange(IMPUTE_FLAG,OVERALL_ISSUE,!!as.symbol(item_indt_1),CLUSTER)

all_combined <- list("Review Skeleton" = rev_skel,"Estimates" = out_t, "Baseline Data Output" = final3, "Run Summary" = version_all, "Discount Estimate"= discount_estimates)

# delete any pre-existing files if there 
invisible(file.remove(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","All_Combined_Result_",category,".xlsx")),full.names = T))
  
write.xlsx(all_combined,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","All_Combined_Result_",category,".xlsx"))


# COMMAND ----------

# # the user should download this 'All_Combined_Result' file, paste parts of it into a Modeling Review Template, and decide whether any manual price fixes or truncation are needed
paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","All_Combined_Result_",category,".xlsx")

# COMMAND ----------

#PGV11
check <- final3%>%group_by(.dots = c(item_indt_1,"CATEGORY","CLUSTER","REG_PRICE"))%>%summarise(n = n())%>%ungroup()%>%filter(n < 3)
# stopifnot((nrow(check)==0)) # vNY202201
if(!(nrow(check)==0)){
  message("Please use the Modeling Review Template to check.") # vNY202201
  display(check)}

# COMMAND ----------

# # manual price fixes and truncation can be entered if this file is downloaded, updated, saved, and uploaded back to DBFS
# # the user should then select another version for the category and run the driver notebook again
# paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Input/modelling_input_file.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ### QCs

# COMMAND ----------

#read elasticity_file - check if all item x clusters have been generated elasticity for
qc_out <- read_csv(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","elast_cons_reg_out_template.csv"))
display(qc_out)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check chart for any item x Cluster

# COMMAND ----------

# item_check <- 540606
# cluster_check <- '1'

# COMMAND ----------

# display(readRDS(datasource)%>%filter(itemid == item_check & Cluster == cluster_check))

# COMMAND ----------

# %python
# user = 'abhisek'
# category = 'Candy'
# category_raw = 'Candy'
# wave = "Nov21_Refresh"
# bu_code = 'DK'
# business_units = 'Denmark'
# zoned = 'no'
# databricks_notebooks_path = './'

# COMMAND ----------

# DBTITLE 1,Elasticity automation: Stage 2 #collation output creation
# MAGIC %python
# MAGIC #sf v6 update to v6 for 01b notebook
# MAGIC dbutils.notebook.run(databricks_notebooks_path+"01b_create_collation_output_v6",1000,\
# MAGIC                      {"user": user, "category_name": category,"business_unit": business_units,"wave": wave,"bu_code": bu_code, "category_raw": category_raw, "zoned": zoned})

# COMMAND ----------

# DBTITLE 1,Elasticity automation: Stage 3 #create reviewed elasticities
# MAGIC %python
# MAGIC #sf v6 update to v5 for 01c notebook
# MAGIC #PG v9 after runnning the notebook, please open the notebook to see output of cmd 64 , should be empty, if not please raise the same
# MAGIC dbutils.notebook.run(databricks_notebooks_path+"01c_create_reviewed_elasticities_v5",1000,\
# MAGIC                      {"user": user, "category_name": category, "business_unit": business_units, "wave": wave, "bu_code": bu_code})

# COMMAND ----------

# DBTITLE 1,03 collate categories
# MAGIC %python
# MAGIC #sf v6 update to v5 for 01c notebook
# MAGIC #PG v9 after runnning the notebook, please open the notebook to see output of cmd 64 , should be empty, if not please raise the same
# MAGIC dbutils.notebook.run(databricks_notebooks_path+"03_Collate_Category_Versions_v2",1000,\
# MAGIC                      {"user": user,"category_name": category,"business_unit": business_units,"wave": wave, "bu_code": bu_code}) #ps v9
