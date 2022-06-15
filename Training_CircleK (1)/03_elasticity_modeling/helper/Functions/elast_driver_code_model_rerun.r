# Databricks notebook source
# MAGIC %md
# MAGIC ##### Installation of Packages

# COMMAND ----------

library(tidyverse)
library(readxl)
library(parallel)
# library(openxlsx)
library(lubridate)
library(trustOptim)
options(scipen = 999)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Widgets

# COMMAND ----------

# DBTITLE 1,Variables: Python
# MAGIC %python
# MAGIC # Define parameters used in notebook
# MAGIC # business_units = '4200 - Great Lakes Division'
# MAGIC # bu_code = 'GL'
# MAGIC business_units = dbutils.widgets.get("business_unit")
# MAGIC bu_code = dbutils.widgets.get("bu_code")
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
# MAGIC # wave = 'Accenture_Refresh'
# MAGIC wave = dbutils.widgets.get("wave")
# MAGIC 
# MAGIC # Define the user
# MAGIC # user = 'sanjo'
# MAGIC user = dbutils.widgets.get("user")
# MAGIC 
# MAGIC # Define the category
# MAGIC # category_raw = "002-CIGARETTES" #'008-CANDY'
# MAGIC # category = "002_CIGARETTES" #'008_CANDY'
# MAGIC category_raw = dbutils.widgets.get("category_raw")
# MAGIC category = dbutils.widgets.get("category_name")
# MAGIC 
# MAGIC 
# MAGIC # Update for each run - Use version 9 for any Model Reruns - custom run
# MAGIC version_num = '9' # Increment from 1 to 8
# MAGIC levelvar = 0.15  # Choose from [0.15,0.12,0.1,0.08,0.05,0.03,0.01,0.008]
# MAGIC # Update for R variables as well
# MAGIC 
# MAGIC # version_num = dbutils.widgets.get("versions")
# MAGIC # levelvar = float(dbutils.widgets.get("levelvar"))
# MAGIC 
# MAGIC # zoned = 'no' # Choose whether 'yes' or 'no'
# MAGIC zoned = dbutils.widgets.get("zoned")
# MAGIC 
# MAGIC # Define the database for tables and directory paths for input & output files
# MAGIC db = 'phase3_extensions'
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
# MAGIC databricks_notebooks_path = "../" # NY v202110
# MAGIC 
# MAGIC # Define input files for modelling
# MAGIC mod_input_master_file = '/dbfs/'+base_directory+'/'+wave+'/'+bu+'/Input/modelling_input_file.xlsx'

# COMMAND ----------

# DBTITLE 1,Variables: R
# Define parameters used in notebook
# business_units <- '4200 - Great Lakes Division'
# bu_code <- 'GL'
business_units <- dbutils.widgets.get("business_unit")
bu_code <- dbutils.widgets.get("bu_code")

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
# wave <- 'Accenture_Refresh'
wave <- dbutils.widgets.get("wave")

# Define the user who is running the driver
# user <- 'sanjo'
user <- dbutils.widgets.get("user")

# Define the category
# category_raw <- "002-CIGARETTES" #'008-CANDY'
# category <- "002_CIGARETTES" #'008_CANDY'
category_raw <- dbutils.widgets.get("category_raw")
category <- dbutils.widgets.get("category_name")

# Update for each run
version_num <- '9' # Increment from 1 to 8 - Use version 9 for any Model Reruns - custom run
levelvar <- 0.15 # Choose from [0.15,0.12,0.1,0.08,0.05,0.03,0.01,0.008]

# version_num <- dbutils.widgets.get("versions")
# levelvar <- as.numeric(dbutils.widgets.get("levelvar"))

# zoned <- 'no' # Choose whether 'yes' or 'no'
zoned <- dbutils.widgets.get("zoned")

# Define run_type
run_type <- "custom_run"  #Use #custom_run #initial basis if we are doing a model re-run or initial run
# run_type <- dbutils.widgets.get("run_type") 

# Define the database for tables and directory paths for input & output files
db <- 'phase3_extensions'
base_directory <- 'Phase4_extensions/Elasticity_Modelling'
pre_wave <- paste0('/dbfs/',base_directory,'/')

# DB table prefix
db_tbl_prefix <- paste0(tolower(bu_code),'_',tolower(wave),'_')

# Define computed variables
bu <- paste0(bu_code,'_Repo')
version <- paste0(tolower(bu_code),'_',category,'_v',version_num)
mod_inputs_path <- paste0('/dbfs/',base_directory,'/',wave,'/',bu,'/Modelling_Inputs/')

# # Define notebook path
# databricks_notebooks_path <- paste0("/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/") # NY v202110
databricks_notebooks_path <- paste0("../") # NY v202110

# Define input files for modelling
mod_input_master_file <- paste0('/dbfs/',base_directory,'/',wave,'/',bu,'/Input/modelling_input_file.xlsx')

# COMMAND ----------

##################################################### USER INPUTS ########################################################################################

# run_type <- "initial"                  #custom_run #initial if we have changes after inital model qc - we call modified; if you want to run custom runs for
# # run_type <- dbutils.widgets.get("run_type") 

# # Auto Change variable run_type if model called for Re-run
# tryCatch(
#     expr = {
#         custom_run_variables <- read_csv(custom_run_variables_file)
#     },
#     error = function(e){ 
#         run_type <- "initial" 
#     },
#     finally = {
#         run_type <- "custom_run" 
#         version <- '9'
#     }
# )

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

# MAGIC %md
# MAGIC # MODELLING

# COMMAND ----------

# MAGIC %md
# MAGIC ##### source codes

# COMMAND ----------

# MAGIC %run "./ucm_functions.R_final"

# COMMAND ----------

# MAGIC %run "./do_parallel.R_final"

# COMMAND ----------

# MAGIC %run "./core_functions.R_final"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User Defined Functions

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
                  support_functions_baseline = paste0("./ucm_functions.R_final"),
                  support_functions_mixed = paste0("./mixed_functions.R"),
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

# MAGIC %run "./STG_2_Elasticity_via_Constrained_Regression_New"

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

