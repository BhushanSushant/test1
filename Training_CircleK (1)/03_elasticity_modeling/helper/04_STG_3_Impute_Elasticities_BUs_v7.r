# Databricks notebook source
# MAGIC %md
# MAGIC ##### Call Libs

# COMMAND ----------

# Input files required to run this notebook
#   1. Reviewed_elasticities_for_imputation.xlsx
#   2. Impute_Flag.xlsx
#   3. Baseline_Collated.xlsx
#   4. store_to_cluster_mapping.csv
#   5. BU_item_store_zones.csv
#   6. Final_Inscope_All_BU_updated.xlsx
#   7. localized_pricing.Group1_NA_Cost_Price_data
#   8. localized_pricing.Active_Stores_30092020
#   9. ADS and Product map rds files

# COMMAND ----------

# install.packages('zip')
# install.packages('tidyverse')
# install.packages('openxlsx')

# COMMAND ----------

#PG v12
library(tidyverse)
library(lubridate)
library(readxl)
library(openxlsx)
library(stringr)
library(dplyr) #ps v5
options(scipen = 999)

# COMMAND ----------

# %python
# # import the necessary packages and prepare pyspark for use during this program
# #versions I used
# dbutils.library.installPyPI('xlrd','1.2.0')
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import numpy
# MAGIC import datetime
# MAGIC import pyspark.sql.functions as sf
# MAGIC from pyspark.sql.window import Window
# MAGIC from operator import add
# MAGIC from functools import reduce
# MAGIC from pyspark.sql.functions import when, sum, avg, col,concat,lit
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC from pyspark.sql.functions import greatest
# MAGIC from pyspark.sql.functions import least
# MAGIC from pyspark.sql.types import DoubleType
# MAGIC import xlrd
# MAGIC 
# MAGIC #enabling delta caching to improve performance
# MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
# MAGIC spark.conf.set("spark.app.name","localized_pricing")
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "false")
# MAGIC sqlContext.clearCache()
# MAGIC 
# MAGIC sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
# MAGIC sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
# MAGIC sqlContext.setConf("spark.sql.shuffle.partitions", "8")
# MAGIC spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "true")
# MAGIC spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

#Define parameters

#list of BUs
american_BUs <- c('fl','gc','rm','cc','se','gr','tx','gl','qe','hd','mw','sa','wc')
canadian_BUs <- c('qw','ce','wd')
european_BUs <- c('ie','sw','no','dk','pl')

#define the base directory here
base_directory <- 'Phase4_extensions/Elasticity_Modelling'

#specify the wave 
#wave <- 'Nov21_Refresh'
wave <- dbutils.widgets.get("wave")

# #specify the business unit
#business_units <- "4200 - Great Lakes Division" #"4200 - Great Lakes Division"
business_units <- dbutils.widgets.get("business_unit")

# #specify the bu code
#bu_code <- 'GL' #'GL'
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


last_52_week_start <- "2019-12-04"     #give date in YYYY-mm-dd format
last_52_week_end <-  "2021-11-30"      #give date in YYYY-mm-dd format
elasti_input_file <- "Reviewed_elasticities_for_imputation.csv"
impute_flag_file <- "Impute_Flag.xlsx"
baseline_collated_file <- 'Baseline_Collated.xlsx'
old_wave_elast_file <- "old_wave_comp_elasticity.csv" # for wave 2 and 3
final_ads_file_name <- 'modelling_ads_cluster_final.Rds' #final ads file name

if (region=='NA'){
  prod_id <- 'PRODUCT_KEY'
  prod_num <- 'UPC'
  item_indt_2 <- "upc"
  item_indt_3 <- "item_desc"
  item_indt_4 <- "category_desc"
  item_indt_5 <- "sub_category_desc"
  item_indt_1_prod <- "product_key"
  product_map <- 'product_map_na.Rds'
}
if (region=='EU'){
  prod_id <- 'TRN_ITEM_SYS_ID' 
  prod_num <- 'JDE_NUMBER' 
  item_indt_2 <- "item_number"
  item_indt_3 <- "item_name"
  item_indt_4 <- "item_category"
  item_indt_5 <- "item_subcategory"
  item_indt_1_prod <- "sys_id"
  product_map <- 'product_map_eu.Rds'
}

#optimization directory path
optimization_directory <- '/Phase4_extensions/Optimization/'

#specify the factors to be multiplied to old elasticities, when deviation > 0.5 or < -0.5. Accenture recommends the values to be 1.2 and 0.8 for them
old_elasticity_up_factor <- 1.5
old_elasticity_lo_factor <- 0.75

baseline_collated_origin_date = '1899-12-30'

#derived paramters:
bu_repo <- paste0(bu_code,"_Repo")
pre_wave <- paste0("/dbfs/",base_directory,"/")

BU <- tolower(bu_code)

if(region=='EU'){
  product_map_path <- paste0('/dbfs/Phase3_extensions/Elasticity_Modelling/',wave,"/",product_map)
  }
else if(region=='NA'){
  product_map_path <- paste0('/dbfs/Phase4_extensions/Elasticity_Modelling/',wave,"/",product_map)
  }

#Refresh DB
Refresh_db_directory <- '/Refresh_History_DB/Elasticity_DB/'

# COMMAND ----------

# MAGIC %python
# MAGIC #Define parameter
# MAGIC 
# MAGIC last_52_week_start = "2019-12-04"     #give date in YYYY-mm-dd format
# MAGIC last_52_week_end =  "2021-11-30"      #give date in YYYY-mm-dd format
# MAGIC 
# MAGIC # wave = 'Nov21_Refresh'
# MAGIC wave = dbutils.widgets.get("wave")
# MAGIC 
# MAGIC # bu_code = 'GR' #'GL'
# MAGIC # business_units = '3100 - Grand Canyon Division' #'4200 - Great Lakes Division'
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
# MAGIC #Product identification columns
# MAGIC if region=='NA':
# MAGIC   prod_id = 'PRODUCT_KEY'
# MAGIC   prod_num = 'UPC'
# MAGIC elif region=='EU':
# MAGIC   prod_id = 'TRN_ITEM_SYS_ID'
# MAGIC   prod_num = 'JDE_NUMBER'
# MAGIC 
# MAGIC 
# MAGIC # new_filename = 'gr_acc' #'fl_v2'
# MAGIC new_filename = bu_code.lower()+'_'+wave.lower()
# MAGIC # overwrite = 'n'
# MAGIC optimization_directory = '/Phase4_extensions/Optimization/'
# MAGIC 
# MAGIC #derived parameters
# MAGIC bu_repo = bu_code + "_Repo"
# MAGIC na_DB_txn = "localized_pricing"
# MAGIC bu_daily_txn_table = "{}.{}_Daily_txn_final_SWEDEN_Nomenclature".format(na_DB_txn,bu_code.lower())
# MAGIC 
# MAGIC base_directory = 'Phase4_extensions/Elasticity_Modelling'
# MAGIC pre_wave = "/dbfs/"+base_directory+"/"
# MAGIC 
# MAGIC BU = bu_code.lower()
# MAGIC 
# MAGIC #Refresh DB
# MAGIC Refresh_db_directory = '/Refresh_History_DB/Elasticity_DB/'
# MAGIC 
# MAGIC # DB table prefix
# MAGIC db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User Inputs

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# %python
# # business unit & wave/refresh selection
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division", "1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division", "2900 - South Atlantic Division", "3100 - Grand Canyon Division", "3800 - Northern Tier Division", "4100 - Midwest Division", "4200 - Great Lakes Division", "4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division", "Western Division"]

# BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"], ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"], ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'], ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# wave = ["JAN2021_TestRefresh","MAY2021_Refresh","JUNE2021_Refresh"] #ps v6
# wave = ["Nov21_Refresh"]

# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Nov21_Refresh", [str(x) for x in wave])
#ps v9

# COMMAND ----------

# %python
# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GR", bu_codes)

# COMMAND ----------

# last_52_week_start <- "2019-08-07"     #give date in YYYY-mm-dd format
# last_52_week_end <-  "2021-08-04"      #give date in YYYY-mm-dd format
# elasti_input_file <- "Reviewed_elasticities_for_imputation.csv"
# impute_flag_file <- "Impute_Flag.xlsx"
# baseline_collated_file <- 'Baseline_Collated.xlsx'
# old_wave_elast_file <- "old_wave_comp_elasticity.csv" # for wave 2 and 3

# #wave <- "JAN2021_TestRefresh"
# #BU <- "fl"

# Business_Units = c("1400 - Florida Division","1600 - Coastal Carolina Division", "1700 - Southeast Division","1800 - Rocky Mountain Division","1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division",  "2900 - South Atlantic Division", "3100 - Grand Canyon Division",  "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division",  "Western Division")
# BU_abbr =tolower(c("FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WC"))

# bu_df =data.frame(Business_Units,BU_abbr)
# bu_abb = bu_df %>% filter(Business_Units %in% dbutils.widgets.get("business_unit")) %>% select(BU_abbr)
# BU = bu_abb[1,1]
# #BU <- paste(toupper(bu_abb), "_Repo", sep="")  # GC_repo #SE_repo #TC: automate based on short forms

# wave <- dbutils.widgets.get("wave")
# # zone_cats <- c('004-BEER','002-CIGARETTES','CIGARETTES-C','Discount/vfm 20s (6002)','Subgener/budget 20s (6003)','Premium Cigarettes 20s (6001)')
# # PG canadian zones_cats
# # zone_cats <- c('Premium Beer (7001)','BIERE-C','Specialty Beer (7006)','CIGARETTES-C','Microbrew Beer (7005)','Imported Beer (7004)','Budget Beer (7002)','Import 20s (6004)','Discount/vfm 20s (6002)','Subgener/budget 20s (6003)','Premium Cigarettes 20s (6001)')
# #ps v9

# #PG v9: just to differentiate based on groups 1 and 2 refresh cycles
# if (wave=='MAY2021_Refresh'){
#   american_BUs <- c('fl','gc','rm','tx')
#   canadian_BUs <- c('qw','ce') #PG v9 adding 'ce' for code sanity
#   }
# else if (wave=='JUNE2021_Refresh'){
#   american_BUs <- c('cc','se','gr')
#   canadian_BUs <- c('ce','qw') #PG v9 adding 'qw' for code sanity
# }
# else{
#   american_BUs <- c('fl','rm','tx')
#   canadian_BUs <- c('ce','qw')
# }

# bu_repo <- paste0(toupper(BU),"_Repo")
# pre_wave <- "/dbfs/Phase3_extensions/Elasticity_Modelling/" #ps v5
# mod_inputs <- paste0(pre_wave,wave,"/",bu_repo,"/Modelling_Inputs/") #ps v5

# COMMAND ----------

# %python
# last_52_week_start = "2019-08-07"     #give date in YYYY-mm-dd format
# last_52_week_end =  "2021-08-04"      #give date in YYYY-mm-dd format

# bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit"))) #ps v9
# BU = bu_abb_ds.collect()[0][1].lower() #ps v9
# #BU = "fl" #ps v9
# #wave = "JAN2021_TestRefresh"
# wave = dbutils.widgets.get("wave")
# # zone_cats = ['Premium Beer (7001)','BIERE-C','Specialty Beer (7006)','CIGARETTES-C','Microbrew Beer (7005)','Imported Beer (7004)','Budget Beer (7002)','Import 20s (6004)','Discount/vfm 20s (6002)','Subgener/budget 20s (6003)','Premium Cigarettes 20s (6001)']
# #ps v9


# wave = 'Accenture_Refresh'
# BU = 'gl'
# bu_repo = BU.upper() + "_Repo"
# na_DB_txn = "localized_pricing"
# bu_daily_txn_table = "{}.{}_Daily_txn_final_SWEDEN_Nomenclature".format(na_DB_txn,BU)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Main Code

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Do the Actual Imputation

# COMMAND ----------

#PG v12
reviewed_elast <- read.csv(paste0(pre_wave,wave,"/",bu_repo,"/Inputs_for_elasticity_imputation/",elasti_input_file))%>% #ps v9
#                            filter(get(prod_id) %in% scoped_items)%>%
                           mutate(CLUSTER = as.character(CLUSTER))


impute_flag <- read.xlsx(paste0(pre_wave,wave,"/",bu_repo,"/Inputs_for_elasticity_imputation/",impute_flag_file))%>%  #ps v9
#                            filter(get(prod_id) %in% scoped_items)%>%
                           mutate(CLUSTER = as.character(CLUSTER))
#PG_V12
product_p <- readRDS(product_map_path)%>% dplyr::mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))%>% 
                   select(c(item_indt_1_prod, item_indt_4), 
                          c(setNames(item_indt_1_prod ,prod_id), setNames(item_indt_4, "item_category")))%>%unique()

#PG_V12
impute_flag <- impute_flag %>%left_join(product_p) %>% select(prod_id, CLUSTER, Impute_Flag, item_category)%>%unique()%>%rename(Category = item_category)
impute_flag_2 <- impute_flag
baseline_collated <- read.xlsx(paste0(pre_wave,wave,"/",bu_repo,"/Inputs_for_elasticity_imputation/",baseline_collated_file))%>%  #ps v9
                           mutate(WEEK_START_DATE = as.Date(WEEK_START_DATE, origin = baseline_collated_origin_date))%>%
#                            filter(get(prod_id) %in% scoped_items)%>%
                           mutate(CLUSTER = as.character(CLUSTER))

# product_map <- readRDS(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu_repo,"/Modelling_Inputs/product_map.Rds"))%>%mutate(get(prod_id) := as.numeric(get(prod_id)))
# Need to update the path in the 03e file
old_wave_elast <- read.csv(paste0(pre_wave,wave,"/",bu_repo,"/Inputs_for_elasticity_imputation/",old_wave_elast_file)) #ps v9

# COMMAND ----------

nrow(reviewed_elast)

# COMMAND ----------

# MAGIC %python
# MAGIC # Add the cases from 01b_create_collation_output_v6 - Identified for Imputation basis the flags generated in the model
# MAGIC uc = pd.read_csv(pre_wave+wave+'/'+bu_repo+'/Input/user_category_'+bu_code.lower()+'.csv')
# MAGIC 
# MAGIC impute_cases = pd.DataFrame()
# MAGIC for a,b in uc.iterrows():
# MAGIC   if (spark.sql("show tables in localized_pricing").filter(col("tableName") == db_tbl_prefix+'input_impute_cases_after_model_'+b[0].lower()+'_'+b[1].lower()).count() > 0):
# MAGIC     df_impute = sqlContext.sql('select * from localized_pricing.'+db_tbl_prefix+'input_impute_cases_after_model_'+b[0].lower()+'_'+b[1].lower()).toPandas()
# MAGIC #     df_impute['Category'] = b[1]
# MAGIC     impute_cases = impute_cases.append(df_impute)
# MAGIC   
# MAGIC impute_cases.to_csv(pre_wave+wave+'/'+bu_repo+'/Input/impute_cases_after_model_all.csv',index=False)

# COMMAND ----------

# Add the cases from 01b_create_collation_output_v6 - Identified for Imputation basis the flags generated in the model
#PG v12
product_d <- readRDS(product_map_path)%>% dplyr::mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))%>% 
                   select(c(item_indt_1_prod, item_indt_4), 
                          c(setNames(item_indt_1_prod ,prod_id), setNames(item_indt_4, "Category")))%>%unique()
impute_cases <- read_csv(paste0(pre_wave,wave,"/",bu_repo,"/Input/impute_cases_after_model_all.csv"),col_types = list(CLUSTER = col_character()))%>%
                     dplyr::left_join(product_d)

impute_cases$Impute_Flag = 1

impute_cases <- impute_cases%>% unique()

# COMMAND ----------

display(impute_cases)

# COMMAND ----------

# Convert cluster column to character
impute_cases$CLUSTER <- as.character(impute_cases$CLUSTER)

# Combine the impute cases selected from collation output notebook and impute flags notebook
impute_flag <- bind_rows(impute_flag,impute_cases)

# COMMAND ----------

display(impute_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Add more item x clusters to imputation - basis if all the price changes for an item x cluster has been within 1% then put those for imputation

# COMMAND ----------

# #first get the prices from the ADSs
# other_cat <- ifelse(bu_repo == 'FL_Repo','modelling_ads_cluster_stateless_new.Rds','modelling_ads_cluster_stateless.Rds')
# beer_cig <- ifelse(bu_repo == 'FL_Repo','modelling_ads_cluster_stateless_zone_new_2.Rds','modelling_ads_cluster_stateless_zone_new.Rds')
# otp_wine <- ifelse(!(bu_repo %in% c('QW_Repo','CE_Repo')),'modelling_ads_cluster_stateless_OTP_Wine.Rds','modelling_ads_cluster_stateless.Rds')

# #get prices from these adss
# other_cat_ads <- readRDS(paste0("/dbfs/Phase_4/Elasticity_Modelling/",wave,"/",bu_repo,"/Modelling_Inputs/",other_cat))%>%select(itemid, Cluster, weekdate,reg_price)%>%unique()%>%
#                                         rename(reg_price_other = reg_price)%>%
#                                         mutate(Cluster = as.character(Cluster))
# beer_cig_ads <- readRDS(paste0("/dbfs/Phase_4/Elasticity_Modelling/",wave,"/",bu_repo,"/Modelling_Inputs/",beer_cig))%>%select(itemid, Cluster,weekdate, reg_price)%>%unique()%>%
#                                         rename(reg_price_beer_cig = reg_price)%>%
#                                         mutate(Cluster = as.character(Cluster))
# otp_wine_ads <- readRDS(paste0("/dbfs/Phase_4/Elasticity_Modelling/",wave,"/",bu_repo,"/Modelling_Inputs/",otp_wine))%>%select(itemid, Cluster, weekdate,reg_price)%>%unique()%>%
#                                         rename(reg_price_otp_wine = reg_price)%>%
#                                         mutate(Cluster = as.character(Cluster))


# writeLines(c(paste0('BU: ',bu_repo),paste0('Other Cat: ',other_cat),paste0('beer cig: ',beer_cig),paste0('otp wine: ',otp_wine)))


#get the prices from the ADSs
final_ads <- readRDS(paste0(pre_wave,wave,"/",bu_repo,"/Modelling_Inputs/",final_ads_file_name))%>%select(itemid, Cluster, weekdate,reg_price)%>%unique()%>%
                                         rename(reg_price_ads = reg_price)%>%
                                         mutate(Cluster = as.character(Cluster))

writeLines(c(paste0('BU: ',bu_repo),paste0('Final ADS: ',final_ads_file_name)))

# COMMAND ----------

#join all of these to baselines_collated and based on category decide the price & then basis that get the extra impute flags
baseline_collated_price <- baseline_collated %>%
                     left_join(final_ads,c(setNames('itemid',prod_id),'CLUSTER' = 'Cluster','WEEK_START_DATE' = 'weekdate'))%>%
                     mutate(reg_price_final = reg_price_ads)

#print if there are any NA Prices coming from the ADS
NA_prices <- nrow(baseline_collated_price%>%filter(is.na(reg_price_final)))

baseline_collated_flagged <- baseline_collated_price%>%
                           arrange(!!! rlang::syms(c("CATEGORY",prod_id, "CLUSTER","WEEK_START_DATE")))%>%
                           filter(!is.na(reg_price_final))%>%
                           mutate(lag_price = lag(reg_price_final,1))%>%
                           filter(!is.na(lag_price))%>%
                           mutate(price_change = abs((1-(reg_price_final/lag_price))))%>%
#                            mutate(price_change = (1-(reg_price_final/lag_price)))%>%
                           group_by(.dots = c("CATEGORY", prod_id, "CLUSTER"))%>%
                           summarise(max_price_change = max(price_change))%>%
                           filter(max_price_change <= 0.01)%>%
                           rename(Category = CATEGORY)%>%
                           select(-max_price_change)%>%
                           mutate(Impute_Flag = 1)
print("Yes")
#update impute flag
initial_impute_flag_row <- nrow(impute_flag)
impute_flag <- bind_rows(impute_flag,baseline_collated_flagged)%>%unique()
updated_impute_flag_row <- nrow(impute_flag)
row_inc <- updated_impute_flag_row - initial_impute_flag_row
#print update
writeLines(c(paste0('BU: ',bu_repo),paste0('missing price rows weeks: ',NA_prices),paste0('impute initial: ',initial_impute_flag_row),paste0('impute increase: ',row_inc)))

# COMMAND ----------

display(impute_flag)

# COMMAND ----------

initial_impute_flag_row

# COMMAND ----------

updated_impute_flag_row

# COMMAND ----------

nrow(baseline_collated_flagged)

# COMMAND ----------

nrow(impute_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Imputation Cases: within BU via Subcat x cluster >> cat x Cluster >> item >> subcat >> cat - then across BU - item >> subcat >> cat

# COMMAND ----------

#qc- check if both baseline file and elasticity file have the same item x clusters or not
base_model <- baseline_collated%>%select(c(prod_id, CLUSTER))%>%distinct()
elast_model <- reviewed_elast%>%select(c(prod_id, CLUSTER))%>%distinct()
writeLines(c(paste0('difference in item x clusters b/w basline and elast file: ',(nrow(base_model)-nrow(elast_model)))))

# COMMAND ----------

setdiff(base_model, elast_model)

# COMMAND ----------

# MAGIC %md
# MAGIC Within BU imputation source

# COMMAND ----------

#create weight from ucm baseline
#get the subcat-wise/cat wise last 52 weeks' ucm baseline per product x subcat x cat x cluster 
weights <- baseline_collated%>%
           filter((WEEK_START_DATE >= last_52_week_start) & (WEEK_START_DATE <= last_52_week_end ))%>%
           group_by(.dots = c(prod_id, "CLUSTER"))%>%
           summarise(weight = sum(UCM_BASELINE, na.rm =T))%>%ungroup()

# display(weights)

# COMMAND ----------

#qc
nrow(weights%>%filter(is.na(weight)))

# COMMAND ----------

#PG v11: A bug fix where we are removing the items flagged for imputing in cmd 23(baseline collated flagged items)
#PG v12: A bug fix where we are removing the items flagged by notebook 03c
reviewed_elast <- reviewed_elast%>% dplyr::mutate(CLUSTER = as.character(CLUSTER),iCluster = paste0(get(prod_id),"_",CLUSTER))
baseline_collated_flagged <- baseline_collated_flagged%>%
              dplyr::mutate(CLUSTER = as.character(CLUSTER),iCluster = paste0(get(prod_id),"_",CLUSTER))
results12 <- setdiff(reviewed_elast$iCluster, baseline_collated_flagged$iCluster)
reviewed_elast <- reviewed_elast[reviewed_elast$iCluster %in% results12, ]
impute_flag_2 <- impute_flag_2%>%
              dplyr::mutate(CLUSTER = as.character(CLUSTER),iCluster = paste0(get(prod_id),"_",CLUSTER))
results13 <- setdiff(reviewed_elast$iCluster, impute_flag_2$iCluster)
reviewed_elast <- reviewed_elast[reviewed_elast$iCluster %in% results13, ]%>%dplyr::select(-iCluster)

# COMMAND ----------

# Prepare the impute cases in the same schema as reviewed elasticities
product_mapping <- readRDS(product_map_path)%>% dplyr::mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))%>% 
                   select(c(item_indt_1_prod, item_indt_5, item_indt_3, item_indt_2), 
                          c(setNames(item_indt_1_prod ,prod_id), setNames(item_indt_5, "item_subcategory"), setNames(item_indt_3, "item_name"), setNames(item_indt_2, prod_num)))%>%unique()

impute_flag_1 = impute_flag %>% select(c(prod_id, CLUSTER, Category)) %>% left_join(product_mapping)%>%dplyr::rename(item_category = Category)

# Append all the cases - Elasticities & Impute cases to get the base data so that we can continue with the process flow
reviewed_elast <- reviewed_elast%>% dplyr::mutate(!!prod_num := as.character(get(prod_num)))
impute_flag_1 <- impute_flag_1%>% dplyr::mutate(!!prod_num := as.character(get(prod_num)))
reviewed_elast_tot <- bind_rows(reviewed_elast,impute_flag_1) %>%unique()

# COMMAND ----------

# product_mapping1 <- readRDS(paste0(pre_wave,wave,"/",product_map))%>% dplyr::mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))
# display(impute_flag)

# COMMAND ----------

nrow(reviewed_elast_tot)

# COMMAND ----------

nrow(reviewed_elast)

# COMMAND ----------

nrow(impute_cases)

# COMMAND ----------

nrow(baseline_collated_flagged)

# COMMAND ----------

nrow(impute_flag_2)

# COMMAND ----------



# COMMAND ----------

# Check if the number of total rows is reviewed elast rows + impute cases rows - This difference should be 0
# print(nrow(reviewed_elast_tot) - nrow(reviewed_elast) - nrow(impute_cases))
# PG v12
print(nrow(reviewed_elast_tot) - nrow(reviewed_elast) - nrow(impute_cases) - nrow(baseline_collated_flagged)-nrow(impute_flag_2))

# COMMAND ----------

nrow(impute_cases)

# COMMAND ----------

#get impute flag into the elasticity file and split in 2 parts - non imputed and to be imputed
reviewed_elast_flagged <- reviewed_elast_tot %>%
                          left_join(impute_flag %>% select(c(prod_id, CLUSTER, Impute_Flag))%>%unique())%>%
                          mutate(Impute_Flag = ifelse(is.na(Impute_Flag),0,Impute_Flag))

non_impute <- reviewed_elast_flagged %>% filter(Impute_Flag == 0)
to_impute <- reviewed_elast_flagged %>% filter(Impute_Flag == 1)

# print(paste0("Impute % :", round(100*nrow(to_impute)/nrow(reviewed_elast_flagged),2), "%"))
print(paste0("Impute % :", round(100*nrow(to_impute)/(nrow(non_impute)+nrow(to_impute)),2), "%"))

# COMMAND ----------

# display(to_impute)

# COMMAND ----------

# #ps v5
# # rev_elast1 <- as.data.frame(read.csv(paste0(pre_wave,wave,"/",bu_repo,"/Inputs_for_elasticity_imputation/Reviewed_elasticities_for_imputation.csv")))
# rev_elast2 <- reviewed_elast_tot%>%
#               dplyr::mutate(CLUSTER = as.character(CLUSTER),iCluster = paste0(get(prod_id),"_",CLUSTER))%>%dplyr::select(c(item_category,prod_id,CLUSTER,iCluster))
# rev_elast3 <- dplyr::distinct(rev_elast2)
# ads_file1 <- readRDS(paste0(pre_wave,wave,'/',bu_repo,'/Modelling_Inputs/',final_ads_file_name))  #ps v9
# ads_file2 <- dplyr::distinct(ads_file1[,c("itemid","Cluster")])

# # item_indt_4 <- "category_desc"
# # item_indt_1_prod <- "product_key"

# prod_map <- readRDS(paste0(pre_wave,wave,"/",product_map))%>%dplyr::mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod))) #ps v9

# if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){
# ads_file3 <- ads_file2%>%
#           dplyr::left_join(prod_map%>%dplyr::select(c(all_of(item_indt_1_prod),all_of(item_indt_4),'sub_category_desc','item_desc','upc'),
#                            c(setNames(item_indt_1_prod ,"itemid"))))
# }

# else if(tolower(bu_code) %in% european_BUs){
#   ads_file3 <- ads_file2%>%
#           dplyr::left_join(prod_map%>%dplyr::select(c(all_of(item_indt_1_prod),all_of(item_indt_4),'item_subcategory','item_name','item_number'),
#                            c(setNames(item_indt_1_prod ,"itemid"))))
# }

# if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){
# ads_file3 <- ads_file3%>%dplyr::rename(item_category = category_desc,
#                                        item_subcategory = sub_category_desc,
#                                        item_name = item_desc, !!prod_num := upc,
#                                        !!prod_id := itemid, CLUSTER = Cluster)
# }
# else if(tolower(bu_code) %in% european_BUs){
# ads_file3 <- ads_file3%>%dplyr::rename(!!prod_id := itemid, CLUSTER = Cluster, !!prod_num := item_number)  
# }

# cats_rev_elast <- unique(rev_elast3$item_category)
# ads_file4 <- ads_file3[ads_file3$item_category %in% cats_rev_elast, ]
# ads_file4$iCluster <- paste0(ads_file4[[prod_id]],"_",ads_file4$CLUSTER)
# results1 <- setdiff(ads_file4$iCluster, rev_elast3$iCluster)   # items in ads_file4 NOT in rev_elast2
# to_impute0 <- ads_file4[ads_file4$iCluster %in% results1, ]
# to_impute0 <- to_impute0%>%dplyr::mutate(BP_ELAST_OPTI = -0.5, Impute_Flag = 1)%>%dplyr::select(-iCluster)

# to_impute1 <- to_impute0[c("item_category", "item_subcategory", "item_name",prod_num,prod_id,"CLUSTER","BP_ELAST_OPTI","Impute_Flag")]
# to_impute1 <- dplyr::distinct(to_impute1)
# to_impute <- dplyr::distinct(rbind(to_impute, to_impute1))

# print(paste0("A ads: ",nrow(ads_file4)," || B rev_elas: ", nrow(rev_elast3)," || C ImputeAdd: ", nrow(to_impute1)," || B+C = ",nrow(rev_elast3)+  nrow(to_impute1)," || D to_impute_updated: ", nrow(to_impute)," || E non_impute = ", nrow(non_impute), " || D+E = ", nrow(to_impute) + nrow(non_impute))) 
# ### Non-issue: A = B+C = D+E 

# COMMAND ----------

#ps v5
# rev_elast1 <- as.data.frame(read.csv(paste0(pre_wave,wave,"/",bu_repo,"/Inputs_for_elasticity_imputation/Reviewed_elasticities_for_imputation.csv")))
#PGv11
rev_elast2 <- reviewed_elast_tot%>%   
              dplyr::mutate(CLUSTER = as.character(CLUSTER),iCluster = paste0(get(prod_id),"_",CLUSTER))%>%dplyr::select(c(item_category,prod_id,CLUSTER,iCluster))
rev_elast3 <- dplyr::distinct(rev_elast2)
ads_file1 <- readRDS(paste0(pre_wave,wave,'/',bu_repo,'/Modelling_Inputs/',final_ads_file_name))  #ps v9
ads_file2 <- dplyr::distinct(ads_file1[,c("itemid","Cluster")])

# item_indt_4 <- "category_desc"
# item_indt_1_prod <- "product_key"

prod_map <- readRDS(product_map_path)%>%dplyr::mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod))) #ps v9

if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){
ads_file3 <- ads_file2%>%
          dplyr::left_join(prod_map%>%dplyr::select(c(all_of(item_indt_1_prod),all_of(item_indt_4),'sub_category_desc','item_desc','upc'),
                           c(setNames(item_indt_1_prod ,"itemid"))))
}

else if(tolower(bu_code) %in% european_BUs){
  ads_file3 <- ads_file2%>%
          dplyr::left_join(prod_map%>%dplyr::select(c(all_of(item_indt_1_prod),all_of(item_indt_4),'item_subcategory','item_name','item_number'),
                           c(setNames(item_indt_1_prod ,"itemid"))))
}

if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){
ads_file3 <- ads_file3%>%dplyr::rename(item_category = category_desc,
                                       item_subcategory = sub_category_desc,
                                       item_name = item_desc, !!prod_num := upc,
                                       !!prod_id := itemid, CLUSTER = Cluster)
}
else if(tolower(bu_code) %in% european_BUs){
ads_file3 <- ads_file3%>%dplyr::rename(!!prod_id := itemid, CLUSTER = Cluster, !!prod_num := item_number)  
}

cats_rev_elast <- unique(rev_elast3$item_category)
# ads_file4 <- ads_file3[ads_file3$item_category %in% cats_rev_elast, ]
ads_file3$iCluster <- paste0(ads_file3[[prod_id]],"_",ads_file3$CLUSTER)
results1 <- setdiff(ads_file3$iCluster, rev_elast3$iCluster)   # items in ads_file4 NOT in rev_elast2
to_impute0 <- ads_file3[ads_file3$iCluster %in% results1, ]
to_impute0 <- to_impute0%>%dplyr::mutate(BP_ELAST_OPTI = NA, Impute_Flag = 1)%>%dplyr::select(-iCluster)  #PGv11

to_impute1 <- to_impute0[c("item_category", "item_subcategory", "item_name",prod_num,prod_id,"CLUSTER","BP_ELAST_OPTI","Impute_Flag")]
to_impute1 <- dplyr::distinct(to_impute1)


to_impute <- dplyr::distinct(rbind(to_impute, to_impute1))

print(paste0("A ads: ",nrow(ads_file3)," || B rev_elas: ", nrow(rev_elast3)," || C ImputeAdd: ", nrow(to_impute1)," || B+C = ",nrow(rev_elast3)+  nrow(to_impute1)," || D to_impute_updated: ", nrow(to_impute)," || E non_impute = ", nrow(non_impute), " || D+E = ", nrow(to_impute) + nrow(non_impute))) 
### Non-issue: A = B+C = D+E 

# COMMAND ----------

display(to_impute1)

# COMMAND ----------

# now on the nonimpute table get the weight joined and then create the same ordered pipeline of imputation
if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){
product_map <- readRDS(product_map_path)%>%
           select(c(item_indt_1_prod, department_desc))%>%distinct()%>%rename(!!prod_id := item_indt_1_prod, item_category_group = department_desc)
}
else if(tolower(bu_code) %in% european_BUs){
product_map <- readRDS(product_map_path)%>%
           select(sys_id, item_category_group)%>%distinct()%>%rename(!!prod_id := sys_id)
}
  
#con
  impute_source_BU <- non_impute %>%
                 filter(grepl("_", CLUSTER))%>%
                 mutate(cluster_wo_zone = str_extract(CLUSTER, "[0-9]?"))%>%
                 right_join(non_impute) %>% 
                 left_join(weights%>%select(c(prod_id,CLUSTER, weight))%>%unique())%>%
                 mutate(weight = ifelse(is.na(weight),0,weight))%>%
                 group_by(.dots = c(prod_id, "cluster_wo_zone"))%>%
                 mutate(elast_item_zone = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 group_by(item_subcategory, CLUSTER)%>%
                 mutate(elast_subcat_clust = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 group_by(item_category, CLUSTER)%>%
                 mutate(elast_cat_clust = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 group_by(.dots = c(prod_id))%>%
                 mutate(elast_item = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 group_by(item_subcategory)%>%
                 mutate(elast_subcat = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 group_by(item_category)%>%
                 mutate(elast_cat = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 left_join(product_map)%>%
                 group_by(item_category_group, CLUSTER)%>%
                 mutate(elast_cat_group_clust = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 group_by(item_category_group)%>%
                 mutate(elast_cat_group = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                 mutate(elast_item_zone = ifelse(!is.na(cluster_wo_zone) ,elast_item_zone,NA))

# COMMAND ----------

# MAGIC %md
# MAGIC Across BU imputation source

# COMMAND ----------

# need to get the pipeline for the same for across BUs if there exists
items_to_impute  <- unique(to_impute[[prod_id]])

if(tolower(bu_code) %in% american_BUs){
  otherBUs <- setdiff(american_BUs, tolower(bu_code))
}

else if(tolower(bu_code) %in% canadian_BUs){
  otherBUs <- setdiff(canadian_BUs, tolower(bu_code))
}
else if(tolower(bu_code) %in% european_BUs){
  otherBUs <- setdiff(european_BUs, tolower(bu_code))
}

# COMMAND ----------

otherBUs

# COMMAND ----------

#main function cons
library(tidyverse)
get_elast_other_BU <- function(otherBU){
otherBU <- paste0(toupper(otherBU),"_Repo")

if(file.exists(paste0(pre_wave,wave,"/",otherBU,"/Inputs_for_elasticity_imputation/",elasti_input_file)) == TRUE){  #ps v9
reviewed_elast_other <- read.csv(paste0(pre_wave,wave,"/",otherBU,"/Inputs_for_elasticity_imputation/",elasti_input_file))%>%
                       mutate(CLUSTER = as.character(CLUSTER))#%>%
                       #mutate(!!prod_num := as.factor(get(prod_num)))                                   #pg v6
impute_flag_other <- read.xlsx(paste0(pre_wave,wave,"/",otherBU,"/Inputs_for_elasticity_imputation/",impute_flag_file))%>% #ps v9
                       mutate(CLUSTER = as.character(CLUSTER))
baseline_collate_other <- read.xlsx(paste0(pre_wave,wave,"/",otherBU,"/Inputs_for_elasticity_imputation/",baseline_collated_file))%>%  #ps v9
                     mutate(WEEK_START_DATE = as.Date(WEEK_START_DATE, origin = baseline_collated_origin_date))%>%
                       mutate(CLUSTER = as.character(CLUSTER))

weights_other <- baseline_collate_other%>%
           filter((WEEK_START_DATE >= last_52_week_start) & (WEEK_START_DATE <= last_52_week_end ))%>%
           group_by(.dots=c(prod_id, "CLUSTER"))%>%
           summarise(weight = sum(UCM_BASELINE, na.rm =T))%>%ungroup()
  
weights_other[is.na(weights_other)] <- 0

impute_source_other_BU <- reviewed_elast_other %>%
                          left_join(impute_flag_other %>% select(c(prod_id, CLUSTER, Impute_Flag))%>%unique())%>%
                          mutate(Impute_Flag = ifelse(is.na(Impute_Flag),0,Impute_Flag))%>%
                          filter(Impute_Flag == 0)%>%
                          left_join(weights_other%>%select(c(prod_id,CLUSTER, weight))%>%unique())%>%
                          mutate(weight = ifelse(is.na(weight),0,weight))%>%
                          group_by(.dots = c(prod_id))%>%
                          mutate(elast_item_other = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                          group_by(item_subcategory)%>%
                          mutate(elast_subcat_other = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                          group_by(item_category)%>%
                          mutate(elast_cat_other = weighted.mean(BP_ELAST_OPTI, weight))%>%ungroup()%>%
                          mutate(BU = otherBU)#%>%
#                           select(BU,item_category,item_subcategory,PRODUCT_KEY,elast_item_other,elast_subcat_other, elast_cat_other)%>%unique()%>%
#                           gather("type",'elast_val',elast_item_other:elast_cat_other)
  
   if(nrow(impute_source_other_BU) == 0){
    impute_source_other_BU <- setNames(data.frame(matrix(ncol = 8, nrow = 0)), c(prod_id, "CLUSTER", "Impute_Flag","elast_item_other","elast_subcat_other","elast_cat_other","item_subcategory","item_category"))%>%
                            mutate(CLUSTER = as.character(CLUSTER))%>%
                            mutate(!!prod_id := as.numeric(get(prod_id)))%>%
                            mutate(Impute_Flag = as.numeric(Impute_Flag))%>%
                            mutate(elast_item_other = as.numeric(elast_item_other))%>%
                            mutate(elast_subcat_other = as.numeric(elast_subcat_other))%>%
                            mutate(elast_cat_other = as.numeric(elast_cat_other))%>%
                            mutate(item_subcategory = as.factor(item_subcategory))%>%
                            mutate(item_category = as.factor(item_category))
  }else{
    impute_source_other_BU <- impute_source_other_BU
  }
}
else{
  impute_source_other_BU <- setNames(data.frame(matrix(ncol = 8, nrow = 0)), c(prod_id, "CLUSTER", "Impute_Flag","elast_item_other","elast_subcat_other","elast_cat_other","item_subcategory","item_category"))%>%
                            mutate(CLUSTER = as.character(CLUSTER))%>%
                            mutate(!!prod_id := as.numeric(get(prod_id)))%>%
                            mutate(Impute_Flag = as.numeric(Impute_Flag))%>%
                            mutate(elast_item_other = as.numeric(elast_item_other))%>%
                            mutate(elast_subcat_other = as.numeric(elast_subcat_other))%>%
                            mutate(elast_cat_other = as.numeric(elast_cat_other))%>%
                            mutate(item_subcategory = as.factor(item_subcategory))%>%
                            mutate(item_category = as.factor(item_category))
}
return(impute_source_other_BU)
}

# COMMAND ----------

otherBUs

# COMMAND ----------

impute_source_other_BU_out <- bind_rows(lapply(otherBUs, function(x) {
  print(x)
  get_elast_other_BU(x)
}))

# COMMAND ----------

if(nrow(impute_source_other_BU_out) == 0){
    impute_source_other_BU_final <- impute_source_other_BU_out
  }else{
    impute_source_other_BU_final <- impute_source_other_BU_out%>%
                                    group_by(.dots = c(prod_id))%>%
                                    mutate(elast_item_other = mean(elast_item_other, na.rm = T))%>%ungroup()%>%
                                    group_by(item_subcategory)%>%
                                    mutate(elast_subcat_other = mean(elast_subcat_other, na.rm = T))%>%ungroup()%>%
                                    group_by(item_category)%>%
                                    mutate(elast_cat_other = mean(elast_cat_other, na.rm = T))%>%ungroup()%>%
                                    select(c(item_category,item_subcategory,prod_id, elast_item_other, elast_subcat_other, elast_cat_other))%>%
                                    group_by(.dots = c("item_category","item_subcategory",prod_id))%>%
                                    summarise_all(funs(mean(.,na.rm =T)))%>%ungroup()
  }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final Imputation Step

# COMMAND ----------

old_wave_elast <- old_wave_elast %>%
                  mutate(Cluster = as.character(Cluster))

old_wave_elast1 <- old_wave_elast %>%
                  mutate(item_category_lower=tolower(item_category)) %>%
                  select(-item_category)   
display(old_wave_elast)
# %>%select(c(prod_id,CLUSTER,Old_Elasticity))%>%unique()

# COMMAND ----------

#v15 Prakhar: added old_elasticity in the condition for edge cases having no category level avg elasticity values

impute_source_other_BU_final$item_subcategory <- as.character(impute_source_other_BU_final$item_subcategory)
impute_source_other_BU_final$item_category <- as.character(impute_source_other_BU_final$item_category)

to_impute_imputed <- to_impute%>%
                    filter(grepl("_", CLUSTER))%>%
                    mutate(cluster_wo_zone = str_extract(CLUSTER, "[0-9]?"))%>%
                    right_join(to_impute) %>%
                    mutate(item_category_lower=tolower(item_category)) %>%
                    left_join(product_map)%>%
                    left_join(impute_source_BU%>%select(c(prod_id, cluster_wo_zone, elast_item_zone))%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_subcategory, CLUSTER, elast_subcat_clust)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category, CLUSTER, elast_cat_clust)%>%unique())%>%
                    left_join(impute_source_BU%>%select(c(prod_id,elast_item))%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_subcategory, elast_subcat)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category, elast_cat)%>%unique())%>%
                    left_join(impute_source_other_BU_final%>%select(c(prod_id,elast_item_other))%>%unique())%>%
                    left_join(impute_source_other_BU_final%>%select(item_subcategory, elast_subcat_other)%>%unique())%>%
                    left_join(impute_source_other_BU_final%>%select(item_category, elast_cat_other)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category_group, CLUSTER,elast_cat_group_clust)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category_group, elast_cat_group)%>%unique())%>%
                    left_join(old_wave_elast1, c(setNames("Sys.ID.Join",prod_id), "CLUSTER" = "Cluster",'item_category_lower'='item_category_lower'))%>%
                    mutate(final_elasticity = ifelse(!is.na(elast_item_zone),elast_item_zone,
                                                     ifelse(!is.na(elast_subcat_clust),elast_subcat_clust,
                                                            ifelse(!is.na(elast_cat_clust),elast_cat_clust,
                                                                   ifelse(!is.na(elast_item),elast_item,
                                                                          ifelse(!is.na(elast_subcat),elast_subcat,
                                                                                 ifelse(!is.na(elast_cat),elast_cat,
                                                                                        ifelse(!is.na(elast_item_other),elast_item_other,
                                                                                               ifelse(!is.na(elast_subcat_other),elast_subcat_other,
                                                                                                      ifelse(!is.na(elast_cat_other),elast_cat_other,
                                                                                                             ifelse(!is.na(elast_cat_group_clust),elast_cat_group_clust,
                                                                                                                   ifelse(!is.na(elast_cat_group),elast_cat_group,Old_Elasticity))))))))))))%>%
                                                                                                                      select(-item_category_lower)

# COMMAND ----------

#v5 Prakhar: added old_elasticity in the condition for edge cases having no category level avg elasticity values
to_impute_imputed_desc <- to_impute_imputed%>%
                    left_join(product_map)%>%
                    left_join(impute_source_BU%>%select(item_subcategory, CLUSTER, elast_subcat_clust)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category, CLUSTER, elast_cat_clust)%>%unique())%>%
                    left_join(impute_source_BU%>%select(c(prod_id,elast_item))%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_subcategory, elast_subcat)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category, elast_cat)%>%unique())%>%
                    left_join(impute_source_other_BU_final%>%select(c(prod_id,elast_item_other))%>%unique())%>%
                    left_join(impute_source_other_BU_final%>%select(item_subcategory, elast_subcat_other)%>%unique())%>%
                    left_join(impute_source_other_BU_final%>%select(item_category, elast_cat_other)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category_group, CLUSTER,elast_cat_group_clust)%>%unique())%>%
                    left_join(impute_source_BU%>%select(item_category_group, elast_cat_group)%>%unique())%>%
#                     left_join(old_wave_elast, c(setNames("Sys.ID.Join",prod_id), "CLUSTER" = "Cluster", "item_category" = "item_category_lower","Old_Elasticity" = "Old_Elasticity"))%>%
                    mutate(elasticity_imputed = ifelse(!is.na(elast_item_zone),'elast_item_zone',
                                                       ifelse(!is.na(elast_subcat_clust),'elast_subcat_clust',
                                                              ifelse(!is.na(elast_cat_clust),'elast_cat_clust',
                                                                     ifelse(!is.na(elast_item),'elast_item',
                                                                            ifelse(!is.na(elast_subcat),'elast_subcat',
                                                                                   ifelse(!is.na(elast_cat),'elast_cat',
                                                                                          ifelse(!is.na(elast_item_other),'elast_item_other',
                                                                                                 ifelse(!is.na(elast_subcat_other),'elast_subcat_other',
                                                                                                        ifelse(!is.na(elast_cat_other),'elast_cat_other',
                                                                                                               ifelse(!is.na(elast_cat_group_clust),'elast_cat_group_clust',
                                                                                                                      ifelse(!is.na(elast_cat_group),'elast_cat_group','old_elasticity'))))))))))))
        

# COMMAND ----------

#qc
# display(to_impute_imputed_desc%>%filter(is.na(final_elasticity)))
# display(to_impute_imputed_desc)

# COMMAND ----------

#qc
if(nrow(to_impute_imputed_desc%>%filter(is.na(final_elasticity))) == 0){
  
}else{
    display(to_impute_imputed_desc%>%filter(is.na(final_elasticity))%>%select(c(item_category,item_name, prod_id,CLUSTER))%>%distinct())
}

# COMMAND ----------

#To change the data type of UPC/JDE_NUMBER number to character if not already for both non_impute and impute dataframes v6 PG
# non_impute <- non_impute%>%
#                         mutate(!!prod_num = as.character(get(prod_num)))
# to_impute_imputed_desc <- to_impute_imputed_desc%>%
#                                               mutate(!!prod_num = as.character(get(prod_num)))

# COMMAND ----------

#finally append the 2 together
final_file <- non_impute%>%
                     mutate(final_elasticity = BP_ELAST_OPTI)%>%
                     bind_rows(to_impute_imputed_desc)%>%
                     select(c(item_category,item_subcategory,item_name,prod_num,prod_id,CLUSTER,Impute_Flag,BP_ELAST_OPTI,elasticity_imputed,final_elasticity))%>%
                     rename(FINAL_BP_ELASTICITY = final_elasticity)
final_file$elasticity_imputed[is.na(final_file$elasticity_imputed)] <- "No"
print(paste0("row mismatch: ",abs(nrow(final_file) - nrow(reviewed_elast))))

# COMMAND ----------

#extra adjustment
#logic for category nature retention +50% and stuff
#to be added
cat_avg <- final_file%>%group_by(item_category, CLUSTER)%>%
                        summarise(cat_avg_elast = mean(FINAL_BP_ELASTICITY))%>%ungroup()
#PG--1. Replace Final_BP_Elasticity to Elasticity_scaled_to_catavg
#PG--2. Replace cat_avg_elast to Elasticity catavg for scaling
final_file_2 <- final_file%>%left_join(cat_avg)%>%
                mutate(category_scaling = ifelse(FINAL_BP_ELASTICITY > 0.5*cat_avg_elast, "cat_avg_elast*0.5",
                                                    ifelse(FINAL_BP_ELASTICITY < 2*cat_avg_elast, 
                                                            "cat_avg_elast*2", "No scaling")))

final_file_2 <- final_file_2%>%left_join(cat_avg)%>%
                mutate(FINAL_BP_ELASTICITY_scaling = ifelse(FINAL_BP_ELASTICITY > 0.5*cat_avg_elast, cat_avg_elast*0.5,
                                                    ifelse(FINAL_BP_ELASTICITY < 2*cat_avg_elast, 
                                                            cat_avg_elast*2, FINAL_BP_ELASTICITY)))%>%
               mutate(FINAL_BP_ELASTICITY_scaling = ifelse(FINAL_BP_ELASTICITY_scaling > -0.5,-0.5,FINAL_BP_ELASTICITY_scaling))%>%
               mutate(FINAL_BP_ELASTICITY_scaling = ifelse(FINAL_BP_ELASTICITY_scaling < -2.5,-2.5,FINAL_BP_ELASTICITY_scaling))

# COMMAND ----------

# logic to make sure we are not drastically changing elasticities from stable ones only for wave 2
final_file_3 <- final_file_2%>%
                left_join(old_wave_elast, c(setNames("Sys.ID.Join",prod_id), "CLUSTER" = "Cluster"))%>%
                mutate(deviation = (FINAL_BP_ELASTICITY_scaling/Old_Elasticity)-1)%>%
                mutate(FINAL_BP_ELASTICITY_adj = ifelse(is.na(Old_Elasticity),FINAL_BP_ELASTICITY_scaling,  # no comparison where last wave didnt have elast
                                                       ifelse(Impute_Flag == 0 & deviation > 0.5, Old_Elasticity*old_elasticity_up_factor,
                                                             ifelse(Impute_Flag == 0 & deviation < -0.5, 
                                                                    Old_Elasticity*old_elasticity_lo_factor,FINAL_BP_ELASTICITY_scaling))))

# COMMAND ----------

# logic to make sure we are not drastically changing elasticities from stable ones only for wave 2

                
final_file_4 <- final_file_3%>%
                mutate(prior_refresh_scaling = ifelse(is.na(Old_Elasticity),"No",  # no comparison where last wave didnt have elast
                                                       ifelse(Impute_Flag == 0 & deviation > 0.5, paste0("Yes by",old_elasticity_up_factor,"times"),
                                                             ifelse(Impute_Flag == 0 & deviation < -0.5, 
                                                                    paste0("Yes by ", old_elasticity_lo_factor," times"),"No"))))
#                 select(-pricing_cycle)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exporting for Optimization

# COMMAND ----------

# MAGIC %python
# MAGIC #PG v6
# MAGIC dbutils.fs.mkdirs(optimization_directory+wave+"/optimization_inputs/"+bu_repo+"/")

# COMMAND ----------

final_file_4_f <- final_file_3%>%
                    select(-item_category.y,-elasticity_imputed,-category_scaling,-FINAL_BP_ELASTICITY_scaling)%>%
                    rename(item_category = item_category.x)
#BU_repo <- paste0(toupper(BU),"_Repo")
write_csv(final_file_4_f,paste0("/dbfs",optimization_directory,wave,"/optimization_inputs/",bu_repo,"/Final_Elasticities_after_Imputation.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Saving the output into table

# COMMAND ----------

#reading the final_file_3 and adding additional columns "Wave" and "BU"
temp <- final_file_3 %>% mutate(Wave = wave) %>% mutate(BU = business_units)

#renaming the column names 
if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){
  
  na_cols <- c("BU","Wave","item_category", "item_subcategory",	"item_name", "UPC", "PRODUCT_KEY", "CLUSTER", "Impute_Flag",	"BP_ELAST_OPTI",	"FINAL_BP_ELASTICITY",	"cat_avg_elast", "Old_Elasticity", "deviation", "FINAL_BP_ELASTICITY_adj","elasticity_imputed","category_scaling","FINAL_BP_ELASTICITY_scaling")
  
  temp_up <- temp %>% rename(item_category = item_category.x)
  temp_up <- temp_up %>% select(na_cols)
  
  }
else if(tolower(bu_code) %in% european_BUs){
  
  eu_cols <- c("BU","Wave","item_category", "item_subcategory",	"item_name", "JDE_NUMBER", "TRN_ITEM_SYS_ID", "CLUSTER", "Impute_Flag",	"BP_ELAST_OPTI",	"FINAL_BP_ELASTICITY",	"cat_avg_elast", "Old_Elasticity", "deviation", "FINAL_BP_ELASTICITY_adj","elasticity_imputed","category_scaling","FINAL_BP_ELASTICITY_scaling")
  
  temp_up <- temp %>% rename(item_category = item_category.x)
  temp_up <- temp_up %>% select(eu_cols)
  
  }
   
# display(temp_up)

# COMMAND ----------

#defining the schema for the final table which can store the output for all the BUs
cols_for_tabl <-c("BU","Wave","item_category", "item_subcategory", "item_name", "UPC", "JDE_NUMBER", "PRODUCT_KEY", "TRN_ITEM_SYS_ID", "CLUSTER", "Impute_Flag", "BP_ELAST_OPTI", "FINAL_BP_ELASTICITY", "cat_avg_elast","Old_Elasticity", "deviation", "FINAL_BP_ELASTICITY_adj","elasticity_imputed","category_scaling","FINAL_BP_ELASTICITY_scaling")

#defining empty dataframe
final_tbl_df <- data.frame(matrix(ncol = 20, nrow = 0))
colnames(final_tbl_df) <- cols_for_tabl

# # Change the column datatype before append
# final_tbl_df$BU <- as.character(final_tbl_df$BU)

# # The following items are changed by Noah Yang
# final_tbl_df$Wave <- as.character(final_tbl_df$Wave)
# final_tbl_df$item_category <- as.character(final_tbl_df$item_category)
# final_tbl_df$item_subcategory <- as.character(final_tbl_df$item_subcategory)
# final_tbl_df$item_name <- as.character(final_tbl_df$item_name)
# final_tbl_df$CLUSTER <- as.character(final_tbl_df$CLUSTER)
# final_tbl_df$elasticity_imputed <- as.character(final_tbl_df$elasticity_imputed)
# final_tbl_df$category_scaling <- as.character(final_tbl_df$category_scaling)
# final_tbl_df$CLUSTER <- as.character(final_tbl_df$CLUSTER)
# final_tbl_df$JDE_NUMBER <- as.character(final_tbl_df$JDE_NUMBER)
# final_tbl_df$TRN_ITEM_SYS_ID <- as.character(final_tbl_df$TRN_ITEM_SYS_ID)

# Schema Modification recommended by Noah Yang - from CK D&A team due to some errors faced at their end while testing
final_tbl_df$BU <- as.character(final_tbl_df$BU)
final_tbl_df$Wave <- as.character(final_tbl_df$Wave)
final_tbl_df$item_category <- as.character(final_tbl_df$item_category)
final_tbl_df$item_subcategory <- as.character(final_tbl_df$item_subcategory)
final_tbl_df$item_name <- as.character(final_tbl_df$item_name)
final_tbl_df$UPC <- as.character(final_tbl_df$UPC)
final_tbl_df$JDE_NUMBER <- as.character(final_tbl_df$JDE_NUMBER)
final_tbl_df$PRODUCT_KEY <- as.character(final_tbl_df$PRODUCT_KEY)
final_tbl_df$TRN_ITEM_SYS_ID <- as.character(final_tbl_df$TRN_ITEM_SYS_ID)
final_tbl_df$CLUSTER <- as.character(final_tbl_df$CLUSTER)
final_tbl_df$Impute_Flag <- as.double(final_tbl_df$Impute_Flag)
final_tbl_df$BP_ELAST_OPTI <- as.double(final_tbl_df$BP_ELAST_OPTI)
final_tbl_df$FINAL_BP_ELASTICITY <- as.double(final_tbl_df$FINAL_BP_ELASTICITY)
final_tbl_df$cat_avg_elast <- as.double(final_tbl_df$cat_avg_elast)
final_tbl_df$Old_Elasticity <- as.double(final_tbl_df$Old_Elasticity)
final_tbl_df$deviation <- as.double(final_tbl_df$deviation)
final_tbl_df$FINAL_BP_ELASTICITY_adj <- as.double(final_tbl_df$FINAL_BP_ELASTICITY_adj)
final_tbl_df$elasticity_imputed <- as.character(final_tbl_df$elasticity_imputed)
final_tbl_df$category_scaling <- as.character(final_tbl_df$category_scaling)
final_tbl_df$FINAL_BP_ELASTICITY_scaling <- as.double(final_tbl_df$FINAL_BP_ELASTICITY_scaling)

temp_up <- temp_up%>% dplyr::mutate(!!prod_id := as.character(get(prod_id)))

#appnending output to the dataframe
final_tbl_df_up <- dplyr::bind_rows(final_tbl_df,temp_up)
final_tbl_df_up <- final_tbl_df_up %>% 
                   mutate(UPC = as.numeric(UPC)) %>% 
                   mutate(JDE_NUMBER = as.numeric(JDE_NUMBER)) %>% 
                   mutate(PRODUCT_KEY = as.numeric(PRODUCT_KEY)) %>% 
                   mutate(TRN_ITEM_SYS_ID = as.numeric(TRN_ITEM_SYS_ID))

display(final_tbl_df_up)

#converting the dataframe into view, making it readable for python code
SparkR::createOrReplaceTempView(SparkR::as.DataFrame(final_tbl_df_up), "final_tbl_temp_view")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #reading the output in python code from the view created in R code
# MAGIC final_tbl_df_up = sqlContext.sql("""select * from final_tbl_temp_view """).\
# MAGIC                                  withColumn("FINAL_BP_ELASTICITY_adj", col('FINAL_BP_ELASTICITY_adj').cast(DoubleType())).\
# MAGIC                                  withColumn("FINAL_BP_ELASTICITY_scaling", col('FINAL_BP_ELASTICITY_scaling').cast(DoubleType()))
# MAGIC 
# MAGIC #fetching the existing data from the table and checking if the data already exist for the same BU, Wave, Item and Cluster
# MAGIC existing_tbl = sqlContext.sql("""select * from localized_pricing.STG_3_Imputed_Elasticities """)
# MAGIC 
# MAGIC existing_tbl = existing_tbl.join(final_tbl_df_up.select("BU","Wave",prod_id,"CLUSTER").withColumn("duplicate", sf.lit("Duplicate values found")),["BU","Wave",prod_id,"CLUSTER"],"left")
# MAGIC 
# MAGIC #dropping the data if the data already exist for the same BU and Wave
# MAGIC existing_tbl_up = existing_tbl.filter(col('duplicate').isNull()).drop('duplicate')
# MAGIC 
# MAGIC final_tbl = existing_tbl_up.unionAll(final_tbl_df_up)
# MAGIC 
# MAGIC #saving data into temp table as directly overwriting the table which is read gives erro: "cannot overwrite table that is also being read from"
# MAGIC final_tbl.write.mode("overwrite").saveAsTable("a_STG_3_Imputed_Elasticities_temp_table")
# MAGIC final_tbl_f = sqlContext.sql("""select * from a_STG_3_Imputed_Elasticities_temp_table """)
# MAGIC 
# MAGIC #writing the output data into the final table
# MAGIC final_tbl_f.write.mode("overwrite").saveAsTable("localized_pricing.STG_3_Imputed_Elasticities_mock")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exporting for Refresh DB

# COMMAND ----------

final_file_4_r <- final_file_4%>%
   select(c(item_category.x,item_subcategory,item_name,prod_num,prod_id,CLUSTER,Impute_Flag,BP_ELAST_OPTI,elasticity_imputed,FINAL_BP_ELASTICITY,cat_avg_elast,category_scaling,FINAL_BP_ELASTICITY_scaling,Old_Elasticity,prior_refresh_scaling,FINAL_BP_ELASTICITY_adj))%>%
rename(item_category = item_category.x)


# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mkdirs(Refresh_db_directory+wave+"/")

# COMMAND ----------

if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){ 
ads_file1 <- readRDS(paste0(pre_wave,wave,'/',bu_repo,'/Modelling_Inputs/',final_ads_file_name))
# str(ads_file1)
# ads_file1$weekdate2<-as.Date(as.character(ads_file1$weekdate), format = "%d-%m-%y")
# aggregate can be used for this type of thing
ads_file2 <- dplyr::distinct(ads_file1[,c("itemid","Cluster")])

dmax <- aggregate(ads_file1$weekdate,by=list(ads_file1$itemid,ads_file1$Cluster),max)%>%dplyr::rename(itemid = Group.1,Cluster = Group.2,date_max = x)
dmin <- aggregate(ads_file1$weekdate,by=list(ads_file1$itemid,ads_file1$Cluster),min)%>%dplyr::rename(itemid = Group.1,Cluster = Group.2,date_min = x)
ads_file_date <- ads_file2%>%dplyr::left_join(dmax%>%dplyr::select(c('itemid','Cluster','date_max')))%>%dplyr::left_join(dmin%>%dplyr::select(c('itemid','Cluster','date_min')))%>%dplyr::rename(!!prod_id := itemid,CLUSTER = Cluster)
final_file_4_rf <- final_file_4_r%>%dplyr::left_join(ads_file_date)
# And merge the result of aggregate 
# with the original data frame
# df2 = merge(df,d,by.x=1,by.y=1)
# display(ads_file3)
display(final_file_4_rf)
}

# COMMAND ----------

# if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){
#   sum(is.na(final_file_4_rf$date_max))
#   }

# COMMAND ----------

# MAGIC %python
# MAGIC #PG v10: To take care of multiple runs inn a refresh 
# MAGIC import os
# MAGIC import pandas as pd
# MAGIC def write_csv_df(path, filename, df):
# MAGIC     # Give the filename you wish to save the file to
# MAGIC     pathfile = os.path.normpath(os.path.join(path,filename))
# MAGIC 
# MAGIC     # Use this function to search for any files which match your filename
# MAGIC     files_present = os.path.isfile(pathfile) 
# MAGIC     # if no matching files, write to csv, if there are matching files, print statement
# MAGIC     if not files_present:
# MAGIC         df.to_csv(pathfile, index=False)
# MAGIC     else:
# MAGIC         dbutils.widgets.text("overwrite","WARNING: " + pathfile + " already exists! Do you want to overwrite <y/n>?: \n")
# MAGIC         overwrite = dbutils.widgets.get("overwrite")
# MAGIC         if overwrite == 'y':
# MAGIC             df.to_csv(pathfile, index=False)
# MAGIC         elif overwrite == 'n':
# MAGIC             dbutils.widgets.text("new_filename","Type new filename:")
# MAGIC             new_filename = dbutils.widgets.get("new_filename")
# MAGIC             write_csv_df(path,new_filename,df)
# MAGIC         else:
# MAGIC             print("Not a valid input. Data is NOT saved!\n")

# COMMAND ----------

if((tolower(bu_code) %in% american_BUs)|(tolower(bu_code) %in% canadian_BUs)){  write_csv(final_file_4_rf,paste0("/dbfs",Refresh_db_directory,wave,"/",BU,"_",wave,"_Final_input_for_Refresh_database_interim.csv"))
}
else if(tolower(bu_code) %in% european_BUs){
 write_csv(final_file_4_r,paste0("/dbfs",Refresh_db_directory,wave,"/",BU,"_",wave,"_Final_input_for_Refresh_database_interim.csv"))
}

# COMMAND ----------

# MAGIC %python
# MAGIC #PG v10: To take care of multiple runs in a refresh 
# MAGIC df = pd.read_csv('/dbfs'+Refresh_db_directory+wave+"/"+BU+"_"+wave+'_Final_input_for_Refresh_database_interim.csv')
# MAGIC write_csv_df('/dbfs'+Refresh_db_directory+wave+"/",BU+"_"+wave+'_Final_input_for_Refresh_database.csv',df)

# COMMAND ----------

# paste0('/dbfs'+Refresh_db_directory+wave+"/",BU+"_"+wave+'_Final_input_for_Refresh_database.csv')

# COMMAND ----------


