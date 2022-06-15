# Databricks notebook source
# MAGIC %md
# MAGIC #### Call Libs

# COMMAND ----------

library(tidyverse)
library(readxl)
library(parallel)
library(openxlsx)
options(scipen = 999)

# COMMAND ----------

# MAGIC %md
# MAGIC #### User Inputs

# COMMAND ----------

# %python
# dbutils.widgets.removeAll()

# COMMAND ----------

# %python
# # #TC
# # # Creating lists for widgets

# # If you are using this notebook and your name isnt here add it
# # user = ['prat','roopa','sterling','neil','taru','colby','kushal','logan','neil','dayton','david','anuj','xiaofan','global_tech','jantest','aadarsh','jantest2','kartik','prakhar','tushar']
# user= ['sanjo']

# #Category
# # sf v8 reordered
# category_name = ['002_CIGARETTES', '003_OTHER_TOBACCO_PRODUCTS', '004_BEER', '005_WINE', '006_LIQUOR', '007_PACKAGED_BEVERAGES', '008_CANDY', '009_FLUID_MILK', '010_OTHER_DAIRY_DELI_PRODUCT', '012_PCKGD_ICE_CREAM_NOVELTIES', '013_FROZEN_FOODS', '014_PACKAGED_BREAD', '015_SALTY_SNACKS', '016_PACKAGED_SWEET_SNACKS', '017_ALTERNATIVE_SNACKS', '019_EDIBLE_GROCERY', '020_NON_EDIBLE_GROCERY', '021_HEALTH_BEAUTY_CARE', '022_GENERAL_MERCHANDISE', '024_AUTOMOTIVE_PRODUCTS', '028_ICE', '030_HOT_DISPENSED_BEVERAGES', '031_COLD_DISPENSED_BEVERAGES', '032_FROZEN_DISPENSED_BEVERAGES', '085_SOFT_DRINKS', '089_FS_PREP_ON_SITE_OTHER', '091_FS_ROLLERGRILL', '092_FS_OTHER', '094_BAKED_GOODS', '095_SANDWICHES', '503_SBT_PROPANE', '504_SBT_GENERAL_MERCH', '507_SBT_HBA']

# # business unit & wave/refresh selection
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division", "1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division", "2900 - South Atlantic Division", "3100 - Grand Canyon Division", "3800 - Northern Tier Division", "4100 - Midwest Division", "4200 - Great Lakes Division", "4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division", "Western Division"]

# # BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"], ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"], ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'], ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# # wave = ["JAN2021_TestRefresh","Mar2021_Refresh_Group1"]
# wave = ["Accenture_Refresh"]

# # #TC:creating widgets

# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("category_name", "013_FROZEN_FOODS", [str(x) for x in category_name])
# dbutils.widgets.dropdown("user", "sanjo", [str(x) for x in user])
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])

# COMMAND ----------

# ##TC: inputs for R
# Business_Units = c("1400 - Florida Division","1600 - Coastal Carolina Division", "1700 - Southeast Division","1800 - Rocky Mountain Division","1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division",  "2900 - South Atlantic Division", "3100 - Grand Canyon Division",  "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division",  "Western Division")
# BU_abbr =tolower(c("FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WC"))

# bu_df =data.frame(Business_Units,BU_abbr)
# bu_abb = bu_df %>% filter(Business_Units %in% dbutils.widgets.get("business_unit")) %>% select(BU_abbr)
# bu_abb = bu_abb[1,1]
# bu <- paste(toupper(bu_abb), "_Repo", sep="")  # GC_repo #SE_repo #TC: automate based on short forms

# #other inputs
# user <- dbutils.widgets.get("user") #3 users were modelling at the same time different categories
# category <- dbutils.widgets.get("category_name")   #for the sake of printing file name & reading same name as user input file
# wave <- dbutils.widgets.get("wave") 

# #user     <- "sterling"     
# # sf use the version of the category name that underscores for spaces and punctuation and special characters
# #category <- "085_SOFT_DRINKS" #user and category should reflect suffixes of collation input file          
# #bu <- "TX_Repo" #SE_Repo #GC_Repo
# # sf v2 added bu abbreviation

# COMMAND ----------

# %python
# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GR", bu_codes)

# COMMAND ----------

#Define parameters

#specify the user here
user <- "sanjo" #"karan"
user <- dbutils.widgets.get("user")

#specify the category name for which the collatio input file needs to be created
category<- "016_PACKAGED_SWEET_SNACKS" #"006_LIQUOR"
category <- dbutils.widgets.get("category_name")

#specify the wave here 
wave <- "Accenture_Refresh"
wave <- dbutils.widgets.get("wave")

#specfiy the BU code
business_units <- '3100 - Grand Canyon Division'
bu_code <- "GR" #"GL"

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

#Item Id column 
if (region=='NA'){
  prod_id <- "PRODUCT_KEY"
}
if (region=='EU'){
  prod_id <- "TRN_ITEM_SYS_ID"
}

#specify base_directory
base_directory = 'Phase4_extensions/Elasticity_Modelling'

#define the base directory here
pre_wave= paste0('/dbfs/',base_directory,'/')

print(paste0('bu_code: ',bu_code, ' || user: ',user,' || category: ', category))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main Code

# COMMAND ----------

#read the collation input
col_input <- read_csv(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Input/","collation_input_",user,"_",category,".csv"))%>% #ps v9
           mutate(ITEMID = as.numeric(ITEMID),
                  CLUSTER = as.character(CLUSTER))
#PG v12
if (nrow(col_input) == 0){
  unique_vers = as.double(1)
}
else {
unique_vers <- unique(col_input$VERSION)
  }
baselines <- c()
estimates <- c()
discounts <- c()
# sf v2 updated paths to all combined result
for(i in unique_vers){
   baseline <- read_excel(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/v",tolower(bu_code),"_",category,"_v",i,"/","All_Combined_Result_",category,".xlsx"), #ps v9
                          sheet = "Baseline Data Output", guess_max = 100000)%>%filter(!(is.na(get(prod_id))))%>%
               mutate(VSOURCE = i)%>%
               mutate(ESTIMATES_MAPE = as.character(ESTIMATES_MAPE))%>%
               mutate(WEEK_START_DATE = as.Date(WEEK_START_DATE))
   cols.char <- c("CLUSTER","UCM_PREDICTION_FLAG", "UCM_SMOOTH_FLAG", "REGULAR_PRICE_FLAG", "SALES_FLAG", "IMPUTE_FLAG", "IMPUTE_COMMENT", "ISSUE_COMMENT", "ESTIMATES_MAPE", "ESTIMATES_MAPE_FLAG", "OVERALL_ISSUE")
#    baseline[cols.char] <- sapply(baseline[cols.char],as.character)
   baseline <- dplyr::mutate_at(baseline, .vars = cols.char, .funs = as.character)
   estimate <- read_excel(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/v",tolower(bu_code),"_",category,"_v",i,"/","All_Combined_Result_",category,".xlsx"), #ps v9
                          sheet = "Estimates", guess_max = 100000)%>%filter(!(is.na(get(prod_id))))%>%
               mutate(VSOURCE = i)%>%
               mutate(ESTIMATES_MAPE = as.character(ESTIMATES_MAPE))
   cols.char <- c("CLUSTER","ESTIMATES_MAPE", "ESTIMATES_MAPE_FLAG")
#    estimate[cols.char] <- sapply(estimate[cols.char],as.character)
   estimate <- dplyr::mutate_at(estimate, .vars = cols.char, .funs = as.character)
   discount <- read_excel(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/v",tolower(bu_code),"_",category,"_v",i,"/","All_Combined_Result_",category,".xlsx"), #ps v9
                          sheet = "Discount Estimate" , guess_max = 100000)%>%filter(!(is.na(ITEMID)))%>%
               mutate(VSOURCE = i)
   cols.char <- c("CLUSTER","VARIABLE")
#    discount[cols.char] <- sapply(discount[cols.char],as.character)
   discount <- dplyr::mutate_at(discount, .vars = cols.char, .funs = as.character)
   baselines <- bind_rows(baselines,baseline)
   estimates <- bind_rows(estimates,estimate)
   discounts <- bind_rows(discounts,discount)
}
if (nrow(col_input) == 0){
  baselines_filter <- baselines %>%
                   mutate(CLUSTER = as.character(CLUSTER))%>%
                   arrange(!!as.name(prod_id),CLUSTER,WEEK_START_DATE)
estimates_filter <- estimates %>%
                   mutate(CLUSTER = as.character(CLUSTER))%>%
                   arrange(!!as.name(prod_id),CLUSTER)
discounts_filter <- discounts %>%
                   mutate(CLUSTER = as.character(CLUSTER))%>%
                   arrange(ITEMID,CLUSTER)
  }
else {
  baselines_filter <- baselines %>%
                   mutate(CLUSTER = as.character(CLUSTER))%>%left_join(col_input, c(setNames("ITEMID",prod_id),"CLUSTER"))%>%
                   filter(VERSION == VSOURCE)%>%select(-VSOURCE, -VERSION)%>%
                   arrange(!!as.name(prod_id),CLUSTER,WEEK_START_DATE)
estimates_filter <- estimates %>%
                   mutate(CLUSTER = as.character(CLUSTER))%>%left_join(col_input, c(setNames("ITEMID",prod_id),"CLUSTER"))%>%
                   filter(VERSION == VSOURCE)%>%select(-VSOURCE, -VERSION)%>%
                   arrange(!!as.name(prod_id),CLUSTER)
discounts_filter <- discounts %>%
                   mutate(CLUSTER = as.character(CLUSTER))%>%left_join(col_input, c("ITEMID","CLUSTER"))%>%
                   filter(VERSION == VSOURCE)%>%select(-VSOURCE, -VERSION)%>%
                   arrange(ITEMID,CLUSTER)
}
all_combined <- list("Estimates" = estimates_filter, "Baseline Data Output" = baselines_filter,"Discount Estimate"= discounts_filter)
invisible(file.remove(file.path(paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/","Final_Combined_Result_",category,".xlsx")),full.names = T)) #ps v9
write.xlsx(all_combined,paste0(pre_wave,wave,"/",bu_code,"_Repo","/Output/final_output/",user,"/","Final_Combined_Result_",category,".xlsx")) #ps v9
