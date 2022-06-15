# Databricks notebook source
# MAGIC %md
# MAGIC #### Meta Data
# MAGIC 
# MAGIC **Creator: *Nayan Mallick* | Updated by: **
# MAGIC 
# MAGIC **Date Created: 09/02/2020**
# MAGIC 
# MAGIC **Date Updated: 09/02/2020**
# MAGIC 
# MAGIC **Description: The notebook to carry out elasticity modelling for 1 item x cluster to debug what is happenning in there**
# MAGIC 
# MAGIC **Highlighting Important Steps to Run the Code:**
# MAGIC   Need to Run line by line

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation of Packages

# COMMAND ----------

# install.packages('tidyverse')
# install.packages('zoo')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### call libs

# COMMAND ----------

library(tidyverse)
library(readxl)
library(parallel)
library(openxlsx)
library(lubridate)
options(scipen = 999)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run STAGE 1 - Baseline Estimation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User Inputs

# COMMAND ----------

# MAGIC %r
# MAGIC user     <- "nayan"                        #3 users were modelling at the same time different categories
# MAGIC run_type <- "initial"                  #custom_run #initial if we have changes after inital model qc - we call modified; if you want to run custom runs for 
# MAGIC                                        #select item x clusters with individual dummies to fix model choose modified    
# MAGIC category <- "fl_marlboro_multi_v0"              #for the sake of printing file name & reading same name as user input file
# MAGIC version  <-  category                      #to keep track of modelling version 
# MAGIC levelvar <- 0.1                        #parameter used to control how much variance of dependent ucm should capture
# MAGIC erase_old_output <- T                  #if we want to keep or replace output from old run for same version
# MAGIC remove_weeks <- c("2099-12-31")           #provision to remove incomplete  weeks especially at the beginning & end of history from modelling data
# MAGIC print("Run Specs..")
# MAGIC print(paste0("User:",user,"||version:",version,"||levelvar:",levelvar))

# COMMAND ----------

# MAGIC %md
# MAGIC #### BU Specific Variables

# COMMAND ----------

wave <- "Wave_1"
bu <- "FL_Repo"                 # GC_repo #SE_repo
item_indt_1 <- "PRODUCT_KEY"    #PRODUCT_KEY #TRN_ITEM_SYS_ID
item_indt_2 <- "upc" #upc #item_number
item_indt_3 <- "item_desc" #item_desc #item_name
item_indt_4 <- "category_desc" #category_desc #item_category
item_indt_2_temp <- "UPC" #UPC #JDE_NUMBER
hierarchy_cols <- c( "nacs_category_desc", "sub_category_desc", "department_desc")
item_indt_1_prod <- "product_key"
ADS_loc <- "modelling_ads_cluster_stateless_zone_new_MARLBORO.Rds"

# COMMAND ----------

# MAGIC %md
# MAGIC # MODELLING

# COMMAND ----------

# MAGIC %md
# MAGIC ##### source codes

# COMMAND ----------

# MAGIC %run "/Localized-Pricing/Phase_3/Store_Item_Elasticity/Wave_1/Master_Setup/Functions/ucm_functions.R_final"

# COMMAND ----------

# MAGIC %run "/Localized-Pricing/Phase_3/Store_Item_Elasticity/Wave_1/Master_Setup/Functions/do_parallel.R_final"

# COMMAND ----------

# MAGIC %run "/Localized-Pricing/Phase_3/Store_Item_Elasticity/Wave_1/Master_Setup/Functions/core_functions.R_final"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User Defined Functions

# COMMAND ----------

# MAGIC %r
# MAGIC print("Specify Location to Modelling ADS...")
# MAGIC #Location to Modelling ADS
# MAGIC datasource <- paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Modelling_Inputs/",ADS_loc)
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
# MAGIC   
# MAGIC     
# MAGIC   
# MAGIC   
# MAGIC   return(model_data)
# MAGIC }
# MAGIC  
# MAGIC 
# MAGIC print("Custom Function for SKU level data registered...")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Decide Category Overall Seasonal Dummy & Check Remove Weeks as well

# COMMAND ----------

category_check <- "017-ALTERNATIVE SNACKS"

# COMMAND ----------

cat_sum <- c()
product_map <- readRDS(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Modelling_Inputs/product_map.Rds"))%>%mutate(!!item_indt_1_prod := as.numeric(get(item_indt_1_prod)))
calendar_dat <- readRDS(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Modelling_Inputs/calendar.Rds"))%>%
                  mutate(fiscal_year = year(week_start_date))%>%
                  select(fiscal_year,week_start_date, Week_No)%>%distinct()


cat_sum <- readRDS(paste0(datasource))%>%
          dplyr::left_join(product_map%>%dplyr::select(c(item_indt_1_prod,item_indt_4)),
                           c(setNames(item_indt_1_prod ,"itemid")))%>%
          left_join(calendar_dat,c("weekdate" = "week_start_date"))%>%
          filter(get(item_indt_4) == category_check)%>%
#           mutate(cluster = parse_number(i))%>%
          group_by(Week_No,fiscal_year)%>%
          summarise(sales_qty = sum(sales_qty))%>%ungroup()#%>%
#           spread(fiscal_year,sales_qty)
          
cat_sum[is.na(cat_sum)] <- 0
display(cat_sum)

# COMMAND ----------

# To take care of it 
cat_seasonal_dummy_num <- c(47,31)  #there needs to be atleast one weekly seasonal dummy for code to function
cat_indiv_week_dummy_num <- c('13_2018')      #48_2019
cat_indiv_month_dummy_num <- c('')

cat_seasonal_dummy <- paste0("WEEK_DUMMY_",cat_seasonal_dummy_num)
cat_indiv_week_dummy <- paste0("INDIV_DUMMY_",cat_indiv_week_dummy_num)
cat_indiv_month_dummy <- paste0("INDIV_MONTH_DUMMY_",cat_indiv_month_dummy_num)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Main Code

# COMMAND ----------

# MAGIC %r
# MAGIC #Logging Locations
# MAGIC 
# MAGIC log_loc_lvl1 = paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Log/lvl1_log/lvl1_log_",user,"_v",version,".txt")
# MAGIC log_loc_lvl2 = paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Log/lvl2_log/lvl2_log_",user,"_v",version,".txt")
# MAGIC nodal_log = paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version,"/")
# MAGIC 
# MAGIC #File writing Locations
# MAGIC 
# MAGIC # if(erase_old_output == T ){
# MAGIC #   print("Creating Folders for writing files...")
# MAGIC #   generate_user_version_folders(user,version,bu)
# MAGIC   
# MAGIC #   #Clearing Old Logs
# MAGIC   
# MAGIC #   # print("Clearing Old Logs...")
# MAGIC #   close(file(paste0(paste0(log_loc_lvl1)), open="w" ) )
# MAGIC #   close(file(paste0(paste0(log_loc_lvl2)), open="w" ) )
# MAGIC #   invisible(do.call(file.remove, list(list.files(nodal_log, full.names = TRUE))))
# MAGIC # }
# MAGIC 
# MAGIC 
# MAGIC pval = 1  #unused parameter but kept for sake of code sanity
# MAGIC non_baseline_vars = c(
# MAGIC   "DISCOUNT_PERC"
# MAGIC )
# MAGIC 
# MAGIC print("Inscope SKU selected from input....")
# MAGIC #Defining Scope from excel
# MAGIC scope <-     read_excel(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Input/modelling_input_",user,"_",category,".xlsx"), sheet = "scope_file")
# MAGIC 
# MAGIC print("Expected Signs registered from modelling input...")
# MAGIC #Recording Sign for variables
# MAGIC sign_file <- read_excel(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Input/modelling_input_",user,"_",category,".xlsx"), sheet = "Signs")
# MAGIC 
# MAGIC print("Independant Variables registered from modelling input...")
# MAGIC baseline_variables_data <- read_excel(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Input/modelling_input_",user,"_",category,".xlsx"),sheet = "Variables") 
# MAGIC mixed_variables_data <- read_excel(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Input/modelling_input_",user,"_",category,".xlsx"),sheet = "Mixed_Input") 
# MAGIC customize_data <- read_excel(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Input/modelling_input_",user,"_",category,".xlsx"),sheet = "Customize_Data") 
# MAGIC 
# MAGIC custom_run_variables <- read_excel(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Input/modelling_input_",user,"_",category,".xlsx"), sheet = "Variables_custom")
# MAGIC print("Initiating Modelling...")

# COMMAND ----------

# DBTITLE 1,Which items are selected to be modelled - according to model input sheet
display(scope%>%filter(STATUS == "Y"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue Debug from do_parallel_final code

# COMMAND ----------

# DBTITLE 1,Select an SKU to test with other params
#user input
sku_list <- 633905           #need to put sku number here 3610097_3
sku <- sku_list

# COMMAND ----------

# DBTITLE 1, other params
mape_cutoff <- 0.1
sigma_cutoff <- 2
correction_cutoff <- 0.2
lvl2_function = parallel_lvl2_run
user = user
version = version
datasource = datasource
closed_stores = c()
sign_file = sign_file
baseline_variables_data = baseline_variables_data
mixed_variables_data = mixed_variables_data
custom_run_variables = custom_run_variables
custom_data_func = custom_data_func
non_baseline_vars = non_baseline_vars
support_functions_baseline = paste0("/Localized-Pricing/Phase_3/Store_Item_Elasticity/",wave,"/Master_Setup/Functions/ucm_functions_final.R")
support_functions_mixed = paste0("/Localized-Pricing/Phase_3/Store_Item_Elasticity/",wave,"/Master_Setup/Functions/mixed_functions_final.R")
scope = scope
customize_data=customize_data
pval = pval
levelvar = levelvar
lvl2_function = parallel_lvl2_run
lvl_1_cores = 20
lvl_2_cores = 5
log_loc_lvl1 = log_loc_lvl1
log_loc_lvl2 = log_loc_lvl2
remove_weeks = remove_weeks
run_type = run_type
bu=bu
wave = wave

# COMMAND ----------

# DBTITLE 1,Check if level 2 (cluster wise) function is running for all clusters for that item
lapply(sku_list, function(x) { #parLapply(cl, 
    
    library(tidyverse)
    library(openxlsx)
    library(parallel)
    library(trustOptim)
    library(numDeriv)
    
#     source("/Localized-Pricing/store_item_elasticity/Functions/do_parallel.R")
#     source("/Localized-Pricing/store_item_elasticity/Functions/core_functions.R")
    
    cat(paste0("Initiating LVL1 loop for SKU,",x ,"\n"),file=paste0(log_loc_lvl1), append=TRUE)
    start_time = Sys.time()
    
    lvl2_function(
      closed_stores = closed_stores,
      user = user,
      version = version,
      datasource = datasource,
      sku = x,
      pval = pval,
      levelvar = levelvar,
      scope = scope,
      customize_data,
      core_function_baseline = generate_ucm_baseline,
      core_function_mixed = generate_mixed_model,
      core_function_baseprice = bp_elasticty_simulation,
      sku_list = sku_list,
      log_loc_lvl1 = log_loc_lvl1,
      log_loc_lvl2 = log_loc_lvl2,
      nodal_log = "dummy",
      sales_qty = "SALES_QTY",
      revenue = "REVENUE",
      reg_price = "REG_PRICE",
      itemid     = "itemid",
      weekdate   = "weekdate",
      sign_file = sign_file,
      baseline_variables_data = baseline_variables_data,
      mixed_variables_data = mixed_variables_data,
      custom_function = custom_data_func,
      non_baseline_vars = non_baseline_vars,
      support_functions_baseline = support_functions_baseline,
      support_functions_mixed = support_functions_mixed,
      threads = lvl_2_cores,
      remove_weeks = remove_weeks,
      run_type = run_type,
      custom_run_variables = custom_run_variables,
      bu=bu,
      cat_seasonal_dummy = cat_seasonal_dummy,
      cat_indiv_week_dummy = cat_indiv_week_dummy,
      cat_indiv_month_dummy = cat_indiv_month_dummy,
      wave = wave
      
    )
  })

# COMMAND ----------

# DBTITLE 1,Check if all clusters have run
test <- readRDS(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/baseline_estimates_",sku,".Rds"))
unique(test$STORE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drilling down to now a single cluster to if not all clusters have run from above part

# COMMAND ----------

display(readRDS(datasource)%>%filter(itemid == 633905)%>%select(Cluster)%>%distinct())

# COMMAND ----------

# DBTITLE 1,Input Cluster
#choose cluster
x <- "1_2"

# COMMAND ----------

# DBTITLE 1,Drilling down to now a single cluster to if not all clusters have run from above part
core_function_baseline = generate_ucm_baseline
core_function_mixed = generate_mixed_model
core_function_baseprice = bp_elasticty_simulation
nodal_log = "dummy"
sales_qty = "SALES_QTY"
revenue = "REVENUE"
reg_price = "REG_PRICE"
itemid     = "itemid"
weekdate   = "weekdate"
sign_file = sign_file
baseline_variables_data = baseline_variables_data
mixed_variables_data = mixed_variables_data
custom_function = custom_data_func
non_baseline_vars = non_baseline_vars
support_functions_baseline = support_functions_baseline
support_functions_mixed = support_functions_mixed
threads = lvl_2_cores
remove_weeks = remove_weeks
run_type = run_type
custom_run_variables = custom_run_variables
bu=bu

# COMMAND ----------

# DBTITLE 1,Run for 1 cluster for item and see where the error is
 library(tidyverse)
      library(stringr)
      library(openxlsx)
      library(trustOptim)
      library(numDeriv)
      
      
      
      cat(paste0("Initiating LVL2 loop for,","SKU:",sku,",STORE:",x ,"\n"),file=paste0(log_loc_lvl2), append=TRUE)
      
      cat(paste0("Initiating Data Prep for,","SKU:",sku,",STORE:",x ,"\n"),file=paste0(log_loc_lvl2), append=TRUE)
      
      
      source_data <- readRDS(paste0(datasource))%>%
         filter(Cluster == x)
      
      source_data[is.na(source_data)] <- 0
      
      max_date <- max(source_data$weekdate)
      source_data <- source_data%>%mutate(MAX_DATE_COMP = max_date)
      
      source_data <- source_data%>%
        filter((!!as.name(itemid)) == sku)%>%
        filter(!(weekdate %in% as.Date(remove_weeks,"%Y-%m-%d")))
  
      
      source_data[is.na(source_data)] <- 0
      source_data <- source_data%>%mutate(ln_sales_qty = ifelse(is.infinite(ln_sales_qty),0,ln_sales_qty))
      

      colnames(source_data) <- toupper(colnames(source_data))
      # check if modeller wants to run custom run with custom vars for item x cluster
      
      if(run_type == 'custom_run'){
        baseline_variables_data <- custom_run_variables %>% filter(itemid == sku & cluster == x) %>% select(-itemid,-cluster)
      }else{
        baseline_variables_data <- baseline_variables_data
      }
      
#       baseline_variables_data <- c(baseline_variables_data,cat_seasonal_dummy)  #cat level vars
      # #Align with Legacy Variable baseline estimation
      # 
      source_data <- source_data %>%
        rename(LEVEL1_IDNT = ITEMID,
               WEEK_SDATE = WEEKDATE,
               STORE = CLUSTER
        ) %>%
#         mutate(STORE = parse_number(x)) %>% 
        ungroup()
      
      ###section for Indiv dummies not seasonal
      indiv_vars <- grep("INDIV",baseline_variables_data$INDEPENDENT, value = T) #weekly
      if(cat_indiv_week_dummy != "INDIV_DUMMY_"){
            indiv_vars <- c(indiv_vars,cat_indiv_week_dummy)  #add cat level vars
        }else{
         cat_indiv_week_dummy =c()
       }
      indiv_month_vars <- grep("INDIV_MONTH",baseline_variables_data$INDEPENDENT, value = T)   #monthly
       if(cat_indiv_month_dummy != "INDIV_MONTH_DUMMY_"){
          indiv_month_vars <- c(indiv_month_vars,cat_indiv_month_dummy) #add cat level vars
       }else{
         cat_indiv_month_dummy =c()
       }
      
      library(lubridate)
      calendar_dat <- readRDS(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Modelling_Inputs/calendar.Rds"))%>%
                          mutate(fiscal_year = year(week_start_date))%>%
                          mutate(fiscal_month = month(week_start_date))%>%  
                          select(fiscal_year,fiscal_month,week_start_date, Week_No)%>%distinct()
      source_data <- source_data %>% left_join(calendar_dat, c("WEEK_SDATE" = "week_start_date"))

      #create individual weekly dummies from input sheet
      if(length(indiv_vars) > 0){
        for(i in indiv_vars){
          Weekz <- as.numeric(unlist(strsplit(i,"_"))[[3]])
          Yearz <- as.numeric(unlist(strsplit(i,"_"))[[4]])
          source_data <- source_data %>%
                 mutate( !!i := ifelse(fiscal_year == Yearz & Week_No == Weekz, 1,0 ))

        }
      }
      
      #create individual monthly dummies from input shet
      if(length(indiv_month_vars) > 0){
        for(j in indiv_month_vars){
          Monthz <- as.numeric(unlist(strsplit(j,"_"))[[4]])
          Yearz <- as.numeric(unlist(strsplit(j,"_"))[[5]])
          source_data <- source_data %>%
                 mutate( !!j := ifelse(fiscal_year == Yearz & fiscal_month == Monthz, 1,0 ))

        }
      }
      
      #create dummies for category dummies assigned by modeller in the main sheet
      
      
      
      source_data <- source_data%>%
                 select(-fiscal_year,-fiscal_month, -Week_No)%>%
      mutate(ISSUE_COMMENT = 'None',
             REGULAR_PRICE_FLAG = "Good",
             SALES_FLAG = "Good")
#       source_data <- source_data%>%
#                  select(-fiscal_year, -Week_No)
      
      ############################ reg price issues fix####################################
      
      #No Price Change by sd = 0 & also create # of price change variable & also consecutive weeks on same price metric as well
      source_data <- source_data %>% arrange(WEEK_SDATE)%>%
                       mutate(IMPUTE_FLAG = ifelse(sd(REG_PRICE) == 0, "Impute", "No Impute"))%>%
#                               REGULAR_PRICE_FLAG = ifelse(IMPUTE_FLAG == "Impute","Issue","Good"))%>%
                       mutate(IMPUTE_COMMENT = ifelse(sd(REG_PRICE) == 0, "No Price Change", "None"))%>%
                       mutate(LAG_PRICE = lag(REG_PRICE))%>%
                       mutate(CHANGE_IND = ifelse((REG_PRICE == LAG_PRICE) | (is.na(LAG_PRICE)),0,1))%>%
                       mutate(PRICE_REGIME = cumsum(CHANGE_IND))%>%
                       group_by(PRICE_REGIME)%>%
                       mutate(WEEKS_ON_PRICE = n())%>%ungroup()%>%
#                        mutate(REGULAR_PRICE_FLAG = ifelse(min(WEEKS_ON_PRICE) <= 2, "Issue", REGULAR_PRICE_FLAG))%>%
#                        mutate(ISSUE_COMMENT = ifelse(WEEKS_ON_PRICE <= 2, "Price not live long enough", "None"))%>%
                       mutate(NUMBER_PRICE_CHANGE = sum(CHANGE_IND))%>%
                       mutate(row_on = row_number(),
                              tot_row = n())%>%
                       mutate(RECENCY_IND = ifelse((row_on <= 12) & (dplyr::n_distinct(year(WEEK_SDATE))) > 1,"Past_Qtr",
                                                   ifelse((tot_row - row_on < 12 ), "Recent_Qtr","Between")))

      #add impute comment for cases where number of these non > 2 week price regime is more than 12 (3 quarters)
      check_too_many_price_changes <- nrow(source_data%>%filter(WEEKS_ON_PRICE <= 3)%>%select(PRICE_REGIME)%>%unique())
      source_data <- source_data%>%
                      mutate(REGULAR_PRICE_FLAG = ifelse(check_too_many_price_changes > 12, "Issue", REGULAR_PRICE_FLAG),
                            ISSUE_COMMENT = ifelse(check_too_many_price_changes > 12, "Too much fluctuation in price",ISSUE_COMMENT))#%>%
                      # add in flag if there are more than 5 price regime fixes you need to do to review as issue
#                      mutate(REGULAR_PRICE_FLAG = ifelse(check_too_many_price_changes > 12, "Issue", "Good"),
#                             ISSUE_COMMENT = ifelse(check_too_many_price_changes > 12, " had to do 12 price fluctuation fix","None"))
      #smoothen the prices where there are <= 2 week's
      
      
      #only keep unique price regimes
      price_regimes <- source_data%>%dplyr::select(REG_PRICE, PRICE_REGIME, WEEKS_ON_PRICE)%>%unique()%>%dplyr::arrange(PRICE_REGIME)
      stable_regimes <- price_regimes%>%dplyr::filter(WEEKS_ON_PRICE > 3)
      to_fix_regimes <- sort(unique((price_regimes%>%dplyr::filter(WEEKS_ON_PRICE <= 3))$PRICE_REGIME))
      to_fix_regimes_padded <- c(to_fix_regimes,rep(0,length(price_regimes$PRICE_REGIME)-length(to_fix_regimes)))
      #if continuous 5 weeks or more are having issues from the beginning then truncate that out
      #if continuous 5 weeks or more are having issues from the beginning then truncate that out
      if(length(to_fix_regimes) >=1){
            conti_issue_till <- max(which(to_fix_regimes_padded - (price_regimes$PRICE_REGIME) == 0))
            if(is.infinite(conti_issue_till) == FALSE ){
                remove_issue_weeks <- sum((price_regimes%>%head(conti_issue_till))$WEEKS_ON_PRICE)
              }else{
              remove_issue_weeks <- 0
            }
        }else{
        remove_issue_weeks <- 0
       }
      
      if(nrow(stable_regimes) == 0){
         source_data$REGULAR_PRICE_FLAG <- "Good"
         source_data$IMPUTE_FLAG <- "Impute"
         source_data$IMPUTE_COMMENT <- "No single stable price in data"
#          source_data <- source_data %>%
#                          mutate(FIXED_PRICE = REG_PRICE)
      }else{  

            ## add in flag if the latest price is not stable
            if((max(price_regimes$PRICE_REGIME) != max(stable_regimes$PRICE_REGIME))){
                source_data$REGULAR_PRICE_FLAG <- "Issue"
                source_data$ISSUE_COMMENT <- "Latest Price not stable"
            }

#             if(length(to_fix_regimes) >=1){
#               fixed_prices_dat <- bind_rows(lapply(to_fix_regimes, function(x) {
#       #           print(x)
#                 price_fix_function(x,stable_regimes, price_regimes,1)
#               }))
#               source_data <- source_data %>%left_join(fixed_prices_dat)%>%
#                              mutate(FIXED_PRICE = ifelse(is.na(FIXED_PRICE), REG_PRICE, FIXED_PRICE))
#             }else{
#               source_data <- source_data %>%
#                                mutate(FIXED_PRICE = REG_PRICE)
#              }

            #check if things have been fixed or not - if not then try the distance method

            #check again based on fixed price if all anomalies have not been fixed
#              check3 <- source_data %>%mutate(LAG_PRICE = lag(FIXED_PRICE))%>%
#                              mutate(CHANGE_IND = ifelse((FIXED_PRICE == LAG_PRICE) | (is.na(LAG_PRICE)),0,1))%>%
#                              mutate(PRICE_REGIME = cumsum(CHANGE_IND))%>%
#                              group_by(PRICE_REGIME)%>%
#                              mutate(WEEKS_ON_PRICE = n())%>%ungroup()%>%
#                              mutate(REG_PRICE = FIXED_PRICE)

#             if(min(check3$WEEKS_ON_PRICE) <= 3){
#               price_regimes <- check3%>%select(REG_PRICE, PRICE_REGIME, WEEKS_ON_PRICE)%>%unique()%>%arrange(PRICE_REGIME)
#               stable_regimes <- price_regimes%>%filter(WEEKS_ON_PRICE > 3)
#               to_fix_regimes <- unique((price_regimes%>%filter(WEEKS_ON_PRICE <= 3))$PRICE_REGIME)

#               fixed_prices_dat2 <- bind_rows(lapply(to_fix_regimes, function(x) {
#               #           print(x)
#                 price_fix_function(x,stable_regimes, price_regimes,2)
#               }))
#               temp_price <- check3 %>%left_join(fixed_prices_dat2%>%rename(FIXED_PRICE2 = FIXED_PRICE))%>%
#                              mutate(FIXED_PRICE2 = ifelse(is.na(FIXED_PRICE2), FIXED_PRICE, FIXED_PRICE2))%>%select(WEEK_SDATE, FIXED_PRICE2)
#               source_data <- source_data %>% left_join(temp_price)%>%
#                              mutate(FIXED_PRICE = ifelse(is.na(FIXED_PRICE2), FIXED_PRICE, FIXED_PRICE2))

#               }
        
        

        # generating flags
        
           #case : when Single Price Change in history
           single_price_change <- source_data%>%mutate(lag_P = lag(REG_PRICE))%>%mutate(change_D = ifelse(REG_PRICE != lag_P, 1, 0)) 
      #       print(length(unique(source_data$FIXED_PRICE))-1)
            if(sum(single_price_change$change_D,na.rm =T) == 1){
                check <- source_data%>%mutate(LAG_PRICE = lag(REG_PRICE))%>%
                             mutate(CHANGE_IND = ifelse((REG_PRICE == LAG_PRICE) | (is.na(LAG_PRICE)),0,1))%>%
                             filter(CHANGE_IND == 1)
#               print(check$RECENCY_IND)
                if(check$RECENCY_IND == "Past_Qtr"){

                  source_data$IMPUTE_FLAG <- "Impute"
                  source_data$IMPUTE_COMMENT <- "Only Price Change too far in past"
                }else{
#                   print(check$RECENCY_IND)
                  if(check$RECENCY_IND == "Recent_Qtr"){                #Truncated to recent 52 weeks

                    source_data$REGULAR_PRICE_FLAG <- "Issue"    #only alerting
                    
                    source_data$ISSUE_COMMENT <- "Might want to Truncate Series due to very recent single price change"   
                  }#else{
#                     source_data$IMPUTE_FLAG <- "No Impute"
#                     source_data$IMPUTE_COMMENT <- "None"
#                   }
                }
              
              check_single <- source_data%>%
                        mutate(LAG_PRICE = lag(REG_PRICE))%>%
                        mutate(abs_change = abs(REG_PRICE - LAG_PRICE),
                               perc_change = abs_change/REG_PRICE)%>%
                        mutate(flag = ifelse((abs_change > 0.50) & (perc_change > 0.2),1,0))
               check_single[is.na(check_single)] <- 0
              
              if(sum(check_single$flag)>0){
      #           print(sum(check2$flag))
                source_data$REGULAR_PRICE_FLAG <- "Issue"
                source_data$ISSUE_COMMENT <- "Has > 20% price change"
              }

              # for more than 1 price change cases           
            }else{
              #again do SD basis no price change flag based on fixed series
              source_data <- source_data %>% arrange(WEEK_SDATE)%>%
                             mutate(IMPUTE_FLAG = ifelse(sd(REG_PRICE) == 0, "Impute", IMPUTE_FLAG))%>% 
                             mutate(IMPUTE_COMMENT = ifelse(sd(REG_PRICE) == 0, "No Price Change", IMPUTE_COMMENT))

              # for price change more than 20% from regime to regime create a flag for a double check
              check2 <- stable_regimes%>%
                        mutate(LAG_PRICE = lag(REG_PRICE))%>%
                        mutate(abs_change = abs(REG_PRICE - LAG_PRICE),
                               perc_change = abs_change/REG_PRICE)%>%
                        mutate(flag = ifelse((abs_change > 0.50) & (perc_change > 0.2),1,0))
               check2[is.na(check2)] <- 0
              if(sum(check2$flag)>0){
      #           print(sum(check2$flag))
                source_data$REGULAR_PRICE_FLAG <- "Issue"
                source_data$ISSUE_COMMENT <- "Has > 20% price change"
              }
            }
    }
      
      #check again based on fixed price if all anomalies have not been fixed
       check5 <- source_data %>%mutate(LAG_PRICE = lag(REG_PRICE))%>%
                       mutate(CHANGE_IND = ifelse((REG_PRICE == LAG_PRICE) | (is.na(LAG_PRICE)),0,1))%>%
                       mutate(PRICE_REGIME = cumsum(CHANGE_IND))%>%
                       group_by(PRICE_REGIME)%>%
                       mutate(WEEKS_ON_PRICE = n())%>%ungroup()
      
      if(min(check5$WEEKS_ON_PRICE) <= 3){
        source_data$REGULAR_PRICE_FLAG <- "Issue"
        source_data <- source_data %>% mutate(ISSUE_COMMENT = ifelse( ISSUE_COMMENT == 'None', "unstable prices exist in series", paste0(ISSUE_COMMENT," ,unstable prices exist in series")) )
      }
      
      # making the series truncated if the initial continuous unstable prices are more than 5 regimes
#       source_data <- source_data %>%
#                    tail(nrow(source_data) - remove_issue_weeks)
      
           
#       independent = c(baseline_variables_data$INDEPENDENT,cat_seasonal_dummy)
      cat(paste0("Initiating Baseline Model for,","SKU:",sku,",STORE:",x ,"\n"),file=paste0(log_loc_lvl2), append=TRUE)
      
      ### UCM model
      baseline_modelled <- generate_ucm_baseline(
        source_data = source_data,
        store_number = x,
        pval = pval,
        levelvar = levelvar,
        sku = sku,
        customize_data=customize_data,
        log_loc = log_loc_lvl2,
        dependent = baseline_variables_data$DEPENDENT[1],
        reg_price = reg_price,
        itemid = itemid,
        weekdate = weekdate,
        sign_file = sign_file,
        independent = c(baseline_variables_data$INDEPENDENT,cat_seasonal_dummy, cat_indiv_week_dummy, cat_indiv_month_dummy),
        custom_function = custom_function,
        non_baseline_vars = non_baseline_vars,
        support_functions = support_functions_baseline
      )
      
      #check if the overll item x cluster level model mape is > 10%
      full_out  <- baseline_modelled[[1]]
#       cat(paste0("Issue struct,","SKU:",sku,",STORE:",type(full_out) ,"\n"),file=paste0(log_loc_lvl2), append=TRUE)
#       print(str(full_out))
#       print(length(baseline_modelled))
#       print(baseline_modelled)

  #make sure we dont put too much dummy
      check_num_dum <- full_out%>%select(OUTLIER_DUMMY,CORRECTION_DUMMY)%>%
                       mutate(FINAL_FIX_DUMMY = ifelse((OUTLIER_DUMMY == 1)|(CORRECTION_DUMMY == 1),1,0))
      check_num_dum <- sum(check_num_dum$FINAL_FIX_DUMMY)

      if((is.data.frame(full_out) == TRUE) & check_num_dum < nrow(full_out)){
          if((unique(full_out$MAPE_UCM) >= mape_cutoff) & (full_out$IMPUTE_COMMENT != "Not enough valid sales to model") & nrow(full_out>0)){
            #update source_data with these dummy_cols
            source_data <- source_data%>%
                         left_join(calendar_dat, c("WEEK_SDATE" = "week_start_date"))%>%
                         left_join(full_out%>%select(ITEMID, WEEKDATE, OUTLIER_DUMMY,CORRECTION_DUMMY), c( "LEVEL1_IDNT"="ITEMID", "WEEK_SDATE" = "WEEKDATE"))%>%
                         mutate(OUTLIER_DUMMY = ifelse(is.na(OUTLIER_DUMMY),0,OUTLIER_DUMMY),
                                CORRECTION_DUMMY = ifelse(is.na(CORRECTION_DUMMY),0,CORRECTION_DUMMY))%>%
                         mutate(FINAL_FIX_DUMMY_NAME = paste0("FIX_DUMMY_",Week_No,"_",fiscal_year))%>%
                         mutate(FINAL_FIX_DUMMY = ifelse((OUTLIER_DUMMY == 1)|(CORRECTION_DUMMY == 1),1,0))%>%
                         mutate(FINAL_FIX_DUMMY_NAME = ifelse(FINAL_FIX_DUMMY == 1, FINAL_FIX_DUMMY_NAME, "Useless"))%>%
                         select(-Week_No, -fiscal_year)%>%
                         spread(FINAL_FIX_DUMMY_NAME,FINAL_FIX_DUMMY)%>%
                         select(-Useless)
            
            source_data[is.na(source_data)] <- 0
            
            #select only those fix dummies which don't have 
                         
            #update independent
            independent <- c(baseline_variables_data$INDEPENDENT,grep("FIX_DUMMY",names(source_data),value =T),cat_seasonal_dummy,cat_indiv_week_dummy, cat_indiv_month_dummy)
            #rerun
            baseline_modelled <- generate_ucm_baseline(
                    source_data = source_data,
                    store_number = x,
                    pval = pval,
                    levelvar = levelvar,
                    sku = sku,
                    customize_data=customize_data,
                    log_loc = log_loc_lvl2,
                    dependent = baseline_variables_data$DEPENDENT[1],
                    reg_price = reg_price,
                    itemid = itemid,
                    weekdate = weekdate,
                    sign_file = sign_file,
                    independent = independent,
                    custom_function = custom_function,
                    non_baseline_vars = non_baseline_vars,
                    support_functions = support_functions_baseline)
          }else{
            baseline_modelled <- baseline_modelled
          }
      }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill Inside UCM Baseline Generation Func - Core_functions.R_final

# COMMAND ----------

# DBTITLE 1,If error is within generate UCM func - then fill params to drill to that level
source_data = source_data
store_number = parse_number(x)
pval = pval
levelvar = levelvar
sku = sku
customize_data=customize_data
log_loc = log_loc_lvl2
dependent = baseline_variables_data$DEPENDENT[1]
reg_price = reg_price
itemid = itemid
weekdate = weekdate
sign_file = sign_file
independent =  c(baseline_variables_data$INDEPENDENT,grep("FIX_DUMMY",names(source_data),value =T),cat_seasonal_dummy,cat_indiv_week_dummy, cat_indiv_month_dummy)
custom_function = custom_function
non_baseline_vars = non_baseline_vars
support_functions = support_functions_baseline

# COMMAND ----------

# DBTITLE 1,Check data_prep func - under ucm_functions.R_final
processed_data<- data_prep(source_data,sku)

# COMMAND ----------

# DBTITLE 1,Check custom_function - under from modelling driver code cmd 16 -17
independent <- c(independent,grep("FIX_DUMMY",names(processed_data),value =T))   #fix due to stock-outs & other sales prediction fix
    
print(paste0("Log_Baseline:Customize Data,","Store:",store_number,",SKU:",sku ,"\n"))
processed_data <- processed_data %>%
  rename(ITEMID=LEVEL1_IDNT,
        WEEKDATE =WEEK_SDATE
  ) 
customized_data <- processed_data
customized_data <- custom_function(processed_data,customize_data) 
customized_data <- customized_data %>%
  rename(LEVEL1_IDNT=ITEMID,
         WEEK_SDATE = WEEKDATE
  ) 

# COMMAND ----------

# DBTITLE 1,Check independent_processing func - under ucm_functions.R_final
independant_selected <-  independent_processing(customized_data,independent)

# COMMAND ----------

independant_selected

# COMMAND ----------

# DBTITLE 1,Check Rest of Baseline Creation Function
if(length(independant_selected) >0){
      
      print(paste0("Log_Baseline:Run Full UCM,","Store:",store_number,",SKU:",sku ,"\n"))
      ucm_result <- ucm_function(customized_data,independant_selected,dependent,levelvar)
      ucm_estimates <- ucm_estimates_func(ucm_result)
      
      if(nrow(ucm_estimates) >0){
        
        print(paste0("Log_Baseline:Flagging,","Store:",store_number,",SKU:",sku ,"\n"))
        flagged<-flagging(ucm_estimates,sign_file)
        
        print(paste0("Log_Baseline:Beginining Validated Run,","Store:",store_number,",SKU:",sku ,"\n"))
        validated_ucm <- invisible(validated_variables(customized_data,dependent,levelvar,flagged,pval,sign_file))
        validated_ucm_estimates <-ucm_estimates_func(validated_ucm)
        
        print(paste0("Log_Baseline:Completed Validated Run,","Store:",store_number,",SKU:",sku ,"\n"))
        
        print(paste0("Log_Baseline:Create Baselines,","Store:",store_number,",SKU:",sku ,"\n"))
        data_baseline_predicted <- base_predict(validated_ucm,customized_data)
        
        print(paste0("Log_Baseline:Calculate Contribution,","Store:",store_number,",SKU:",sku ,"\n"))
        with_contri <- contribution_stage_1(validated_ucm_estimates,data_baseline_predicted,non_baseline_vars,reg_price)
        
        print(paste0("Log_Baseline:Baseline Output,","Store:",store_number,",SKU:",sku ,"\n"))
        regression_output <- regression(with_contri,validated_ucm_estimates,non_baseline_vars,reg_price)
        
        validated_est <- validated_ucm_estimates %>%
          mutate(ITEMID = sku,STORE = store_number)
        
        contri_cols = colnames(regression_output)[grepl("cont_",colnames(regression_output))]
#         print(customized_data)
        
        full_output <- customized_data %>% 
          left_join(regression_output %>% 
                      select_(.dots = c("LEVEL1_IDNT" ,"WEEK_SDATE",contri_cols,"S_LEVEL","UCM_BASELINE","BASELINE_SALES","PREDICTED_SALES","PREDICTED_FINAL_REV","BASELINE_REV")),
                    c("LEVEL1_IDNT", "WEEK_SDATE"))
        
       #prediction improvement 
        full_output <- full_output %>%
         rename(ITEMID = LEVEL1_IDNT ,
                 WEEKDATE = WEEK_SDATE)%>%
         mutate(perc_abs_error = abs(PREDICTED_FINAL_REV - SALES_QTY)/SALES_QTY)%>%
         mutate(MAPE_UCM = mean(perc_abs_error))%>%
         mutate(mean_series = mean(SALES_QTY),
                sigma_series = sd(SALES_QTY))%>%
         mutate(OUTLIER_DUMMY = ifelse((SALES_QTY > (mean_series + sigma_cutoff*sigma_series)) |
                                      (SALES_QTY < (mean_series - sigma_cutoff*sigma_series)),1,0))%>%
         mutate(CORRECTION_DUMMY_0 = ifelse(perc_abs_error >= correction_cutoff, 1, 0))%>%
         group_by(CORRECTION_DUMMY_0)%>%
         mutate(error_rank = rank(desc(perc_abs_error)))%>%ungroup()%>%
         mutate(sum_fix = sum(CORRECTION_DUMMY_0) + sum(OUTLIER_DUMMY))
        
        if(full_output$sum_fix > 20){
            full_output <- full_output%>%
            mutate(CORRECTION_DUMMY = ifelse(error_rank <= (20 - sum(OUTLIER_DUMMY)) & (CORRECTION_DUMMY_0 == 1),1,0))%>%
            select(-sum_fix,-CORRECTION_DUMMY_0)
          }else{
            full_output <- full_output%>%
            mutate(CORRECTION_DUMMY = CORRECTION_DUMMY_0)%>%
            select(-sum_fix,-CORRECTION_DUMMY_0)
          }
        full_output <- full_output%>%mutate(UCM_PREDICTION_FLAG = ifelse(MAPE_UCM >= 0.3, "Issue","Good"))   
        #check smoothness of Baseline
        firstD <- diff(full_output$UCM_BASELINE)
        normFirstD <- (firstD - mean(firstD)) / sd(firstD)
        roughness <- (diff(normFirstD) ** 2) / 4
        rough_coef <- mean(roughness)
        full_output <- full_output %>% 
            mutate(ROUGH_COEF = rough_coef,
                   UCM_SMOOTH_FLAG = ifelse((ROUGH_COEF > 0.15) | (ROUGH_COEF < 0.005), "Issue","Good")) #lowe and upper bound given
        #normal distri qc
        if(nrow(processed_data)<30){
#           full_output$SALES_FLAG <- "Issue"
          full_output$IMPUTE_FLAG <- "Impute"
          full_output$IMPUTE_COMMENT <- "Not enough valid sales to model"
          
        }else{
          full_output <- full_output
        }
        
        if((sum(full_output$CORRECTION_DUMMY)+ sum(full_output$OUTLIER_DUMMY))> 20){
          full_output$SALES_FLAG <- "Issue"
          full_output$ISSUE_COMMENT <- "More than 20 correction dummy used"
        }
        
         full_output <- full_output%>%
              mutate(IMPUTE_FLAG = ifelse(sd(REG_PRICE) == 0, "Impute","None"),
                     IMPUTE_COMMENT = ifelse(sd(REG_PRICE) == 0, "No Price Change", "None")
                     )
      } 
    }
    
    all_output <- list(full_output,validated_est)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drill Down to ucm helper functions - from ucm_functions.R_final

# COMMAND ----------

# DBTITLE 1,data_prep_func
filtered_data <- source_data
prdt <- sku

library(dplyr)
  
  data <- filtered_data
  data_prdt_v1<-data %>% 
    filter(LEVEL1_IDNT %in% prdt) %>%
    mutate(MEAN_SALES = mean(SALES_QTY,na.rm = T)) %>%
    filter(SALES_QTY > mean(SALES_QTY,na.rm = T)*.25)   #remove very outier sales
  
  MIN_DATE = min(data_prdt_v1$WEEK_SDATE)
  MAX_DATE = max(data_prdt_v1$WEEK_SDATE)
  
  data$MIN_DATE <- rep(MIN_DATE,nrow(data))
  data$MAX_DATE <- rep(MAX_DATE,nrow(data))
  
  data_prdt_v2<-data %>% 
    filter(LEVEL1_IDNT %in% prdt & WEEK_SDATE >= MIN_DATE & WEEK_SDATE <= MAX_DATE) %>%
    mutate(MEAN_SALES = unique(data_prdt_v1$MEAN_SALES)) %>%
    arrange(WEEK_SDATE)
  
      #more than latest 30 weeks missing
    max_date_data <- unique(data_prdt_v2$MAX_DATE_COMP)
    max_date_current_case <- max(data_prdt_v2$WEEK_SDATE)
    gap_in_sales <- as.numeric(max_date_data - max_date_current_case)
  
##############################################  section for different sales issue Flags ################################################################################  
  
   mean_weekly_sales <- as.numeric(mean(data_prdt_v2$SALES_QTY,na.rm = T))
   mean_weekly_price <- as.numeric(mean(data_prdt_v2$REG_PRICE,na.rm = T))
   if((mean_weekly_sales <= 10) & (bu != 'IE_Repo') & (mean_weekly_price <= 25)){
      data_prdt_v2$IMPUTE_FLAG = "Impute"
      data_prdt_v2$IMPUTE_COMMENT = "Very low average weely sales to model <= 10 units per week"
   }else{

        if(gap_in_sales >30){
          data_prdt_v2$IMPUTE_FLAG = "Impute"
          data_prdt_v2$IMPUTE_COMMENT = "More than 30 days of data missing from latest week of data"
        }else{
           #growing series
              data_prdt_v3 <- data_prdt_v2%>%
                              arrange(WEEK_SDATE)%>%
                              mutate(qtr_start = ifelse(((row_number()-1)%% 12)== 0, 1, 0))%>%
                              mutate(qtr = cumsum(qtr_start))%>%
                             group_by(qtr)%>%summarise(SALES_QTY = mean(SALES_QTY, na.rm = T),
                                                      max_date = max(WEEK_SDATE))%>%
                             ungroup()%>%arrange(qtr)%>%
                             mutate(next_qtr_sales = lead(SALES_QTY, 1))%>%
                             mutate(next_year_qtr_sales =lead(SALES_QTY, 4))%>%
                             mutate(check = abs(next_qtr_sales/SALES_QTY - 1))%>%
                             mutate(pre_issue_index = ifelse(abs(next_qtr_sales/SALES_QTY - 1) >= 1,1,0))%>%
                             mutate(pre_issue_index = ifelse(is.na(pre_issue_index),0, pre_issue_index))%>%
                             mutate(pre_issue_index_next_year = ifelse(abs(next_year_qtr_sales/SALES_QTY - 1) >= 1,1,0))%>%
                             mutate(pre_issue_index_next_year = ifelse(is.na(pre_issue_index_next_year),0, pre_issue_index_next_year))%>%
                             mutate(final_growing_series_index = ifelse((pre_issue_index == 1) & (pre_issue_index_next_year ==1),1,0))%>%
                             select(qtr, final_growing_series_index)%>%unique()%>%arrange(qtr)

              check_1 <- sum((data_prdt_v3[2,])$final_growing_series_index)
              check_1[is.na(check_1)] <- 0
              if(check_1 != 0){
                data_prdt_v2 <- data_prdt_v2%>%
                          mutate(ISSUE_COMMENT = ifelse(ISSUE_COMMENT == "None","Might consider truncation from beginning  as inconsitent starting sales",paste0(ISSUE_COMMENT,",Might consider truncation from beginning  as inconsitent starting sales")),
                                    SALES_FLAG = "Issue")       
              }

          #discontinuing series  
              data_prdt_v4 <- data_prdt_v2%>%
                              arrange(desc(WEEK_SDATE))%>%
                              mutate(qtr_start = ifelse(((row_number()-1)%% 12)== 0, 1, 0))%>%
                              mutate(qtr = cumsum(qtr_start))%>%
                             group_by(qtr)%>%summarise(SALES_QTY = mean(SALES_QTY, na.rm = T),
                                                      max_date = max(WEEK_SDATE))%>%
                             ungroup()%>% arrange(qtr)%>%
                             mutate(prev_qtr_sales = lead(SALES_QTY, 1))%>%
                             mutate(prev_year_qtr_sales =lead(SALES_QTY, 4))%>%
                             mutate(check = abs(prev_qtr_sales/SALES_QTY - 1))%>%
                             mutate(post_issue_index = ifelse(abs(prev_qtr_sales/SALES_QTY - 1) >= 1,1,0))%>%
                             mutate(post_issue_index = ifelse(is.na(post_issue_index),0, post_issue_index))%>%
                             mutate(post_issue_index_next_year = ifelse(abs(prev_year_qtr_sales/SALES_QTY - 1) >= 1,1,0))%>%
                             mutate(post_issue_index_next_year = ifelse(is.na(post_issue_index_next_year),0, post_issue_index_next_year))%>%
                             mutate(final_discont_series_index = ifelse((post_issue_index_next_year == 1) & (post_issue_index_next_year ==1),1,0))%>%
                             select(qtr, final_discont_series_index)%>%unique()%>%arrange(qtr)

              check_2 <- sum((data_prdt_v4[2,])$final_discont_series_index)
              check_2[is.na(check_2)] <- 0
              if((check_2 != 0)){
                data_prdt_v2 <- data_prdt_v2%>%
                          mutate(ISSUE_COMMENT = ifelse(ISSUE_COMMENT == "None","Seems to be dying series ",paste0(ISSUE_COMMENT,",","Seems to be dying series ")),
                                    SALES_FLAG = "Issue")       
              }

          #find holes
              data_prdt_v5_0 <- data_prdt_v2%>%arrange(WEEK_SDATE)%>%
                                mutate(Week_No = week(WEEK_SDATE),
                                     Year = year(WEEK_SDATE))%>%
                                mutate(pre_year = Year -1, 
                                       post_year = Year +1)%>%
                                select(WEEK_SDATE,Week_No,Year,SALES_QTY,pre_year,post_year)%>%unique()


                library(zoo)
                data_prdt_v5_1 <- data_prdt_v5_0%>%left_join(data_prdt_v5_0%>%select(Week_No,Year,SALES_QTY)%>%rename(past_sales = SALES_QTY),c("Week_No","pre_year" ="Year"))%>%
                                        left_join(data_prdt_v5_0%>%select(Week_No,Year,SALES_QTY)%>%rename(post_sales = SALES_QTY),c("Week_No","post_year" ="Year"))%>%
                                       mutate(roll_window = ifelse(row_number()>=12,12,row_number()))%>%
                                       mutate(M_avg12=rollapply(SALES_QTY,roll_window,mean,align='right',fill=NA))%>%
                                       rowwise()%>%
                                       mutate(mean_pre_post = mean(c(post_sales,past_sales),na.rm =T))%>%ungroup()%>%
                                       mutate(moving_comp = ((M_avg12/SALES_QTY)-1) ,
                                             pre_post_comp = ((mean_pre_post/SALES_QTY)-1))%>%
                                       mutate(hole_index = ifelse((moving_comp >1)& (pre_post_comp >1),
                                                                 1,0),
                                             hole_index = ifelse(is.na(hole_index),0,hole_index),
                                             hole_lag = lag(hole_index,1))%>%
                                       mutate(hole_lag = ifelse(is.na(hole_lag),0,hole_lag))%>%
                                       mutate(regime_change = ifelse(hole_index != hole_lag,1,0))%>%
                                       mutate(hole_regime = cumsum(regime_change),
                                             hole_regime = ifelse(hole_index == 1, hole_regime,0))%>%
                                       filter(hole_regime !=0)%>%
                                       group_by(hole_regime)%>%
                                       mutate(stock_out_weeks = sum(hole_index, na.rm =T))%>%ungroup()

              if(nrow(data_prdt_v5_1)!=0){
                if(max(data_prdt_v5_1$stock_out_weeks)> 4){
                  data_prdt_v2 <- data_prdt_v2%>%
                          mutate(ISSUE_COMMENT = ifelse(ISSUE_COMMENT == "None","Might be too stockouts in the sales series spanning more than 4 weeks",paste0(ISSUE_COMMENT,",","Might be too stockouts in the sales series spanning more than 4 weeks")),
                                    SALES_FLAG = "Issue")
                  }else{
                  data_prdt_v2 <- data_prdt_v2%>%
                       left_join(data_prdt_v5_1%>%select(WEEK_SDATE, Week_No, Year, hole_index))%>%
                       mutate(FINAL_FIX_DUMMY_NAME = paste0("FIX_DUMMY_",Week_No,"_",Year))%>%
                                         mutate(FINAL_FIX_DUMMY = ifelse(is.na(hole_index), 0, hole_index))%>%
                                         mutate(FINAL_FIX_DUMMY_NAME = ifelse(FINAL_FIX_DUMMY == 1, FINAL_FIX_DUMMY_NAME, "Useless"))%>%
                                         select(-Week_No, -Year)%>%
                                         spread(FINAL_FIX_DUMMY_NAME,FINAL_FIX_DUMMY)%>%
                                         select(-Useless)

                       data_prdt_v2[is.na(data_prdt_v2)] <- 0

                }
              }

              #massive YOY shift
             data_prdt_v6 <- data_prdt_v5_0%>%
                                      left_join(data_prdt_v5_0%>%select(Week_No,Year,SALES_QTY)%>%rename(post_sales = SALES_QTY),c("Week_No","post_year" ="Year"))%>%
                                      filter(!is.na(post_sales))%>%
                                      group_by(Year)%>%
                                      summarise(SALES_QTY = mean(SALES_QTY, na.rm =T),
                                               post_sales = mean(post_sales, na.rm =T))%>%ungroup()%>%
                                      mutate(trend_shift = abs((post_sales/SALES_QTY)-1))


            if(max(data_prdt_v6$trend_shift)>= 1){
               data_prdt_v2 <- data_prdt_v2%>%
                          mutate(ISSUE_COMMENT = ifelse(ISSUE_COMMENT == "None","Might be massive YOY sales shift in data ",paste0(ISSUE_COMMENT,",","Might be massive YOY sales shift in data")),
                                    SALES_FLAG = "Issue") 
            }

        }

    }

# COMMAND ----------

# DBTITLE 1,independent_processing func
library(reshape2)
  data_init <- customized_data  %>% 
    select_(.dots = independent) %>%
    summarise_all(funs(sum(.,na.rm=T))) %>%
    gather(KEY,VALUE) %>%
    filter(VALUE > 0)

# COMMAND ----------

independent

# COMMAND ----------

display(customized_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constrained Regression

# COMMAND ----------

slope_type <- "extrapolated" # normal if you want continuous trend
trend_inflex_date <- "2019-01-01" #if most significant price change is after this date then trend before most significant price change is extrapolated only 
#                                     #if slope_type <- "extrapolated" is selected
erase_old_output = F

# COMMAND ----------

# MAGIC %run "/Localized-Pricing/Phase_3/Store_Item_Elasticity/Wave_1/Master_Setup/Functions/Elasticity_opti_funcs"

# COMMAND ----------

# MAGIC %run "/Localized-Pricing/Phase_3/Store_Item_Elasticity/Wave_1/Master_Setup/Functions/do_parallel_elast_opti"

# COMMAND ----------

opti_type <- "nloptr" #not to be changed
inscope_items <- sku

# COMMAND ----------

baseline_rdss = grep(sku,paste0("baselines_",unique(parse_number(list.files(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")))),".Rds"), value = T)

read_baselines <- c()
for(i in baseline_rdss){
  read_in <- readRDS(paste0("/dbfs/Phase_3/Elasticity_Modelling/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/",i)) 
  read_baselines <- bind_rows(read_baselines,read_in)
}
read_baselines <- read_baselines%>%
        select(ITEMID, WEEKDATE, STORE, SALES_QTY, REVENUE, REG_PRICE, NET_PRICE, LN_SALES_QTY, LN_REG_PRICE, UCM_BASELINE)%>%
        rename(!!item_indt_1 := ITEMID,
               WEEK_START_DATE = WEEKDATE,
               CLUSTER = STORE)%>%
       mutate(!!item_indt_1 := as.numeric(get(item_indt_1)))



main_dat <- read_baselines %>% 
  mutate(KEY = paste0(get(item_indt_1),"_",CLUSTER))%>%
         filter(get(item_indt_1) %in% inscope_items)

keys <- unique(main_dat$KEY)

# COMMAND ----------

out <- bind_rows(lapply(keys, function(x){   #parLapply(cl,
  final_out <- elast_opti_func(x,item_indt_1)
  return(final_out)
}))

# COMMAND ----------

display(out)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### do parallel elast opt

# COMMAND ----------

key = "3605167_1"

# COMMAND ----------


print(which(keys == key) ) 
  print(key)
  library(tidyverse)
  library(nloptr)
  library(lubridate)
  
  #some data preps
  
  test_data_0 <- main_dat%>%filter(KEY == key)
  
  test_data_1 <- custom_data_func(test_data_0,customize_data)
  
  test_data_2 <- test_data_1%>%
    mutate(mon_var = paste0("MONTH_DUMMY_",lubridate::month(WEEK_START_DATE,label = T,abbr = T)),
           mon_val = 1)%>%
    spread(mon_var,mon_val) %>%
    mutate(LN_UCM_BASELINE = ifelse(UCM_BASELINE > 0,log(UCM_BASELINE),0))%>%
    mutate(LN_REG_PRICE = ifelse(REG_PRICE > 0,log(REG_PRICE),0))%>%
    mutate(lag_price = lag(REG_PRICE)) %>%
    mutate(perc_diff = (abs(REG_PRICE - lag_price)*100/lag_price)) %>%
    mutate(indicator = ifelse(perc_diff == max(perc_diff, na.rm = T),1,NA)) %>%
    fill(indicator,.direction = "up")
  
  rel_data <- test_data_2 %>% 
    filter(indicator == 1)
  max_date <- max(rel_data$WEEK_START_DATE)  
  
  if(max_date>= trend_inflex_date & slope_type == "extrapolated"){
    rel_data <- rel_data%>%
      mutate(TREND = row_number()) %>%
      mutate(LN_TREND = log(TREND)) %>%
      mutate(LN_BASE = LN_UCM_BASELINE)
    
    reg_1 <- lm(LN_BASE ~ LN_TREND , data = rel_data)
    trend_coef = reg_1$coefficients[[2]]
    
    #Reg 2
    rel_data <- test_data_2 %>%
      mutate(TREND = row_number()) %>%
      mutate(LN_TREND = log(TREND)) %>%
      mutate(ADJ_LN_TREND = LN_TREND*trend_coef)
  }else{
    rel_data <- test_data_2 %>%
      mutate(TREND = row_number()) %>%
      mutate(ADJ_LN_TREND = log(TREND))
  }
  
  #actual optimization
  rel_data[is.na(rel_data)] <- 0
  row_num <- nrow(rel_data)
  
  x_reg <- rel_data$LN_REG_PRICE
  x_trend <- rel_data$ADJ_LN_TREND
  x_jan <- rel_data$MONTH_DUMMY_Jan
  x_feb <- rel_data$MONTH_DUMMY_Feb
  x_mar <- rel_data$MONTH_DUMMY_Mar
  x_apr <- rel_data$MONTH_DUMMY_Apr
  x_may <- rel_data$MONTH_DUMMY_May
  x_jun <- rel_data$MONTH_DUMMY_Jun
  x_jul <- rel_data$MONTH_DUMMY_Jul
  x_aug <- rel_data$MONTH_DUMMY_Aug
  x_sep <- rel_data$MONTH_DUMMY_Sep
  x_oct <- rel_data$MONTH_DUMMY_Oct
  x_nov <- rel_data$MONTH_DUMMY_Nov
  x_dec <- rel_data$MONTH_DUMMY_Dec
  
  actuals <- rel_data$LN_UCM_BASELINE
  
  intercept <- rep(1,row_num)
  all_x_vars <- c(intercept,x_reg,x_trend,x_jan,x_feb,x_mar,x_apr,x_may,x_jun,x_jul,x_aug,x_sep,x_oct,x_nov,x_dec)
  
  vars <- matrix(data = all_x_vars, nrow = row_num, ncol = 15)

# COMMAND ----------

coefs_0 <- c(
      5,-1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,
      0.1,0.1,0.1,0.1,0.1,0.1
    )
    
    vars_t <- t(vars)
    
    # obj_sum_SSE <- sum((actuals - colSums(coefs*vars_t))^2)
    
    # objective function
    eval_f0 <- function( coefs, actuals, vars_t ){
      return( sum((actuals - colSums(coefs*vars_t))^2) )
    }
    
    res1 <- nloptr( x0= coefs_0,
                    eval_f=eval_f0,
                    lb = c(-Inf,-2.5,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf), #this is changed to -2.5 from -2
                    ub = c(Inf,-0.5,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf),
                    opts = list("algorithm"="NLOPT_LN_COBYLA",
                                "xtol_rel"=1.0e-8,
                                # "print_level"=2,
                                "maxeval"=50000
                    ),       
                    actuals = actuals,
                    vars_t = vars_t )
#    print(res1$solution)
    mape <- mean(abs((actuals - colSums(res1$solution*vars_t))/actuals))
    
    if(length(unlist(strsplit(key,"_"))) > 2){
     identifier <- unlist(strsplit(key,"_"))[1]
     cls <- paste((unlist(strsplit(key,"_")))[2:length((unlist(strsplit(key,"_"))))],collapse = "_")
    }else{
     identifier <- unlist(strsplit(key,"_"))[1]
     cls <- unlist(strsplit(key,"_"))[2]
    }
    
    output <- data.frame(IDENTIFIER = identifier,
                         CLUSTER = cls,
                         BP_ELAST_OPTI = res1$solution[2],
                         coef_intercept = res1$solution[1],
                         MAPE = mape,
                         coef_trend = res1$solution[3],
                         coef_jan = res1$solution[4],
                         coef_feb = res1$solution[5],
                         coef_mar = res1$solution[6],
                         coef_apr = res1$solution[7],
                         coef_may = res1$solution[8],
                         coef_jun = res1$solution[9],
                         coef_jul = res1$solution[10],
                         coef_aug = res1$solution[11],
                         coef_sep = res1$solution[12],
                         coef_oct = res1$solution[13],
                         coef_nov = res1$solution[14],
                         coef_dec = res1$solution[15] 
                        ) %>% rename(!!item_indt_1 := IDENTIFIER)
