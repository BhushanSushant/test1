# Databricks notebook source
library(tidyverse)
library(stringr)
library(parallel)
library(openxlsx)

#some params for deciding degree of prediction improvement
mape_cutoff <- 0.1
sigma_cutoff <- 2
correction_cutoff <- 0.2
# base_directory <- 'Phase3_extensions/Elasticity_Modelling'

parallel_lvl1_run <- function(
  user,
  version,
  datasource,
  closed_stores,
  sign_file,
  baseline_variables_data,
  mixed_variables_data,
  custom_data_func,
  non_baseline_vars,
  support_functions_baseline,
  support_functions_mixed,
  scope,
  customize_data,
  pval,
  levelvar,
  lvl2_function,
  lvl_1_cores,
  lvl_2_cores,
  log_loc_lvl1,
  log_loc_lvl2,
  remove_weeks,
  run_type,
  custom_run_variables,
  bu,
  cat_seasonal_dummy,
  cat_indiv_week_dummy,
  cat_indiv_month_dummy,
  wave,
  base_directory
)
{
  #Parellized Models
  
  ncores <- lvl_1_cores
  cl <- makeCluster(ncores)
  clusterExport(cl,c(
    "user",
    "version",
    "datasource",
    "closed_stores",
    "sign_file",
    "baseline_variables_data",
    "mixed_variables_data",
    "custom_data_func",
    "non_baseline_vars",
    "support_functions_baseline",
    "support_functions_mixed",
    "scope",
    "customize_data",
    "pval",
    "levelvar",
    "lvl2_function",
    "lvl_2_cores",
    "log_loc_lvl1",
    "log_loc_lvl2",
    "generate_ucm_baseline",
    "generate_mixed_model",
    "bp_elasticty_simulation",
    "analyse_log",
    "reg_price_impute",
    "parallel_lvl2_run",
    "data_prep",
    "independent_processing",
    "ucm_function",
    "ucm_estimates_func",
    "flagging",
    "validated_variables",
    "base_predict",
    "contribution_stage_1",
    "regression",
    "ucm_cus",
    "fitSSM_cus",
    "algo",
    "tol_con",
    "remove_weeks",
    "run_type",
    "custom_run_variables",
    "bu",
    "mape_cutoff",
    "sigma_cutoff",
    "correction_cutoff",
    "price_fix_function",
    "cat_seasonal_dummy",
    "cat_indiv_week_dummy",
    "cat_indiv_month_dummy",
    "wave",
    "base_directory"
    
  ),environment())
  
  clusterEvalQ(cl, sink(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version,"/",paste0("Lvl1_",Sys.getpid()), ".txt")))
  cat(paste0("SKU list extraction from scope\n"),file=paste0(log_loc_lvl1), append=TRUE)
  sku_list = scope$ITEMID[scope$STATUS == "Y"] #c(83196,83198,83202,2567,2611) #
  
  
  parLapply(cl,sku_list, function(x) { #parLapply(cl, 
    
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
      wave = wave,
      base_directory = base_directory
      
    )
    
    end_time = Sys.time()
    time_diff = round(difftime(end_time,start_time, units='mins'),2)
    cat(paste0("Completed LVL1 loop in ",time_diff," mins for SKU,",x ,"\n"),file=paste0(log_loc_lvl1), append=TRUE)
   
    
  })
  
  stopCluster(cl)
  return("Modelling Completed")
}


parallel_lvl2_run <- function(
  closed_stores,
  user,
  version,
  datasource,
  sku,
  pval,
  levelvar,
  scope,
  customize_data,
  core_function_baseline,
  core_function_mixed,
  core_function_baseprice,
  sku_list,
  log_loc_lvl1,
  log_loc_lvl2,
  nodal_log,
  sales_qty,
  revenue,
  reg_price,
  itemid,
  weekdate,
  sign_file,
  baseline_variables_data,
  mixed_variables_data,
  custom_function,
  non_baseline_vars,
  support_functions_baseline,
  support_functions_mixed,
  threads,
  remove_weeks,
  run_type,
  custom_run_variables,
  bu,
  cat_seasonal_dummy,
  cat_indiv_week_dummy,
  cat_indiv_month_dummy,
  wave,
  base_directory
){

    start_time = Sys.time()
    
    ncores <- threads
    cl <- makeCluster(ncores)
    clusterExport(cl,c(
      "user",
      "version",
      "sku",
      "pval",
      "levelvar",
      "core_function_baseline",
      "core_function_mixed",
      "core_function_baseprice",
      "sku_list",
      "log_loc_lvl1",
      "log_loc_lvl2",
      "nodal_log",
      "sales_qty",
      "reg_price",
      "itemid",
      "weekdate",
      "sign_file",
      "customize_data",
      "baseline_variables_data",
      "mixed_variables_data",
      "custom_function",
      "non_baseline_vars",
      "support_functions_baseline",
      "support_functions_mixed",
      "threads",
      "generate_ucm_baseline",
      "generate_mixed_model",
      "bp_elasticty_simulation",
      "analyse_log",
      "reg_price_impute",
      "parallel_lvl2_run",
      "data_prep",
      "independent_processing",
      "ucm_function",
      "ucm_estimates_func",
      "flagging",
      "validated_variables",
      "base_predict",
      "contribution_stage_1",
      "regression",
      "ucm_cus",
      "fitSSM_cus",
      "algo",
      "tol_con",
      "remove_weeks",
      "run_type",
      "custom_run_variables",
      "bu",
      "mape_cutoff",
      "sigma_cutoff",
      "correction_cutoff",
      "price_fix_function",
      "cat_seasonal_dummy",
      "cat_indiv_week_dummy",
      "cat_indiv_month_dummy",
      "wave",
      "base_directory"
    ),environment())
    
    clusterEvalQ(cl, sink(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version,"/",paste0("Lvl2_",sku,"_task_",Sys.getpid()), ".txt")))
    
    ADS <- readRDS(paste0(datasource))%>%
            filter((!!as.name(itemid)) == sku)
  
    if(run_type == 'custom_run'){
      check_clust_exist <- custom_run_variables %>% filter(itemid==sku)
      clust_check <- unique(check_clust_exist$cluster)
      store_list <- clust_check
#       store_list <- grep("trans_",list.files(datasource),value = T)
#       fin <- c()
#       for (m in clust_check){
#         check <- grep(m,store_list, value =T )
#         fin <- c(fin,check)
#       }
#       store_list <- fin
    }else{
      store_list = unique(ADS$Cluster)
    }
    
    out_list <- parLapply(cl,store_list, function(x) {#parLapply(cl,
      
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
        # Edit: Sanjo: Added levelvar variable for custom run
        if('LEVEL_VAR' %in% names(custom_run_variables)){
          level_var_check <- custom_run_variables %>% filter(itemid == sku & cluster == x) %>% select(LEVEL_VAR) %>% distinct()
          level_var2 <- level_var_check$LEVEL_VAR
          if(is.na(level_var2)==FALSE){
            levelvar <- level_var2
          }}
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
      
#       ### create top 2 and least 2 weeks dummy
      source_data <- source_data%>%
                     arrange(LEVEL1_IDNT,STORE,-SALES_QTY)%>%
                     group_by(LEVEL1_IDNT,STORE)%>%
                     mutate(rank_sale = row_number())%>%ungroup()%>%
                     mutate(WEEK_DUMMY_TOP = ifelse(rank_sale <=2,1,0))%>%
                     arrange(LEVEL1_IDNT,STORE,SALES_QTY)%>%
                     group_by(LEVEL1_IDNT,STORE)%>%
                     mutate(rank_sale_low = row_number())%>%ungroup()%>%
                     mutate(WEEK_DUMMY_LOW = ifelse(rank_sale_low <=2,1,0))%>%
                     select(-rank_sale,-rank_sale_low)%>%
                     arrange(LEVEL1_IDNT,STORE,WEEK_SDATE)
      
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
      
       if(cat_seasonal_dummy == "WEEK_DUMMY_"){
            cat_seasonal_dummy =c()
        }else{
            cat_seasonal_dummy
       }
      
      ## add a top 2 and least 2 week dummy 
      cat_seasonal_dummy <- c(cat_seasonal_dummy,"WEEK_DUMMY_TOP","WEEK_DUMMY_LOW")
      
      library(lubridate)
      calendar_dat <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/calendar.Rds"))%>%
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

              # for price change more than 20% from regime to regime create a flag for a double check, PGv12:Updated this price change for Nov21 refresh
              check2 <- stable_regimes%>%
                        mutate(LAG_PRICE = lag(REG_PRICE))%>%
                        mutate(abs_change = abs(REG_PRICE - LAG_PRICE),
                               perc_change = abs_change/REG_PRICE)%>%
                        mutate(flag = ifelse((abs_change > 3.00) & (perc_change > 0.35),1,0))
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
#PG v12: Removed this price check for Nov refresh
#       if(min(check5$WEEKS_ON_PRICE) <= 3){
#         source_data$REGULAR_PRICE_FLAG <- "Issue"
#         source_data <- source_data %>% mutate(ISSUE_COMMENT = ifelse( ISSUE_COMMENT == 'None', "unstable prices exist in series", paste0(ISSUE_COMMENT," ,unstable prices exist in series")) )
#       }
      
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
        support_functions = support_functions_baseline,
        base_directory = base_directory
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
                    support_functions = support_functions_baseline,
                    base_directory = base_directory)
          }else{
            baseline_modelled <- baseline_modelled
          }
      }
      cat(paste0("Completed Baseline Model for,","SKU:",sku,",STORE:",x ,"\n"),file=paste0(log_loc_lvl2), append=TRUE)
      
      return(baseline_modelled)
      print(x)
    })
    
    stopCluster(cl)
    
    cat(paste0("Saving Bp Model for,","SKU:",sku,"\n"),file=paste0(log_loc_lvl2), append=TRUE)
    
    modelled = out_list[!is.na(out_list)]
    model_baseline_series  <- bind_rows(lapply(1:length(modelled), function(x) modelled[[x]][[1]]))
    model_baseline_estimates  <- bind_rows(lapply(1:length(modelled), function(x) modelled[[x]][[2]]))
    
    #for snuff keeping the nat dummy peaks in check
    # model_baseline_series <- model_baseline_series%>%
    #   mutate(lagged = lag(UCM_BASELINE, 1))%>%
    #   mutate(UCM_BASELINE = ifelse(NAT_HLDY_DUMMY == 1,lagged,UCM_BASELINE))%>%
    #   select(-lagged)
    
    saveRDS(model_baseline_series,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/baselines_",sku,".Rds"))
    saveRDS(model_baseline_estimates,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/baseline_estimates_",sku,".Rds"))
                                                  
    print(sku)                                              
                                                     
} 
