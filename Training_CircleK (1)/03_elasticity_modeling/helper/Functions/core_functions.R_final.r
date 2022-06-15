# Databricks notebook source
library(trustOptim)
library(numDeriv)

#some params for deciding degree of prediction improvement
mape_cutoff <- 0.1
sigma_cutoff <- 2
correction_cutoff <- 0.2
# base_directory <- 'Phase3_extensions/Elasticity_Modelling'

generate_user_version_folders <- function(user,version,bu,base_directory){
  
  invisible(
    dir.create(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/")), recursive = T, showWarnings = F )
  )
  invisible(
    dir.create(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")), recursive = T, showWarnings = F )
  )
  invisible(
    file.remove(list.files(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")),full.names = T))
  )
  invisible(
    dir.create(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")),recursive = T, showWarnings = F )
  )
  invisible(
    file.remove(list.files(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")),full.names = T))
  )
  invisible(
    dir.create(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/model_failure_report/",user,"/v",version,"/")),recursive = T, showWarnings = F )
  )
  invisible(
    file.remove(list.files(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/model_failure_report/",user,"/v",version,"/")),full.names = T))
  )
  invisible(
    dir.create(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version,"/")),recursive = T, showWarnings = F )
  )
  invisible(
    file.remove(list.files(file.path(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version,"/")),full.names = T))
  )
  return("Folders Created")
}



generate_ucm_baseline <- function(
  source_data,
  store_number,
  pval,
  levelvar,
  sku,
  customize_data,
  log_loc,
  dependent,
  reg_price,
  itemid,
  weekdate,
  sign_file,
  independent,
  custom_function,
  non_baseline_vars,
  support_functions,
  base_directory
)
{
  
#   source(support_functions)
  
  tryCatch({
    
    print(paste0("Log_Baseline:Process Data,","Store:",store_number,",SKU:",sku ,"\n"))
    processed_data<- data_prep(source_data,sku)
    
    independent <- c(independent,grep("FIX_DUMMY",names(processed_data),value =T))   #fix due to stock-outs
    
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
    
    print(paste0("Log_Baseline:Select Independent,","Store:",store_number,",SKU:",sku ,"\n"))
    independant_selected <-  independent_processing(customized_data,independent)
    
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
         mutate(perc_abs_error = ifelse(SALES_QTY == 0, NA, perc_abs_error))%>%
         mutate(MAPE_UCM = mean(perc_abs_error, na.rm = T))%>%
         mutate(perc_abs_error = ifelse(is.na(perc_abs_error),0,perc_abs_error))%>%
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
        full_output <- full_output%>%mutate(UCM_PREDICTION_FLAG = ifelse(MAPE_UCM >= 0.2, "Issue","Good"))   
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
          full_output <- full_output %>% mutate(ISSUE_COMMENT = ifelse(ISSUE_COMMENT == 'None', "More than 20 correction dummy used",paste0(ISSUE_COMMENT,",More than 20 correction dummy used"))) 
        }
        
         full_output <- full_output%>%
              mutate(IMPUTE_FLAG = ifelse(sd(REG_PRICE) == 0, "Impute",IMPUTE_FLAG),
                     IMPUTE_COMMENT = ifelse(sd(REG_PRICE) == 0, "No Price Change", IMPUTE_COMMENT)
                     )
      } 
    }
    
    all_output <- list(full_output,validated_est)
    print(paste0("Log_Baseline:Baseline Completed,","Store:",store_number,",SKU:",sku ,"\n"))
    
  },
  
  error = function(e){
    all_output <<- NA
    print(e)
    print(paste0("Log_Baseline:Baseline Failed,","Store:",store_number,",SKU:",sku ,"\n"))
    error_log <- file(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/model_failure_report/",user,"/v",version,"/","output.txt"))
    write(as.character(e),error_log, append = T)
  }
  
  )
  
  return(all_output)
}



generate_mixed_model <- function(
  item_no,
  source_data,
  pval,
  baseline_var,
  variables_data,
  customize_data,
  weekdate,
  sign_data,
  scope,
  support_functions,
  custom_function,
  log_loc,
  base_directory
){
  
#   source(support_functions)
  
  
  tryCatch({
    
    print(paste0("Log_Mixed:Initiating Modelling,","Item:",item_no,"\n"))
    
    model_results <- source_data 
    colnames(model_results)[colnames(model_results) == baseline_var] <- 'BASELINE_PREPARED'
    
    print(paste0("Log_Mixed:Initiating Custom Data Prep,","Item:",item_no,"\n"))
    model_results <- custom_function(model_results,customize_data)
    
    print(paste0("Log_Mixed:Initiating Uplift Creation,","Item:",item_no,"\n"))
    model_results_rel <- model_results %>%
      mutate(UPLIFT = SALES_QTY/BASELINE_PREPARED) %>%
      mutate(UPLIFT = ifelse(UPLIFT <= 0,1,UPLIFT)) %>%
      mutate(LOG_UPLIFT = log(UPLIFT)) %>%
      filter(LOG_UPLIFT > 0) %>%
      select_(.dots = c("ITEMID","WEEKDATE","STORE","LOG_UPLIFT",variables_data$INDEPENDENT))
    
    model_results_rel[is.na(model_results_rel)] <- 0
    
    
    # is.na(model_results_rel)<-sapply(model_results_rel, is.infinite)
    # model_results_rel[is.na(model_results_rel)]<-0
    
    print(paste0("Log_Mixed:Initiating Mixed Modelling,","Item:",item_no,"\n"))
    
    mixed_model_full <- mixed_model_function(variables_data,model_results_rel)
    
    print(paste0("Log_Mixed:Initiating Mixed Model Elasticity Extraction,","Item:",item_no,"\n"))
    
    if(class(mixed_model_full) != "lm"){
      fixEffect <-broom::tidy(mixed_model_full,"fixed")
      colnames(fixEffect)[1] <- "VARIABLE"
    } else {
      fixEffect <- as.data.frame(coefficients(summary(mixed_model_full)))
      fixEffect$VARIABLE = rownames(fixEffect)
      fixEffect <- fixEffect[,-4]
      colnames(fixEffect) <- c("estimate","std.error","statistic","VARIABLE")
    }
    
    
    
    print(paste0("Log_Mixed:Initiating Flagging,","Item:",item_no,"\n"))
    results <- flagging(fixEffect, sign_data )
    
    print(paste0("Log_Mixed:Initiating Valiadtion,","Item:",item_no,"\n"))
    final_result <- validated_variables(model_results_rel,variables_data,results,pval,sign_data)
    
    print(paste0("Log_Mixed:Initiating Mixed Estimates Prep,","Item:",item_no,"\n"))
    estimates_check <- mixed_estimates_prep(final_result,model_results,model_results_rel,scope)
    
    
    # cat(paste0("Mixed Model Completed\n"),file=paste0("Log/log.txt"), append=TRUE)
    
    #Removing Highly Negative Random Effects for discount
    
    print(paste0("Log_Mixed:Initiating Negative Estimates Imputation,","Item:",item_no,"\n"))
    
    disc_selected <- grep("DISCOUNT",unique(estimates_check$VARIABLES), value = T)
    
    if(sum(colnames(estimates_check) %in% "rand") > 0){
      
      tot_neg <- estimates_check %>%
        filter(total < 0 & !is.na(rand)) %>%
        filter(VARIABLES %in% disc_selected)
    } else {
      tot_neg <- estimates_check %>%
        mutate(rand = NA) %>%
        filter(total < 0 & !is.na(rand)) %>%
        filter(VARIABLES %in% disc_selected)
    }
    
    
    if(nrow(tot_neg) > 0){
      rel_key <- unique(paste0(tot_neg$ITEMID,tot_neg$VARIABLES,tot_neg$STORE))
      
      print(paste0("Log_Mixed:Initiating Prediction output Prep,","Item:",item_no,"\n"))
      
      model_results_rel_gathered <- model_results_rel %>%
        select_(.dots = c("ITEMID","WEEKDATE","STORE",unique(tot_neg$VARIABLES))) %>%
        gather_("KEY","VALUE",unique(tot_neg$VARIABLES)) %>%
        mutate(filter_criterion = paste0(ITEMID,KEY))
      
      model_results_rel_cp <- model_results_rel_gathered %>%
        filter(filter_criterion %in% rel_key) %>%
        mutate(VALUE = 0) %>% 
        select(-filter_criterion) %>%
        bind_rows(model_results_rel_gathered %>% 
                    filter(!filter_criterion %in% rel_key) %>%
                    select(-filter_criterion)
        ) %>%
        spread(KEY,VALUE)
      
      model_results_rel_zeroed <- model_results_rel %>%
        select(-one_of(unique(tot_neg$VARIABLES))) %>%
        left_join(model_results_rel_cp, c("ITEMID","WEEKDATE","STORE"))
      
      
      variables_data_selected = variables_data %>% filter(INDEPENDENT %in% unique(estimates_check$VARIABLES))  
      
      print(paste0("Log_Mixed:Final Model,","Item:",item_no,"\n"))
      final_result <- mixed_model_function(variables_data_selected,model_results_rel_zeroed)
      estimates_final <- mixed_estimates_prep(final_result,model_results,model_results_rel,scope)
      
    } else {
      estimates_final = estimates_check
    }
    
    print(paste0("Log_Mixed:Final Preditions,","Item:",item_no,"\n"))
    predicted_uplift <- model_results_rel %>% 
      mutate(LOG_UPLIFT_PREDICTED = predict(final_result)) %>%
      mutate(UPLIFT_PREDICTED = exp(LOG_UPLIFT_PREDICTED)) %>%
      select_(.dots = c("WEEKDATE","ITEMID","STORE","UPLIFT_PREDICTED"))
    
    print(paste0("Log_Mixed:PREDICTED_SALES_MIXED,","Item:",item_no,"\n"))    
    full_results_data <- model_results %>% 
      left_join(predicted_uplift,c("WEEKDATE","ITEMID","STORE")) %>%
      mutate(UPLIFT_PREDICTED = ifelse(is.na(UPLIFT_PREDICTED),1,UPLIFT_PREDICTED)) %>%
      mutate(UPLIFT_PREDICTED = ifelse(DISCOUNT_PERC == 0,1,UPLIFT_PREDICTED))%>% #to reduce base vs pred gap
      mutate(PREDICTED_SALES_MIXED = UPLIFT_PREDICTED*BASELINE_PREPARED)
    
    
    colnames(full_results_data)[colnames(full_results_data) == 'BASELINE_PREPARED'] <- baseline_var
    
    all_data <- list(full_results_data,estimates_final)
    print(paste0("Log_Mixed:Completed Mixed,","Item:",item_no,"\n"))
    
  },
  
  error = function(e){
    all_data <<- NA
    print(paste0("Log_Mixed:Failed Mixed,","Item:",item_no,"\n"))
    error_log <- file(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/model_failure_report/",user,"/v",version,"/","output.txt"))
    write(as.character(e),error_log, append = T)
    
    
  })
  
  return(all_data)
  
}


bp_elasticty_simulation <- function(full_output, slope_type,customize_data){
  
  item_no = unique(full_output$ITEMID)
  print(paste0("SKU:",item_no," ,Cluster: ",unique(full_output$STORE)))
  
  full_output <- custom_function(full_output,customize_data)
  
  tryCatch({
    
    # full_output$CONTRI <- rowSums(full_output[ , grepl( "cont_" , names( full_output ) ) ])
    # 
    # full_output$BASELINE_WITHOUT_DUMMY <- full_output$BASELINE_REV - full_output$CONTRI
    
    bp_data <- full_output %>%
      group_by(WEEKDATE,ITEMID,STORE) %>%
      summarise(SALES_QTY = sum(SALES_QTY, na.rm = T),
                BASELINE = sum(UCM_BASELINE,na.rm = T),
                REG_PRICE = mean(REG_PRICE,na.rm = T),
                #keeping monthly dummies here
                MONTH_DUMMY_JAN = max(MONTH_DUMMY_JAN, na.rm =T),
                MONTH_DUMMY_FEB = max(MONTH_DUMMY_FEB, na.rm =T),
                MONTH_DUMMY_MAR = max(MONTH_DUMMY_MAR, na.rm =T),
                MONTH_DUMMY_APR = max(MONTH_DUMMY_APR, na.rm =T),
                MONTH_DUMMY_MAY = max(MONTH_DUMMY_MAY, na.rm =T),
                MONTH_DUMMY_JUN = max(MONTH_DUMMY_JUN, na.rm =T),
                MONTH_DUMMY_JUL = max(MONTH_DUMMY_JUL, na.rm =T),
                MONTH_DUMMY_AUG = max(MONTH_DUMMY_AUG, na.rm =T),
                MONTH_DUMMY_SEP = max(MONTH_DUMMY_SEP, na.rm =T),
                MONTH_DUMMY_OCT = max(MONTH_DUMMY_OCT, na.rm =T),
                MONTH_DUMMY_NOV = max(MONTH_DUMMY_NOV, na.rm =T),
                MONTH_DUMMY_DEC = max(MONTH_DUMMY_DEC, na.rm =T)
      ) %>%
      ungroup() %>%
      mutate(lag_price = lag(REG_PRICE)) %>%
      mutate(perc_diff = (abs(REG_PRICE - lag_price)*100/lag_price)) %>%
      mutate(indicator = ifelse(perc_diff == max(perc_diff, na.rm = T),1,NA)) %>%
      fill(indicator,.direction = "up") 
    
    #Reg 1
    rel_data <- bp_data %>% 
      filter(indicator == 1)
    max_date <- max(rel_data$WEEKDATE)
    
    if(max_date>=as.Date(trend_inflex_date) & slope_type == "extrapolated"){
            rel_data <- rel_data%>%
            mutate(TREND = row_number()) %>%
            mutate(LN_TREND = log(TREND)) %>%
            mutate(LN_BASE = log(BASELINE))
          
          reg_1 <- lm(LN_BASE ~ LN_TREND , data = rel_data)
          trend_coef = reg_1$coefficients[[2]]
          
          #Reg 2
          rel_data <- bp_data %>%
            mutate(TREND = row_number()) %>%
            mutate(LN_TREND = log(TREND)) %>%
            mutate(adj_ln_trend = LN_TREND*trend_coef) %>%
            mutate(LN_PRICE = log(REG_PRICE)) %>%
            mutate(LN_BASE = log(BASELINE))
    }else{
          rel_data <- bp_data %>%
            mutate(TREND = row_number()) %>%
            mutate(LN_TREND = log(TREND)) %>%
            mutate(adj_ln_trend = LN_TREND) %>%
            mutate(LN_PRICE = log(REG_PRICE)) %>%
            mutate(LN_BASE = log(BASELINE))
    }     
      #adding monthly here
    reg_2 = lm(LN_BASE ~ LN_PRICE + adj_ln_trend, + MONTH_DUMMY_JAN + MONTH_DUMMY_FEB +
                 MONTH_DUMMY_MAR + MONTH_DUMMY_APR + MONTH_DUMMY_MAY + MONTH_DUMMY_JUN +
                 MONTH_DUMMY_JUL + MONTH_DUMMY_AUG + MONTH_DUMMY_SEP + MONTH_DUMMY_OCT +
                 MONTH_DUMMY_NOV + MONTH_DUMMY_DEC,
               data = rel_data)
    
    #find MAPE
    
    MAPE <- cbind(reg_2$model,reg_2$fitted.values)%>%
      mutate(errors = abs((`reg_2$fitted.values`- LN_BASE)/LN_BASE))%>%
      mutate(errors = ifelse(is.na(errors),0,errors))
    mape <- mean(MAPE$errors, na.rm =T)
    rsq <- summary(reg_2)$r.squared
    adj_rsq <- summary(reg_2)$adj.r.squared
    
    if(is.na(reg_2$coefficients[[2]])){
      pval_reg_price <- NA
    }else{
      pval_reg_price <- coef(summary(reg_2))[2,4]
    }
        
    reg_results <- rel_data %>% 
      mutate(BP_ELASTICITY = reg_2$coefficients[[2]],
             MAPE = mape,
             PVAL_REG_PRICE = pval_reg_price,
             RSq = rsq,
             Adj_Rsq = adj_rsq) %>%
      select(ITEMID,STORE,BP_ELASTICITY,MAPE,PVAL_REG_PRICE,RSq,Adj_Rsq) %>%
      distinct()
   
    #if monthly dummies are causing Price to be collinear then coefficient of price is coming to be NA
    #bypassing those cases
    if(is.na(reg_results$BP_ELASTICITY)){
      
      #adding monthly here
      reg_2 = lm(LN_BASE ~ LN_PRICE + adj_ln_trend, #+ MONTH_DUMMY_JAN + MONTH_DUMMY_FEB +
                 # MONTH_DUMMY_MAR + MONTH_DUMMY_APR + MONTH_DUMMY_MAY + MONTH_DUMMY_JUN +
                 # MONTH_DUMMY_JUL + MONTH_DUMMY_AUG + MONTH_DUMMY_SEP + MONTH_DUMMY_OCT +
                 # MONTH_DUMMY_NOV + MONTH_DUMMY_DEC,
                 data = rel_data)
      
      #find MAPE
      
      MAPE <- cbind(reg_2$model,reg_2$fitted.values)%>%
        mutate(errors = abs((`reg_2$fitted.values`- LN_BASE)/LN_BASE))%>%
        mutate(errors = ifelse(is.na(errors),0,errors))
      mape <- mean(MAPE$errors, na.rm =T)
      rsq <- summary(reg_2)$r.squared
      adj_rsq <- summary(reg_2)$adj.r.squared
      
      if(is.na(reg_2$coefficients[[2]])){
        pval_reg_price <- NA
      }else{
        pval_reg_price <- coef(summary(reg_2))[2,4]
      }
      
      reg_results <- rel_data %>% 
        mutate(BP_ELASTICITY = reg_2$coefficients[[2]],
               MAPE = mape,
               PVAL_REG_PRICE = pval_reg_price,
               RSq = rsq,
               Adj_Rsq = adj_rsq) %>%
        select(ITEMID,STORE,BP_ELASTICITY,MAPE,PVAL_REG_PRICE,RSq,Adj_Rsq) %>%
        distinct()
      
    }
    
  }, 
  error = function(e){
    reg_results <<- full_output %>%
      select(ITEMID,STORE) %>% distinct() %>%
      mutate(BP_ELASTICITY = NA,
             MAPE = NA,
             PVAL_REG_PRICE = NA,
             RSq = NA,
             Adj_Rsq = NA)
    # error_log <- file("output.txt")
    # write(as.character(e),error_log, append = T)
    
  })
  
  return(reg_results)
}




analyse_log <- function(log_loc_lvl1,log_loc_lvl2,nodal_log){
  library(tidyverse)
  library(stringr)
  
  #Processing Item Level Log
  lvl1_log <- read.csv(log_loc_lvl1,header = F, col.names = c("model_status","sku")) %>%
    mutate(sku = parse_number(sku))
  lvl2_log <- read.csv(log_loc_lvl2,header = F, col.names = c("model_status","sku","store")) %>%
    mutate(sku = parse_number(sku),
           store = parse_number(store)
    )
  
  nodal_log_base <- bind_rows(lapply(grep("base_",list.files(nodal_log, full.names = T), value = T),function(x){
    check <- read.csv(x,header = F) %>%
      filter(grepl("Log_",V1)) %>%
      mutate(V1 = gsub("\\n", "", V1)) %>%
      separate(V1,c("last_task","store","sku"),sep = ",") %>%
      mutate(store = parse_number(store),
             sku = parse_number(sku),
             last_task = sapply(strsplit(last_task, ":"), "[", 2)
      ) %>%
      group_by(store,sku) %>%
      mutate(task_id = row_number()) %>%
      filter(task_id == max(task_id) - 1)

     # filter(last_task == "Baseline Completed")
    
  }))
  
  tryCatch ({
    nodal_log_mxd <- bind_rows(lapply(grep("mxd_bp_",list.files(nodal_log, full.names = T), value = T),function(x){
      check <- read.csv(x,header = F) %>%
        filter(grepl("Log_Mixed",V1)) %>%
        mutate(V1 = gsub("\\n", "", V1)) %>%
        separate(V1,c("last_task","sku"),sep = ",") %>%
        mutate(
          sku = parse_number(sku),
          last_task = sapply(strsplit(last_task, ":"), "[", 2)
        ) %>%
        filter(last_task == "Completed Mixed")
    }))
    
    report_mixed <-   nodal_log_mxd %>%
      select(sku) %>%
      mutate(mixed_completed = 1)
  },
  error = function(e){
    nodal_log_mxd <<- NA
    report_mixed<<- NA
  }
  )
  
  tryCatch ({
    nodal_log_bp <- bind_rows(lapply(grep("mxd_bp_",list.files(nodal_log, full.names = T), value = T),function(x){
      check <- read.csv(x,header = F) %>%
        filter(grepl("Log_BP",V1)) %>%
        mutate(V1 = gsub("\\n", "", V1)) %>%
        separate(V1,c("last_task","sku"),sep = ",") %>%
        mutate(sku = parse_number(sku),
               last_task = sapply(strsplit(last_task, ":"), "[", 2)
        ) %>%
        filter(last_task == "Full Data")
    }))
    
    report_bp <-   nodal_log_bp %>%
      select(sku) %>%
      mutate(bp_completed = 1)
    
  },
  error = function(e){
    nodal_log_bp <<- NA 
    report_bp <<- NA
  }
  )
  
  
  report_base <-   nodal_log_base %>%
    mutate(base_completed = 1) %>%
    select(sku,store,base_completed) %>%
    drop_na()%>%
    group_by(sku,store) %>%
    summarise(base_completed = sum(base_completed, na.rm = T))
  
  
  
  
  tryCatch({
    full_report = report_base %>%
      left_join(report_mixed, "sku") %>%
      left_join(report_bp, "sku")
  },
  
  error = function(e){
    full_report <<- report_base
  }
  )  
  
  
  
  
  return(full_report)
}

################ reg price imputation ###########################
reg_price_impute <- function(source_data,item){
  
  in_dat <- source_data%>%
    filter(itemid == item)%>%
    mutate(reg_price = ifelse(reg_price == 0 , NA, reg_price))%>%
    mutate(mean = mean(reg_price, na.rm =T),
           sdev = sd(reg_price, na.rm = T))%>%
    mutate(reg_price = ifelse(is.na(reg_price),0 ,reg_price))%>%
    mutate(musig = mean - 2*sdev)%>%
    mutate(Flag = ifelse(reg_price < musig, 1, 0))
  
  #prepare imputation 
  to_impute <- in_dat%>%select(itemid,weekdate,reg_price,Flag)%>%
    arrange(weekdate)
  
  to_impute_1 <- to_impute%>%filter(Flag == 0)%>%
    select(itemid,weekdate, reg_price)%>%
    rename(reg_price_imputed = reg_price)
  
  to_impute_2 <- to_impute%>%
    mutate(Flag2 = cumsum(Flag))%>%
    mutate(fill_flag = ifelse(Flag == 0,NA,Flag2))%>%
    fill(fill_flag, .direction = "up")%>%
    group_by(fill_flag)%>%
    mutate(copy_date = max(weekdate)-7)%>%ungroup()%>%
    filter(Flag == 1)%>%select(-reg_price)%>%
    left_join(to_impute_1, c("itemid"= "itemid","copy_date"= "weekdate"))%>%
    select(itemid, weekdate, reg_price_imputed)%>%
    arrange(weekdate)%>%
    fill(reg_price_imputed, .direction = "up")
  
  out <- source_data%>%
    filter(itemid == item)%>%
    left_join(to_impute_2)%>%
    mutate(reg_price = ifelse(is.na(reg_price_imputed),reg_price,reg_price_imputed))%>%
    select(-reg_price_imputed)
  
  return(out)
}


######### Updated Regular Price Fix FUnc ##########
price_fix_function <- function(to_fix_regime, stable_regimes, price_regimes, fix_type = 1){
        stable_future <- min(stable_regimes$PRICE_REGIME[which(stable_regimes$PRICE_REGIME > to_fix_regime)])

        if(is.infinite(stable_future)){
          stable_future = max(price_regimes$PRICE_REGIME)
        }
        stable_past <- max(stable_regimes$PRICE_REGIME[which(stable_regimes$PRICE_REGIME < to_fix_regime)])

        if(is.infinite(stable_past)){
          stable_past = min(price_regimes$PRICE_REGIME)
        }

       if(fix_type == 1){   #based on price variaton 1st
            stable_future_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_future),"REG_PRICE"]$REG_PRICE
            stable_past_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_past),"REG_PRICE"]$REG_PRICE
            issue_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == to_fix_regime),"REG_PRICE"]$REG_PRICE
            #boundary_condition_fix
            if((to_fix_regime == min(price_regimes$PRICE_REGIME)) | (to_fix_regime == max(price_regimes$PRICE_REGIME))){
              fixed_price <- stable_future_price
            }else{
              if(abs(issue_price - stable_future_price) <= abs(issue_price - stable_past_price)){
                fixed_price <- stable_future_price
              }else{
                fixed_price <- stable_past_price
              }
            }
       }else{ #if that has failed try distance from closest price regime for type other than 1
          
          if(abs(to_fix_regime - stable_future) <= abs(to_fix_regime - stable_past)){
              fixed_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_future),"REG_PRICE"]$REG_PRICE
            }else{
              fixed_price <- price_regimes[which(price_regimes["PRICE_REGIME"] == stable_past),"REG_PRICE"]$REG_PRICE
            }
         
         #boundary condition fix
          
          if((to_fix_regime == max(price_regimes)) & (stable_past == to_fix_regime-1) & (price_regimes[which(price_regimes["PRICE_REGIME"] == max(price_regimes["PRICE_REGIME"])),]$WEEKS_ON_PRICE < 3)){
            fixed_price <- stable_regimes[which(stable_regimes["PRICE_REGIME"] == stable_past),"REG_PRICE"]$REG_PRICE
          }
          
          if(to_fix_regime == min(price_regimes) & stable_future == to_fix_regime+1){
            fixed_price <- stable_regimes[which(stable_regimes["PRICE_REGIME"] == stable_future),"REG_PRICE"]$REG_PRICE
          }
          
       }
        return(data.frame(PRICE_REGIME = to_fix_regime,
                          FIXED_PRICE = fixed_price))
}


#################### compiling intermediate files function #########################

# combine_files <- function()

