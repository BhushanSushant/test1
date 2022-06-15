# Databricks notebook source
elast_opti_func <- function(key,item_indt_1){  #
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
  
  if(opti_type == 'nloptr'){
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
    
    if(is.infinite(mape) == TRUE){
      mape <- -999
    }else{
      mape <- mape
    }
    
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
    return(output)
  }else{
    
    # coefs_0 <- c(
    #   7,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    # )
    
    coefs_0 <- c(
      7,-2,1,1,1,1,1,1,1,
      1,1,1,1,1,1
    )
    # coefs_0 <- c(
    #   5,-1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,
    #   0.1,0.1,0.1,0.1,0.1,0.1
    # )
    alpha <- .09
    num_iters <- 50000
    results <- gradDescent(vars, actuals, coefs_0, alpha, num_iters)
    coefs <- results[[1]]
    cost_hist <- results[[2]]
    # print(coefs)
    # plot(1:num_iters, cost_hist, type = 'l')
    
    # X <- vars
    # y <- actuals
    # theta <-c (8.098835248,-0.5,0.011068953,0.033719049,0,0,0.018459166,0.077666254,0.269171391,0.286827834,0.304350912,
    #           0.333810352,0.338647304,0.267008114,0.150041197)
    # compCost(X, y, theta)
    mape <- mean(abs((actuals - colSums(coefs*vars_t))/actuals))
    output <- data.frame(IDENTIFIER = unlist(strsplit(key,"_"))[1],
                         CLUSTER = unlist(strsplit(key,"_"))[2],
                         BP_ELAST_OPTI = coefs[2],
                         MAPE = mape,
                         coef_intercept = coefs[1],
                         coef_trend = coefs[3],
                         coef_jan = coefs[4],
                         coef_feb = coefs[5],
                         coef_mar = coefs[6],
                         coef_apr = coefs[7],
                         coef_may = coefs[8],
                         coef_jun = coefs[9],
                         coef_jul = coefs[10],
                         coef_aug = coefs[11],
                         coef_sep = coefs[12],
                         coef_oct = coefs[13],
                         coef_nov = coefs[14],
                         coef_dec = coefs[15])%>% rename(!!item_indt_1 := IDENTIFIER)
    return(output)
    
  }
}

# COMMAND ----------

elast_opti_func_cus1 <- function(key,item_indt_1){  #
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
  x_index <- rel_data$TOT_CAT_INDEX
  actuals <- rel_data$LN_UCM_BASELINE
  
  intercept <- rep(1,row_num)
  all_x_vars <- c(intercept,x_reg,x_trend,x_jan,x_feb,x_mar,x_apr,x_may,x_jun,x_jul,x_aug,x_sep,x_oct,x_nov,x_dec,x_index)
  
  vars <- matrix(data = all_x_vars, nrow = row_num, ncol = 16)
  
  if(opti_type == 'nloptr'){
    coefs_0 <- c(
      5,-1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,
      0.1,0.1,0.1,0.1,0.1,0.1,0.1
    )
    
    vars_t <- t(vars)
    
    # obj_sum_SSE <- sum((actuals - colSums(coefs*vars_t))^2)
    
    # objective function
    eval_f0 <- function( coefs, actuals, vars_t ){
      return( sum((actuals - colSums(coefs*vars_t))^2) )
    }
    
    res1 <- nloptr( x0= coefs_0,
                    eval_f=eval_f0,
                    lb = c(-Inf,-2.5,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf), #this is changed to -2.5 from -2
                    ub = c(Inf,-0.5,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf),
                    opts = list("algorithm"="NLOPT_LN_COBYLA",
                                "xtol_rel"=1.0e-8,
                                # "print_level"=2,
                                "maxeval"=50000
                    ),       
                    actuals = actuals,
                    vars_t = vars_t )
#    print(res1$solution)
    mape <- mean(abs((actuals - colSums(res1$solution*vars_t))/actuals))
    
    output <- data.frame(IDENTIFIER = unlist(strsplit(key,"_"))[1],
                         CLUSTER = unlist(strsplit(key,"_"))[2],
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
                         coef_dec = res1$solution[15],
                         coef_index = res1$solution[16]
                        ) %>% rename(!!item_indt_1 := IDENTIFIER)
    return(output)
  }else{
    
    # coefs_0 <- c(
    #   7,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    # )
    
    coefs_0 <- c(
      7,-2,1,1,1,1,1,1,1,
      1,1,1,1,1,1
    )
    # coefs_0 <- c(
    #   5,-1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,
    #   0.1,0.1,0.1,0.1,0.1,0.1
    # )
    alpha <- .09
    num_iters <- 50000
    results <- gradDescent(vars, actuals, coefs_0, alpha, num_iters)
    coefs <- results[[1]]
    cost_hist <- results[[2]]
    # print(coefs)
    # plot(1:num_iters, cost_hist, type = 'l')
    
    # X <- vars
    # y <- actuals
    # theta <-c (8.098835248,-0.5,0.011068953,0.033719049,0,0,0.018459166,0.077666254,0.269171391,0.286827834,0.304350912,
    #           0.333810352,0.338647304,0.267008114,0.150041197)
    # compCost(X, y, theta)
    mape <- mean(abs((actuals - colSums(coefs*vars_t))/actuals))
    output <- data.frame(IDENTIFIER = unlist(strsplit(key,"_"))[1],
                         CLUSTER = unlist(strsplit(key,"_"))[2],
                         BP_ELAST_OPTI = coefs[2],
                         MAPE = mape,
                         coef_intercept = coefs[1],
                         coef_trend = coefs[3],
                         coef_jan = coefs[4],
                         coef_feb = coefs[5],
                         coef_mar = coefs[6],
                         coef_apr = coefs[7],
                         coef_may = coefs[8],
                         coef_jun = coefs[9],
                         coef_jul = coefs[10],
                         coef_aug = coefs[11],
                         coef_sep = coefs[12],
                         coef_oct = coefs[13],
                         coef_nov = coefs[14],
                         coef_dec = coefs[15])%>% rename(!!item_indt_1 := IDENTIFIER)
    return(output)
    
  }
}

# COMMAND ----------

elast_opti_func_cus2 <- function(key,item_indt_1){  #
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
  x_index <- rel_data$TOT_CAT_INDEX_LAST_52
  actuals <- rel_data$LN_UCM_BASELINE
  
  intercept <- rep(1,row_num)
  all_x_vars <- c(intercept,x_reg,x_trend,x_jan,x_feb,x_mar,x_apr,x_may,x_jun,x_jul,x_aug,x_sep,x_oct,x_nov,x_dec,x_index)
  
  vars <- matrix(data = all_x_vars, nrow = row_num, ncol = 16)
  
  if(opti_type == 'nloptr'){
    coefs_0 <- c(
      5,-1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,
      0.1,0.1,0.1,0.1,0.1,0.1,0.1
    )
    
    vars_t <- t(vars)
    
    # obj_sum_SSE <- sum((actuals - colSums(coefs*vars_t))^2)
    
    # objective function
    eval_f0 <- function( coefs, actuals, vars_t ){
      return( sum((actuals - colSums(coefs*vars_t))^2) )
    }
    
    res1 <- nloptr( x0= coefs_0,
                    eval_f=eval_f0,
                    lb = c(-Inf,-2.5,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf,-Inf), #this is changed to -2.5 from -2
                    ub = c(Inf,-0.5,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf,Inf),
                    opts = list("algorithm"="NLOPT_LN_COBYLA",
                                "xtol_rel"=1.0e-8,
                                # "print_level"=2,
                                "maxeval"=50000
                    ),       
                    actuals = actuals,
                    vars_t = vars_t )
#    print(res1$solution)
    mape <- mean(abs((actuals - colSums(res1$solution*vars_t))/actuals))
    
    output <- data.frame(IDENTIFIER = unlist(strsplit(key,"_"))[1],
                         CLUSTER = unlist(strsplit(key,"_"))[2],
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
                         coef_dec = res1$solution[15],
                         coef_index = res1$solution[16]
                        ) %>% rename(!!item_indt_1 := IDENTIFIER)
    return(output)
  }else{
    
    # coefs_0 <- c(
    #   7,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    # )
    
    coefs_0 <- c(
      7,-2,1,1,1,1,1,1,1,
      1,1,1,1,1,1
    )
    # coefs_0 <- c(
    #   5,-1,0.1,0.1,0.1,0.1,0.1,0.1,0.1,
    #   0.1,0.1,0.1,0.1,0.1,0.1
    # )
    alpha <- .09
    num_iters <- 50000
    results <- gradDescent(vars, actuals, coefs_0, alpha, num_iters)
    coefs <- results[[1]]
    cost_hist <- results[[2]]
    # print(coefs)
    # plot(1:num_iters, cost_hist, type = 'l')
    
    # X <- vars
    # y <- actuals
    # theta <-c (8.098835248,-0.5,0.011068953,0.033719049,0,0,0.018459166,0.077666254,0.269171391,0.286827834,0.304350912,
    #           0.333810352,0.338647304,0.267008114,0.150041197)
    # compCost(X, y, theta)
    mape <- mean(abs((actuals - colSums(coefs*vars_t))/actuals))
    output <- data.frame(IDENTIFIER = unlist(strsplit(key,"_"))[1],
                         CLUSTER = unlist(strsplit(key,"_"))[2],
                         BP_ELAST_OPTI = coefs[2],
                         MAPE = mape,
                         coef_intercept = coefs[1],
                         coef_trend = coefs[3],
                         coef_jan = coefs[4],
                         coef_feb = coefs[5],
                         coef_mar = coefs[6],
                         coef_apr = coefs[7],
                         coef_may = coefs[8],
                         coef_jun = coefs[9],
                         coef_jul = coefs[10],
                         coef_aug = coefs[11],
                         coef_sep = coefs[12],
                         coef_oct = coefs[13],
                         coef_nov = coefs[14],
                         coef_dec = coefs[15])%>% rename(!!item_indt_1 := IDENTIFIER)
    return(output)
    
  }
}
