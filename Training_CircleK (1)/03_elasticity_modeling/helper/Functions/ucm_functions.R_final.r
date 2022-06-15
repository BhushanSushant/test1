# Databricks notebook source
algo = "SR1"
tol_con = .0001

data_prep <- function(filtered_data,prdt){
  library(dplyr)
  
  data <- filtered_data
  data_prdt_v1<-data %>% 
    filter(LEVEL1_IDNT %in% prdt) %>%
    mutate(MEAN_SALES = mean(SALES_QTY,na.rm = T)) #%>%
#     filter(SALES_QTY > mean(SALES_QTY,na.rm = T)*.25)   #remove very outier sales
  
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
          data_prdt_v2$IMPUTE_COMMENT = "More than 30 weeks of data missing"
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
                             mutate(prev_qtr_sales = lag(SALES_QTY, 1))%>%
                             mutate(prev_year_qtr_sales =lag(SALES_QTY, 4))%>%
                             mutate(check = abs(prev_qtr_sales/SALES_QTY - 1))%>%
                             mutate(post_issue_index = ifelse(abs(prev_qtr_sales/SALES_QTY - 1) >= 1,1,0))%>%
                             mutate(post_issue_index = ifelse(is.na(post_issue_index),0, post_issue_index))%>%
                             mutate(post_issue_index_next_year = ifelse(abs(prev_year_qtr_sales/SALES_QTY - 1) >= 1,1,0))%>%
                             mutate(post_issue_index_next_year = ifelse(is.na(post_issue_index_next_year),0, post_issue_index_next_year))%>%
                             mutate(final_discont_series_index = ifelse((post_issue_index_next_year == 1) & (post_issue_index_next_year ==1),1,0))%>%
                             select(qtr, final_discont_series_index)%>%unique()%>%arrange(qtr)

              check_2 <- sum((data_prdt_v4[2,])$final_discont_series_index)
              check_2[is.na(check_2)] <- 0
              if(check_2 != 0){
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

#PGv12: chnage stockout duration to 6 weeks for nov 21 refresh
              if(nrow(data_prdt_v5_1)!=0){
                if(max(data_prdt_v5_1$stock_out_weeks)> 6){
                  data_prdt_v2 <- data_prdt_v2%>%
                          mutate(ISSUE_COMMENT = ifelse(ISSUE_COMMENT == "None","Might be too stockouts in the sales series spanning more than 6 weeks",paste0(ISSUE_COMMENT,",","Might be too stockouts in the sales series spanning more than 6 weeks")),
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
  
  return(data_prdt_v2)
}

independent_processing <- function(customized_data,independent){
  library(reshape2)
  data_init <- customized_data  %>% 
    select_(.dots = independent) %>%
    summarise_all(funs(sum(.,na.rm=T))) %>%
    gather(KEY,VALUE) %>%
    filter(VALUE > 0)
  return(data_init$KEY)
}

ucm_function <- function(customized_data,independant_selected,dependent,level_var){
  library(rucm)
  
  RHS_ucm_final<-paste(independant_selected ,collapse="+")
  model_init<- ucm_cus(as.formula(paste(dependent, "~",RHS_ucm_final)), data = customized_data,irregular = TRUE, level = TRUE)
  vslevel<-model_init$vs.level
  avs_level<-mean(vslevel)
  avs_level_1<-avs_level*level_var
  model_final <-ucm_cus(as.formula(paste(dependent, "~", RHS_ucm_final)), data = customized_data,irregular = TRUE, level = TRUE, level.var = avs_level_1)
  return(model_final)
}


ucm_estimates_func <- function(ucm_result){
  ucm_estimates<-data.frame(ucm_result$est)
  est<-as.vector(ucm_result$est)
  t.val<-est/attr(ucm_result, "se")
  p.value<-2*pt( -abs(t.val), df = attr(ucm_result,"df"))
  ucm_estimates$PVALUE <- p.value
  ucm_estimates$VARIABLE <- rownames(ucm_estimates)
  row.names(ucm_estimates)<-NULL
  colnames(ucm_estimates)[1] <- "ESTIMATES"
  ucm_estimates <- ucm_estimates[,c("VARIABLE","ESTIMATES","PVALUE")]
  return(ucm_estimates)
}


flagging <- function(ucm_estimates,sign_file){
  
  # print(class(sign_file))
  
  sign_merged <- ucm_estimates %>% 
    left_join(sign_file,"VARIABLE")
  
  sign_int <- ifelse(is.na(sign_merged$EXPECTED_SIGN),NA,ifelse(sign_merged$ESTIMATES > 0 ,"pos","neg"))
  sign_flag <- ifelse(is.na(sign_int),1,ifelse(sign_merged$EXPECTED_SIGN == sign_int ,1,0))
  sign_merged$FLAG <- sign_flag
  return(sign_merged)
}


validated_variables <- function(customized_data,dependent,level_var,flagged,pval,sign){
  drop <- flagged %>%
    filter(FLAG == 0 | PVALUE > pval) %>%
    arrange(FLAG, PVALUE)
  drop_count <-nrow(drop)
  
  # print(drop_count)
  
  # drop_count = 0
  if(drop_count == 0){
    new_varlist <- flagged$VARIABLE
    ucm_result_new<-ucm_function(customized_data,new_varlist,dependent,level_var)
  }else{
    while(drop_count > 0){
      
      # print("Started While")
      
      drop <- drop[-c(1,2),]
      drop_count_check <- nrow(drop)
      accepted <- flagged %>%
        filter(FLAG == 1 & PVALUE <= pval)
      new_varlist <- c(drop$VARIABLE,accepted$VARIABLE)
      
      ucm_result_new<-ucm_function(customized_data,new_varlist,dependent,level_var)
      
      # print("UCM Done")
      
      ucm_estimates_new <- ucm_estimates_func(ucm_result_new)
      
      # print(nrow(ucm_estimates_new))
      # print("Start Flagging")
      
      flagged<-flagging(ucm_estimates_new,sign)
      
      
      drop <- flagged %>%
        filter(FLAG == 0 | PVALUE > pval) %>%
        arrange(FLAG,PVALUE)
      drop_count <- nrow(drop)
      
      # print("Ended Round")
      if(drop_count <= 2 & nrow(accepted) == 0){drop_count <- 0}
      if(drop_count_check == 0){drop_count <- 0}
    }
  }
  return(ucm_result_new)
}

base_predict <- function(validated_ucm,customized_data){
  smooth_dat <- data.frame(ORDER = 1:nrow(customized_data), BASE = exp(as.vector(validated_ucm$s.level)))
  UCM_SM <- loess(BASE ~ ORDER,span=0.75,smooth_dat)
  data <- data.frame(WEEK_SDATE = customized_data$WEEK_SDATE,
                     S_LEVEL = as.vector(validated_ucm$s.level),
                     S_TREG = as.vector(predict(validated_ucm)),
                     UCM_BASELINE = as.vector(smooth_dat$BASE),#as.vector(smooth_dat$BASE),#as.vector(predict(UCM_SM))
                     PREDICTED = exp(as.vector(predict(validated_ucm)))
  )
  data_merged <- merge(customized_data,data,by.customized_data = WEEK_SDATE,all.x = T)
  data_merged$SALES <- exp(data_merged$LN_SALES_QTY)
  
  return(data_merged)
}

#--------------------------------------------------------Contribution----------------------------------------------#

contribution_stage_1 <- function(validated_ucm_estimates,data_baseline_predicted,customized_vars,reg_price){
  var_list <- validated_ucm_estimates$VARIABLE
  data_relevant <- data_baseline_predicted[,c(var_list)]
  reference <- data.frame(VARIABLE = validated_ucm_estimates$VARIABLE, ESTIMATES = validated_ucm_estimates$ESTIMATES)
  if(length(var_list) > 1){
    contri <- exp(data_relevant*reference$ESTIMATES[match(names(data_relevant), reference$VARIABLE)][col(data_relevant)])
  }else{
    contri <- data.frame(Column = exp(data_relevant*reference$ESTIMATES))
    colnames(contri) <- var_list
  }
  
  contri$WEEK_SDATE <- data_baseline_predicted$WEEK_SDATE
  rearranged<- contri[,c("WEEK_SDATE",var_list)]
  colnames(rearranged)[-1] <- paste("cont_",colnames(rearranged)[-1],sep="")
  final <- merge(data_baseline_predicted,rearranged,by.data_baseline_predicted = WEEK_SDATE)
  if(length(var_list) >0){
    #For baseline sales
    relevent_varlist <- var_list[!var_list %in% c(customized_vars)]
    if(length(relevent_varlist) >0){
      relevant_contri <- contri[,c(relevent_varlist)]
      if(length(relevent_varlist)==1){
        baseline_coeff <- relevant_contri
      }else{
        baseline_coeff <- apply(relevant_contri, 1, prod)
      }
      final$BASELINE_SALES <- baseline_coeff*final$UCM_BASELINE
    }else{
      final$BASELINE_SALES <- final$UCM_BASELINE
    }
    #For predicted sales
    relevent_varlist_pred <- var_list[var_list %in% customized_vars]
    if(length(relevent_varlist_pred) >0){
      relevant_contri_pred <- contri[,c(relevent_varlist_pred)]
      if(length(relevent_varlist_pred)==1){
        pred_coeff <- relevant_contri_pred
      }else{
        pred_coeff <- apply(relevant_contri_pred, 1, prod)
      }
      final$PREDICTED_SALES <- pred_coeff*final$BASELINE_SALES
    }else{
      final$PREDICTED_SALES  <- final$BASELINE_SALES
    }
    final$SYSTEM_PREDICTED <- final$PREDICTED
  }else{
    final$BASELINE_SALES <- final$UCM_BASELINE
    final$PREDICTED_SALES <- final$BASELINE_SALES
    final$SYSTEM_PREDICTED <- final$UCM_BASELINE
  }
  
  final_output <- final[,c("LEVEL1_IDNT","WEEK_SDATE","S_LEVEL",paste("cont_",var_list,sep= ""),
                           "S_TREG","UCM_BASELINE","BASELINE_SALES","PREDICTED_SALES","SYSTEM_PREDICTED", 
                           "SALES_QTY","REG_PRICE","NET_PRICE",customized_vars)] 
  
  return(final_output)
}

#-----------------------------------------------------------Regression----------------------------------------------#

regression <- function(with_contri,validated_ucm_estimates,customized_vars,reg_price){
  var_list <- validated_ucm_estimates$VARIABLE
  data <- with_contri
  p<-lm(SALES_QTY ~ 0 + PREDICTED_SALES,data = data)
  data$EST_PRED <- rep(as.vector(p$coefficients),nrow(data))
  data$PREDICTED_FINAL_REV <- data$PREDICTED_SALES*data$EST_PRED
  data$BASELINE_REV <- data$BASELINE_SALES*data$EST_PRED
  
  data$PREDICTED_FINAL_REV  <- ifelse(is.na(data$PREDICTED_FINAL_REV),data$PREDICTED_SALES,data$PREDICTED_FINAL_REV)
  data$BASELINE_REV <- ifelse(is.na(data$BASELINE_REV),data$BASELINE_SALES,data$BASELINE_REV)
  
  final_data <- data[,c("LEVEL1_IDNT","WEEK_SDATE","S_LEVEL",paste("cont_",var_list,sep= ""),
                        "S_TREG","UCM_BASELINE","BASELINE_SALES","PREDICTED_SALES","SYSTEM_PREDICTED",
                        "NET_PRICE","REG_PRICE","SALES_QTY",
                        customized_vars,
                        "PREDICTED_FINAL_REV","EST_PRED","BASELINE_REV"
  )]
  return(final_data)
}

#---------------------------------------------------BP Elasticity-------------------------------------------#

# base_price_elasticity <- function(regression_output,customized_data,reg_price){
#   reg_rel <- regression_output[,c("LEVEL1_IDNT","WEEK_SDATE","UCM_BASELINE")]
#   cus_rel <- customized_data[,c("LEVEL1_IDNT","WEEK_SDATE","LN_REG_PRICE",reg_price)]
#   full_data <- merge(cus_rel,reg_rel)
#   full_data$LN_UCM_BASELINE <- ifelse(full_data$UCM_BASELINE > 0,log(full_data$UCM_BASELINE),0)
#   
#   full_data <- full_data %>%
#     arrange(WEEK_SDATE) %>%
#     mutate(TREND = 1:n())
#   
#   vif_frame <- full_data[,c(reg_price,"TREND")]
#   vif_calc <- usdm::vif(vif_frame)
#   vif_rel <- unique(vif_calc$VIF)[1]
#   
#   model <- lm(LN_UCM_BASELINE ~ LN_REG_PRICE, data = full_data)
#   model_trend <- lm(LN_UCM_BASELINE ~ LN_REG_PRICE + TREND, data = full_data)
#   
#   
#   
#   out_data <- data.frame(LEVEL1_IDNT = unique(regression_output$LEVEL1_IDNT),
#                          BASEPRICE_ELASTICITY =  model$coefficients[2],
#                          BASEPRICE_ELASTICITY_TREND =  model_trend$coefficients[2],
#                          TREND = model_trend$coefficients[3],
#                          REG_PRICE_CV = (sd(full_data[,which(names(full_data) %in% reg_price)],na.rm=T)/mean(full_data[,which(names(full_data) %in% reg_price)],na.rm=T))*100,
#                          VIF_BP_TREND = vif_rel
#   )
#   rownames(out_data) <- NULL
#   return(out_data)
# }
# 

#---------------------------------------------------ucm_CG------------------------------------------------


ucm_cus <-
  function (formula, data, irregular = TRUE, irregular.var = NA, 
            level = TRUE, level.var = NA, slope = FALSE, slope.var = NA, 
            season = FALSE, season.length = NA, season.var = NA, cycle = FALSE, 
            cycle.period = NA, cycle.var = NA) 
  {
    if (missing(data)) 
      stop("Data required")
    if (!(level) && slope) 
      stop("Level to be included to have slope")
    if (season && is.na(season.length)) 
      stop("Specify season length")
    if (cycle && is.na(cycle.period)) 
      stop("Specify cycle period")
    if (irregular) 
      H <- irregular.var
    if (level) {
      comp_trend <- deparse(substitute(SSMtrend(degree = degree, 
                                                Q = Q), list(degree = 1, Q = level.var)))
    }
    if (slope) {
      comp_trend <- deparse(substitute(SSMtrend(degree = degree, 
                                                Q = Q), list(degree = 2, Q = list(level.var, slope.var))))
    }
    if (season) {
      comp_sea <- deparse(substitute(SSMseasonal(period = period, 
                                                 Q = Q), list(period = season.length, Q = season.var)))
    }
    if (cycle) {
      comp_cyc <- deparse(substitute(SSMcycle(period = period, 
                                              Q = Q), list(period = cycle.period, Q = cycle.var)))
    }
    all_terms <- terms(formula)
    indep.var <- attr(all_terms, "term.labels")
    dep.var <- all_terms[[2]]
    comp <- "0"
    if (length(indep.var) > 0) 
      comp <- paste(indep.var, collapse = "+")
    if (exists("comp_trend")) 
      comp <- paste(comp, comp_trend, sep = "+")
    if (exists("comp_sea")) 
      comp <- paste(comp, comp_sea, sep = "+")
    if (exists("comp_cyc")) 
      comp <- paste(comp, comp_cyc, sep = "+")
    ssm.formula <- paste0(dep.var, "~", comp)
    init.times <- 1
    if (irregular) 
      init.times <- init.times + 1
    if (level) 
      init.times <- init.times + 1
    if (slope) 
      init.times <- init.times + 1
    if (season) 
      init.times <- init.times + 1
    if (cycle) 
      init.times <- init.times + 2
    if (is.null(rownames(data))) 
      parm <- log(var(data))
    else parm <- log(var(data[, as.character(dep.var)]))
    inits <- rep(parm, times = init.times)
    if (irregular) 
      modelH <- SSModel(as.formula(ssm.formula), H = H, data = data, tol = tol_con)
    else modelH <- SSModel(as.formula(ssm.formula), data = data)
    modelH <- fitSSM_cus(inits = inits, model = modelH)$model
    out <- KFS(modelH, filtering = "state", smoothing = "state")
    irr.var <- modelH$H
    names(irr.var) <- "Irregular_Variance"
    if (length(indep.var) > 0) {
      find.indep.var <- match(indep.var, rownames(modelH$T))
      est <- out$alphahat[nrow(out$alphahat), find.indep.var]
      se <- sapply(find.indep.var, function(x) mean(sqrt(out$V[x, 
                                                               x, ]),na.rm=T))
    }
    else {
      est <- NULL
      se <- NULL
    }
    if (level) {
      find.level <- match("level", rownames(modelH$T))
      s.level <- out$alphahat[, find.level]
      vs.level <- out$V[find.level, find.level, ]
      est.var.level <- modelH$Q[find.level - length(indep.var), 
                                find.level - length(indep.var), ]
      names(est.var.level) <- "Level_Variance"
    }
    else {
      s.level <- NULL
      vs.level <- NULL
      est.var.level <- NULL
    }
    if (slope) {
      find.slope <- match("slope", rownames(modelH$T))
      s.slope <- out$alphahat[, find.slope]
      vs.slope <- out$V[find.slope, find.slope, ]
      est.var.slope <- modelH$Q[find.slope - length(indep.var), 
                                find.slope - length(indep.var), ]
      names(est.var.slope) <- "Slope_Variance"
    }
    else {
      s.slope <- NULL
      vs.slope <- NULL
      est.var.slope <- NULL
    }
    if (season) {
      find.season <- match("sea_dummy1", rownames(modelH$T))
      s.season <- out$alphahat[, find.season]
      vs.season <- out$V[find.season, find.season, ]
      est.var.season <- modelH$Q[find.season - length(indep.var), 
                                 find.season - length(indep.var), ]
      names(est.var.season) <- "Season_Variance"
    }
    else {
      s.season <- NULL
      vs.season <- NULL
      est.var.season <- NULL
    }
    if (cycle) {
      find.cycle <- match("cycle", rownames(modelH$T))
      s.cycle <- out$alphahat[, find.cycle]
      vs.cycle <- out$V[find.cycle, find.cycle, ]
      est.var.cycle <- modelH$Q[find.cycle - length(indep.var), 
                                find.cycle - length(indep.var), ]
      names(est.var.cycle) <- "Cycle_Variance"
    }
    else {
      s.cycle <- NULL
      vs.cycle <- NULL
      est.var.cycle <- NULL
    }
    if (is.ts(data)) {
      df <- length(data) - length(indep.var)
    }
    else df <- nrow(data) - length(indep.var)
    mc <- match.call(expand.dots = TRUE)
    res.out <- list(est = est, irr.var = irr.var, est.var.level = est.var.level, 
                    est.var.slope = est.var.slope, est.var.season = est.var.season, 
                    est.var.cycle = est.var.cycle, s.level = s.level, s.slope = s.slope, 
                    s.season = s.season, s.cycle = s.cycle, vs.level = vs.level, 
                    vs.slope = vs.slope, vs.season = vs.season, vs.cycle = vs.cycle)
    res.out$call <- mc
    res.out$model <- modelH
    class(res.out) <- "ucm"
    attr(res.out, "se") <- se
    attr(res.out, "df") <- df
    invisible(res.out)
  }

#-----------------------------------fitSSM---------------------------------#

fitSSM_cus <-
  function (model, inits, updatefn, checkfn, ...) 
  {
    if (missing(updatefn)) {
      if (max(dim(model$H)[3], dim(model$Q)[3]) > 1) 
        stop("No model updating function supplied, but cannot use default function as covariance matrices are time varying.")
      updatefn <- function(pars, model, ...) {
        Q <- as.matrix(model$Q[, , 1])
        naQd <- which(is.na(diag(Q)))
        naQnd <- which(upper.tri(Q[naQd, naQd]) & is.na(Q[naQd, 
                                                          naQd]))
        Q[naQd, naQd][lower.tri(Q[naQd, naQd])] <- 0
        diag(Q)[naQd] <- exp(0.5 * pars[1:length(naQd)])
        Q[naQd, naQd][naQnd] <- pars[length(naQd) + 1:length(naQnd)]
        model$Q[naQd, naQd, 1] <- crossprod(Q[naQd, naQd])
        if (!identical(model$H, "Omitted")) {
          H <- as.matrix(model$H[, , 1])
          naHd <- which(is.na(diag(H)))
          naHnd <- which(upper.tri(H[naHd, naHd]) & is.na(H[naHd, 
                                                            naHd]))
          H[naHd, naHd][lower.tri(H[naHd, naHd])] <- 0
          diag(H)[naHd] <- exp(0.5 * pars[length(naQd) + 
                                            length(naQnd) + 1:length(naHd)])
          H[naHd, naHd][naHnd] <- pars[length(naQd) + length(naQnd) + 
                                         length(naHd) + 1:length(naHnd)]
          model$H[naHd, naHd, 1] <- crossprod(H[naHd, naHd])
        }
        model
      }
    }
    is.SSModel(updatefn(inits, model, ...), na.check = TRUE, 
               return.logical = FALSE)
    if (missing(checkfn)) {
      likfn <- function(pars, model, ...) {
        model <- updatefn(pars, model, ...)
        -logLik(object = model, check.model = TRUE, ...)
      }
    }
    else {
      likfn <- function(pars, model, ...) {
        model <- updatefn(pars, model, ...)
        if (checkfn(model)) {
          return(-logLik(object = model, ...))
        }
        else return(.Machine$double.xmax)
      }
    }
    gradient <- function(x,model,...){
      return(grad(likfn,x,model=model,...))
    }
    out <- NULL
    #   out$optim.out <- optim(par = inits, fn = likfn, model = model, control = list(maxit = 2000),
    #                          ...)
    #   out$model <- updatefn(out$opt$par, model, ...)
    out$optim.out <- trust.optim(x = inits, fn = likfn, gr= gradient,method = algo, model= model, control = list(trace = 0, report.level = -1,start.trust.radius = 5, maxit = 1000), ...)
    out$model <- updatefn(out$opt$solution, model, ...)
    # nloptr
    # out$optim.out <- auglag(x0 = inits, fn = likfn, model = model,...)
    # out$model <- updatefn(out$opt$par, model, ...)
    
    # out$optim.out <- nlm(p = inits, f = likfn, model = model,...)
    # out$model <- updatefn(out$opt$estimate, model, ...)
    #   
    out
  }
