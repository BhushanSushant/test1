# Databricks notebook source
# MAGIC %md
# MAGIC ##### Call Libs

# COMMAND ----------

library(tidyverse)
library(nloptr)
library(lubridate)
library(parallel)
library(readxl)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Source Codes

# COMMAND ----------

# MAGIC %run "./Elasticity_opti_funcs"

# COMMAND ----------

# MAGIC %run "./do_parallel_elast_opti_new"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Main Code

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Constrained Regression

# COMMAND ----------

#Logging Locations

log_loc = paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/Elasticity_opti_log/elasticty_opti_log_",user,"_v",version,".txt")
opti_type <- "nloptr" #not to be changed
if(erase_old_output == T ){
  print("Creating Folders for writing files...")
  invisible(file.remove(log_loc))
  generate_user_version_folders_elast_opt(user,version,bu,base_directory)
  
  #Clearing Old Logs
  
  # print("Clearing Old Logs...")
  close(file(paste0(paste0(log_loc)), open="w" ) )
#   invisible(do.call(file.remove, list(list.files(nodal_log, full.names = TRUE))))
}
#read all files for which baseline is created in stage 1
baseline_rdss = paste0("baselines_",unique(parse_number(list.files(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/")))),".Rds")

read_baselines <- c()
for(i in baseline_rdss){
  read_in <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/intermediate_files/",user,"/v",version,"/",i)) 
  read_baselines <- bind_rows(read_baselines,read_in)
}
read_baselines <- read_baselines%>%
        select(ITEMID, WEEKDATE, STORE, SALES_QTY, REVENUE, REG_PRICE, NET_PRICE, LN_SALES_QTY, LN_REG_PRICE, UCM_BASELINE)%>%
        rename(!!item_indt_1 := ITEMID,
               WEEK_START_DATE = WEEKDATE,
               CLUSTER = STORE)%>%
       mutate(!!item_indt_1 := as.numeric(get(item_indt_1)))

inscope_items <- scope[scope$STATUS == "Y",]$ITEMID

main_dat <- read_baselines %>% 
  mutate(KEY = paste0(get(item_indt_1),"_",CLUSTER))%>%
         filter(get(item_indt_1) %in% inscope_items)

keys <- unique(main_dat$KEY)

ncores <- 16
cl <- makeCluster(ncores)
clusterExport(cl,c(
  "gradDescent","compCost","main_dat","keys","main_dat","opti_type","customize_data","custom_data_func","log_loc","elast_opti_func","slope_type","trend_inflex_date","user","version","bu","item_indt_1","item_indt_1_prod","item_indt_2_temp","item_indt_2","item_indt_3","item_indt_4","wave","base_directory"
),environment())

clusterEvalQ(cl, sink(paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Log/nodal_log/",user,"/v",version,"/",paste0("elast_",Sys.getpid()), ".txt")))

out <- bind_rows(parLapply(cl,keys, function(x){   #parLapply(cl,
  cat(paste0("Model_No : ",which(keys == x),"\n"),file=paste0(log_loc), append=TRUE)
  cat(paste0("Item_Cluster: ",x,"\n"),file=paste0(log_loc), append=TRUE)
  final_out <- elast_opti_func(x,item_indt_1)
  return(final_out)
}))
stopCluster(cl)

if (region=='NA'){
  product_map <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/product_map_na.Rds"))
}
if (region=='EU'){
 product_map <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/product_map_eu.Rds"))
}

# product_map <- readRDS(paste0("/dbfs/",base_directory,"/",wave,"/product_map_na.Rds"))
out_cols <- names(out)
prod_cols <- setdiff(names(product_map),item_indt_1_prod)
out_final <- out%>%
            mutate(!!item_indt_1 := as.numeric(as.character(get(item_indt_1)))) %>%
             left_join(product_map, c(setNames(item_indt_1_prod,item_indt_1)))%>%
             select(c(prod_cols,out_cols))%>%arrange(!!as.name(item_indt_1),CLUSTER)%>%
             rename(!!item_indt_2_temp := !!item_indt_2,
                    ITEM_DESCRIPTION = !!item_indt_3, 
                    CATEGORY = !!item_indt_4,
                    BP_ELASTICITY = BP_ELAST_OPTI,
                    ESTIMATES_MAPE = MAPE)%>%
             mutate(ESTIMATES_MAPE_FLAG = ifelse(ESTIMATES_MAPE <.10, "Good","Issue"))
out_t <- out_final%>%
           select(c(item_indt_1,item_indt_2_temp,ITEM_DESCRIPTION,CATEGORY,CLUSTER,BP_ELASTICITY,ESTIMATES_MAPE,ESTIMATES_MAPE_FLAG
))

write_csv(out_final,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","elast_cons_reg_out.csv"))
write_csv(out_t,paste0("/dbfs/",base_directory,"/",wave,"/",bu,"/Output/final_output/",user,"/v",version,"/","elast_cons_reg_out_template.csv"))

