# Databricks notebook source
# modify the list below to reflect which user ran which categories for the refresh, in terms of the final runs that will get fed into stage 3 of accenture's elasticity code

# sf v3
# users_and_categories = [
#                          ['user','002_CIGARETTES']
#                         ,['prat','003_OTHER_TOBACCO_PRODUCTS']
#                         ,['user','004_BEER']
#                         ,['user','005_WINE']
#                         ,['user','006_LIQUOR']
#                         ,['user','007_PACKAGED_BEVERAGES']
#                         ,['user','008_CANDY']
#                         ,['user','009_FLUID_MILK']
#                         ,['user','010_OTHER_DAIRY_DELI_PRODUCT']
#                         ,['user','012_PCKGD_ICE_CREAM_NOVELTIES']
#                         ,['user','013_FROZEN_FOODS']
#                         ,['user','014_PACKAGED_BREAD']
#                         ,['user','015_SALTY_SNACKS']
#                         ,['user','016_PACKAGED_SWEET_SNACKS']
#                         ,['user','017_ALTERNATIVE_SNACKS']
#                         ,['user','019_EDIBLE_GROCERY']
#                         ,['user','020_NON_EDIBLE_GROCERY']
#                         ,['user','021_HEALTH_BEAUTY_CARE']
#                         ,['user','022_GENERAL_MERCHANDISE']
#                         ,['user','024_AUTOMOTIVE_PRODUCTS']
#                         ,['user','028_ICE']
#                         ,['user','030_HOT_DISPENSED_BEVERAGES']
#                         ,['user','031_COLD_DISPENSED_BEVERAGES']                       
#                         ,['user','032_FROZEN_DISPENSED_BEVERAGES']
#                         ,['user','085_SOFT_DRINKS']
#                         ,['user','089_FS_PREP_ON_SITE_OTHER']
#                         ,['user','091_FS_ROLLERGRILL']
#                         ,['user','092_FS_OTHER']
#                         ,['user','094_BAKED_GOODS']
#                         ,['user','095_SANDWICHES']
#                         ,['user','503_SBT_PROPANE']
#                         ,['user','504_SBT_GENERAL_MERCH']
#                         ,['user','507_SBT_HBA']
#                          ]

# COMMAND ----------

### Comment this command once testing is done
# users_and_categories = [['sanjo','016_PACKAGED_SWEET_SNACKS']  # NY v202110
#                         ]  # NY v202110

# COMMAND ----------

# # business unit & wave/refresh selection
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division", "1900 - Gulf Coast Division", "2600 - West Coast Division", "2800 - Texas Division", "2900 - South Atlantic Division", "3100 - Grand Canyon Division", "3800 - Northern Tier Division", "4100 - Midwest Division", "4200 - Great Lakes Division", "4300 - Heartland Division", "QUEBEC OUEST", "QUEBEC EST - ATLANTIQUE", "Central Division", "Western Division"]

# BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"], ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"], ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'], ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# wave = ["JAN2021_TestRefresh","MAY2021_Refresh","JUNE2021_Refresh"] #ps v2
# wave = ["Accenture_Refresh"]

# #creating widgets
# dbutils.widgets.dropdown("business_unit", "1600 - Coastal Carolina Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])

# COMMAND ----------

# import pandas as pd
# #get bu 2 character code
# bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# bu_abb = bu_abb_ds.collect()[0][1].lower()
# wave = dbutils.widgets.get("wave")
# bu = bu_abb_ds.collect()[0][1]+"_Repo"

# #paths
# pre_wave = "/dbfs/Phase3_extensions/Elasticity_Modelling/" #ps v9
# dbfs_path = pre_wave+wave+"/"+bu+"/Modelling_Inputs/"
# databricks_notebooks_path = "/Localized-Pricing/Phase3_extensions/Elasticity_Modelling/"+wave+"/"+bu+"/" #ps v9
# #sf
# databricks_perm_path = "/Localized-Pricing/Phase3_extensions/Elasticity_Modelling/JAN2021_TestRefresh/GR_Repo/" #ps 10

# COMMAND ----------

# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GR", bu_codes)

# COMMAND ----------

#define parameters
import pandas as pd

#specify the business_unit
# business_units = '1900 - Gulf Coast Division' #'4200 - Great Lakes Division'  # NY v202110
business_units = dbutils.widgets.get("business_unit")  # NY v202110

#specify bu code here
bu_code = 'GC' #'GL'  # NY v202110
bu_code = dbutils.widgets.get("bu_code")  # NY v202110

#specify bu country
# bu_cat = 'us' ## 'ca' | 'us' | 'eu'  # NY v202110
US_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD"]
CA_BU = ["QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]
if bu_code in US_BU:
  bu_cat = 'us'
elif bu_code in CA_BU:
  bu_cat = 'ca'
elif bu_code in EU_BU:
  bu_cat = 'eu'   # NY v202110
  
# define user
# user= 'sanjo'   # NY v202110

#define the wave here
# wave = 'Accenture_Refresh'   # NY v202110
wave = dbutils.widgets.get("wave")   # NY v202110

#specify the base directory here
base_directory = 'Phase4_extensions/Elasticity_Modelling'

#derived parameters
pre_wave = "/dbfs/Phase4_extensions/Elasticity_Modelling/" #ps v9

# databricks_notebooks_path = "/Localized-Pricing/LP_Process_Improvement/Final_Codes/Elasticity_Modelling/"   # NY v202110
databricks_notebooks_path = './'   # NY v202110

db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

#PG v11: If rerunning or running for the first time, please put 'y' as the overwrite value, otherwise 'n' followed by new_filename

overwrite = 'y'
new_filename = bu_code+'_'+wave+'Final_input_for_Refresh_database_v2.csv'

# COMMAND ----------

### Input the list of users and categories   # NY v202110
users_and_categories = sqlContext.sql("select * from localized_pricing."+db_tbl_prefix+"users_and_categories_df").toPandas().values.tolist()   # NY v202110
print(users_and_categories)   # NY v202110

# COMMAND ----------

### Save user category map as a csv for 3b, 3c to read
uc_df = pd.DataFrame(users_and_categories, columns = ['User', 'Category'])
uc_df.to_csv(pre_wave+wave+'/'+bu_code +"_Repo"+'/Input/user_category_'+bu_code.lower()+'.csv', index=False)

# COMMAND ----------

# DBTITLE 1,03b_create_baseline_collated
dbutils.notebook.run(databricks_notebooks_path+"03b_create_baseline_collated_v3",1000,\
                     {"business_unit": business_units,"wave": wave, "bu_code":bu_code })

# COMMAND ----------

# DBTITLE 1,03c_Create_Impute_Flag_file
dbutils.notebook.run(databricks_notebooks_path+"03c_Create_Impute_Flag_file_v2",1000,\
                     {"business_unit": business_units,"wave": wave, "bu_code":bu_code })

# COMMAND ----------

# DBTITLE 1,03d_create_reviewed_elasticities_at _BU
dbutils.notebook.run(databricks_notebooks_path+"03d_create_reviewed_elasticities_at_BU_v3",1000,\
                     {"business_unit": business_units,"wave": wave, "bu_code":bu_code })

# COMMAND ----------

# DBTITLE 1,03e_create_old_wave_elasticities
# if ((bu_cat == 'us')|(bu_cat == 'ca')):
#   dbutils.notebook.run(databricks_notebooks_path+"03e_create_old_wave_elasticities_v3",1000,\
#                      {"business_unit": business_units,"wave": wave, "bu_code": bu_code})   # NY v202110
# else:
#   dbutils.notebook.run(databricks_notebooks_path+"03e_create_old_wave_elasticities_v3_"+bu_cat,1000,\
#                      {"business_unit": business_units,"wave": wave, "bu_code":bu_code })   # NY v202110
dbutils.notebook.run(databricks_notebooks_path+"03e_create_old_wave_elasticities_v4",1000,\
                     {"business_unit": business_units,"wave": wave, "bu_code": bu_code})   # NY v202110

# COMMAND ----------

# DBTITLE 1,04_STG_3_Impute_Elasticities_NA_BUs
#PG v10 : Include "y/n": dbutils.widgets.get("overwrite") if you are running for the second time based if its a full rerun or a mini one. If a full rerun, put y, else put n. If n, then put a new filename as well, "BU,"_",wave,"_Final_input_for_Refresh_database_v2/v3.csv": dbutils.widgets.get("new_filename")
#PG v11
dbutils.notebook.run(databricks_notebooks_path+"04_STG_3_Impute_Elasticities_BUs_v7",1000,\
                     {"business_unit": business_units,"wave": wave, "bu_code":bu_code, "overwrite":overwrite,"new_filename":new_filename })
