# Databricks notebook source
# MAGIC %md
# MAGIC For global_tech:
# MAGIC 
# MAGIC Widget selections tested successful so far:
# MAGIC 
# MAGIC business_unit: 1400 -Florida Division 1400 - Florida Division
# MAGIC 
# MAGIC wave:  JAN2021_TestRefresh
# MAGIC 
# MAGIC category_name: 022_GENERAL_MERCHANDISE, 024_AUTOMOTIVE_PRODUCTS
# MAGIC 
# MAGIC user widget value 'global_tech' can be used for testing

# COMMAND ----------

# Modified by: Sanjo Jose
# Date Modified: 26/08/2021
# Modifications: Test run for Phase 4 BUs, Converted widgets to parameters, Other Minor modifications

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# Define parameters used in notebook
# business_units = '4200 - Great Lakes Division'
# bu_code = 'GL'
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'

# Define the database for tables and directory paths for input & output files
db = 'phase4_extensions'
base_directory = 'Phase4_extensions/Elasticity_Modelling'

# Modify for each new wave
# wave = 'Accenture_Refresh'
wave = dbutils.widgets.get("wave")

# Define the user
# user = 'sanjo'
user = dbutils.widgets.get("user")

# Define the category
# category_raw = '008-CANDY'
# category = '008_CANDY'
# category_raw = '002-CIGARETTES'
# category = '002_CIGARETTES'
category = dbutils.widgets.get("category_name")

# Define computed variables
bu = bu_code+'_Repo'

# DB table prefix
db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# # user = ['prat','roopa','sterling','neil','taru','colby','kushal','logan','prakhar','neil','dayton','david','anuj','xiaofan','global_tech','jantest','aadarsh','jantest2','tushar']
# user = ['sanjo']

# category_name = ['002_CIGARETTES','003_OTHER_TOBACCO_PRODUCTS','004_BEER','005_WINE','006_LIQUOR','007_PACKAGED_BEVERAGES','008_CANDY','009_FLUID_MILK','010_OTHER_DAIRY_DELI_PRODUCT', '012_PCKGD_ICE_CREAM_NOVELTIES','013_FROZEN_FOODS','014_PACKAGED_BREAD','015_SALTY_SNACKS','016_PACKAGED_SWEET_SNACKS','017_ALTERNATIVE_SNACKS','019_EDIBLE_GROCERY','020_NON_EDIBLE_GROCERY','021_HEALTH_BEAUTY_CARE','022_GENERAL_MERCHANDISE','024_AUTOMOTIVE_PRODUCTS','028_ICE','030_HOT_DISPENSED_BEVERAGES',	'031_COLD_DISPENSED_BEVERAGES',	'032_FROZEN_DISPENSED_BEVERAGES','085_SOFT_DRINKS','089_FS_PREP_ON_SITE_OTHER','091_FS_ROLLERGRILL','092_FS_OTHER','094_BAKED_GOODS','095_SANDWICHES','503_SBT_PROPANE','504_SBT_GENERAL_MERCH','507_SBT_HBA']

# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                   "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                   "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                  "Central Division","Western Division"]

# BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
#           ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
#           ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
#           ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# # wave = ["JAN2021_TestRefresh","Mar2021_Refresh_Group1"]
# wave = ['Accenture_Refresh']


# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave])
# dbutils.widgets.dropdown("user", "sanjo", [str(x) for x in user])
# dbutils.widgets.dropdown("category_name", "013_FROZEN_FOODS", [str(x) for x in category_name])

# # #get bu 2 character code
# # bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(dbutils.widgets.get("business_unit")))
# # bu_abb = bu_abb_ds.collect()[0][1].lower()

# COMMAND ----------

# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GL", bu_codes)

# COMMAND ----------

# user=dbutils.widgets.get("user")
# category_name=dbutils.widgets.get("category_name")
# wave=dbutils.widgets.get("wave")
# pre_wave = '/dbfs/Phase3_extensions/Elasticity_Modelling/' #ps v9

# COMMAND ----------

cat_sum = pd.read_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu+'/Output/intermediate_files/cat_sum_'+user+'_'+category+'.csv') #ps v9
cat_sum = cat_sum.sort_values(by=['fiscal_year','Week_No']) # sort to remove half-weeks
cat_sum = cat_sum[cat_sum.index>0] # remove first week, which could be half week with half sales
cat_sum = cat_sum[cat_sum.index<len(cat_sum)-1] # remove last week, which could be half week with half sales
cat_sum = cat_sum[cat_sum['Week_No']<53] # sf remove zeroes in week 53
cat_sum = cat_sum[cat_sum['sales_qty']!=0] # sf remove zeroes in other weeks
# display(cat_sum.head())

# COMMAND ----------

category_level_dummies = cat_sum.groupby('Week_No')['sales_qty'].mean().to_frame().reset_index()

category_level_dummies.columns = ['Week_No','SALES_QTY']

# COMMAND ----------

# def get_custom_percentile(cell):
#     val = (cell - category_level_dummies['SALES_QTY'].min())/(category_level_dummies['SALES_QTY'].max() -
#                                                              category_level_dummies['SALES_QTY'].min())
#     return val

# COMMAND ----------

def get_coef_var(week):
    week_num_slice = cat_sum[cat_sum['Week_No']==week]
    sales_week_num = list(week_num_slice['sales_qty'].values)
    std_dev = np.std(sales_week_num)
    avg_val = np.mean(sales_week_num)
    coef_var = 100 * std_dev / avg_val
    return coef_var

# COMMAND ----------

def find_eight_max(category_level_dummies):
  max_test = category_level_dummies.sort_values(by=['coef_var'],ascending=False)
  max_test = max_test.reset_index()
  
  # Old code
#   max_1 = max_test.at[0,'Week_No']
#   max_2 = max_test.at[1,'Week_No']
#   max_3 = max_test.at[2,'Week_No']
#   max_4 = max_test.at[3,'Week_No']
#   max_5 = max_test.at[4,'Week_No']
#   max_6 = max_test.at[5,'Week_No']
#   max_7 = max_test.at[6,'Week_No']
#   max_8 = max_test.at[7,'Week_No']
#   max_list = [max_1,max_2,max_3,max_4,max_5,max_6,max_7,max_8]

  # New loop added by NY202201 to solve the problem that there is not enough data
  max_list = []
  temp_max = np.nan
  n = 8
  for i in range(n):
    try:
      temp_max = max_test.at[i,'Week_No']
    except:
      pass
    max_list.append(temp_max)
  return max_list

# COMMAND ----------

# def find_four_max(category_level_dummies,avg_coef_var):
#     max_test = category_level_dummies.sort_values(by=['percentile'],ascending=False)
#     max_1_f = False
#     max_2_f = False
#     max_3_f = False
#     max_4_f = False
#     for wk_num in list(max_test.index.values):
#         if max_1_f == False and max_test.at[wk_num,'coef_var'] < avg_coef_var:
#             max_1 = wk_num
#             max_1_f = True
#         elif max_1_f == True and max_2_f == False and max_test.at[wk_num,'coef_var'] < avg_coef_var:
#             max_2 = wk_num
#             max_2_f = True
#         elif ( max_1_f 
#               == True ) and ( max_2_f 
#                              == True ) and max_3_f == False and max_test.at[wk_num,'coef_var'] < avg_coef_var:
#             max_3 = wk_num
#             max_3_f = True
#         elif ( max_1_f 
#               == True ) and ( max_2_f 
#                              == True ) and ( max_3_f 
#                                             == True ) and (max_4_f 
#                                                            == False) and ( max_test.at[wk_num,'coef_var'] 
#                                                                           < avg_coef_var ):
#             max_4 = wk_num
#             max_4_f = True
#         else:
#             continue
#     max_list = [max_1,max_2,max_3,max_4]
#     return max_list

# COMMAND ----------

# max_test = category_level_dummies.sort_values(by=['coef_var'],ascending=False)
# max_test.head(20)

# COMMAND ----------

# def find_four_min(category_level_dummies,avg_coef_var):
#     min_test = category_level_dummies.sort_values(by=['percentile'],ascending=True)
#     min_1_f = False
#     min_2_f = False
#     min_3_f = False
#     min_4_f = False
#     for wk_num in list(min_test.index.values):
#         if min_1_f == False and min_test.at[wk_num,'coef_var'] < avg_coef_var:
#             min_1 = wk_num
#             min_1_f = True
#         elif min_1_f == True and min_2_f == False and min_test.at[wk_num,'coef_var'] < avg_coef_var:
#             min_2 = wk_num
#             min_2_f = True
#         elif ( min_1_f 
#               == True ) and ( min_2_f 
#                              == True ) and min_3_f == False and min_test.at[wk_num,'coef_var'] < avg_coef_var:
#             min_3 = wk_num
#             min_3_f = True
#         elif ( min_1_f 
#               == True ) and ( min_2_f 
#                              == True ) and ( min_3_f 
#                                             == True ) and (min_4_f 
#                                                            == False) and ( min_test.at[wk_num,'coef_var'] 
#                                                                           < avg_coef_var ):
#             min_4 = wk_num
#             min_4_f = True
#         else:
#             continue
#     min_list = [min_1,min_2,min_3,min_4]
#     return min_list

# COMMAND ----------

# def combine_lists(max_week_num,max_list,min_week_num,min_list):
#     new_list = [max_week_num,min_week_num]
#     for num in max_list:
#         new_list.append(int(num))
#     for item in min_list:
#         new_list.append(int(item))
#     final_list = list(set(new_list))
#     return final_list

# COMMAND ----------

# category_level_dummies['percentile'] = category_level_dummies['SALES_QTY'].apply(get_custom_percentile)

# COMMAND ----------

# category_level_dummies.head()

# COMMAND ----------

# max_week = category_level_dummies[category_level_dummies['percentile']==category_level_dummies['percentile'].max()]
# max_week_num = list(max_week.index)[0]
# min_week = category_level_dummies[category_level_dummies['percentile']==category_level_dummies['percentile'].min()]
# min_week_num = list(min_week.index)[0]

# COMMAND ----------

# max_week

# COMMAND ----------

# min_week

# COMMAND ----------

category_level_dummies['coef_var'] = category_level_dummies.index.map(get_coef_var)

# COMMAND ----------

# category_level_dummies.tail()

# COMMAND ----------

# avg_coef_var = category_level_dummies['coef_var'].mean()
# avg_coef_var

# COMMAND ----------

#category_level_dummies['coef_var'] = category_level_dummies.index.map(get_coef_var)
#avg_coef_var = category_level_dummies['coef_var'].mean()
max_list = find_eight_max(category_level_dummies)
#max_list = find_four_max(category_level_dummies,avg_coef_var)
#min_list = find_four_min(category_level_dummies,avg_coef_var)
#fin_lst = combine_lists(max_week_num,max_list,min_week_num,min_list)

# COMMAND ----------

max_list

# COMMAND ----------

#df = pd.DataFrame(fin_lst,columns=['dummies'])
df = pd.DataFrame(max_list,columns=['dummies'])
df.to_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu+'/Output/intermediate_files/cat_dummies_'+user+'_'+category+'.csv',index=False) #ps v9

spark.createDataFrame(df).write.mode('overwrite').saveAsTable('localized_pricing.'+db_tbl_prefix+'output_int_cat_dummies_'+user+'_'+category)
