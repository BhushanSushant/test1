# Databricks notebook source
# import the necessary packages and prepare pyspark for use during this program
#versions I used
# sf added the install for xlsxwriter
# !pip install xlsxwriter
# dbutils.library.installPyPI('xlrd','1.2.0')
# dbutils.library.restartPython()

# COMMAND ----------

# import the necessary packages and prepare pyspark for use during this program
import pandas as pd
import numpy as np
import glob
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from dateutil.relativedelta import relativedelta
import xlrd

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# business_units = "3100 - Grand Canyon Division"
# bu_code = "GR" 
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

# Define Region
NA_BU = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
EU_BU = ["IE", "SW", "NO", "DK", "PL"]

if bu_code in NA_BU:
  region = 'NA'
elif bu_code in EU_BU:
  region = 'EU'

# wave = "Accenture_Refresh"
wave = dbutils.widgets.get("wave")

pre_wave = "/Phase4_extensions/Elasticity_Modelling/"
bu_abb = bu_code.lower()
bu = bu_code+'_Repo'

if region=='NA':
  product_id_type = 'PRODUCT_KEY'
elif region=='EU':
  product_id_type = 'TRN_ITEM_SYS_ID'

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# #sf
# Business_Units = ["1400 - Florida Division", "1600 - Coastal Carolina Division",  "1700 - Southeast Division", "1800 - Rocky Mountain Division",
#                  "1900 - Gulf Coast Division","2600 - West Coast Division", "2800 - Texas Division","2900 - South Atlantic Division","3100 - Grand Canyon Division",
#                  "3800 - Northern Tier Division", "4100 - Midwest Division","4200 - Great Lakes Division","4300 - Heartland Division","QUEBEC OUEST","QUEBEC EST - ATLANTIQUE",
#                 "Central Division","Western Division"]

# # BU_abbr = [["1400 - Florida Division", 'FL'],["1600 - Coastal Carolina Division" ,'CC'] ,["1700 - Southeast Division", "SE"],["1800 - Rocky Mountain Division", 'RM'],["1900 - Gulf Coast Division", "GC"],\
# #           ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'],["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"],\
# #           ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'],["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'],\
# #           ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'],["Western Division", 'WC']]

# # Busness_Units = [bu_pair[0] for bu_pair in BU_abbr]

# #wave_list = ["JAN2021_TestRefresh","MAY2021_Refresh","JUNE2021_Refresh"] #ps v4
# wave_list = ['Accenture_Refresh']

# dbutils.widgets.dropdown("business_unit", "3100 - Grand Canyon Division", Business_Units)
# dbutils.widgets.dropdown("wave", "Accenture_Refresh", [str(x) for x in wave_list])
# # business_unit = '1600 - Coastal Carolina Division'

# # product_id_type = 'PRODUCT_KEY' # as of 9/1/2021 'PRODUCT_KEY' for NA, 'TRN_ITEM_SYS_ID' for EU

# #get bu 2 character code
# # bu_abb_ds = sqlContext.createDataFrame(BU_abbr, ['bu', 'bu_abb']).filter('bu = "{}"'.format(business_unit))
# # bu_abb = bu_abb_ds.collect()[0][1].lower()
# # bu = bu_abb.upper()+'_Repo'
# # wave= 'JAN2021_TestRefresh'
# # pre_wave = '/Phase3_extensions/Elasticity_Modelling/'   #ps v9

# COMMAND ----------

# bu_codes = ["FL" ,"CC", "SE" ,"RM" ,"GC" ,"WC", "TX", "SA" ,"GR", "NT", "MW" ,"GL" ,"HLD" ,"QW", "QE", "CE", "WD"]
# dbutils.widgets.dropdown("bu_code", "GR", bu_codes)

# COMMAND ----------

uc = pd.read_csv('/dbfs'+pre_wave+wave+'/'+bu+'/Input/user_category_'+bu_abb+'.csv') #ps v4
assert not uc.empty

# COMMAND ----------

baseline_df = pd.DataFrame()
for a,b in uc.iterrows():  #ps v4
  # use this line for testing with wave 1
  print(a, b)
  print(r"/dbfs"+pre_wave+wave+"/"+bu+"/Output/final_output/"+b[0]+"/Final_Combined_Result_"+b[1]+".xlsx")
  baseline_data_output = pd.read_excel(r"/dbfs"+pre_wave+wave+"/"+bu+"/Output/final_output/"+b[0]+"/Final_Combined_Result_"+b[1]+".xlsx", sheet_name = "Baseline Data Output") #ps v9
  # use this line for the refreshes
  # baseline_data_output = pd.read_excel(r"/dbfs/Phase3_extensions/Elasticity_Modelling/"+wave+"/"+bu+"/Output/final_output/"+pair[0]+"/Final_Combined_Result_"+bu_abb+"_"+pair[1]+".xlsx", sheet_name = "Baseline Data Output")
#   key_fields = baseline_data_output[['PRODUCT_KEY', 'CATEGORY', 'CLUSTER', 'WEEK_START_DATE', 'UCM_BASELINE']]
  baseline_df = baseline_df.append(baseline_data_output)
assert not baseline_df.empty

# COMMAND ----------

# get the items for the category
items = list(baseline_df[product_id_type].unique())
assert items

# COMMAND ----------

# baseline_df.loc[baseline_df['CATEGORY'] == '017-ALTERNATIVE SNACKS']
baseline_df[baseline_df['WEEK_START_DATE'].isna()]
assert not baseline_df.empty

# COMMAND ----------

nan = baseline_df.ESTIMATES_MAPE.isnull().groupby([baseline_df['CATEGORY']]).sum().astype(int)
nan

# COMMAND ----------

nan_test_prop = baseline_df.groupby([product_id_type,'CLUSTER','CATEGORY'])['ESTIMATES_MAPE'].mean().to_frame().reset_index()
nan_items = nan_test_prop[nan_test_prop['ESTIMATES_MAPE'].isnull()]
nan_cats = nan_items['CATEGORY'].value_counts() / 5
nan_cats

# COMMAND ----------

baseline_df['WEEK_START_DATE'] = pd.to_datetime(baseline_df['WEEK_START_DATE'], format='%Y%m%d')

# COMMAND ----------

# initialize the dataframe that will hold the intermediate results
intermediate_table = baseline_df[[product_id_type, 'CLUSTER', 'WEEK_START_DATE', 'SALES_QTY', 'REG_PRICE','ESTIMATES_MAPE']].copy()

# COMMAND ----------

intermediate_table[intermediate_table['WEEK_START_DATE'].isna()]
assert intermediate_table[intermediate_table['WEEK_START_DATE'].isna()].empty

# COMMAND ----------

# get rid of unnecessary decimal points
# intermediate_table['PRODUCT_KEY'] = pd.to_numeric(intermediate_table['PRODUCT_KEY'], downcast='integer')
# intermediate_table['CLUSTER'] = pd.to_numeric(intermediate_table['CLUSTER'], downcast='integer')

# COMMAND ----------

intermediate_table['WEEK_START_DATE'] = intermediate_table['WEEK_START_DATE'].astype(str)

# COMMAND ----------

# get all the weeks in the time series
full_weeks_list = sorted(list(intermediate_table.WEEK_START_DATE.unique()))
max_num_weeks = len(list(intermediate_table.WEEK_START_DATE.unique()))
assert full_weeks_list

# COMMAND ----------

last_five_weeks = full_weeks_list[-5:]
assert last_five_weeks

# COMMAND ----------

def single_price_tester():
  baseline_item_cluster = baseline_item[baseline_item['CLUSTER']==cluster]
  single_price_test = baseline_item_cluster['REG_PRICE'].std()
  if single_price_test == 0:
    final_table.at[row_number, product_id_type] = item
    final_table.at[row_number, 'CLUSTER'] = cluster
    final_table.at[row_number, 'Impute_Flag'] = 1
    row_number = row_number + 1
    
# a sum of sales function that assists the dying series code below 
def sum_of_sales(sales_list,time):
  total_sales = 0
  if time == 'recent':
    for weekly_sales in sales_list[-12:]:
      total_sales = total_sales + weekly_sales
  else:
    for weekly_sales in sales_list[-64:-52]:
      total_sales = total_sales + weekly_sales 
  return total_sales

# another sum of sales function 
def sum_of_early_sales(sales_list,time):
  total_sales = 0
  if time == 'early':
    for weekly_sales in sales_list[:12]:
      total_sales = total_sales + weekly_sales
  else:
    for weekly_sales in sales_list[52:64]:
      total_sales = total_sales + weekly_sales 
  return total_sales

# COMMAND ----------

# initialize the dataframe that will hold the final results
final_table = pd.DataFrame(columns=[product_id_type, 'CLUSTER', 'Impute_Flag'])

# set a row number variable that will help add rows to the final results table
row_number = 0


# test for no price change in the time series for each item-cluster combination
for item in items:
  baseline_item = intermediate_table[intermediate_table[product_id_type]==item]
  # get the clusters specific to the item, which in some data files might be less than the default value
  clusters = list(baseline_item.CLUSTER.unique())
  for cluster in clusters:
    baseline_item_cluster = baseline_item[baseline_item['CLUSTER']==cluster]
    
    # test for no price change in the time series for each item-cluster combination
    single_price_test = baseline_item_cluster['REG_PRICE'].std()
    if single_price_test == 0:
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
    
    # test each item-cluster combination for smoothness across its entire time series except for unintentional price changes (meaning they lasted less than four weeks)
    price_spans = list(baseline_item_cluster['REG_PRICE'].value_counts())
    price_spans.pop(0)    
    if all(i < 4 for i in price_spans) and len(price_spans)>0:
      impute = True
    else:
      impute = False
    if impute == True:
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
    
    # test for discontinued items for each item-cluster combination
    # this means they have no sales in the last five weeks, even though most other items do
    weeks_list = [str(week) for week in list(baseline_item_cluster['WEEK_START_DATE'].values)]
    last_five_weeks = full_weeks_list[-5:]
    if len(list(set(last_five_weeks) - set(weeks_list)))<5:
      continue
    else:
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
      
    sales_list = list(baseline_item_cluster.SALES_QTY.values)
    recent_12_weeks_sales = sum_of_sales(sales_list,'recent')
    last_year_12_weeks_sales = sum_of_sales(sales_list,'last year')
          
    # test if total sales across the last 12 weeks has dropped more than 75% compared to the same 12 weeks a year ago, for each item-cluster combination
    # these are also known as dying series
    if recent_12_weeks_sales < ( last_year_12_weeks_sales / 4 ):
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
      
    # test if total sales across the first 12 weeks is more than 75% lower than the same 12 weeks a year later, for each item-cluster combination
    # these are also known as slow start series
    first_12_weeks_sales = sum_of_sales(sales_list,'early')
    next_year_12_weeks_sales = sum_of_sales(sales_list,'year later')
    if first_12_weeks_sales < ( next_year_12_weeks_sales / 4 ):
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
      
    # test for item-cluster combinations with no price changes in the last 24 months
    last_two_years = full_weeks_list[len(full_weeks_list)-(52*2):]
    reduced_weeks = [week for week in last_two_years if week in weeks_list]
    price_series = []
    week_price_dict = {week:price for week, price in zip(baseline_item_cluster.WEEK_START_DATE, baseline_item_cluster.REG_PRICE) }
    for week in reduced_weeks:
      price_series.append(week_price_dict[week])
    if np.std (price_series) == 0:
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
      
    # test for item-cluster combinations that are missing a dozen or more consecutive weeks since the start of their time series
    start_week = weeks_list[0]
    start_week_indx = full_weeks_list.index(start_week)
    reduced_full_weeks = full_weeks_list[start_week_indx:]
    missing_weeks_indx = []
    for indx, week in enumerate(reduced_full_weeks):
      if week not in weeks_list:
        missing_weeks_indx.append(indx)
    consecutive_count = 0
    for num in range(len(missing_weeks_indx)-1):
      if missing_weeks_indx[num] + 1 == missing_weeks_indx[num+1]:
        consecutive_count = consecutive_count + 1
      else:
        consecutive_count = 0
    if consecutive_count >= 12:
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
      
    # test for item-cluster combinations that have more than 10 weekly-average prices in their time series
    # we have sometimes called this zigzag, but more generally it is a pattern of erratic prices throughout the series that interfere with the constrained regression
    max_num_prices = len(list(baseline_item_cluster.REG_PRICE.unique()))
    if max_num_prices > 10:
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1
      
    # identify item-cluster combinations with MAPE values at or above 0.1
    if list(baseline_item_cluster['ESTIMATES_MAPE'].values)[0] >= 0.1:
      final_table.at[row_number, product_id_type] = item
      final_table.at[row_number, 'CLUSTER'] = cluster
      final_table.at[row_number, 'Impute_Flag'] = 1
      row_number = row_number + 1

# COMMAND ----------

assert (len(list(set(last_five_weeks) - set(weeks_list)))) == 0

# COMMAND ----------

# assert not final_table.empty -- #Final table for imputation can be empty here if all imputation cases are identified correctly in 01b_create_collation_output_v6

# COMMAND ----------

# drop the duplicate rows from the final table
final_table = final_table.drop_duplicates()

# COMMAND ----------

# sort the final table
final_table = final_table.sort_values(by=[product_id_type,'CLUSTER'])

# COMMAND ----------

compare_impute_flag = baseline_df[[product_id_type, 'CLUSTER', 'IMPUTE_FLAG']].copy()

# COMMAND ----------

compare_impute_flag = compare_impute_flag.drop_duplicates()

# COMMAND ----------

compare_impute_flag = compare_impute_flag.sort_values(by=[product_id_type,'CLUSTER'])

# COMMAND ----------

# display(compare_impute_flag)

# COMMAND ----------

final_table['CLUSTER'] = final_table['CLUSTER'].astype(str)

# COMMAND ----------

dbutils.fs.rm(pre_wave+wave+"/"+bu+"/Inputs_for_elasticity_imputation/Impute_Flag.xlsx") #ps v9
writer3 = pd.ExcelWriter('Impute_Flag.xlsx', engine='xlsxwriter')
final_table.to_excel(writer3, sheet_name='Impute_Flag', index=False)
writer3.save()

# COMMAND ----------

from shutil import move
move('Impute_Flag.xlsx', '/dbfs'+pre_wave+wave+'/'+bu+'/Inputs_for_elasticity_imputation/') #ps v9
