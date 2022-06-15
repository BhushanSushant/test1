# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC The **Main Tasks** which are being executed in the this Notebook:
# MAGIC * create item level, optimization strategy, csc level input, price family level input and product gap rules tab for BU to provide inputs
# MAGIC * populate last refresh's data as reference
# MAGIC 
# MAGIC #### Enhancements from last refresh:
# MAGIC 
# MAGIC * optimization strategy and csc level input tab combine subcategory and csc into one column (when there is csc, use csc, otherwise use subcategory name)
# MAGIC * add a flag to identify whether csc or subcategory is used in that column
# MAGIC * PF Level Inputs tab add a flag to identify whether item has price family or it's no_family
# MAGIC * added additional ending number rules to accomodate phase 4 bus
# MAGIC * reformat the prod gap rules tab by adding rule class, value type and min, max value column
# MAGIC * prod gap rules will be prepopulated using last refresh's data
# MAGIC * added a flag in prod gap rule (bu to review) to identify price family/item not in current scope

# COMMAND ----------

# !pip install xlsxwriter

# COMMAND ----------

# Load Libraries
import pandas as pd, xlrd, xlwt, xlsxwriter, warnings
from xlsxwriter.utility import *
import numpy as np
import os
from shutil import move
warnings.filterwarnings("ignore")

# COMMAND ----------

##widgets
#Create widgets for business unit selection
dbutils.widgets.removeAll()

## BU & abbreviations
Business_units = ["1400 - Florida Division",
                  "1600 - Coastal Carolina Division",
                  "1700 - Southeast Division",
                  "1800 - Rocky Mountain Division",
                  "1900 - Gulf Coast Division",
                  "2600 - West Coast Division",
                  "2800 - Texas Division",
                  "2900 - South Atlantic Division",
                  "3100 - Grand Canyon Division",
                  "3800 - Northern Tier Division",
                  "4100 - Midwest Division",
                  "4200 - Great Lakes Division",
                  "4300 - Heartland Division",                  
                  "Central Division",
                  "QUEBEC OUEST",
                  "QUEBEC EST - ATLANTIQUE",
                  "Western Division",
                  "Denmark",
                  "Ireland",
                  "Poland",
                  "Sweden",
                  "Norway"]

BU_to_reg_abb = {"1400 - Florida Division": ("FL", "USA"),
                 "1600 - Coastal Carolina Division": ("CC", "USA"),
                 "1700 - Southeast Division": ("SE", "USA"),
                 "1800 - Rocky Mountain Division": ("RM", "USA"),
                 "1900 - Gulf Coast Division": ("GC", "USA"),
                 "2600 - West Coast Division": ("WC", "USA"),
                 "2800 - Texas Division": ("TX", "USA"),
                 "2900 - South Atlantic Division": ("SA", "USA"),
                 "3100 - Grand Canyon Division": ("GR", "USA"),
                 "3800 - Northern Tier Division": ("NT", "USA"),
                 "4100 - Midwest Division": ("MW", "USA"),
                 "4200 - Great Lakes Division": ("GL", "USA"),
                 "4300 - Heartland Division": ("HD", "USA"),
                 "QUEBEC OUEST": ("QW", "Canada"),
                 "QUEBEC EST - ATLANTIQUE": ("QE", "Canada"),
                 "Central Division": ("CE", "Canada"),
                 "Western Division": ("WD", "Canada"),
                 "Denmark": ("DK", "Europe"),
                 "Ireland": ("IE", "Europe"),
                 "Poland": ("PL", "Europe"),
                 "Sweden": ("SW", "Europe"),
                 "Norway": ("NO", "Europe")}

Refresh=['Mock_GL_Test']

dbutils.widgets.dropdown("01.Business Unit", "1400 - Florida Division", Business_units)
dbutils.widgets.dropdown("02.Refresh", "Apr2022Refresh", Refresh)

# Reading the Widget Inputs
business_unit = dbutils.widgets.get('01.Business Unit')
refresh = dbutils.widgets.get('02.Refresh')

#get bu 2 character code
bu_code_upper, geography = BU_to_reg_abb.get(business_unit)
bu_code_lower = bu_code_upper.lower()

print("Geography: ", geography)
print("Business Unit: ", business_unit, "; BU Code (Upper Case): ", bu_code_upper, "; BU Code (Lower Case): ", bu_code_lower)
print("Refresh Name: ", refresh)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Define file path to read
# MAGIC 
# MAGIC Given last refresh bu input file have different format, file path is not standardized, **Please change the file path according to your BU**. we'll need to read in following files:
# MAGIC 
# MAGIC * price family file for testing, **use scope table when actually running price refresh** (when scope table is not ready, will use scope table when scope table is finalized)
# MAGIC * last refresh pricing rules, including optimization strategy, csc and price family level inputs and prod gap rules, this can vary from bu to bu
# MAGIC * master input sheet from last refresh, use to prepopulate new prod gap rules sheet

# COMMAND ----------

# define parameters here first
if business_unit=='1400 - Florida Division':
  price_family_file = "/dbfs/Phase3_extensions/Optimization/Nov21_Refresh/templates/round1/{}_initial_scope_Nov21_refresh.xlsx".format(bu_code_lower) ## Latest PF/CSC/Scope file
  opti_strat_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/inputs/fl/FL_optimization_master_input_file.xlsx' ## File from the previous Refreshes
  csc_level_input_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/temp_files/karthik/FL_Optimization_Rules Final_edited.xlsx' ## File from the previous Refreshes
  prod_gap_rule_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/temp_files/karthik/FL_Optimization_Rules Final_edited.xlsx' ## File from the previous Refreshes

  #sheet names
  price_family_sheet = 'Final Scope'
  opti_strat_sheet = 'Weight_Map'
  csc_level_input_sheet = 'Category level Optim Inputs'
  prod_gap_rule_sheet = 'Produt Gap Rules'
  
  
if business_unit=='1600 - Coastal Carolina Division':
  opti_strat_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/cc/CC_Optimization_Rules_Input_Template_Final.xlsx'
  csc_level_input_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/cc/CC_Optimization_Rules_Input_Template_Final.xlsx'
  prod_gap_rule_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/cc/CC_Optimization_Rules_Input_Template_Final.xlsx'
  prod_gap_master_input_file = '/dbfs/Phase3_extensions/Optimization/Nov21_Refresh/inputs/cc/CC_optimization_master_input_file.xlsx'

  opti_strat_sheet = 'Competitive Strategy'
  csc_level_input_sheet = 'Category Level Optim Inputs'
  prod_gap_rule_sheet = 'Product Gap Rules'
  prod_gap_master_input_sheet = 'Prod_Gap_Rules'
  
if business_unit=='Sweden':
  price_family_file = "/dbfs/Phase3_extensions/Optimization/Nov21_Refresh/templates/round1/sw_initial_scope_Nov21_refresh_updated.xlsx"  # taru added 11/29/2021
  opti_strat_file = '/dbfs/SE_Repo/Price_Refresh_MAY2021/Optimization/Inputs/Weight_map.xlsx'
  csc_level_input_file = '/dbfs/SE_Repo/Price_Refresh_MAY2021/Optimization/Inputs/LB_UB_Cat_wise.xlsx'
  csc_level_input_file2 = '/dbfs/SE_Repo/Price_Refresh_MAY2021/Optimization/Inputs/Cat_UB_LB.csv'
  csc_level_input_file3 = '/dbfs/SE_Repo/Price_Refresh_MAY2021/Optimization/Inputs/round_off_rule.xlsx'
  prod_gap_rule_file = '/dbfs/SE_Repo/Price_Refresh_MAY2021/Optimization/Inputs/Product_Family_Map.xlsx'

  price_family_sheet = 'Final Scope' ## 
  opti_strat_sheet = 'Sheet1'
  csc_level_input_sheet = 'Sheet1'
  prod_gap_rule_sheet = 'Sheet1'

if business_unit=='QUEBEC OUEST':
  price_family_file = "/dbfs/Phase3_extensions/Optimization/Nov21_Refresh/templates/round1/qw_initial_scope_Nov21_refresh.xlsx" ## Latest PF/CSC/Scope file
  opti_strat_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/inputs/qw/QW_optimization_master_input_file.xlsx' ## File from the previous Refreshes
  csc_level_input_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/inputs/qw/QW_Pricing_Rules_May2021.xlsx' ## File from the previous Refreshes
  prod_gap_rule_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/inputs/qw/QW_Pricing_Rules_May2021.xlsx' ## File from the previous Refreshes

  #sheet names
  price_family_sheet = 'Final Scope'
  opti_strat_sheet = 'Weight_Map'
  csc_level_input_sheet = 'Category Level Price Inputs'
  prod_gap_rule_sheet = 'Product Gap Rules'
  
if business_unit=='Central Division':
  price_family_file = "/dbfs/Localized-Pricing/November2021/ce/ce_initial_scope_Nov21_refresh_v2.xlsx" ## Latest PF/CSC/Scope file
  opti_strat_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/ce/CE_optimization_master_input_file.xlsx' ## File from the previous Refreshes
  csc_level_input_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/ce/CE_Pricing_Rules_June2021.xlsx' ## File from the previous Refreshes
  prod_gap_rule_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/ce/CE_Pricing_Rules_June2021.xlsx' ## File from the previous Refreshes

  #sheet names
  price_family_sheet = 'Final Scope'
  opti_strat_sheet = 'Weight_Map'
  csc_level_input_sheet = 'Category Level Price Inputs'
  prod_gap_rule_sheet = 'Product Gap Rules'
  
if business_unit == "1900 - Gulf Coast Division":
  price_family_file = "/dbfs/Phase3_extensions/Optimization/Nov21_Refresh/templates/round1/gc_initial_scope_Nov21_refresh_v2.xlsx"
  opti_strat_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/inputs/gc/GC_optimization_master_input_file.xlsx'
  csc_level_input_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/inputs/gc/GC_Opti_Rules_Input_0412.xlsx'
  prod_gap_rule_file = '/dbfs/Phase3_extensions/Optimization/MAY2021_Refresh/inputs/gc/GC_Opti_Rules_Input_0412.xlsx'


  price_family_sheet = 'Final Scope' # put sheet name here
  opti_strat_sheet = 'Weight_Map'
  csc_level_input_sheet = 'Category Level Optim Inputs'
  prod_gap_rule_sheet = 'Product Gap Rules'

if business_unit == '1700 - Southeast Division':
  
  opti_strat_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/se/SE_Price_Family_Final_June2021_Refresh_v9.xlsx'
  csc_level_input_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/se/SE_Price_Family_Final_June2021_Refresh_v9.xlsx'
  prod_gap_rule_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/se/SE_Price_Family_Final_June2021_Refresh_v9.xlsx'
  scope_file = '/dbfs/Phase3_extensions/Optimization/JUNE2021_Refresh/inputs/se/SE_Price_Family_Final_June2021_Refresh_v9.xlsx'
  prod_gap_master_input_file = '/dbfs/FileStore/Stella/se_optimization_master_input_file_with_dummy.xlsx' #tw - used to populate new product gap rule sheet
  
  opti_strat_sheet = 'Optimization Strategy'
  csc_level_input_sheet = 'Category level Optim Inputs'
  prod_gap_rule_sheet = 'Product Gap Rules'
  scope_sheet = 'Price Family'
  prod_gap_master_input_sheet = 'Prod_Gap_Rules'
  
if business_unit == '4200 - Great Lakes Division':
  
  opti_strat_file = '/dbfs/Phase4_extensions/Optimization/MAY2021_Refresh/inputs/gl/GL_optimization_master_input_file.xlsx'
  csc_level_input_file = '/dbfs/Phase4_extensions/Optimization/MAY2021_Refresh/inputs/gl/GL_Optimization_Rules_Inputs_Template.xlsx'
  prod_gap_rule_file = '/dbfs/Phase4_extensions/Optimization/MAY2021_Refresh/inputs/gl/GL_Optimization_Rules_Inputs_Template.xlsx'
  scope_file ="/dbfs/Phase4_extensions/Optimization/Mock_GL_Test/templates/round1/{}_initial_scope_mock_gl_test.xlsx".format(bu_code_lower)
  prod_gap_master_input_file = '/dbfs/Phase4_extensions/Optimization/MAY2021_Refresh/inputs/gl/GL_optimization_master_input_file.xlsx' #tw - used to populate new product gap rule sheet
  
  opti_strat_sheet = 'Weight_Map'
  csc_level_input_sheet = 'Category level Optim Inputs'
  prod_gap_rule_sheet = 'Product Gap Rules'
  scope_sheet = 'Price Family'
  prod_gap_master_input_sheet = 'Prod_Gap_Rules'

# COMMAND ----------

# previous refresh optimization strategy file
# change usecols parameter accordingly
optimization_strategy = pd.read_excel(opti_strat_file, sheet_name = opti_strat_sheet,usecols = "B:G", skiprows=5)

optimization_strategy_old = optimization_strategy.copy()
display(optimization_strategy_old)

# COMMAND ----------

# previous refresh csc level input file
# change usecols parameter accordingly
csc_level_input = pd.read_excel(csc_level_input_file, sheet_name = csc_level_input_sheet,usecols = "B:M",skiprows = 3)
csc_level_input['BU'] = business_unit
csc_level_input_old = csc_level_input.copy()
csc_level_input_old

# COMMAND ----------

state_cats=[]
zoned_cats=[]
total_state_cats=[]

#Zoned Categories
if geography in ['USA']:
  state_cats = pd.read_excel('/dbfs/Phase_3/optimization/wave_1/optimization_inputs/State_level_cats.xlsx') ## for Phase 3 BUs
  # state_cats = pd.read_excel('/dbfs/Phase3_extensions/Optimization/Accenture_Refresh/optimization_inputs/State_level_cats.xlsx') ## for Phase 4 BUs
  
  state_cats = spark.createDataFrame(state_cats)
  
  if business_unit != '3100 - Grand Canyon Division':
    zoned_cats= spark.createDataFrame([ (business_unit, '004-BEER', 'Y'), (business_unit, '005-WINE', 'Y')], ['BU', 'category_desc', 'state_flag'])
    total_state_cats=zoned_cats.union(state_cats)
  else:
    state_cats = spark.createDataFrame([('3100 - Grand Canyon Division', '006-LIQUOR', 'Y'),\
                                        ('3100 - Grand Canyon Division', '004-BEER', 'Y'),\
                                        ('3100 - Grand Canyon Division', '005-WINE', 'Y'),\
                                        ('3100 - Grand Canyon Division', '003-OTHER TOBACCO PRODUCTS', 'Y')],\
                                        ['BU', 'category_desc', 'state_flag'])
    zoned_cats= spark.createDataFrame([ (business_unit, '', ''), (business_unit, '', '')], ['BU', 'category_desc', 'state_flag']) ## GR does not have zones
    total_state_cats=state_cats
  zoned_cats=zoned_cats.toPandas()
  total_state_cats=total_state_cats.toPandas()
  total_state_cats=total_state_cats[total_state_cats['BU']==business_unit]
  display(total_state_cats)
  
elif geography in ['Canada']:
  if business_unit =='Central Division':
    zoned_cats= spark.createDataFrame([(business_unit, 'Cigarettes (60)', 'Y')], ['BU', 'category_desc', 'state_flag'])
    total_state_cats= spark.createDataFrame([(business_unit, '', ''), (business_unit, '', '')], ['BU', 'category_desc', 'state_flag']) ##  does not have state cats
    
  if business_unit =='QUEBEC OUEST':
    zoned_cats= spark.createDataFrame([ (business_unit, '', ''), (business_unit, '', '')], ['BU', 'category_desc', 'state_flag']) ##  does not have zone cats
    total_state_cats= spark.createDataFrame([ (business_unit, '', ''), (business_unit, '', '')], ['BU', 'category_desc', 'state_flag']) ##  does not have state cats
    
  zoned_cats=zoned_cats.toPandas()
  total_state_cats=total_state_cats.toPandas()
  total_state_cats=total_state_cats[total_state_cats['BU']==business_unit]
  display(total_state_cats)
else:
  print (" No zones in Europe. For Canada check with Manager if zoned categories are inscope")

# COMMAND ----------

# previous refresh product gap rules
# change usecols parameter accordingly
prod_gap_rule = pd.read_excel(prod_gap_rule_file, sheet_name = prod_gap_rule_sheet,usecols = "B:H",skiprows = 7).astype('str')
prod_gap_rule = prod_gap_rule.replace('nan', '')
prod_gap_rule_old = prod_gap_rule.copy() 

display(prod_gap_rule_old)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Read in the final in scope/price family file from Excel or from table
# MAGIC 
# MAGIC * circlek_db.lp_dashboard_items_scope
# MAGIC * data will be sorted in the following order
# MAGIC   * BU
# MAGIC   * Category
# MAGIC   * Sub Category
# MAGIC   * Category Special Classification
# MAGIC   * Price Family
# MAGIC   
# MAGIC ##### Populate Item Level Input sheet
# MAGIC 
# MAGIC * Item level Input Sheet will be the exact copy of final in scope/price family file

# COMMAND ----------

pf_org_spk_df = spark.sql("""SELECT
                                   BU 
                                   ,category_name
                                   ,subcategory_name
                                   ,item_name
                                   ,upc
                                   ,item_number
                                   ,product_key
                                   ,sell_unit_qty
                                   ,scope
                                   ,Case When modelling_level Is Null Then '' Else modelling_level End modelling_level -- make modelling level blank for NA bu so when convert to pandas without error
                                   ,price_family
                                   ,category_special_classification
                                   FROM circlek_db.lp_dashboard_items_scope 
                                   WHERE BU = '{0}'""".format(business_unit))

pf_org = pf_org_spk_df.toPandas()

pf_org['item_number'] = pf_org['item_number'].fillna(0)
pf_org['product_key'] = pf_org['product_key'].fillna(0)
# pf_org = pf_org.astype({"upc": "int64", "item_number": "int64", "product_key": "int64"})

# COMMAND ----------

# mapper for renaming columns
us_canada_old_to_new_colnames = {'category_name': 'category',
                                 'subcategory_name': 'sub_category',
                                 'item_name': 'item_desc',
                                 'category_special_classification': 'Category.Special.Classification'}

eu_old_to_new_colnames = {'category_name': 'category',
                          'subcategory_name': 'sub_category',
                          'item_name': 'item_desc',
                          'upc': 'EAN',
                          'item_number': 'JDE Number',
                          'product_key': 'trn_item_sys_id',
                          'category_special_classification': 'Category.Special.Classification'}

geography_to_colnames = {'USA': us_canada_old_to_new_colnames,
                         'Canada': us_canada_old_to_new_colnames,
                         'Europe': eu_old_to_new_colnames}

pf = pf_org.copy()
if geography == 'Europe':
  pf['sys_id_final'] = pf.apply(lambda row: row['product_key'] if row['modelling_level'] == 'Sys ID' else row['upc'], axis='columns')
else:
  pf['sys_id_final'] = pf['product_key']

pf = pf.rename(mapper=geography_to_colnames[geography], axis='columns')

item_level_input = pf.copy().sort_values(['BU','category','sub_category','Category.Special.Classification','price_family','item_desc'])

item_level_input_prod_gap = item_level_input.copy() #tw 04/25 added a copy of item level input with sys_id_final column, used in joins in the code below

item_level_input = item_level_input.drop(['sell_unit_qty', 'sys_id_final'], axis='columns') if geography == 'Europe' else item_level_input.drop('modelling_level', axis='columns')
# display(item_level_input)

pf = pf.loc[pf['scope'] == 'Y', ]



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Create skeleton of Category/Sub Category/CSC table

# COMMAND ----------

# generate skeleton of cat/subcat/csc combination
# create a new column subcat_csc, when csc is no_csc, use subcat otherwise use csc

no_csc = ['No_CSC', np.nan, None, 'None']

pf['subcat_csc'] = np.where(pf['Category.Special.Classification'].isin(no_csc), pf['sub_category'],pf['Category.Special.Classification'])
pf['subcat_csc_flag'] = np.where(pf['Category.Special.Classification'].isin(no_csc), 'sub_category','CSC')

hierarchy_csc = pf[['BU', 'category','subcat_csc','subcat_csc_flag']].drop_duplicates().dropna().reset_index(drop = True)

## TV : new code
c1 = pf[['BU', 'category','subcat_csc']].drop_duplicates()
c2 = pf[['BU', 'category','subcat_csc']].drop_duplicates().dropna()

if c1.shape[0] == c2.shape[0]:
  print ("all good")
else:
  print("nulls genererated. Stop and validate for the final run. Its possible that some product keys are incorrect or missing")

# COMMAND ----------

hierarchy_csc.head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate Optimization Strategy new and Optimization Strategy old sheet
# MAGIC 
# MAGIC * Optimization Strategy old table will be the exact copy of optimization strategy used in the last refresh
# MAGIC * Optimization Strategy new table will be populated by the following steps
# MAGIC   * Standardizing optimization strategy value in the last refresh (Initial Cap/Trim Space/Remove special characters)
# MAGIC   * For some BU, in the last refresh, CSC column are populated with a combination of CSC and Sub category, we need to create a separate column called Sbucat/CSC for the join to work
# MAGIC   * Left join on csc hierarchy table generated in the previous step with the last refresh optimization strategy table on
# MAGIC     * BU
# MAGIC     * Category
# MAGIC     * Subcat/CSC
# MAGIC   * Populate **optimization strategy new** column and **Comments** column as blank value for the BU to populate
# MAGIC   * Rename columns accordingly

# COMMAND ----------

display(optimization_strategy_old)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Please rename ***optimization_strategy_new*** to ONLY have the following columns so that the joins can work
# MAGIC - BU
# MAGIC - category
# MAGIC - subcat_csc
# MAGIC - optimization_strategy_last_refresh

# COMMAND ----------

# generate optimization strategy file 


optimization_strategy_new = optimization_strategy_old.copy() ## TV: changed this

optimization_strategy_new['BU'] = business_unit

# rename column name accordingly

optimization_strategy_new = optimization_strategy_new[['BU', 'Category','Sub-Cat. Classification','Competitors Considered while pricing']].drop_duplicates().\
rename(columns={'Competitors Considered while pricing': 'optimization_strategy_last_refresh',
               'Category' : 'category',
               'Sub-Cat. Classification' : 'subcat_csc'})

# remove extra spaces
optimization_strategy_new['subcat_csc'] = optimization_strategy_new['subcat_csc'].str.replace(" - ","-")
optimization_strategy_new['subcat_csc'] = optimization_strategy_new['subcat_csc'].str.replace("- ","-")
optimization_strategy_new['subcat_csc'] = optimization_strategy_new['subcat_csc'].str.replace(" -","-")
optimization_strategy_new['subcat_csc'] = optimization_strategy_new['subcat_csc'].str.strip() #tw added - trim space from the beginning and end


optimization_strategy_new = pd.merge(hierarchy_csc,optimization_strategy_new , on = ['BU','category','subcat_csc'],how = 'left')

optimization_strategy_new['optimization_strategy_new']= np.nan
optimization_strategy_new['comments']= np.nan


os_col_mapper = {'BU': 'BU',
                 'category': 'Category',
                 'subcat_csc': 'Subcat/CSC',
                 'subcat_csc_flag': 'Subcat/CSC Flag',
                 'optimization_strategy_last_refresh': 'Optimization.Strategy (Last Refresh)',
                 'optimization_strategy_new': 'Optimization.Strategy (New)',
                 'comments': 'Comments'}

optimization_strategy_new = optimization_strategy_new.rename(mapper=os_col_mapper, axis='columns')


display(optimization_strategy_new)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate CSC Level Input new and CSC Level Input old sheet
# MAGIC 
# MAGIC * CSC Level Input old will be the exact copy of CSC Level Input used in the last refresh
# MAGIC * CSC Level Input new table will be populated by the following steps
# MAGIC   * Left join the csc hierarchy file with CSC Level Input old file on
# MAGIC     * BU
# MAGIC     * Category
# MAGIC     * CSC
# MAGIC   * Create blank columns for the following columns for the BU to populate
# MAGIC     * Max Price Decrease (%)
# MAGIC     * Max Price Increase (%)
# MAGIC     * Item Minimum Margin Rate (%)
# MAGIC     * Average Category Subset Minimum Margin Rate (%)
# MAGIC     * Max drop in Qty  allowed for an item (%)
# MAGIC     * Ending Number Rule
# MAGIC     * Comments
# MAGIC   * Rename columns accordingly

# COMMAND ----------

# generate csc level input file

csc_level_input_new = hierarchy_csc.copy()


csc_level_input_new = csc_level_input_new.sort_values(by=['BU','category','subcat_csc','subcat_csc_flag']).reset_index(drop = True)

for col in ['Max Price Decrease (%)','Max Price Increase (%)','Item Minimum Margin Rate (%)','Average Category Subset Minimum Margin Rate (%)'
            ,'Max drop in Qty  allowed for an item (%)','Ending Number Rule','Comments']: #tw deleted 'Ending Number Rule - Other'
    csc_level_input_new[col] = np.nan


csc_col_mapper = {'BU': 'BU',
                 'category': 'Category',
                 'subcat_csc': 'Subcat/CSC',
                 'subcat_csc_flag': 'Subcat/CSC Flag'}

csc_level_input_new = csc_level_input_new.rename(mapper=csc_col_mapper, axis='columns')
    
display(csc_level_input_new)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate Price Family Level Input sheet
# MAGIC * In order to populate price family for **No_Family** items, we need to create a separate column **price_family_new** to get the UPC for orphan items
# MAGIC * Create a hierarchy file for price family by getting the dictinct combination of 
# MAGIC   * BU
# MAGIC   * Category
# MAGIC   * Sub_csc
# MAGIC   * subcat_csc_flag
# MAGIC   * Price Family
# MAGIC   * price_family_flag
# MAGIC   * Price Family New (UPC for orphan item, else blank)
# MAGIC * Left Join on the price family hierarchy with final in scope/ price family file to get the following columns for orphan items
# MAGIC   * item desc
# MAGIC   * sell unit qty
# MAGIC   * tempe product key
# MAGIC * Populate **Price Ceiling ($)** and **Price Floor/Promo Price Floor ($)** as blank for BU to populate
# MAGIC * Rename columns accordingly

# COMMAND ----------

if geography in ['USA', 'Canada']:
  pf_level_input = pf.copy()
  pf_level_input = pf_level_input[['BU', 'category', 'subcat_csc','subcat_csc_flag', 'price_family', 'item_desc', 'upc', 'product_key', 'Category.Special.Classification', 'sell_unit_qty']]
  pf_level_input['product_key'] = pf_level_input.apply(lambda row: row['product_key'] if row['price_family'] == 'No_Family' else np.nan, axis='columns')
  pf_level_input['upc'] = pf_level_input.apply(lambda row: row['upc'] if row['price_family'] == 'No_Family' else np.nan, axis='columns')
  pf_level_input['item_desc'] = pf_level_input.apply(lambda row: row['item_desc'] if row['price_family'] == 'No_Family' else np.nan, axis='columns')
  pf_level_input['sell_unit_qty'] = pf_level_input.apply(lambda row: row['sell_unit_qty'] if row['price_family'] == 'No_Family' else np.nan, axis='columns')
#   pf_level_input['subcat_csc'] = np.where(pf_level_input['Category.Special.Classification'].isin(no_csc), pf_level_input['sub_category'],pf_level_input['Category.Special.Classification'])
#   pf_level_input['subcat_csc_flag'] = np.where(pf_level_input['Category.Special.Classification'].isin(no_csc), 'sub_category','CSC')
  pf_level_input['price_family_flag'] = np.where(pf_level_input['price_family'] == 'No_Family', 'orphan_item','price_family')
  pf_level_input = pf_level_input.drop_duplicates()
  pf_level_input['Price Ceiling ($)']= np.nan
  pf_level_input['Price Floor/Promo Price Floor ($)']= np.nan
  
  pf_level_input = pf_level_input[['BU','category','subcat_csc','subcat_csc_flag','price_family','price_family_flag','upc','item_desc'
                                   ,'sell_unit_qty','product_key','Price Ceiling ($)','Price Floor/Promo Price Floor ($)']] # leo added 11/29/2021

  pf_level_input.insert(8, 'EAN Flag', np.nan)
  

if geography in ['Europe']:
  pf_level_input = pf.copy()
  pf_level_input['EAN Flag'] = pf_level_input['modelling_level'].apply(lambda x: 'Y' if x == 'EAN' else 'N')  
  pf_level_input = pf_level_input[['BU', 'category', 'subcat_csc','subcat_csc_flag', 'price_family', 'item_desc', 'JDE Number', 'sys_id_final', 'EAN Flag', 'Category.Special.Classification', 'sell_unit_qty']]
  pf_level_input['sys_id_final'] = pf_level_input.apply(lambda row: row['sys_id_final'] if row['price_family'] == 'No_Family' else np.nan, axis='columns')
  pf_level_input['JDE Number'] = pf_level_input.apply(lambda row: row['JDE Number'] if row['price_family'] == 'No_Family' else np.nan, axis='columns')
  pf_level_input['item_desc'] = pf_level_input.apply(lambda row: row['item_desc'] if row['price_family'] == 'No_Family' else np.nan, axis='columns')
#   pf_level_input['subcat_csc'] = np.where(pf_level_input['Category.Special.Classification'].isin(no_csc), pf_level_input['sub_category'],pf_level_input['Category.Special.Classification'])
#   pf_level_input['subcat_csc_flag'] = np.where(pf_level_input['Category.Special.Classification'].isin(no_csc), 'sub_category','CSC')
  pf_level_input['price_family_flag'] = np.where(pf_level_input['price_family'] == 'No_Family', 'orphan_item','price_family')

  pf_level_input = pf_level_input.drop_duplicates()
  
  pf_level_input.rename(columns = {'JDE Number': 'upc',
                                   'sys_id_final':'product_key'}, inplace = True)
  
  pf_level_input['Price Ceiling ($)']= np.nan
  pf_level_input['Price Floor/Promo Price Floor ($)']= np.nan
  
  pf_level_input = pf_level_input[['BU','category','subcat_csc','subcat_csc_flag','price_family','price_family_flag','upc','item_desc','EAN Flag',
                                   'sell_unit_qty','product_key','Price Ceiling ($)','Price Floor/Promo Price Floor ($)']]

pf_col_mapper = {'category':'Category',
                'subcat_csc':'Category',
                'subcat_csc':'Subcat/CSC',
                'subcat_csc_flag':'Subcat/CSC Flag',
                'price_family':'Price Family',
                'price_family_flag':'Price Family Flag',
                'upc':'UPC/Item Number',
                'item_desc':'Item Name',
                'sell_unit_qty':'Sell Unit Qty',
                'product_key':'Product Key/Trn Item Sys ID/EAN No.'}

pf_level_input = pf_level_input.rename(mapper = pf_col_mapper, axis = 'columns')


display(pf_level_input)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate state level and zone level category sheet
# MAGIC * This tab is populated from a static excel sheet stored on databrick
# MAGIC * need to check with Darren for any changes/updates on the source file

# COMMAND ----------

# generate state/zone category level file
if geography in ['USA','Canada']:
  state_level_category = total_state_cats ##TV: changed
  zone_level_category = zoned_cats ##TV: changed

  state_level_category.rename({'bu':'BU','category_desc':'Categories Optimized at State Level (Last Refresh)'}, axis=1,inplace = True)
  zone_level_category.rename({'bu':'BU','category_desc':'Categories Optimized at Zone Level (Last Refresh)'}, axis=1,inplace = True)

  state_level_category['New State Level Categories'] = np.nan
  zone_level_category['New Zone Level Categories'] = np.nan

  state_level_category = state_level_category[['BU','Categories Optimized at State Level (Last Refresh)','New State Level Categories']]
  zone_level_category = zone_level_category[['BU','Categories Optimized at Zone Level (Last Refresh)','New Zone Level Categories']]

else:
  d1 = {'BU': business_unit, 'Categories Optimized at State Level (Last Refresh)': ['Not Applicable'], 'New State Level Categories': ['Not Applicable']}
  d2 = {'BU': business_unit, 'Categories Optimized at Zone Level (Last Refresh)': ['Not Applicable'], 'New Zone Level Categories': ['Not Applicable']}
  state_level_category = pd.DataFrame(data=d1)
  zone_level_category = pd.DataFrame(data=d2)
  
display(state_level_category)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate Prod Gap Rule new and Prod Gap Rule old sheet
# MAGIC * Prod Gap Rule old will be the exact copy used in the last refresh
# MAGIC * Prod Gap Rule new will be generated based on past master input file and item level inputs
# MAGIC * **Essential BU Review** will be added for those PF/Item exists in last refresh but are no longer inscope for this refresh for **BU to verify/correct**

# COMMAND ----------

prod_gap_old = pd.read_excel(prod_gap_master_input_file, sheet_name = prod_gap_master_input_sheet,usecols = "A:M")
prod_gap_old = prod_gap_old.astype({"Dependent": "string", "Independent": "string"})
item_level_input_prod_gap['product_key'] = item_level_input_prod_gap['product_key'].astype('str')


UPC_Item_Number = 'JDE Number' if geography == 'Europe' else 'upc'
Product_Key_Sys_ID_EAN_No = 'sys_id_final' if geography == 'Europe' else 'product_key'

item_level_input_prod_gap.loc[item_level_input_prod_gap['modelling_level'] == 'EAN','modelling_level'] = 'Y'
item_level_input_prod_gap.loc[item_level_input_prod_gap['modelling_level'] == 'Sys ID','modelling_level'] = 'N'

#get dep category & PF from item level input
prod_gap_old = pd.merge(prod_gap_old, item_level_input_prod_gap[['price_family']], left_on='Dependent', right_on='price_family', how = 'left').rename({'price_family':'Dep.PF.Name'}, axis=1).drop_duplicates()

#get ind category & PF from item level input
prod_gap_old = pd.merge(prod_gap_old, item_level_input_prod_gap[['price_family','category']], left_on='Independent', right_on='price_family', how = 'left').rename({'category':'Ind.Category', 'price_family':'Ind.PF.Name'}, axis=1).drop_duplicates()

#get dep item info from item level input sheet
prod_gap_old = pd.merge(prod_gap_old, item_level_input_prod_gap[[Product_Key_Sys_ID_EAN_No, UPC_Item_Number,'item_desc','sell_unit_qty','price_family', 'modelling_level']], left_on='Dependent', right_on= Product_Key_Sys_ID_EAN_No, how = 'left').rename({UPC_Item_Number:'Dep.UPC/Item.Number','item_desc':'Dep.Item.Name','sell_unit_qty':'Dep.Sell Unit Qty', Product_Key_Sys_ID_EAN_No:'Dep.Product Key/Sys_ID/EAN No.','price_family':'Dep.PF.Name.Item', 'modelling_level':'Dep.EAN.Flag'}, axis=1)

#get ind item info from item level input sheet
prod_gap_old = pd.merge(prod_gap_old, item_level_input_prod_gap[[Product_Key_Sys_ID_EAN_No ,UPC_Item_Number,'item_desc','sell_unit_qty','category','price_family', 'modelling_level']], left_on='Independent', right_on= Product_Key_Sys_ID_EAN_No , how = 'left').rename({UPC_Item_Number:'Ind.UPC/Item.Number','item_desc':'Ind.Item.Name','sell_unit_qty':'Ind.Sell Unit Qty',Product_Key_Sys_ID_EAN_No :'Ind.Product Key/Sys_ID/EAN No.', 'category':'Ind.Category.Item','price_family':'Ind.PF.Name.Item', 'modelling_level':'Ind.EAN.Flag'}, axis=1)

#combine info from item rules and PF rules
prod_gap_old['Ind.Category'] = prod_gap_old['Ind.Category'].fillna(prod_gap_old['Ind.Category.Item'])
prod_gap_old['Dep.PF.Name'] = prod_gap_old['Dep.PF.Name'].fillna(prod_gap_old['Dep.PF.Name.Item'])
prod_gap_old['Ind.PF.Name'] = prod_gap_old['Ind.PF.Name'].fillna(prod_gap_old['Ind.PF.Name.Item'])

#fill in PF for out out scope PF
prod_gap_old.loc[(prod_gap_old['Dep.PF.Name'].isnull()) & (prod_gap_old['Category.Dep']=='PF'),'Dep.PF.Name'] = prod_gap_old.loc[(prod_gap_old['Dep.PF.Name'].isnull()) & (prod_gap_old['Category.Dep']=='PF'),'Dependent'] 
prod_gap_old.loc[(prod_gap_old['Ind.PF.Name'].isnull()) & (prod_gap_old['Category.Indep']=='PF'),'Ind.PF.Name'] = prod_gap_old.loc[(prod_gap_old['Ind.PF.Name'].isnull()) & (prod_gap_old['Category.Indep']=='PF'),'Independent']

#fill in PK for out out scope PK
# prod_gap_old.loc[(prod_gap_old['Dep.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Dep']=='Item'),'Dep.Product Key/Sys_ID/EAN No.'] = prod_gap_old.loc[(prod_gap_old['Dep.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Indep']=='Item'),'Dependent'] 
# prod_gap_old.loc[(prod_gap_old['Ind.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Dep']=='Item'),'Ind.Product Key/Sys_ID/EAN No.'] = prod_gap_old.loc[(prod_gap_old['Ind.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Indep']=='Item'),'Independent'] 

prod_gap_old.loc[(prod_gap_old['Dep.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Dep']=='Item'),'Dep.Product Key/Sys_ID/EAN No.'] = prod_gap_old.loc[(prod_gap_old['Dep.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Dep']=='Item'),'Dependent'] 
prod_gap_old.loc[(prod_gap_old['Ind.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Indep']=='Item'),'Ind.Product Key/Sys_ID/EAN No.'] = prod_gap_old.loc[(prod_gap_old['Ind.Product Key/Sys_ID/EAN No.'].isnull()) & (prod_gap_old['Category.Indep']=='Item'),'Independent'] 

#add columns
prod_gap_old['Relationship Type'] = prod_gap_old['Category.Dep'] + ' ' + 'to' + ' ' + prod_gap_old['Category.Indep']
prod_gap_old['BU'] = business_unit

#prod_gap_old.tail()

# COMMAND ----------

#map product gap rules from past master input file to new product gap rules

prod_gap_rule_mapping = {('Price','Higher'): 'More Exp',
                         ('Price','Lower'): 'Less Exp',
                         ('Price','Same'): 'Same Price',
                         ('Price','Within'): 'Within Price Range',
                         ('Value','Higher'): 'Cheaper Per Unit',
                         ('Value','Lower'): 'Expensive Per Unit',
                         ('Value','Same'): 'Same Price Per Unit',
                         ('Margin','Higher'): 'Higher Margin $',
                         ('Margin','Lower'): 'Lower Margin $',
                         ('Margin','Same'): 'Same Margin $',
                         ('Margin Percentage','Higher'):'Higher Margin %',
                         ('Margin Percentage','Lower'):'Lower Margin %',
                         ('Margin Percentage','Same'):'Same Margin %'
                        }

prod_gap_rule_mapping_min10 = {'At minimum 10% lower retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% more penny profit than competing NB item': 'Min 10 Lower',
                               'At minimum 10% higher retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% less penny profit than competing NB item': 'Min 10 Higher'
                              }

def prod_gap_rule_mapper(classy, side):
  if prod_gap_rule_mapping.get((classy, side)):
    return prod_gap_rule_mapping.get((classy, side))

def prod_gap_rule_mapper_min10(init_rule):
  if prod_gap_rule_mapping_min10.get((init_rule)):
    return prod_gap_rule_mapping_min10.get((init_rule))

  
prod_gap_old_min10 = prod_gap_old.loc[prod_gap_old['Initial.Rule'].str.startswith('At minimum 10%')]
prod_gap_old_other = prod_gap_old.loc[~prod_gap_old['Initial.Rule'].str.startswith('At minimum 10%')]

prod_gap_old_other['Standardized Rule Description'] = prod_gap_old_other.apply (lambda row: prod_gap_rule_mapper(row['Rule.Classification'], row['Side']), axis=1)
if len(prod_gap_old_min10) > 0:
  prod_gap_old_min10['Standardized Rule Description'] = prod_gap_old_min10.apply (lambda row: prod_gap_rule_mapper_min10(row['Initial.Rule']), axis=1)
  prod_gap_old_min10['Rule.Classification'] = 'Min 10'
  prod_gap_old_min10['Side'] = 'Min 10'
prod_gap_combined = prod_gap_old_other.append(prod_gap_old_min10, ignore_index=True)
#prod_gap_combined['Rule.Classification'] = ''
prod_gap_combined.drop_duplicates(inplace = True)
#prod_gap_combined

# COMMAND ----------

prod_gap_col_mapper = {'Category.Name.Dep':'Dep.Category',
                      'Rule.Classification':'Rule Class',
                      'Type':'Value Type',
                      'Min':'Min Value',
                      'Max':'Max Value'}

prod_gap_combined = prod_gap_combined.rename(mapper = prod_gap_col_mapper, axis = 'columns')

prod_gap_rule_new = prod_gap_combined[['BU','Relationship Type','Dep.Category','Dep.PF.Name','Dep.Item.Name','Dep.UPC/Item.Number','Dep.EAN.Flag','Dep.Sell Unit Qty','Dep.Product Key/Sys_ID/EAN No.','Ind.Category',
                                       'Ind.PF.Name','Ind.Item.Name','Ind.UPC/Item.Number','Ind.EAN.Flag','Ind.Sell Unit Qty','Ind.Product Key/Sys_ID/EAN No.','Standardized Rule Description','Rule Class',
                                       'Value Type','Min Value','Max Value']]


prod_gap_rule_new

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Retrieve orphan item information that are out of scope in current refresh but exists in last refresh prod gap rules
# MAGIC * Columns retrieved are: Dep.PF.Name/Dep.Item.Name/Dep.UPC/Item.Number/Dep.EAN.Flag/Dep.Sell Unit Qty
# MAGIC * Note: we are only able to get ean/sys id for european data, which are not unique identifier for eu items, in such case, those item filled with blank

# COMMAND ----------

# populate items which are not in current scope but in previous prod gap rule, get item information based on prod key

def find_item(key):
  if geography == 'Europe':
    table_name = 'dw_eu_common.item_d'
    trn_table = 'dw_eu_rep_pos.pos_transactions_f'
    site_table = 'dw_eu_common.site_d'
    # get item info for eu bu when modelling on sys_id level
    item = spark.sql('''
        Select
          'No_Family' price_family
          ,item_name
          ,item_number upc_item_number
          ,'N' ean_flag
          ,'' sell_unit_qty
          ,i.sys_id pk_sys_id_ean
        From {} i
        Where i.sys_id = {}
        '''.format(table_name, key)
                  )
    # get item info for eu bu when modelling on ean level
    if item.count() == 0:
      item = spark.sql('''
        select
          Distinct 'No_Family' price_family
          ,a.item_name
          ,a.item_number upc_item_number
          ,'Y' ean_flag
          ,'' sell_unit_qty
          ,b.trn_barcode pk_sys_id_ean
        from {} a
        join {} b on a.sys_id = b.trn_item_sys_id
        Join {} s On s.sys_id = b.trn_site_sys_id and s.site_bu_name = '{}'
        where b.trn_barcode = {}
        '''.format(table_name, trn_table, site_table, business_unit, key)
                  )
#     when return multiple items, since we are not able to uniquely identify item only based on ean, we'll populate blank value
    elif item.count() > 1:
      item = spark.sql('''
        select
          'No_Family' price_family
          ,'' item_name
          ,'' upc_item_number
          ,'' ean_flag
          ,'' sell_unit_qty
          ,{} pk_sys_id_ean
          '''.format(key))
    
  else: # get item info for na bu
    table_name = 'dl_localized_pricing_all_bu.product'
    item = spark.sql('''
    Select
      'No_Family' price_family
      ,item_desc item_name
      ,upc upc_item_number
      ,'' ean_flag
      ,sell_unit_qty
      ,i.product_key pk_sys_id_ean
    From {} i
    Where i.product_key = {}
    '''.format(table_name, key)
              )
  
  item = item.toPandas()
  
  return item

dep_column_to_fill = {
  'Dep.PF.Name': 'price_family'
  ,'Dep.Item.Name': 'item_name'
  ,'Dep.UPC/Item.Number': 'upc_item_number'
  ,'Dep.EAN.Flag': 'ean_flag'
  ,'Dep.Sell Unit Qty': 'sell_unit_qty'
}

ind_column_to_fill = {
  'Ind.PF.Name': 'price_family'
  ,'Ind.Item.Name': 'item_name'
  ,'Ind.UPC/Item.Number': 'upc_item_number'
  ,'Ind.EAN.Flag': 'ean_flag'
  ,'Ind.Sell Unit Qty': 'sell_unit_qty'
}

# df = prod_gap_rule_new



for key, value in dep_column_to_fill.items():
  prod_gap_rule_new[key] = prod_gap_rule_new.apply(lambda row : find_item(row['Dep.Product Key/Sys_ID/EAN No.'])[value].item() if row['Relationship Type'] in ('Item to Item','Item to PF') and pd.isnull(row[key]) else row[key], axis = 1)

for key, value in ind_column_to_fill.items():
  prod_gap_rule_new[key] = prod_gap_rule_new.apply(lambda row : find_item(row['Ind.Product Key/Sys_ID/EAN No.'])[value].item() if row['Relationship Type'] in('Item to Item','PF to Item') and pd.isnull(row[key]) else row[key], axis = 1)

prod_gap_rule_new

# COMMAND ----------

item_level_input_prod_gap['prod_gap_key_pf'] = item_level_input_prod_gap['category'] + '_' + item_level_input_prod_gap['price_family']
item_level_input_prod_gap['prod_gap_key_pk'] = item_level_input_prod_gap['category'] + '_' + item_level_input_prod_gap['sys_id_final'].map(str)
prod_gap_key = item_level_input_prod_gap['prod_gap_key_pf'].unique().tolist() + item_level_input_prod_gap['prod_gap_key_pk'].unique().tolist()

prod_gap_rule_new['dep_prod_gap_key'] = np.where(prod_gap_rule_new['Dep.PF.Name'] == 'No_Family',prod_gap_rule_new['Dep.Category'].map(str) + '_' + prod_gap_rule_new['Dep.Product Key/Sys_ID/EAN No.'].map(str), prod_gap_rule_new['Dep.Category'].map(str) + '_' + prod_gap_rule_new['Dep.PF.Name'].map(str))

prod_gap_rule_new['ind_prod_gap_key'] = np.where(prod_gap_rule_new['Ind.PF.Name'] == 'No_Family',prod_gap_rule_new['Ind.Category'].map(str) + '_' + prod_gap_rule_new['Ind.Product Key/Sys_ID/EAN No.'].map(str), prod_gap_rule_new['Ind.Category'].map(str) + '_' + prod_gap_rule_new['Ind.PF.Name'].map(str))

prod_gap_rule_new['flag1'] = np.where(prod_gap_rule_new['dep_prod_gap_key'].isin (prod_gap_key),0,1)
prod_gap_rule_new['flag2'] = np.where(prod_gap_rule_new['ind_prod_gap_key'].isin (prod_gap_key),0,1)
#tw - item rule for PF item flag
prod_gap_rule_new['flag3'] = np.where((prod_gap_rule_new['Dep.PF.Name'] != 'No_Family') & (prod_gap_rule_new['Dep.Product Key/Sys_ID/EAN No.'].notnull()),1,0)
prod_gap_rule_new['flag4'] = np.where((prod_gap_rule_new['Ind.PF.Name'] != 'No_Family') & (prod_gap_rule_new['Ind.Product Key/Sys_ID/EAN No.'].notnull()),1,0)


def flag_df(df):

    if df['flag1'] == 1 and df['flag2'] == 1:
        return 'Both dependent and independent not in scope'
    elif df['flag1'] == 1 and df['flag2'] == 0:
        if df['flag4'] == 1:
            return 'Dependent not in scope, independent item not orphan item'
        else:
            return 'Dependent not in scope'
    elif df['flag1'] == 0 and df['flag2'] == 1:
        if df['flag3'] == 1:
            return 'Independent not in scope, dependent item not orphan item'
        else:
            return 'Independent not in scope'
    else:
        if df['flag3'] == 1 and df['flag4'] == 1:
            return 'Both dependent and independent item not orphan item'
        elif df['flag3'] == 1 and df['flag4'] == 0:
            return 'Dependent item not orphan item'
        elif df['flag3'] == 0 and df['flag4'] == 1: 
            return 'Independent item not orphan item'
        else:
            return 'good'

prod_gap_rule_new['Essential BU Review'] = prod_gap_rule_new.apply(flag_df, axis = 1)
      
prod_gap_rule_new.drop(columns = ['flag1', 'flag2', 'flag3', 'flag4', 'dep_prod_gap_key', 'ind_prod_gap_key'], inplace = True)

prod_gap_rule_new['Comments'] = ''
display(prod_gap_rule_new)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate Optimization Strategy Static reference table
# MAGIC * The purpose of this table is the provide BU better understanding of how optimization strategy is associated with Global Strategy

# COMMAND ----------

# Optimization Strategy	Global Strategy
# Balanced	Transaction Building
# Grow /Maximize Revenue	Traffic Building
# Grow /Maximize Margin	Profit Generating

# static table for bu reference

opti_strategy_value = [['Balanced', 'Transaction Building'],
                       ['Grow Revenue', 'Traffic Building'],
                       ['Maximize Revenue', 'Traffic Building'],
                       ['Grow Margin', 'Profit Generating'],
                       ['Maximize Margin', 'Profit Generating']]

opti_strategy = pd.DataFrame(opti_strategy_value, columns = ['Optimization Strategy', 'Global Strategy'])
opti_strategy


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate Few mapping table and lists used to populate in Excel Dropdown list
# MAGIC * Due to limitation of xlsx package dowpdown only allows max 255 character, the following tables are created to map the abbrevieated dropdown item name and their full name
# MAGIC   * Ending Number Rule Dropdown
# MAGIC   * Prod Gap Rule Dropdown
# MAGIC * Create list for other dropdowns
# MAGIC   * Optimization Strategy Dropdown
# MAGIC   * Prod Gap Rule Type Dropdown

# COMMAND ----------

# populate two reference table to map values used in dropdown and values used in master input file
# because xlsx package have restriction of 255 character of dropdown list
ending_nbr_rule_mapping_value = [['No Rule', 'No Rule']
                                  ,['Whole Numbers', 'No Decimals; Straight Pricing i.e Whole Numbers']
                                  ,['5','Decimals; Ending with 5']
                                  ,['9','Decimals; Ending with 9']
                                  ,['0;5','Decimals; Ending with 0;5']
                                  ,['0;9','Decimals; Ending with 0;9']
                                  ,['5;9','Decimals; Ending with 5;9']
                                  ,['0;5;9','Decimals; Ending with 0;5;9']
                                  ,['0.99','Decimals; Ending with 0.99']
                                  ,['0.49;0.99','Decimals; Ending with 0.49;0.99']
                                  ,['0.29;0.49;0.69;0.99','Decimals; Ending with .29; .49; .69; .99']
                                  ,['0;0.50;0.75','Decimals; Ending with 0;0.50;0.75']
                                  ,['0;0.25;0.50;0.75','Decimals; Ending with 0;0.25;0.50;0.75']
                                  ,['SW Rule 1','No Decimals. 0- 90 SEK: all ending numbers; Over 90 SEK: 5;9'] #Sweden
                                  ,['PL Rule 1', '0-10 LPN: 0.29;0.49;0.79;0.99; over 10 LPN, 0;0.50'] # Poland
                                  ,['DK Rule 1', '0-25 DKK: 0;0.5; over 25 DKK: No Decimals'] # Denmark
                                  ,['HD Rule 1', '0-5: 9; 5-10: 0.49;0.99; over 10: 0.99'] # Heatland
                                 ] #tw removed 'other'

ending_nbr_rule_mapping = pd.DataFrame(ending_nbr_rule_mapping_value, columns = ['Ending Number Rule Dropdown Value', 'Ending Number Rule'])

prod_gap_rule_mapping_value = [
  ['Dependent more expensive than independent','More Exp','Price']
  ,['Dependent less expensive than independent','Less Exp','Price']
  ,['Dependent priced same as independent','Same Price','Price']
  ,['Dependent priced within the specified range of the Independent','Within Price Range','Price']
  ,['Dependent cheaper per unit of measure than independent','Cheaper Per Unit','Value']
  ,['Dependent expensive per unit of measure than independent','Expensive Per Unit','Value']
  ,['Dependent priced same per unit of measure than independent','Same Price Per Unit','Value']
  ,['Dependent margin higher than independent','Higher Margin $','Margin $']
  ,['Dependent margin lower than independent','Lower Margin $','Margin $']
  ,['Dependent margin same as independent','Same Margin $','Margin $']
  ,['Dependent margin % higher than independent','Higher Margin %','Margin Percentage']
  ,['Dependent margin % lower than independent','Lower Margin %','Margin Percentage']
  ,['Dependent margin % same as independent','Same Margin %','Margin Percentage']
  ,['Dependent a minimum 10% lower retail than the Independent (National Brand equivalent in the sub-category tier) while delivering a minimum of 10% more penny profit than competing Independent item','Min 10 Lower','Min 10']
  ,['Dependent (National Brand equivalent in the sub-category tier) a minimum 10% higher retail than the Independent  while delivering a minimum of 10% less penny profit than competing Independent item','Min 10 Higher','Min 10']

]
prod_gap_rule_mapping = pd.DataFrame(prod_gap_rule_mapping_value, columns = ['Detail','Standardized Rule Description','Rule Class'])

opti_strategy_lst = ['Grow Revenue', 'Maximize Revenue','Balanced', 'Grow Margin','Maximize Margin'] # leo added 11/29/2021
prod_gap_rule_type_lst = ['Item to Item','PF to PF','PF to Item','Item to PF']
prod_gap_rule_value_type_lst = ['Percentage','Absolute']

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Populate a helper table mapping Excel Sheet Name,table name and their relative position in the Excel file

# COMMAND ----------

# create row and column offset mapping for all dataframes, they will be used in writing the excel files

df_mapping_col = ['Name', 'Sheet_Name', 'Start_Row', 'End_Row', 'Start_Col', 'End_Col', 'Off_Set_Row']
df_mapping = pd.DataFrame(columns = df_mapping_col)
df_name_str = ['item_level_input','optimization_strategy_new','optimization_strategy_old','csc_level_input_new','csc_level_input_old','pf_level_input','state_level_category','zone_level_category','prod_gap_rule_new','prod_gap_rule_old']
df_name = [item_level_input,optimization_strategy_new,optimization_strategy_old,csc_level_input_new,csc_level_input_old,pf_level_input,state_level_category,zone_level_category,prod_gap_rule_new,prod_gap_rule_old]
df_sheet_name = ['Item Level Inputs','Optimization Strategy','Optimization Strategy (Past)','CSC Level Inputs','CSC Level Inputs (Past)','PF Level Inputs','State & Zone Level Cats','State & Zone Level Cats','Product Gap Rules','Product Gap Rules (Past)']

for i in range(len(df_name_str)):
  start_row = 6
  end_row = df_name[i].shape[0] + 6
  if df_name_str[i] == 'zone_level_category':
    start_col = 4
    end_col = df_name[i].shape[1] + 4
  else:
    start_col = 0
    end_col = df_name[i].shape[1]
  if df_name_str[i] in ['optimization_strategy_new','csc_level_input_new','pf_level_input','prod_gap_rule_new']:
    off_set_row = 1
  else:
    off_set_row = 0
  
  lst = [df_name_str[i],df_sheet_name[i],start_row,end_row,start_col,end_col,off_set_row]
  df_mapping = df_mapping.append(pd.DataFrame([lst], columns=df_mapping.columns), ignore_index=True)
  
df_mapping

# COMMAND ----------

# write file to excel with proper formatting

sheet_name = ['Item Level Inputs','Optimization Strategy','Optimization Strategy (Past)','CSC Level Inputs',
             'CSC Level Inputs (Past)','PF Level Inputs','State & Zone Level Cats','Product Gap Rules','Product Gap Rules (Past)']

file_name = bu_code_upper+'_Opti_Input_Format.xlsx'

with pd.ExcelWriter(file_name) as writer:

  #define some excel formats

  header_format_1 = writer.book.add_format({'bold': True, 'text_wrap': False, 'fg_color': '#FFE699','border': 1,'align': 'center'}) # for columns that will be populated by data scientist team
  header_format_2 = writer.book.add_format({'bold': True, 'text_wrap': False, 'fg_color': '#FFFF00','border': 1,'align': 'center'}) # for columns that will be populated by BU
  sub_header_format = writer.book.add_format({'text_wrap': True, 'italic': True,'border': 1, 'size': 8}) # for sub header row formats
  extreme_input_format = writer.book.add_format({'bg_color': '#FFC7CE','font_color': '#9C0006'}) # for extreme input format red
  
  header_format_1_center_accross = writer.book.add_format({'bold': True, 'text_wrap': False, 'fg_color': '#FFE699','border': 1})
  header_format_1_center_accross.set_center_across()
  
  pct_format = writer.book.add_format({'num_format' : '0.00%'}) # for percentage format

  #item level input to excel format
 
  item_level_input.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][0], startrow = 5, index = False)
  writer.sheets[df_mapping['Sheet_Name'][0]].write(0,0,'For reference purpose only. The content in this tab should not be changed',writer.book.add_format({'bold': True}))
  writer.sheets[df_mapping['Sheet_Name'][0]].write(1,0,'This is the final signed off version of scope/PF/CSC inputs from BUs',writer.book.add_format({'bold': True}))
  

  for col_num, value in enumerate(item_level_input.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][0]].write(5, col_num, value, header_format_1)
  
  #optimization strategy new to excel format
  
  #populate subheader row to provide column description

  new_value = [''] * 2 + ['Populated with CSC wherever present, else column will be SubCategory'] + ['Flag to identify whether CSC or Subcategory is populated for Subcat/CSC Column'] + ['Populated with optimization strategy from last refresh when Sub Category and CSC remain unchanged'] + ['Please select value from dropdown'] + ['Please enter comments if any']
  new_row = pd.DataFrame([new_value], columns =list(optimization_strategy_new.columns),index =[0])
  optimization_strategy_new_f = pd.concat([new_row, optimization_strategy_new]).reset_index(drop = True)

  optimization_strategy_new_f.to_excel(writer,sheet_name = df_mapping['Sheet_Name'][1],startrow = 5,index = False)
  
  opti_strategy.to_excel(writer,sheet_name = df_mapping['Sheet_Name'][1],startrow = 0, startcol = 8,index = False)
  writer.sheets[df_mapping['Sheet_Name'][1]].set_column(8, 9, 25)
  writer.sheets[df_mapping['Sheet_Name'][1]].conditional_format(0,8,5,9,{'type': 'formula', 'criteria': 'True','format': writer.book.add_format({'border': 1})})

  writer.sheets[df_mapping['Sheet_Name'][1]].merge_range('A5:E5', 'Populated by the Data Science Team', header_format_1)
  writer.sheets[df_mapping['Sheet_Name'][1]].merge_range('F5:G5', 'BU to populate', header_format_2)
  
  for col_num, value in enumerate(opti_strategy.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][1]].write(0, col_num + 8, value, header_format_1)
  

  for col_num, value in enumerate(optimization_strategy_new_f.columns.values):
    if col_num < list(optimization_strategy_new_f.columns).index('Optimization.Strategy (New)'):
      writer.sheets[df_mapping['Sheet_Name'][1]].write(5, col_num, value, header_format_1)
    else:
      writer.sheets[df_mapping['Sheet_Name'][1]].write(5, col_num, value, header_format_2)
  
  for col_num in range(new_row.shape[1]):
    writer.sheets[df_mapping['Sheet_Name'][1]].write(6, col_num, new_row.loc[0][col_num], sub_header_format)
        
  writer.sheets[sheet_name[1]].data_validation(df_mapping['Start_Row'][1] + 1, # first row (skip header and subheader row)
                                               5, # first col
                                               df_mapping['End_Row'][1], # last_row
                                               5, # last_col
                                               {'validate': 'list','source': opti_strategy_lst})

  #optimization strategy past to excel format
  optimization_strategy_old.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][2], startrow = 5, index = False)
  for col_num, value in enumerate(optimization_strategy_old.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][2]].write(5, col_num, value, header_format_1)
    
    
  #csc level input new to excel format
  new_value = [''] * 2 + ['Populated with CSC wherever present, else column will be SubCategory'] + ['Flag to identify whether CSC or Subcategory is populated for Subcat/CSC Column'] + ['Please enter only negative values, Extreme value (<-40%) will be flagged for further review'] + ['Please enter only positive values, Extreme value (>+40%) will be flagged for further review'] * 3 + \
   ['only negative values allowed + Extreme value flagging(<-40%)'] + ['Please select from drop down'] + ['Please enter comments if any'] #tw removed guide for 'Other' 
  new_row = pd.DataFrame([new_value], columns =list(csc_level_input_new.columns),index =[0])
  csc_level_input_new_f = pd.concat([new_row,csc_level_input_new]).reset_index(drop = True)
  
  
  csc_level_input_new_f.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][3], startrow = 5, index = False)
  
  writer.sheets[df_mapping['Sheet_Name'][3]].merge_range('A5:D5', 'Populated by the Data Science Team', header_format_1)
  writer.sheets[df_mapping['Sheet_Name'][3]].merge_range('E5:K5', 'BU to populate', header_format_2) #tw changed L5 to K5
  
  ending_nbr_rule_mapping.to_excel(writer,sheet_name = df_mapping['Sheet_Name'][3],startrow = 6, startcol = 12,index = False) #tw changed 13 to 12, 14 to 13
  writer.sheets[df_mapping['Sheet_Name'][3]].set_column(12, 12, 40)
  writer.sheets[df_mapping['Sheet_Name'][3]].set_column(13, 13, 80)
  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(6,12,23,13,{'type': 'formula', 'criteria': 'True','format': writer.book.add_format({'border': 1})}) #tw changed 22 to 21
  
  for col_num, value in enumerate(ending_nbr_rule_mapping.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][3]].write(6, col_num + 12, value, header_format_1) #tw changed 13 to 12
  
  for col_num, value in enumerate(csc_level_input_new_f.columns.values):
    if col_num < list(csc_level_input_new_f.columns).index('Max Price Decrease (%)'): # columns before max price decrease will be populated by data scientist team
      writer.sheets[df_mapping['Sheet_Name'][3]].write(5, col_num, value, header_format_1)
    else:
      writer.sheets[df_mapping['Sheet_Name'][3]].write(5, col_num, value, header_format_2)
      
  for col_num in range(new_row.shape[1]):
    writer.sheets[df_mapping['Sheet_Name'][3]].write(6, col_num, new_row.loc[0][col_num], sub_header_format)
    
    
  writer.sheets[df_mapping['Sheet_Name'][3]].data_validation(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                           4, # first col
                                                           df_mapping['End_Row'][3], # last_row
                                                           4, # last_col, 
                                                           {'validate': 'decimal',
                                                            'criteria': '<=','value': 0,
                                                            'error_title': 'Input value not valid!',
                                                            'error_message': 'It should be a negative number'})

  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                                4, # first col
                                                                df_mapping['End_Row'][3], # last_row
                                                                4, # last_col, 
                                                                {'type': 'cell','criteria': '<','value': -0.4,'format':extreme_input_format})

  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                                4, # first col
                                                                df_mapping['End_Row'][3], # last_row
                                                                4, # last_col, 
                                                                {'type': 'cell'
                                                                 ,'criteria': 'between'
                                                                 ,'minimum': -0.01
                                                                 ,'maximum' : -0.00001
                                                                 ,'format':extreme_input_format})


  
  writer.sheets[df_mapping['Sheet_Name'][3]].data_validation(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                         5, # first col
                                                         df_mapping['End_Row'][3], # last_row
                                                         7, # last_col, 
                                                         {'validate': 'decimal',
                                                          'criteria': '>=','value': 0,
                                                          'error_title': 'Input value not valid!',
                                                          'error_message': 'It should be a positive number'})

  
  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                                5, # first col
                                                                df_mapping['End_Row'][3], # last_row
                                                                7, # last_col, 
                                                                {'type': 'cell','criteria': '>','value': 0.4,'format':extreme_input_format})

  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                                5, # first col
                                                                df_mapping['End_Row'][3], # last_row
                                                                7, # last_col, 
                                                                {'type': 'cell'
                                                                 ,'criteria': 'between'
                                                                 ,'minimum': 0.00001
                                                                 ,'maximum' : 0.01
                                                                 ,'format':extreme_input_format})
  
  writer.sheets[df_mapping['Sheet_Name'][3]].data_validation(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                           8, # first col
                                                           df_mapping['End_Row'][3], # last_row
                                                           8, # last_col, 
                                                           {'validate': 'decimal',
                                                            'criteria': '<=','value': 0,
                                                            'error_title': 'Input value not valid!',
                                                            'error_message': 'It should be a negative number'})

  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                                8, # first col
                                                                df_mapping['End_Row'][3], # last_row
                                                                8, # last_col, 
                                                                {'type': 'cell','criteria': '<','value': -0.4,'format':extreme_input_format})

  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                                8, # first col
                                                                df_mapping['End_Row'][3], # last_row
                                                                8, # last_col, 
                                                                {'type': 'cell'
                                                                 ,'criteria': 'between'
                                                                 ,'minimum': -0.01
                                                                 ,'maximum' : -0.00001
                                                                 ,'format':extreme_input_format})
  
  
  
 
  writer.sheets[sheet_name[3]].data_validation(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                           9, # first col
                                           df_mapping['End_Row'][3], # last_row
                                           9, # last_col
                                           {'validate': 'list','source': ending_nbr_rule_mapping['Ending Number Rule Dropdown Value'].to_list()})
  
  writer.sheets[df_mapping['Sheet_Name'][3]].conditional_format(df_mapping['Start_Row'][3] + 1, # first row (skip header and subheader row)
                                                              10, # first col
                                                              df_mapping['End_Row'][3], # last_row
                                                              10, # last_col,
                                                              {'type': 'formula',
                                                               'criteria': '=IF(AND($J8="Other",ISBLANK($K8)),TRUE,FALSE)',
                                                               'format':extreme_input_format})
    
  #csc level input old to excel format
  csc_level_input_old.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][4], startrow = 5, index = False)
  for col_num, value in enumerate(csc_level_input_old.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][4]].write(5, col_num, value, header_format_1)
    
  #price family level input to excel format
  
  new_value = [''] * 2 + ['Populated with CSC wherever present, else column will be SubCategory'] + ['Flag to identify whether CSC or Subcategory is populated for Subcat/CSC Column'] + ['Column populated with Price Family or "No_Family" when dealing with an orphan item'] + ['Flag to identify whether price family or item is populated for price family column'] + ['Column only populated for orphan item'] * 2 + \
  ['Column only populated for orphan items (NA : blank, EU : Set to "Y" if we are doing EAN level pricing else set to "N")'] + \
  ['Column only populated for orphan items (NA : sell unit qty, EU : leave blank)'] + ['Column only populated for orphan items (NA : tempe product key, EU: EAN if doing EAN level pricing else Sys id)'] + ['Please enter only positive values'] * 2
  new_row = pd.DataFrame([new_value], columns =list(pf_level_input.columns),index =[0])
  pf_level_input_f = pd.concat([new_row, pf_level_input]).reset_index(drop = True)
  
  
  pf_level_input_f.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][5], startrow = 5, index = False)
  
  writer.sheets[df_mapping['Sheet_Name'][5]].merge_range('A5:K5', 'Populated by the Data Science Team', header_format_1)
  writer.sheets[df_mapping['Sheet_Name'][5]].merge_range('L5:M5', 'BU to populate', header_format_2)
  
  
  for col_num, value in enumerate(pf_level_input_f.columns.values):
    if col_num < list(pf_level_input_f.columns).index('Price Ceiling ($)'): # columns before price ceiling decrease will be populated by data scientist team
      writer.sheets[df_mapping['Sheet_Name'][5]].write(5, col_num, value, header_format_1)
    else:
      writer.sheets[df_mapping['Sheet_Name'][5]].write(5, col_num, value, header_format_2)
      
  for col_num in range(new_row.shape[1]):
    writer.sheets[df_mapping['Sheet_Name'][5]].write(6, col_num, new_row.loc[0][col_num], sub_header_format)
    
  writer.sheets[df_mapping['Sheet_Name'][5]].data_validation(df_mapping['Start_Row'][5] + 1, # first row (skip header and subheader row)
                                                             10, # first col
                                                             df_mapping['End_Row'][5], # last_row
                                                             11, # last_col, 
                                                             {'validate': 'decimal',
                                                              'criteria': '>=','value': 0,
                                                              'error_title': 'Input value not valid!',
                                                              'error_message': 'It should be a positive number'})
  
  writer.sheets[df_mapping['Sheet_Name'][5]].conditional_format(df_mapping['Start_Row'][5] + 1, # first row (skip header and subheader row)
                                                          11, # first col
                                                          df_mapping['End_Row'][5], # last_row
                                                          11, # last_col,
                                                          {'type': 'formula',
                                                           'criteria': '=IF(AND($L8 >= $K8, LEN($L8) > 0,LEN($K8) > 0),TRUE,FALSE)',
                                                           'format':extreme_input_format})

    
    
  #State & Zone Level Cats to excel format
  state_level_category.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][6],startrow = 5, index = False)
  
  writer.sheets[df_mapping['Sheet_Name'][6]].write(0,0,'State Level Cateogry',writer.book.add_format({'bold': True}))
  
  for col_num, value in enumerate(state_level_category.columns.values):
    if col_num < list(state_level_category.columns).index('New State Level Categories'): # columns before New State Level Categories will be populated by data scientist team
      writer.sheets[df_mapping['Sheet_Name'][6]].write(5, col_num, value, header_format_1)
    else:
      writer.sheets[df_mapping['Sheet_Name'][6]].write(5, col_num, value, header_format_2)

  row_idx, col_idx = state_level_category.shape
  for r in range(row_idx):
    for c in range(col_idx):
      if pd.isna(state_level_category.values[r, c]):
        val = ''
      else:
        val = state_level_category.values[r, c]
      writer.sheets[sheet_name[6]].write(r + 6, c, val, writer.book.add_format({'border': 1}))

  zone_level_category.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][7],startrow = 5,startcol = 4, index = False)
  
  writer.sheets[df_mapping['Sheet_Name'][7]].write(0,4,'Zone Level Cateogry',writer.book.add_format({'bold': True}))
  
  for col_num, value in enumerate(zone_level_category.columns.values):
    if col_num < list(zone_level_category.columns).index('New Zone Level Categories'): # columns before New Zone Level Categories will be populated by data scientist team
      writer.sheets[df_mapping['Sheet_Name'][7]].write(5, col_num + 4, value, header_format_1)
    else:
      writer.sheets[df_mapping['Sheet_Name'][7]].write(5, col_num + 4, value, header_format_2)
  
  row_idx, col_idx = zone_level_category.shape
  for r in range(row_idx):
    for c in range(col_idx):
      if pd.isna(zone_level_category.values[r, c]):
        val = ''
      else:
        val = zone_level_category.values[r, c]
      writer.sheets[sheet_name[6]].write(r + 6, c + 4, val, writer.book.add_format({'border': 1}))
      
  writer.sheets[df_mapping['Sheet_Name'][7]].merge_range('A5:B5', 'Populated by the Data Science Team', header_format_1)
  writer.sheets[df_mapping['Sheet_Name'][7]].write(4,2,'BU to populate',header_format_2)
    
  writer.sheets[df_mapping['Sheet_Name'][7]].merge_range('E5:F5', 'Populated by the Data Science Team', header_format_1)
  writer.sheets[df_mapping['Sheet_Name'][7]].write(4,6,'BU to populate',header_format_2)
  
  #prod gap rule new to excel format

  new_value = [''] + ['Please select from drop down'] + [''] * 2 + ['Column only populate for orphan items'] + ['Column only populate for orphan items (NA : UPC || EU : Item No.)'] + ['Column only populate for orphan items (NA : blank, EU : Set to "Y" if we are doing EAN level pricing else set to "N")'] + \
  ['Column only populated for orphan items(NA : sell unit qty, EU : leave blank)'] + ['Column only populated for orphan items (NA : tempe product key, EU:  EAN if doing EAN level pricing else Sys id)'] + \
  [''] * 2 + ['Column only populate for orphan items'] + ['Column only populate for orphan items (NA : UPC || EU : Item No.)'] + ['Column only populated for orphan items (NA : blank , EU : Set to "Y" if  we are doing EAN level pricing else set to "N")'] + \
  ['Column only populated for orphan items (NA : sell unit qty, EU : leave blank)'] + ['Column only populated for orphan items (NA : tempe product key, EU: EAN if doing EAN level pricing else Sys id)'] + ['Please select from drop down'] + ['Column will automatically populate based on standardized rule selection'] + ['Please select from drop down'] + ['Please enter only positive values'] * 2 + ['We recommend BU review all product gap rules to determine if they are still relevant. However , in the very least we request reviewing the ones flagged in this column'] + ['Please enter comments if any']
  new_row = pd.DataFrame([new_value], columns=list(prod_gap_rule_new.columns), index =[0])
  prod_gap_rule_new_f = pd.concat([new_row, prod_gap_rule_new]).reset_index(drop = True)
  
  prod_gap_rule_new_f.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][8], startrow = 5, index = False)
  
  writer.sheets[df_mapping['Sheet_Name'][8]].merge_range('A5:W5','BU to populate', header_format_2)
  
  prod_gap_rule_mapping.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][8], startrow = 6, startcol = 24, index = False)
  writer.sheets[df_mapping['Sheet_Name'][8]].set_column(24, 24, 130)
  writer.sheets[df_mapping['Sheet_Name'][8]].set_column(25, 25, 40)
  writer.sheets[df_mapping['Sheet_Name'][8]].set_column(26, 26, 20)
  
  writer.sheets[df_mapping['Sheet_Name'][8]].conditional_format(6,24,21,26,{'type': 'formula', 'criteria': 'True','format': writer.book.add_format({'border': 1})})
    
  for col_num, value in enumerate(prod_gap_rule_mapping.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][8]].write(6, col_num + 24, value, header_format_1)
  
  
  for col_num, value in enumerate(prod_gap_rule_new_f.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][8]].write(5, col_num, value, header_format_2)

  for col_num in range(new_row.shape[1]):
    writer.sheets[df_mapping['Sheet_Name'][8]].write(6, col_num, new_row.loc[0][col_num], sub_header_format)

#   The following code is used to populate item information based on prod key/ean/sys_id, this is no longer needed, commented in case needed in future
#   if geography != 'Europe':

#     for row_num in range(500):
#       dep_key1 = xl_rowcol_to_cell(row_num + 7,5)
#       dep_key2 = xl_rowcol_to_cell(row_num + 7,7)
#       dep_key = dep_key1 + '&"_"&' + dep_key2
#       dep_flag = xl_rowcol_to_cell(row_num + 7,6)
#   #     need to fix this formular for EU depending on the columns in the item level input sheet. when EAN flag is Y, then get EAN No. Else get trn_item_sys_id
#       writer.sheets[df_mapping['Sheet_Name'][8]].write_formula(row_num + 7, 8,'=IF(AND(ISBLANK(%s),ISBLANK(%s)),"",XLOOKUP(%s,\'Item Level Inputs\'!E:E&"_"&\'Item Level Inputs\'!H:H,\'Item Level Inputs\'!G:G))' % (dep_key1,dep_key2,dep_key))

#       ind_key1 = xl_rowcol_to_cell(row_num + 7,12)
#       ind_key2 = xl_rowcol_to_cell(row_num + 7,14)
#       ind_key = ind_key1 + '&"_"&' + ind_key2
#       ind_flag = xl_rowcol_to_cell(row_num + 7,13)
#       writer.sheets[df_mapping['Sheet_Name'][8]].write_formula(row_num + 7, 15,'=IF(AND(ISBLANK(%s),ISBLANK(%s)),"",XLOOKUP(%s,\'Item Level Inputs\'!E:E&"_"&\'Item Level Inputs\'!H:H,\'Item Level Inputs\'!G:G))' % (ind_key1,ind_key2,ind_key))

#   #   original excel formular for reference
#   #   =XLOOKUP(F9&"_"&H9,'Item Level Inputs'!E:E&"_"&'Item Level Inputs'!O:O,'Item Level Inputs'!G:G)

#   if geography == 'Europe':
#     for row_num in range(500):
#       dep_key = xl_rowcol_to_cell(row_num + 7,5) #F8
#       dep_ean_flag = xl_rowcol_to_cell(row_num + 7,6) #G8
#       writer.sheets[df_mapping['Sheet_Name'][8]].write_formula(row_num + 7, 8,
#                                                                '=IF(AND(ISBLANK(%s),ISBLANK(%s)),"",IF(%s="Y",XLOOKUP(%s,\'Item Level Inputs\'!$F:$F,\'Item Level Inputs\'!$E:$E),XLOOKUP(%s,\'Item Level Inputs\'!$F:$F,\'Item Level Inputs\'!$G:$G)))' 
#                                                                % (dep_key,dep_ean_flag,dep_ean_flag,dep_key,dep_key))
      
#       ind_key = xl_rowcol_to_cell(row_num + 7,12) #M8
#       ind_ean_flag = xl_rowcol_to_cell(row_num + 7,13) #N8
#       writer.sheets[df_mapping['Sheet_Name'][8]].write_formula(row_num + 7, 15,
#                                                                '=IF(AND(ISBLANK(%s),ISBLANK(%s)),"",IF(%s="Y",XLOOKUP(%s,\'Item Level Inputs\'!$F:$F,\'Item Level Inputs\'!$E:$E),XLOOKUP(%s,\'Item Level Inputs\'!$F:$F,\'Item Level Inputs\'!$G:$G)))' 
#                                                                % (ind_key,ind_ean_flag,ind_ean_flag,ind_key,ind_key))
      
      
  for row_num in range(500):
    key = xl_rowcol_to_cell(row_num + 7, 16) # Q8
    writer.sheets[df_mapping['Sheet_Name'][8]].write_formula(row_num + 7, 17,'=IF(ISBLANK(%s),"",VLOOKUP(%s,$Z$8:$AA$22,2,0))'%(key,key)) #tw added 4/22  

      


#       =IF(AND(ISBLANK(F8),ISBLANK(G8)),"",IF(G8="Y",XLOOKUP(F8,'Item Level Inputs'!$F:$F,'Item Level Inputs'!$E:$E),XLOOKUP(F8,'Item Level Inputs'!$F:$F,'Item Level Inputs'!$G:$G)))
      
      #   original excel formular for reference for EU (When there are new ean columns in item level inputs tab)
#       =IF(AND(ISBLANK(F8),ISBLANK(G8)),"",IF(G8="Y",IF(ISBLANK(XLOOKUP(F8,'Item Level Inputs'!$G:$G,'Item Level Inputs'!$F:$F)),XLOOKUP(F8,'Item Level Inputs'!$G:$G,'Item Level Inputs'!$E:$E),XLOOKUP(F8,'Item Level Inputs'!$G:$G,'Item Level Inputs'!$F:$F)),XLOOKUP(F8,'Item Level Inputs'!$G:$G,'Item Level Inputs'!$H:$H)))



  writer.sheets[df_mapping['Sheet_Name'][8]].data_validation(df_mapping['Start_Row'][8] + 1, # first row (skip header and subheader row)
                                                             1, # first col
                                                             df_mapping['End_Row'][8], # last_row
                                                             1, # last_col, 
                                                             {'validate': 'list','source': prod_gap_rule_type_lst})
  
  writer.sheets[df_mapping['Sheet_Name'][8]].data_validation(df_mapping['Start_Row'][8] + 1, # first row (skip header and subheader row)
                                                           16, # first col
                                                           df_mapping['End_Row'][8], # last_row
                                                           16, # last_col, 
                                                           {'validate': 'list','source': prod_gap_rule_mapping['Standardized Rule Description'].to_list()})

  
  writer.sheets[df_mapping['Sheet_Name'][8]].data_validation(df_mapping['Start_Row'][8] + 1, # first row (skip header and subheader row)
                                                           18, # first col
                                                           df_mapping['End_Row'][8], # last_row
                                                           18, # last_col, 
                                                           {'validate': 'list','source': prod_gap_rule_value_type_lst})
  
#   writer.sheets[df_mapping['Sheet_Name'][8]].conditional_format(df_mapping['Start_Row'][8] + 1, # first row (skip header and subheader row)
#                                                             17, # first col
#                                                             df_mapping['End_Row'][8], # last_row
#                                                             17, # last_col,
#                                                             {'type': 'formula',
#                                                              'criteria': '=IF(AND($Q8="Other",ISBLANK($R8)),TRUE,FALSE)',
#                                                              'format':extreme_input_format})

   
  #prod gap rule old to excel format  
  prod_gap_rule_old.to_excel(writer, sheet_name = df_mapping['Sheet_Name'][9], startrow = 5, index = False)
  for col_num, value in enumerate(prod_gap_rule_old.columns.values):
    writer.sheets[df_mapping['Sheet_Name'][9]].write(5, col_num, value, header_format_1)
  

  # adjusting column width and add border for all sheets for better formatting
  
  data_frame_for_writer=[item_level_input,optimization_strategy_new,optimization_strategy_old,csc_level_input_new,csc_level_input_old,pf_level_input,state_level_category,prod_gap_rule_new,prod_gap_rule_old]
  
  
  for i,j,k in zip(df_name,df_sheet_name,df_name_str):

      workbook=writer.book
      
      for column in i:
        
        first_row = df_mapping.loc[df_mapping['Name'] == k]['Start_Row'].values[0]
        first_col = df_mapping.loc[df_mapping['Name'] == k]['Start_Col'].values[0]
        last_row = df_mapping.loc[df_mapping['Name'] == k]['End_Row'].values[0]
        last_col = df_mapping.loc[df_mapping['Name'] == k]['End_Col'].values[0]
        off_set_row = df_mapping.loc[df_mapping['Name'] == k]['Off_Set_Row'].values[0]
        
       # freeze top rows for better view
      
        writer.sheets[j].freeze_panes(first_row + off_set_row, 0)

      # adjusting column width and adding border
      # border is added as a conditional format to avoid rewriting cell values again and improve performance
              
        if (j == 'CSC Level Inputs'):
            col_max = i[column].astype(str).map(len).max()
            column_length = max(30 if np.isnan(col_max) else col_max , len(column)) # give a min column width with the sheet is blank
            col_idx = i.columns.get_loc(column)
            start_col = df_mapping.loc[df_mapping['Name'] == k]['Start_Col'].values[0]
            if col_idx in [4,5,6,7,8]:
              writer.sheets[j].set_column(col_idx + start_col, col_idx + start_col, column_length,pct_format)
            else:
              writer.sheets[j].set_column(col_idx + start_col, col_idx + start_col, column_length)

        else:
          col_max = i[column].astype(str).map(len).max()
          column_length = max(30 if np.isnan(col_max) else col_max , len(column)) # give a min column width with the sheet is blank
          col_idx = i.columns.get_loc(column)
          start_col = df_mapping.loc[df_mapping['Name'] == k]['Start_Col'].values[0]
          writer.sheets[j].set_column(col_idx + start_col, col_idx + start_col, column_length)
          
        writer.sheets[j].conditional_format(first_row + off_set_row, first_col,last_row + off_set_row - 1, last_col - 1,
                                            {'type': 'formula', 'criteria': 'True',
                                             'format': workbook.add_format({'border': 1})})

# move(os.path.join(src, file_name), os.path.join(dst, file_name)) # specify full directory to overwrite the files

# COMMAND ----------

# Put in where you want to save the output
# save initial file in round1 folder and final folder in final folder, create folders for all intermediate files
cwd = os.getcwd() 

dst = '/Phase4_extensions/Optimization/Mock_GL_Test/templates/round1/'
move(os.path.join(cwd, file_name), os.path.join(dst, file_name)) # specify full directory to overwrite the files
# move(file_name, dst)

print ("Output Path: ", dst)
print ("Output File Name: ", file_name)

# COMMAND ----------

# command to download file from dbfs
# dbfs cp dbfs:/FileStore/LYou/Opti_Input_Format_Sample_Auto_Test.xlsx ./ --overwrite
