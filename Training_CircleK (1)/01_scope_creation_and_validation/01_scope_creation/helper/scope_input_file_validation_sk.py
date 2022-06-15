# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Scope Input File Validation
# MAGIC This notebook is used to do preliminary data validation on the filled scope/pf/csc file
# MAGIC 
# MAGIC ##### Created By: Anson Park, Darren Wang
# MAGIC ##### Last Update on 2021-11-11

# COMMAND ----------

# MAGIC %md
# MAGIC #### IMPORT PACKAGES

# COMMAND ----------

import pandas as pd
import numpy as np
import re
# eliminate chained assignments warning
pd.options.mode.chained_assignment = None  # default='warn'

# COMMAND ----------

# MAGIC %md
# MAGIC #### FUNCTIONS

# COMMAND ----------

#will use for various validations throughout
def new_flag_val(row, col_name, category_list):
  if row[col_name] in category_list:
    return 0
  else:
    return 1
  
#function used to rename price & cost columns
def rename_price_cost_columns(df):
  price_cost_colname_mapper = dict()
  price_colnames_orig = []
  cost_colnames_orig = []

  for colname in df.columns:
    if 'price' in colname and not 'family' in colname:
      price_colnames_orig.append(colname)
    elif 'cost' in colname:
      cost_colnames_orig.append(colname)

  assert len(price_colnames_orig) == num_clusters, 'Number of price columns not equal to number of clusters'
  assert len(cost_colnames_orig) == num_clusters, 'Number of cost columns not equal to number of clusters'

  price_colnames_orig.sort()
  cost_colnames_orig.sort()

  for i, colname in enumerate(price_colnames_orig, 1):
    cluster = str(i)
    assert cluster in colname, f'Check price columns includes all clusters, check cluster {cluster}'
    price_cost_colname_mapper[colname] = 'price' + cluster

  for i, colname in enumerate(cost_colnames_orig, 1):
    cluster = str(i)
    assert cluster in colname, f'Check cost columns includes all clusters, check cluster {cluster}'
    price_cost_colname_mapper[colname] = 'cost' + cluster
  
  # rename colnames
  df = df.rename(mapper=price_cost_colname_mapper, axis='columns')
  
  return df

#function used to summarised changes applied to each price family
def summarize_type_of_change_by_pf(row):
  global pf_to_type_of_change
  global pf_anchor_item_change_alert
  if row['new_scope'] == 'add':
    #todo: check what should BU put if standalone item is added to a family
    if row['price_family'] == 'No_Family':
      pf_to_type_of_change[row['price_family_final']] = (row['product_key'], 
                                                       'Prevous standalone item added to a price family')
    elif row['new_price_family'] == 'No_Family':
      pf_to_type_of_change[row['price_family_final']] = (row['product_key'], 
                                                       'New standalone item added to scope')
    else:
      pf_to_type_of_change[row['price_family_final']] = (row['product_key'], 'New item added to price family')
  elif row['new_scope'] == 'remove':
    if row['price_family'] == 'No_Family':
      pf_to_type_of_change['Dropped'] = (row['product_key'], 'Standalone item removed from scope')
    elif row['scope'] == 'primary':
      # add alert
      pf_anchor_item_change_alert[row['product_key']] = (row['price_family'], 'Anchor item removed from scope')
    elif row['scope'] == 'secondary':
      pf_to_type_of_change['Dropped'] = (row['product_key'], row['price_family'], 'Non-anchor item removed from price family')
  elif row['new_price_family'] == 'No_Family' and row['price_family'] != 'No_Family':
    if row['scope'] == 'primary':
      # add alert
      pf_anchor_item_change_alert[row['product_key']] = (row['price_family'], 'Anchor item becomes a standalone item')
    elif row['scope'] == 'secondary':
      pf_to_type_of_change['No_Family'] = (row['product_key'], 'Non-anchor item becomes a standalone item')

# COMMAND ----------

# MAGIC %md
# MAGIC #### USER INPUTS + BU SPECIFIC CHANGES

# COMMAND ----------

#ENTER INPUTS HERE##########################################################################################################################################################
#enter the number of clusters in the BU; this will determine the number of price/cost columns in the template
num_clusters = 5

#Enter revenue PERCENTAGE limit (will be used to flag dropped items with "high revenue" - dropped item contributes more than "revenue_lim_pct" percent to total CATEGORY revenue)
revenue_lim_pct = 10

#price and cost variation PERCENTAGE limits (will be used to flag PFs with high cost or price variation)
price_var_lim = 20
cost_var_lim = 20

#pack size variation (SELL UNIT QUANTITY) PERCENTAGE limits (will be used to flag PFs with high pack size variation)
packsize_var_lim = 10

# replace this with the location of your scope file on dbfs
FILE_DIR = "/dbfs/Phase3_extensions/Elasticity_Modelling/Apr22_Test_Refresh/GR_Repo/Input/gr_final_scope_Apr22_Test_refresh_0.6.xlsx"
#"/dbfs/FileStore/Anson/tx_initial_scope_Nov21_refresh.xlsx" # for TX BU pre-BU inputs
# FILE_DIR = "/dbfs/FileStore/Darren/Phase3_Extension/Nov21_Refresh/Scope_Validation_Test_Cases/tx_initial_scope_template_nov21_refresh_v1_1.xlsx" # Test Case NA Version 
# FILE_DIR = '/dbfs/FileStore/Darren/Phase3_Extension/Nov21_Refresh/Scope_Validation_Test_Cases/sw_initial_scope_Nov21_refresh.xlsx' # Test Case EU Version
# df is initial LP scope data after BU input; save as CSV file
# df = pd.read_csv("/dbfs/FileStore/Anson/tx_initial_scope_template_nov21_refresh-1.csv", keep_default_na=False)
# FILE_DIR = "/dbfs/FileStore/Darren/Phase3_Extension/Nov21_Refresh/Scope_Validation_Initial_Files/rm_initial_scope_Nov21_refresh.xlsx" 
df = pd.read_excel(FILE_DIR, sheet_name='Final Scope', keep_default_na=False)#, skiprows=3)

# COMMAND ----------

# TODO: get the data quality flags from module 2 output for added items

bu_code = 'GR'
phase = 'Phase3_extensions' #dbutils.widgets.get("phase")
wave = 'Apr22_Test_Refresh' #dbutils.widgets.get("wave")
pre_wave = '/Phase3_extensions/Elasticity_Modelling/' #dbutils.widgets.get("base_directory_spark")+"/"
bu = bu_code + '_Repo'

cumulative_sales_perc = '0.6'
scope_m2_path = bu_code.lower() + '_item_scoping_data_final_'+ cumulative_sales_perc + '.csv'
scope_from_m2 = pd.read_csv('/dbfs'+pre_wave+wave+'/'+bu+'/Input/'+scope_m2_path)
scope_flags = scope_from_m2[['upc','latest_date_diff_flag','consecutive_flag','discont_flag','data_enough_flag']]

df_add = df.loc[df['New LP Scope'] != '']
df_add = df_add.loc[:,~df_add.columns.isin(['latest_date_diff_flag','consecutive_flag','discont_flag','data_enough_flag'])]
df_add = df_add.merge(scope_flags, how="left", on="upc")

df_rest = df.loc[df['New LP Scope'] == '']
df = pd.concat([df_add, df_rest])

# COMMAND ----------

df['primary/secondary'] = np.where(df['final_recommendation']== 'goodstuff', 'primary', 'secondary')

# COMMAND ----------

df

# COMMAND ----------

count_series = df.groupby(['price_family','primary/secondary']).size()
count_series
new_df = count_series.to_frame(name = 'size').reset_index()
out = new_df[(new_df['primary/secondary'] == 'primary') & (new_df['size'] == 0)]
out

# COMMAND ----------

# TODO: filter for price families that just have secondary items
# TODO: come up with data-driven to identify primary items
# TODO: cum_sales_flag - within top 80.0% of sales as primary - let BU/ DS decide this threshold
# check how many pf still don't have primary

# COMMAND ----------

# MAGIC %md 
# MAGIC #### INPUT FORMAT VALIDATION
# MAGIC 
# MAGIC Steps:
# MAGIC 0. Standardize colnames to all lower cases and use underscore as separation between words
# MAGIC 1. Rename price and cost column, make sure price & cost exist for every cluster
# MAGIC 2. Rename columns. EU & NA will have the same colnames from this step on

# COMMAND ----------

#standardize colnames to all lower cases and underscore as deliminator between words
df.rename(mapper=lambda colname: '_'.join(substr.lower() for substr in re.split('\.|_| ', colname)), axis='columns', inplace=True)

df = rename_price_cost_columns(df)

#rename columns for consistency
df.rename(mapper={'tempe_product_key': 'product_key',
                  'trn_item_sys_id': 'product_key',
                  'ean': 'upc',
                  'new_ean': 'new_upc',
                  'lp_scope': 'scope',
                  'new_lp_scope': 'new_scope',
                  'category_special_classification': 'csc',
                  'item_subcategory': 'sub_category',
                  'jde_number': 'item_number'},
                  axis='columns',
                  inplace = True)

#European specific columns
#determine if belongs to European BU
if 'pricing_level' in df.columns:
  EU_flag = 1
  # add in sell_unit_qty for later checks to not throw errors or flag
  df['sell_unit_qty'] = 0
  df['size_unit_of_measure'] = 0
else:
  EU_flag = 0
  
#CONVERT COLUMNS TO CORRECT TYPE
#convert 'total_sales_amount' column to float after adding in 0s for all blanks
df.loc[df['total_sales_amount'].isin(['']), 'total_sales_amount'] = '0'
df['total_sales_amount'] = df['total_sales_amount'].astype(float)

# todo: need to test for Sweden
df['csc'] = df['csc'].fillna('None')
df['new_csc'] = df['new_csc'].fillna('None')

#convert upc columns to object
df['upc'] = df['upc'].apply(lambda x: x if type(x) == str else str(int(x)))
df['new_upc'] = df['new_upc'].apply(lambda x: x if type(x) == str else str(int(x)))

# df.dtypes
# df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Wrangle Scope Template for Validation

# COMMAND ----------

#***ADD UPC CHECK HERE - what are the criteria of a valid UPC??***
# Check if UPC + sell unit quantity returns an item; product key is unique ID, make sure they match

#combine 'upc' and 'new_upc' columns into 'upc_final' column 
df['upc_final'] = df['upc'].where(df['new_upc'] == '', df['new_upc'])

#combine 'price_family' and 'new_price_family' columns into 'price_family_final' column 
df['price_family_final'] = df['price_family'].where(df['new_price_family'] == '', df['new_price_family'])

#combine 'csc' and 'new_csc' columns into 'csc_final' column
df['csc_final'] = df['csc'].where(df['new_csc'] == '', df['new_csc'])

#combine 'scope' and 'new_scope' columns into 'csc_final' column 
df['new_scope'] = df['new_scope'].str.lower()
df['scope'] = df['scope'].str.lower()
df['scope_final'] = df['scope'].where(df['new_scope'] == '', df['new_scope'])

#determine all unique price family items and CSCs
Price_families = df['price_family_final'].unique()
Classifications = df['csc_final'].unique()

#remove blank values from price families and CSCs if they are present
Price_families = np.delete(Price_families, np.where((Price_families == '') | (Price_families == 'No_Family')))
Classifications = np.delete(Classifications, np.where(Classifications == ''))

#standalone item count
standalone_item_cnt = sum(df['price_family_final'] == 'No_Family')

print('There are', len(Price_families), 'price families present.')
print('There are', standalone_item_cnt, 'standalone items present.')
print('There are', len(Classifications), 'category special classifications present.')

#df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC #### SCOPE VALIDATIONS

# COMMAND ----------

#CHECK: determine if all values entered in lp_scope_new column are valid - must have values of only '', 'add', 'remove'
#convert to all lower case for consistency
New_lp_scopes = df['new_scope'].unique()

#display 
assert len(New_lp_scopes) <= 3, 'Invalid inputs for new LP scope - valid inputs are add, remove'

#determine values/counts of all LATEST unique price family and CSC values 
PF_counts = np.unique(df['price_family_final'], return_counts = True)
CSC_counts = np.unique(df['csc_final'], return_counts = True)

#determine values/counts of all HISTORIC unique price family and CSC values 
PF_counts_historic = np.unique(df['price_family'], return_counts = True)
CSC_counts_historic = np.unique(df['csc'], return_counts = True)

# pair pf and element cnt - this will print each PF's name and number of its items
# print(*list(zip(*PF_counts)), sep="\n")

# COMMAND ----------

#VALIDATIONS FOR ADDED ITEMS

#create new dataframe for added items
df_added = df.loc[df['new_scope'] == 'add']

#VALIDATION 1 - Check if new items added to scope have every required values provided by the BU
columns_BU_need_to_fill = ['new_upc', 'new_price_family', 'sell_unit_qty', 'size_unit_of_measure', 'new_csc', 'product_key'] 
columns_to_display = ['new_upc', 'new_price_family', 'new_csc', 'product_key'] if EU_flag else columns_BU_need_to_fill
err_message = f'Some newly added items have required BU input values missing, call df_added for all added items. required columns are: {repr(columns_to_display)}' 
assert df_added.loc[(df_added[columns_BU_need_to_fill].isin(['', np.nan])).sum(axis=1) > 0, ].shape[0] == 0, err_message

#VALIDATION 2 - Check if new items added to scope belong to an already in-scope HISTORIC PF 
#new_pf_flag =  0 indicates new item added DOES belong to a HISTORIC in scope PF, 1 indicates new item added DOES NOT belong to HISTORIC in scope PF 
# df_added = df.loc[:2]
if not df_added.empty:
  df_added['new_pf_flag'] = df_added.apply(lambda row: new_flag_val(row, 'price_family_final', PF_counts_historic[0]), axis=1)

  #VALIDATION 3 - Get a count of new items added to scope
  #num_adds_newpf counts number of new items added to scope that DO NOT belong to an already in-scope HISTORIC PF
  num_adds_newpf = sum(df_added['new_pf_flag'])
  #num_adds counts the number of added items that DO belong to an already in-scope HISTORIC PF
  num_adds = df_added.shape[0]-num_adds_newpf

  print('There were', df_added.shape[0], 'items added in the scope validation tempalte.', num_adds, 'added items already belong to a previous price family;', num_adds_newpf, 'items belong to a new price family.')

  #uncomment line below and run to display all added items
  df_added

else:
  print('There were no items added to LP scope.')

# COMMAND ----------

#VALIDATIONS FOR DROPPED ITEMS

#create new dataframe for added items
df_dropped = df.loc[df['new_scope'] == 'remove']

#VALIDATION 3 - Check revenue of items dropped from scope.  Flag item if it has high revenue.

#determine revenue distribution for each category
#category_revenue_group = df.loc[df['total_sales_amount'] != 0].groupby('category')['total_sales_amount'].agg(['sum', 'count'])  #to exclude zero revenue items
category_revenue_group = df.groupby('category')['total_sales_amount'].agg(['sum', 'count'])
category_revenue_group.reset_index(inplace = True)

#determine revenue limit for each item category; if dropped item has total revenue higher than revenue_limit, it will be flagged
category_revenue_group['revenue_lim'] = category_revenue_group['sum']*revenue_lim_pct

if not df_dropped.empty:
  #join df_dropped to category_revenue_group to get 'revenue_lim' column
  df_dropped = pd.merge(df_dropped, category_revenue_group[['category','revenue_lim']], on = 'category', how = 'left')

  #create flag column
  df_dropped['high_revenue_flag'] = 0

  #flag dropped items that have total_sales_amount greater than revenue_lim - revenue lim in parentheses
  df_dropped.loc[df_dropped['total_sales_amount'] >= df_dropped['revenue_lim'], 'high_revenue_flag'] = 1

  print('There were', df_dropped.shape[0], 'items dropped in the scope validation template.', sum(df_dropped['high_revenue_flag']), 'dropped items have high revenue.')

  df_dropped

  #UNCOMMENT LINE BELOW to display dropped items with high_revenue_flag = 1
  #df_dropped.loc[df_dropped['high_revenue_flag'] == 1]
else:
  print('There were no items removed from LP scope.')

# COMMAND ----------

# TODO: checking if BU has removed goodstuff item from a price family - might be already there

# COMMAND ----------

#VALIDATIONS FOR PRICE FAMILY LEVEL CHANGES AND ANCHOR ITEM CHANGES
#ordered tuples for each price familys' members
pf_old = set(df.loc[(df['price_family'] != 'No_Family') & (df['new_scope'] != 'add')]\
             .groupby('price_family')['product_key'].apply(lambda x: tuple(sorted(list(x)))).values)

new_pf_name_to_pf = df.loc[(df['price_family_final'] != 'No_Family') & (df['new_scope'] != 'remove')]\
             .groupby('price_family_final')['product_key'].apply(lambda x: tuple(sorted(list(x))))
#ordered tuples for each price familys' members
pf_new = set(new_pf_name_to_pf.values)

#orderred tuples to new price family name
pf_to_new_pf_name = {items: name for name, items in [*zip(new_pf_name_to_pf.index, new_pf_name_to_pf.values)]}
      
pf_to_type_of_change = dict()
pf_anchor_item_change_alert = dict()
_ = df.apply(summarize_type_of_change_by_pf, axis=1)

changed_pf = []
for pf in pf_new:
  if pf not in pf_old:
    changed_pf.append(pf_to_new_pf_name.get(pf))
    
print(len(changed_pf), 'price families had changes applied to it.')
print(len(pf_anchor_item_change_alert), 'anchor items not serving in the same price family.')
for item in pf_anchor_item_change_alert:
  former_pf, status = pf_anchor_item_change_alert.get(item)
  print('product_key: ',item, ', former price family: ', former_pf, ', status: ', status)

# UNCOMMENT LINE BELOW for detialed summary of Price Family Changes
# pf_to_type_of_change

# COMMAND ----------

# MAGIC %md
# MAGIC #### PF VALIDATIONS

# COMMAND ----------

#VALIDATION 0 - standalone items should use 'No_Family' as their price family, no exception allowed
standalone_secondary_items = df.loc[df['price_family_final'].isin(['No Price Family'])]
if standalone_secondary_items.shape[0] > 0:
  print(f"{standalone_secondary_items.shape[0]} standalone items are using 'No Price Family', please change it to 'No_Family' so later validation won't fail")
else:
  print('All standalone items are using correct assignment as their price family name')
  
standalone_secondary_items

# COMMAND ----------

#VALIDATION 1 - check if every price family has at least one primary item
pf_scoped = df.groupby('price_family_final')['scope_final'].unique().apply(lambda x: 'primary' not in x)
if pf_scoped.any():
  print(f'{pf_scoped.sum()} price families do not have anchor items')
else:
  print('All price families contain one or more anchor items')
  
pf_scoped = pf_scoped.reset_index()
pf_scoped.loc[pf_scoped['scope_final'].isin([True]), 'price_family_final']

# COMMAND ----------

#VALIDATION 2 - check if there are secondary item flagged as no_family -> if so, they should be re-valued as primary
standalone_secondary_items = df.loc[(df['price_family_final'].isin(['No_Family'])) & df['scope_final'].isin(['secondary']),]
if standalone_secondary_items.shape[0] > 0:
  print(f'{standalone_secondary_items.shape[0]} items are standalone seceondary items')
else:
  print('All standalone items presented are marked as primary')

standalone_secondary_items

# COMMAND ----------

#VALIDATION 3 - check if all items within a PF have different CSCs, flag if >1 CSC is present within a PF
#groupby df rows where csc_final is not blank
csc_group = df.loc[df['csc_final'] != ''].groupby('price_family_final')['csc_final'].nunique()

df_csc = pd.DataFrame({'csc_count':csc_group})

df_csc['multiple_csc_flag'] = 0
df_csc.loc[df_csc['csc_count'] > 1, 'multiple_csc_flag'] = 1

#pull out all items with multiple_csc_flag == 1
df_csc_flagged = df_csc.loc[df_csc['multiple_csc_flag'] == 1]
csc_labels = df_csc_flagged.index.values

#remove 'No_Family' items
csc_labels = csc_labels[csc_labels != 'No_Family']

# Note: This might looks bad for Sweden as of now but will be resolved when CSC inputs format are finalized
print('There are', len(csc_labels), 'price families with more than one category special classification:\n\n', csc_labels)
#df_csc

# COMMAND ----------

# VALIDATION 4 - check price variation between all items in a PF
# VALIDATION 5 - check cost variation between all items in a PF
# VALIDATION 6 - check sell unit quantity (pack size) variation between all items in a PF

# price and cost columns
price_columns = ['price' + str(i) for i in range(1, num_clusters+1)]
cost_columns = ['cost' + str(i) for i in range(1, num_clusters+1)]

#change price and cost columns to float
cols = price_columns + cost_columns
df[cols] = df[cols].apply(pd.to_numeric, downcast='float', errors='coerce')

#add min/max price columns
df['min_price'] = df[price_columns].min(axis = 1)
df['max_price'] = df[price_columns].max(axis = 1)

#add min/max cost columns
df['min_cost'] = df[cost_columns].min(axis = 1)
df['max_cost'] = df[cost_columns].max(axis = 1)

# ADDED VALIDATION - Flag items with min_cost > min_price, max_cost > max_price, or any min/max prices or costs with 0
df['invalid_price_cost_flag'] = 0
df.loc[(df['min_cost'] >= df['min_price']) | (df['max_cost'] >= df['max_price']) | (any(df[['min_price', 'max_price', 'min_cost', 'max_cost']]) == 0), 'invalid_price_cost_flag'] = 1

invalid_items = df.loc[df['invalid_price_cost_flag'] == 1]

print('There are', invalid_items.shape[0], 'items that were flagged due to price/cost issues.  These items will not be used when examining price/cost/sell unit quantity variation between PF items.')
#print(invalid_items)

#PERFORM VALIDATIONS 2-4 ON ITEMS WITH invalid_price_cost_flag = 0  
valid_items = df.loc[df['invalid_price_cost_flag'] == 0]

df_mins = valid_items.groupby('price_family_final')[['sell_unit_qty', 'min_price', 'min_cost']].min()
df_mins.rename(columns={'sell_unit_qty': 'min_sell_unit_qty'}, inplace = True)

df_max = valid_items.groupby('price_family_final')[['sell_unit_qty', 'max_price', 'max_cost']].max()
df_max.rename(columns={'sell_unit_qty': 'max_sell_unit_qty'}, inplace = True)

df_min_max = pd.concat([df_mins, df_max[['max_sell_unit_qty', 'max_price', 'max_cost']]], axis = 1)
df_min_max.reset_index(inplace=True)

#create calculated variation columns
df_min_max['price_var'] = ((df_min_max['max_price']-df_min_max['min_price'])/df_min_max['min_price'])*100
df_min_max['cost_var'] = ((df_min_max['max_cost']-df_min_max['min_cost'])/df_min_max['min_cost'])*100
df_min_max['sell_qty_var'] = ((df_min_max['max_sell_unit_qty']-df_min_max['min_sell_unit_qty'])/df_min_max['min_sell_unit_qty'])*100

#create flags 
df_min_max['price_flag'] = 0
df_min_max['cost_flag'] = 0
df_min_max['sell_qty_flag'] = 0

#flag items that meet conditions
df_min_max.loc[df_min_max['price_var'] > price_var_lim, 'price_flag'] = 1
df_min_max.loc[df_min_max['cost_var'] > cost_var_lim, 'cost_flag'] = 1
df_min_max.loc[df_min_max['sell_qty_var'] > packsize_var_lim, 'sell_qty_flag'] = 1

print('There are', df_min_max['price_flag'].sum(), 'price families flagged for high price variation.')
print('There are', df_min_max['cost_flag'].sum(), 'price families flagged for high cost variation.')
print('There are', df_min_max['sell_qty_flag'].sum(), 'price families flagged for high pack size variation.\n')
print('All flagged price families are shown below: \n')

flagged_pf = df_min_max.loc[(df_min_max['price_flag'] == 1) | (df_min_max['cost_flag'] == 1) | (df_min_max['sell_qty_flag'] == 1)]
flagged_pf = flagged_pf.drop(['min_sell_unit_qty', 'max_sell_unit_qty'], axis=1) if EU_flag else flagged_pf
flagged_pf

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### CSC VALIDATIONS

# COMMAND ----------

#VALIDATION 1 - check if CSC is provided when needed
### When a category has one or more valid CSCs, all anchor(primary) items need to be provided with a valid CSC

# list of categories that contains one or more valid CSC
categories_w_csc = df.loc[~df['csc_final'].isin(['', 'Overall', 'None']), 'category'].unique()

# list of product violating the rule
violating_products = df.loc[(df['csc_final'].isin(['', 'Overall', 'None'])) &
                            (df['category'].isin(categories_w_csc)) & 
                            (df['scope_final'].isin(['add', 'primary'])), ['category', 'sub_category', 'item_desc', 'product_key', 'upc_final', 'scope_final', 'price_family_final',]]

assert violating_products.shape[0] == 0, 'BU needs to provide a valid CSC for all anchor(primary) items that belongs to a categories using CSC'
violating_products

# COMMAND ----------

#VALIDATION 2 - check if a CSC spans multiple categories, flag if >1 CSC is present within multiple categories

#groupby df rows where csc_final is not blank
category_summary = df.loc[df['csc_final'] != ''].groupby('csc_final')['category']\
                                           .apply(lambda x: set(x)).reset_index()\
                                           .rename(columns={'category': 'unique_category'})

category_summary['category_count'] = category_summary['unique_category'].apply(lambda x: len(x))

cat_labels = category_summary.loc[category_summary['category_count'] > 1, 'csc_final']
# avoid printing an empty series
cat_labels = [] if cat_labels.empty else cat_labels

print('There are', len(cat_labels), 'category special classifications that span multiple categories:\n', cat_labels)
category_summary

# COMMAND ----------

#VALIDATION 3 - check if sell unit quantity is the same for all items within a CSC 

csc_summary = df.loc[df['csc_final'] != ''].groupby('csc_final')['sell_unit_qty']\
                                           .apply(lambda x: set(x)).reset_index()\
                                           .rename(columns={'sell_unit_qty': 'unique_sell_qty'})

csc_summary['sell_unit_qty_count'] = csc_summary['unique_sell_qty'].apply(lambda x: len(x))
csc_lables = csc_summary.loc[csc_summary['sell_unit_qty_count'] > 1, 'csc_final']

print('There are', len(csc_lables), 'category special classifications that contains multiple sell_unit_qty:\n', csc_lables.values)

csc_summary.loc[csc_summary['sell_unit_qty_count'] > 1]

# COMMAND ----------

#VALIDATION 4 - Create a flag for items whose CSCs have changed since last refresh 

df.loc[df['csc_final'] != df['csc']]

# COMMAND ----------

# TODO: 
# 1. in the updated file received from the BU, create another column called 'primary/secondary' where we'll flag all goodstuff items are primary and rest as secondary
# 2. run the primary and secondary item checks
# 3. out of removed items, check if any pf doesn't have any goodstuff
# 4. out of added items, lookup data quality flag in the output of module 2 - no_family vs family
