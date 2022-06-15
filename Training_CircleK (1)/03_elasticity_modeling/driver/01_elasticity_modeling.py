# Databricks notebook source
# MAGIC %md
# MAGIC ### Metadata
# MAGIC ### Please read the operation guide below before running this notebook!
# MAGIC ##### This is the driver notebook for elasticity modeling. 
# MAGIC #### Operation Guide 
# MAGIC - Please run each cmd one by one. It is because you can only know the categories for step 2 after you run step 1 
# MAGIC - Run step 1: create modeling input 
# MAGIC  - Please change the parameters wave, business_unit, and bu_code as yours before running. Keep the wave as "Nov21_Refresh" for this refresh. For the business_unit and bu_code. Please look for them in the appendix: parameter options at the end of this notebook. Other parameters do not need to be changed at this time 
# MAGIC - Run step 2: driver code
# MAGIC  - Please set the parameters user, category_raws, category_names, and zoned_labels before running.  For the user, please type in the lowercase letters of your first name. You can know the available category_raws after you run step 1. Usually the available category_raws in the step 1 are the categories you want to model in the step 2. Sometimes you can select several categories for analysis. You can find the corresponding category_names in the appendix: parameter options. You can also create them according to category_raws by changing " ", ".", "/", "-", "&", and "(" to an underline and deleting ")." If a category is zoned, please mark it as "yes" in the zoned_labels, or just mark it as "no." Other parameters do not need to be revised at this time
# MAGIC  - While running this step, it is possible that you meet some errors due to the failure of the job cluster. In this case, please create new cmds, revise the modeling parameters for the remaining work and rerun them
# MAGIC - Run step 3: final steps
# MAGIC  - The users and categories for analysis should be set as input parameters for the final steps. In this notebook, it is generated automatically. But you can change them based on your choice
# MAGIC 
# MAGIC #### User Input
# MAGIC - Basic parameters
# MAGIC   - wave: String type. The wave of the refresh
# MAGIC   - business_unit: String type. The business unit
# MAGIC   - bu_code: String type. The corresponding BU code of the business_unit
# MAGIC   - is_to_create_modeling_input: Boolean type. If creating modeling input will be run
# MAGIC   - is_to_run_driver_code: Boolean type. If the driver code will be run
# MAGIC   - is_to_run_final_steps: Boolean type. If the final steps will be run
# MAGIC   - db_tbl_prefix: String type. The parameter used to save data in Database Tables. It is generated automatically here
# MAGIC - Modeling parameters
# MAGIC   - user: String type. The user to run the notebook
# MAGIC   - category_raws: List of string types. The raw category names
# MAGIC   - category_names: List of string types. The corresponding standard category names
# MAGIC   - zoned_labels: List of string types. The labels indicate if the categories are zoned. You can choose 'yes' or 'no'
# MAGIC   - levelvars: List of float types. The level of vars
# MAGIC   - version_nums: List of float types. The corresponding version numbers of the levels of vars
# MAGIC 
# MAGIC #### Additional Information
# MAGIC - The rules to select which steps to run
# MAGIC   - If you want to model for the first time, set is_to_create_modeling_input, is_to_run_driver_code, and is_to_run_final_steps as True (default setting)
# MAGIC   - If the input has been created and you want to build the elasticity of some specific categories, set is_to_create_modeling_input as False and set is_to_run_driver_code and is_to_run_final_steps as True
# MAGIC   - If all categories have been modeled and you want to update the final steps, set is_to_create_modeling_input and is_to_run_driver_code as False and set is_to_run_final_steps as True

# COMMAND ----------

# MAGIC %pip install xlsxwriter

# COMMAND ----------

# %r
# install.packages('tidyverse')
# install.packages('zoo')
# install.packages('openxlsx')
# install.packages('trustOptim')

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md # Step 1: Create Modeling Input

# COMMAND ----------

# DBTITLE 1,Set Basic Parameters
wave = "Mock_GL_Test" # Please change this parameter

business_unit = '4200 - Great Lakes Division' # Please change this parameter
bu_code = 'GL' # Please change this parameter

is_to_create_modeling_input = True
is_to_run_driver_code = True 
is_to_run_final_steps = True 

db_tbl_prefix = bu_code.lower()+'_'+wave.lower()+'_'

# COMMAND ----------

if is_to_create_modeling_input == True:
  dbutils.notebook.run("../helper/0_create_modeling_input_v3", 0, {"bu_code": bu_code, "business_unit": business_unit, "wave": wave})

# COMMAND ----------

# Show the available category_raws
base_directory = 'Phase4_extensions/Elasticity_Modelling'
scope_file = pd.read_csv('/dbfs/'+base_directory+'/'+wave+'/'+bu_code+'_Repo/'+'Input/scope_file.csv')
print('The available category_raws are\n',list(scope_file['CATEGORY'].unique()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2:  Driver Code

# COMMAND ----------

# DBTITLE 1,Set Modeling Parameters
# User
user = 'user-name' # Please change this parameter

# Categories
category_raws = ['009-FLUID MILK','013-FROZEN FOODS','014-PACKAGED BREAD','019-EDIBLE GROCERY','020-NON EDIBLE GROCERY','028-ICE','038-HOT SANDWICHES','043-BETTER FOR YOU','044-CHILLED SANDWICHES','046-SNACKS'] # Please change this parameter
category_names = ['009_FLUID_MILK','013_FROZEN_FOODS','014_PACKAGED_BREAD','019_EDIBLE_GROCERY','020_NON_EDIBLE_GROCERY','028_ICE', '038_HOT_SANDWICHES','043_BETTER_FOR_YOU','044_CHILLED_SANDWICHES','046_SNACKS'] # Please change this parameter
zoned_labels = ['no','no','no','no','no','no','no','no','no','no'] # Please change this parameter

# Level of vars and versions
levelvars = [0.15, 0.12, 0.1, 0.08, 0.05, 0.03, 0.01, 0.008]
version_nums = [1, 2, 3, 4, 5, 6, 7, 8]

# COMMAND ----------

def run_driver_code(bu_code, business_unit, category_raws, category_names, levelvars, user, version_nums, wave, zoned_labels):
  # Select a category
  for i_category in range(len(category_raws)):
    category_raw = category_raws[i_category]
    category_name = category_names[i_category]
    zoned = zoned_labels[i_category]
  
    # Select a version
    for i_version in range(len(version_nums)):
      version_num = version_nums[i_version]
      levelvar = levelvars[i_version]
    
      # Run driver code
      dbutils.notebook.run("../helper/01_elast_driver_code_v11", 0, {"bu_code": bu_code, "business_unit": business_unit, "category_name": category_name, "category_raw": category_raw, "levelvar": levelvar, "user": user, "versions": version_num, "wave": wave, "zoned": zoned})

      # print
      print('The task of category_raw =', category_raw, ', category_name =', category_name, ', zoned =', zoned, ', version_num =', version_num, 'and levelvar =',levelvar, ' has been completed.\n')

# COMMAND ----------

if is_to_run_driver_code == True:
  run_driver_code(bu_code, business_unit, category_raws, category_names, levelvars, user, version_nums, wave, zoned_labels)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: Final Steps

# COMMAND ----------

if is_to_run_final_steps == True:
  # Generate the users and categories
  users = [user for i in range(len(category_names))]
  
#   # If you want to run the final steps for other users and categories, please manually define it here
#   users = ['tushar', 'noah']
#   category_names = ['004_BEER', '005_WINE']
  
  # Save users and categories in a data frame
  users_and_categories_df = pd.DataFrame({'user':users, 'category_name':category_names})
  spark.createDataFrame(users_and_categories_df).write.mode('overwrite').saveAsTable("localized_pricing."+db_tbl_prefix+"users_and_categories_df")
  display(users_and_categories_df)

# COMMAND ----------

if is_to_run_final_steps == True:
  # Run the notebook
  dbutils.notebook.run("../helper/03a_Elasticity_final_steps_v4", 0, {"bu_code": bu_code, "business_unit": business_unit, "wave": wave})

# COMMAND ----------

# MAGIC %md # Appendix: Parameter Options

# COMMAND ----------

# # Potential business_unit and bu_code
# BU_abbr = [["1400 - Florida Division", 'FL'], ["1600 - Coastal Carolina Division" ,'CC'], ["1700 - Southeast Division", "SE"], ["1800 - Rocky Mountain Division", 'RM'], ["1900 - Gulf Coast Division", "GC"], ["2600 - West Coast Division", "WC"], ["2800 - Texas Division", 'TX'], ["2900 - South Atlantic Division", "SA"], ["3100 - Grand Canyon Division", "GR"], ["3800 - Northern Tier Division", "NT"], ["4100 - Midwest Division", 'MW'], ["4200 - Great Lakes Division", "GL"], ["4300 - Heartland Division", 'HLD'], ["QUEBEC OUEST", 'QW'], ["QUEBEC EST - ATLANTIQUE", 'QE'], ["Central Division", 'CE'], ["Western Division", 'WD'], ["Sweden", 'SW'], ["Ireland" ,'IE'], ['Denmark','DK'], ['Norway','NO']]

# # Potential category_raws and category_names
# if business_unit == 'Central Division':
#   category_raws = ['1 Litre Milk (5001)','2 Litre Milk (5002)','4 Litre Milk (5003)','Bagged&prepacked (9001)','Batteries (9202)','Bread (9301)','Breakfast (8504)','Breakfst/cereal (1041)','Butter/marg (1012)','Car Care (1123)','Chc/non Chc Brs (9004)','Coffee/tea (1040)','Cold Disp Bevs (8201)','Condiments (1042)','Cottage/crm/sur (1013)','Cigars Tob (6502)','Crmr Prdct (5004)','Discount/vfm 20s (6002)','Dish Care (1102)','Dsd Vendor (8502)','Eggs (1011)','Energy Drinks (4501)','Foodsrvce Other (8001)','Frz Disp Bevs (8301)','Greeting Cards (9205)','Grnla/frt Snck (2402)','Gum/mint/rlls (9005)','Hba (1110)','Hot Disp Bevs (8101)','Hot Dogs (8401)','Ice (1050)','Iced Tea (4502)','Juice (4503)','Laundry Care (1101)','Lunch Packs (1003)','Meat Snacks (2401)','Nov/season Cndy (9006)','Othr Edible Gro (1043)','Other Additives (1124)','Other Froz Fds (1022)','Other Pckgd Mts (1002)','Other Ref Bevs (4505)','Packaged Cheese (1010)','Pap/plstc/foil (1104)','PENNY CANDY (2101)','Pet Care (1105)','Pizza (1021)','Premium Cigarettes 20s (6001)','Propane (9214)','Salt Sncks Sngs (1501)','Salt Sncks Thom (1502)','Sing Srv Fld Mk (5006)','Sing Srv I/crm (5501)','Sing Srv S Drks (4001)','Sing Srv Water (3501)','Smkls Tob (6501)','Smoking Access (9208)','Sports Drinks (4506)','S Srv Pk Sw Gds (2001)','Subgener/budget 20s (6003)','Take Hm Sft Drk (4003)','Take Home I/crm (5502)','Telecom Access (9201)','Tk Home Water (3502)','Tob Access (6504)']

#   category_names = ['1_Litre_Milk_5001','2_Litre_Milk_5002','4_Litre_Milk_5003','Bagged_prepacked_9001','Batteries_9202','Bread_9301','Breakfast_8504','Breakfst_cereal_1041','Butter_marg_1012','Car_Care_1123','Chc_non_Chc_Brs_9004','Coffee_tea_1040','Cold_Disp_Bevs_8201','Condiments_1042','Cottage_crm_sur_1013','Cigars_Tob_6502','Crmr_Prdct_5004','Discount_vfm_20s_6002','Dish_Care_1102','Dsd_Vendor_8502','Eggs_1011','Energy_Drinks_4501','Foodsrvce_Other_8001','Frz_Disp_Bevs_8301','Greeting_Cards_9205','Grnla_frt_Snck_2402','Gum_mint_rlls_9005','Hba_1110','Hot_Disp_Bevs_8101','Hot_Dogs_8401','Ice_1050','Iced_Tea_4502','Juice_4503','Laundry_Care_1101','Lunch_Packs_1003','Meat_Snacks_2401','Nov_season_Cndy_9006','Othr_Edible_Gro_1043','Other_Additives_1124','Other_Froz_Fds_1022','Other_Pckgd_Mts_1002','Other_Ref_Bevs_4505','Packaged_Cheese_1010','Pap_plstc_foil_1104','PENNY_CANDY_2101','Pet_Care_1105','Pizza_1021','Premium_Cigarettes_20s_6001','Propane_9214','Salt_Sncks_Sngs_1501','Salt_Sncks_Thom_1502','Sing_Srv_Fld_Mk_5006','Sing_Srv_I_crm_5501','Sing_Srv_S_Drks_4001','Sing_rv_Water_3501','Smkls_Tob_6501','Smoking_Access_9208','Sports_Drinks_4506','S_Srv_Pk_Sw_Gds_2001','Subgener_budget_20s_6003','Take_Hm_Sft_Drk_4003','Take_Home_I_crm_5502','Telecom_Access_9201','Tk_Home_Water_3502','Tob_Access_6504']
  
# elif business_unit == 'QUEBEC OUEST':
#   category_raws = ['AUTR PROD LAITIER/CHARCUTERIE','AUTRES BREUVAGES-C','AUTRES GRIGNOTISES-C','AUTRES PRODUITS DU TABAC','BIERE-C','BOISSONS GAZEUSES-C','BREUVAGES CHAUDS-C','BREUVAGES FROIDS-C','BREUVAGES GLACES-C','CONFISERIE-C','EPICERIE COMESTIBLE','EPICERIE NON-COMESTIBLE','GLACE','GRIGNOTISES SALEES-C','GRIGNOTISES SUCREES-C','NON-ALIMENTAIRE-C','PAIN','PRODUITS AUTOMOBILES','PRODUITS LAITIERS-C','SANTE ET BEAUTE-C','VIN']

#   category_names = ['AUTR_PROD_LAITIER_CHARCUTERIE','AUTRES_BREUVAGES_C','AUTRES_GRIGNOTISES_C','AUTRES_PRODUITS_DU_TABAC','BIERE_C','BOISSONS_GAZEUSES_C','BREUVAGES_CHAUDS_C','BREUVAGES_FROIDS_C','BREUVAGES_GLACES_C','CONFISERIE_C','EPICERIE_COMESTIBLE','EPICERIE_NON_COMESTIBLE','GLACE','GRIGNOTISES_SALEES_C','GRIGNOTISES_SUCREES_C','NON_ALIMENTAIRE_C','PAIN','PRODUITS_AUTOMOBILES','PRODUITS_LAITIERS_C','SANTE_ET_BEAUTE_C','VIN']
  
# elif business_unit == 'Ireland':
#   category_raws = ['Adblue (Packed)','Bake-Off','Beer/Cider','Bottled Gas','Candy','Car Accessories','Chocolate','Cigarettes','Cold Drinks','Dairy','Dry Goods','Food Line Other','Fresh Foods','Fruits And Vegetables','Hot Coffee','Hot Dog','Ice-Cream','Lubes','Non-Alcoholic Drinks','Nonfood','Other Hot Drinks','Pharmacy','Salads','Sandwiches','Seasonal And Home Leisure','Snacking','Snacks','Soups','Strong Alcohol','Tobacco And Others','Traffic Products','Wine']

#   category_names = ['Adblue_Packed','Bake_Off','Beer_Cider','Bottled_Gas','Candy','Car_Accessories','Chocolate','Cigarettes','Cold_Drinks','Dairy','Dry_Goods','Food_Line_Other','Fresh_Foods','Fruits_And_Vegetables','Hot_Coffee','Hot_Dog','Ice_Cream','Lubes','Non_Alcoholic_Drinks','Nonfood','Other_Hot_Drinks','Pharmacy','Salads','Sandwiches','Seasonal_And_Home_Leisure','Snacking','Snacks','Soups','Strong_Alcohol','Tobacco_And_Others','Traffic_Products','Wine']

# elif business_unit == 'Sweden':
#   category_raws = ['Beer/Cider','Candy','Car Accessories','Chocolate','Dairy','Dry Goods','Fresh Foods','Frozen Foods','Lubes','Non-Alcoholic Drinks','Nonfood','Pharmacy','Salads','Sandwiches','Snacks','Traffic Products'] #'Ice-Cream'

#   category_names = ['Beer_Cider','Candy','Car_Accessories','Chocolate','Dairy','Dry_Goods','Fresh_Foods','Frozen_Foods','Lubes','Non_Alcoholic_Drinks','Nonfood','Pharmacy','Salads','Sandwiches','Snacks','Traffic_Products'] #'Ice-Cream'
  
# else:
#   category_raws = ['002-CIGARETTES','003-OTHER TOBACCO PRODUCTS','004-BEER','005-WINE','006-LIQUOR','007-PACKAGED BEVERAGES','008-CANDY','009-FLUID MILK','010-OTHER DAIRY & DELI PRODUCT','012-PCKGD ICE CREAM/NOVELTIES','013-FROZEN FOODS','014-PACKAGED BREAD','015-SALTY SNACKS','016-PACKAGED SWEET SNACKS','017-ALTERNATIVE SNACKS','019-EDIBLE GROCERY','020-NON-EDIBLE GROCERY','021-HEALTH & BEAUTY CARE','022-GENERAL MERCHANDISE','024-AUTOMOTIVE PRODUCTS','028-ICE','030-HOT DISPENSED BEVERAGES','031-COLD DISPENSED BEVERAGES','032-FROZEN DISPENSED BEVERAGES','085-SOFT DRINKS','089-FS PREP-ON-SITE OTHER','091-FS ROLLERGRILL','092-FS OTHER','094-BAKED GOODS','095-SANDWICHES','503-SBT PROPANE','504-SBT GENERAL MERCH','507-SBT HBA']

#   category_names = ['002_CIGARETTES', '003_OTHER_TOBACCO_PRODUCTS', '004_BEER', '005_WINE', '006_LIQUOR', '007_PACKAGED_BEVERAGES', '008_CANDY', '009_FLUID_MILK', '010_OTHER_DAIRY_DELI_PRODUCT', '012_PCKGD_ICE_CREAM_NOVELTIES', '013_FROZEN_FOODS', '014_PACKAGED_BREAD', '015_SALTY_SNACKS', '016_PACKAGED_SWEET_SNACKS', '017_ALTERNATIVE_SNACKS', '019_EDIBLE_GROCERY', '020_NON_EDIBLE_GROCERY', '021_HEALTH_BEAUTY_CARE', '022_GENERAL_MERCHANDISE', '024_AUTOMOTIVE_PRODUCTS', '028_ICE', '030_HOT_DISPENSED_BEVERAGES', '031_COLD_DISPENSED_BEVERAGES', '032_FROZEN_DISPENSED_BEVERAGES', '085_SOFT_DRINKS', '089_FS_PREP_ON_SITE_OTHER', '091_FS_ROLLERGRILL', '092_FS_OTHER', '094_BAKED_GOODS', '095_SANDWICHES', '503_SBT_PROPANE', '504_SBT_GENERAL_MERCH', '507_SBT_HBA']
