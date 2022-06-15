# Databricks notebook source
# DBTITLE 1,Import Libraries
import datetime
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import *
from datetime import *

import pandas as pd
import numpy as np
from datetime import timedelta
import datetime as dt




import pandas as pd
import datetime
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from operator import add
from functools import reduce
from pyspark.sql.functions import when, sum, avg, col, concat, lit
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import greatest
from pyspark.sql.functions import least
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType
import xlrd

# COMMAND ----------

# DBTITLE 1,Capture Initial Data
#### USER INPUTS #######
db = "phase4_extensions"
business_units = '4200 - Great Lakes Division' # change this input
BU_type = "NA" # Options are NA(North America) or EU(Europe) # change this input
user = 'jagruti.joshi@circlek.com' # change this input
phase = 'phase4 # change this input whether 'phase3' or 'phase4'

bu_codes = {"1400 - Florida Division": 'FL',
           "1600 - Coastal Carolina Division":"CC",
           "1700 - Southeast Division":"SE",
           "1800 - Rocky Mountain Division":'RM',
           "1900 - Gulf Coast Division":"GC",
           "2600 - West Coast Division":"WC",
           "2800 - Texas Division":'TX',
           "2900 - South Atlantic Division":"SA",
           "3100 - Grand Canyon Division":"GR",
           "3800 - Northern Tier Division":"NT",
           "4100 - Midwest Division":'MW',
           "4200 - Great Lakes Division":"GL",
           "4300 - Heartland Division":'HLD',
           "QUEBEC OUEST":'QW',
           "QUEBEC EST - ATLANTIQUE":'QE',
           "Central Division":'CE',
           "Western Division":'WC',
           "Norway":'NO',
           "Sweden":'SW',
           "Ireland":'IE',
           "Denmark":'DK'}

bu_names_tc_map = {"1400 - Florida Division": 'Florida',
           "1600 - Coastal Carolina Division":"Coastal Carolinas",
           "1700 - Southeast Division":"Southeast",
           "1800 - Rocky Mountain Division":'Rocky Mountain',
           "1900 - Gulf Coast Division":"Gulf Coast",
           "2600 - West Coast Division":"West Coast",
           "2800 - Texas Division":'Texas',
           "2900 - South Atlantic Division":"South Atlantic",
           "3100 - Grand Canyon Division":"Grand Canyon",
           "4100 - Midwest Division":'Midwest',
           "4200 - Great Lakes Division":"Great Lakes",
           "4300 - Heartland Division":'Heartland',
           "QUEBEC OUEST":'Quebec West',
           "QUEBEC EST - ATLANTIQUE":'Quebec East',
           "Central Division":'Central Canada',
           "Western Division":'Western Canada',
           "Norway":'Norway',
           "Sweden":'Sweden',
           "Ireland":'Ireland',
           "Denmark":'Denmark'}

bu_code = bu_codes[business_units]
bu_name_tc_map = bu_names_tc_map[business_units]

wave = 'Mock_GL_Test' #wave = "MAY2021_Refresh"
bu_daily_txn_table = "{}.{}_Daily_txn_final_SWEDEN_Nomenclature_{}".format(db,bu_code.lower(),wave) #bu_daily_txn_table = "{}.{}_txn_data_may2021_refresh".format(na_DB_txn,BU)
start_week = '2019-12-04' # the Wed before 2019-02-04
end_week = '2021-11-30'

weight_date = "2020-11-30" ## Should be EndDate - 1 year. Used for calculating missing weekly level prices using item_store_clust_weights dataframe. Weights to be taken on the basis of last One Year.
bu_repo = "{}_Repo".format(bu_code) #bu_repo = "RM_Repo"
state = ''

no_smoothen_after = '2099-01-01' #ACC - we can keep a bigger date here
bu_test_control = business_units


#  ["1400 - Florida Division", 'FL'],
#  ["1600 - Coastal Carolina Division", "CC"],
#  ["1700 - Southeast Division", "SE"],
#  ["1800 - Rocky Mountain Division", 'RM'],
#  ["1900 - Gulf Coast Division", "GC"],
#  ["2600 - West Coast Division", "WC"],
#  ["2800 - Texas Division", 'TX'],
#  ["2900 - South Atlantic Division", "SA"],
#  ["3100 - Grand Canyon Division", "GR"],
#  ["3800 - Northern Tier Division", "NT"],
#  ["4100 - Midwest Division", 'MW'],
#  ["4200 - Great Lakes Division", "GL"],
#  ["4300 - Heartland Division", 'HLD'],
#  ["QUEBEC OUEST", 'QW'],
#  ["QUEBEC EST - ATLANTIQUE", 'QE'],
#  ["Central Division", 'CE'],
#  ["Western Division", 'WC']]

# #select BU for filtering the test control file from the options below #unique to the May2021 refresh
# bu_test_control = "Southeast" #bu_test_control ="Rocky Mountain"
# # [Rocky Mountain, Gulf Coast, Quebec West, Florida, Texas]

# COMMAND ----------

if (BU_type == "NA"):{
  dbutils.notebook.run("/Repos/{}/dna-main/lp_refresh/02_data_preparation/helper/4_1a Creating Modeling RDS NA".format(user), 0, {"db": "{}".format(db),\
                                                       "BU": "{}".format(bu_code),\
                                                       "business_units": "{}".format(business_units),\
                                                       "wave": "{}".format(wave),\
                                                       "bu_daily_txn_table": "{}".format(bu_daily_txn_table),\
                                                       "start_week": "{}".format(start_week),\
                                                       "end_week": "{}".format(end_week),\
                                                       "bu_repo": "{}".format(bu_repo),\
                                                       "state": "{}".format(state),\
                                                       "weight_date": "{}".format(weight_date),
                                                       "phase": "{}".format(phase),
                                                       "bu_name_tc_map": "{}".format(bu_name_tc_map)})} 
else: {
  dbutils.notebook.run("/Repos/{}/dna-main/lp_refresh/02_data_preparation/helper/4_1b Creating Modeling RDS EU".format(user), 0, {"db": "{}".format(db),\
                                                       "BU": "{}".format(bu_code),\
                                                       "business_units": "{}".format(business_units),\
                                                       "phase": "{}".format(phase),\
                                                       "wave": "{}".format(wave),\
                                                       "bu_daily_txn_table": "{}".format(bu_daily_txn_table),\
                                                       "start_week": "{}".format(start_week),\
                                                       "end_week": "{}".format(end_week),\
                                                       "bu_repo": "{}".format(bu_repo),\
                                                       "state": "{}".format(state),\
                                                       "weight_date": "{}".format(weight_date),\
                                                       "no_smoothen_after": "{}".format(no_smoothen_after)})}

# COMMAND ----------


