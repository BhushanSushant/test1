# Databricks notebook source
# DBTITLE 1,Install library
# !pip uninstall xlrd
!pip install xlrd==1.2.0

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages('tidyverse')
# MAGIC install.packages("openxlsx") 

# COMMAND ----------

# DBTITLE 1,Import library
import pandas as pd
import datetime
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from operator import add
from functools import reduce
from pyspark.sql.functions import when, sum, avg, col, concat, lit,countDistinct
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import greatest
from pyspark.sql.functions import least
from pyspark.ml.feature import Bucketizer
from pyspark.sql.types import IntegerType
import xlrd

# COMMAND ----------

# MAGIC %r
# MAGIC library(openxlsx)
# MAGIC library(tidyverse)

# COMMAND ----------

# DBTITLE 1,Parameters
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
           "Sweeden":'SW'}

# common parameters across all notebooks
desired_bu = '4200 - Great Lakes Division' # change this input
user = 'jagruti.joshi@circlek.com' # change this input

bu_code=bu_codes[desired_bu]
phase = 'phase4' ##Input whether 'phase3' or 'phase4'
db = 'phase4_extensions'
wave = "Mock_GL_Test"


# parameters for notebook 05
file_directory = 'Phase4_extensions/Elasticity_Modelling'
wave = 'Mock_GL_Test' #Modify for each new wave
pdi_price_notebook_name = "/Repos/"+user+"/dna-main/lp_refresh/02_data_preparation/helper/99_regular_price"

# COMMAND ----------

# DBTITLE 1,Price Imputation Helper  Notebook
dbutils.notebook.run("/Repos/{}/dna-main/lp_refresh/02_data_preparation/helper/05_na_reg_price_impute_NM_Tempe_v1".format(user), 0, {"business_units": "{}".format(desired_bu),\
                                                     "bu_code":"{}".format(bu_code),\
                                                     "db":"{}".format(db),\
                                                     "phase": "{}".format(phase),\
                                                     "wave": "{}".format(wave),\
                                                     "file_directory":"{}".format(file_directory),\
                                                     "pdi_price_notebook_name":"{}".format(pdi_price_notebook_name)})

# COMMAND ----------

# MAGIC %md
# MAGIC Reach out to Zoning team (Steven/ Feng) once the above steps are completed successfully
