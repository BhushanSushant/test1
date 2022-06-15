# Databricks notebook source
# import the necessary packages and prepare pyspark for use during this program
import pandas as pd
import numpy as np
import os
import glob
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from dateutil.relativedelta import relativedelta

sqlContext.setConf("spark.databricks.delta.optimizeWrite.enabled", "true")
sqlContext.setConf("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

import pyspark.sql.functions as sf
from pyspark.sql.functions import when, sum, avg, col,concat,lit

# COMMAND ----------

#define parameters
business_units = "4200 - Great Lakes Division"
bu_code = "GL" 
business_units = dbutils.widgets.get("business_unit")
bu_code = dbutils.widgets.get("bu_code")

# wave = "Accenture_Refresh"
wave = "Nov21_Refresh"
wave = dbutils.widgets.get("wave")

base_directory = 'Phase4_extensions/Elasticity_Modelling'

pre_wave = "/dbfs/"+base_directory+"/" 
bu = bu_code + "_Repo"

# COMMAND ----------

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
old_elast = sqlContext.sql('SELECT * from refresh_history_db.refresh_history_elasticity where business_unit = "{}" order by end_date desc'.format(bu_code))
old_elast1 = old_elast.select('pricing_cycle_name','pricing_cycle','item_category','product_key_trn_item_sys_id','cluster','elasticity_final').sort('pricing_cycle', ascending = False).dropDuplicates(['product_key_trn_item_sys_id','cluster']).withColumnRenamed('product_key_trn_item_sys_id','Sys.ID.Join').withColumnRenamed('cluster','Cluster').withColumnRenamed('elasticity_final','Old_Elasticity').select('`Sys.ID.Join`','Cluster','Old_Elasticity','item_category')


# COMMAND ----------



# COMMAND ----------

# old_elast_1 = sqlContext.sql('SELECT distinct(business_unit)from refresh_history_db.refresh_history_elasticity')
# display(old_elast_1)

# COMMAND ----------

old_elast1.toPandas().to_csv(pre_wave+wave+'/'+bu+'/Inputs_for_elasticity_imputation/old_wave_comp_elasticity.csv',  index=False)
