# Databricks notebook source
## Importing Libraries 
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import *
from datetime import *
from pyspark.sql.types import StringType,BooleanType,DateType, IntegerType, FloatType

 
import pandas as pd
import numpy as np
from datetime import timedelta
import datetime as dt

!pip uninstall xlrd
!pip install xlrd==1.2.0
!pip install xlsxwriter
#!pip install pandas --upgrade
!pip3 install pyxlsb

# COMMAND ----------

# DBTITLE 1,Input Parameters
bu_code_lower='gl'
refresh='Nov21_refresh'
zone_tbl='circlek_db.phase4_gl_qe_beer_wine_zone'
store_tbl='circlek_db.grouped_store_list'
start_date='2021-11-01' # date after which the data will be filtered for which the price trend needs to be checked
no_of_items=20 #no. of items in each category for which trend needs to be checked (picks highest revenue item first)
six_months_date='2021-06-01'

# COMMAND ----------

# DBTITLE 1,Read Data Prep Transaction Table for Skeleton
#read sweden nomenclature table
bu_daily_txn_table = "{}.{}_daily_txn_final_sweden_nomenclature_{}".format("phase3_extensions",bu_code_lower,refresh.lower()) 
trn_skeleton=sqlContext.sql("select * from {}".format(bu_daily_txn_table))
display(trn_skeleton)

# COMMAND ----------

# DBTITLE 1,Read Zone Table
#read zoning table 
zone_table=sqlContext.sql("select * from {}".format(zone_tbl)).\
           withColumnRenamed('category_name','category_desc').\
           select('BU','category_desc','tempe_product_key','trn_site_sys_id','price_zones')
display(zone_table)

# COMMAND ----------

#join above two tables
trn_skeleton_with_zones=trn_skeleton.join(zone_table, on=['BU','category_desc','tempe_product_key','trn_site_sys_id'])
display(trn_skeleton_with_zones)

# COMMAND ----------

# DBTITLE 1,Read Store Table for Test/Control and Cluster Mapping
#read store to cluster mapping
store_table=sqlContext.sql("select * from {}".format(store_tbl)).\
            withColumnRenamed('site_id','trn_site_sys_id').\
            select('business_unit','trn_site_sys_id','group','cluster').\
            filter(col('active_status')=='Active')
display(store_table)

# COMMAND ----------

#join with above table
trn_skeleton_with_zones_cluster=trn_skeleton_with_zones.join(store_table,on='trn_site_sys_id',how='left')
display(trn_skeleton_with_zones_cluster)

# COMMAND ----------

# DBTITLE 1,Add Item Revenue Data
trn_skeleton_with_zones_cluster_item=trn_skeleton_with_zones_cluster.groupBy('BU','category_desc','tempe_product_key','item_name').\
                                     agg(sum('trn_gross_amount').alias('item_revenue'))
ranked =  trn_skeleton_with_zones_cluster_item.withColumn(
  "rank", dense_rank().over(Window.partitionBy('BU','category_desc').orderBy(desc("item_revenue"))))
                               
display(ranked)

# COMMAND ----------

# DBTITLE 1,Base Skeleton with All Relevant Information
#join back this rank and item revenue with trn_skeleton
trn_skeleton_with_zones_cluster_item_ranked=trn_skeleton_with_zones_cluster.join(ranked,on=['BU','category_desc','tempe_product_key','item_name'])
display(trn_skeleton_with_zones_cluster_item_ranked)

# COMMAND ----------

# DBTITLE 1,Filter for Selected (Latest) Time Period and Top Revenue Driving Items
#filter data as per required period and top selling items in each category basis rank
trn_skeleton_with_zones_cluster_filtered=trn_skeleton_with_zones_cluster_item_ranked.filter(col('trn_transaction_date')>start_date).\
                                         filter(col('rank')<=no_of_items)
display(trn_skeleton_with_zones_cluster_filtered)

# COMMAND ----------

# DBTITLE 1,Summary- Get Distinct Price Pts, Max/Min price for each Item*Date in every Cluster*Zone
#create summaries
summary1=trn_skeleton_with_zones_cluster_filtered.groupBy('BU','category_desc','tempe_product_key','item_name','rank','group','cluster','price_zones','trn_transaction_date').\
                                         agg(countDistinct('avg_trn_item_unit_price').alias('distinct_price_pts_in_cluster_zone'),max('avg_trn_item_unit_price').alias('max_price'),min('avg_trn_item_unit_price').alias('min_price')).\
filter(col('group')=='Test')
display(summary1)

# COMMAND ----------

# DBTITLE 1,Summary- Get number of stores at each price pt. in a Cluster*Zone for an Item*Date
#number of stores at that price pt
summary2=trn_skeleton_with_zones_cluster_filtered.groupBy('BU','category_desc','tempe_product_key','item_name','rank','group','cluster','price_zones','trn_transaction_date','avg_trn_item_unit_price').\
agg(countDistinct('trn_site_sys_id').alias('no_of_stores_at_price_pt'))
display(summary2)

# COMMAND ----------

summary3=summary1.join(summary2,on=['BU','category_desc','tempe_product_key','item_name','rank','trn_transaction_date','group','cluster','price_zones'])
display(summary3)

# COMMAND ----------

# DBTITLE 1,% Stores at each price pt
#add % stores at a price pt.
total_no_of_stores_in_zone=trn_skeleton_with_zones_cluster_filtered.groupBy('BU','category_desc','tempe_product_key','item_name','rank','trn_transaction_date','group','cluster','price_zones').\
         agg(countDistinct('trn_site_sys_id').alias('total_no_of_stores_in_cluster_zone'))

#join this back with summary 3 to calculate % stores
summary4=summary3.join(total_no_of_stores_in_zone,on=['BU','category_desc','tempe_product_key','item_name','rank','trn_transaction_date','group','cluster','price_zones'])

#calcluate % stores
summary5=summary4.withColumn("%stores at given price pt",round(col('no_of_stores_at_price_pt')/col('total_no_of_stores_in_cluster_zone')*100,2))
display(summary5)

# COMMAND ----------

# DBTITLE 1,Final Summary with CoV Price and above data
#add cov price=std dev/mean
average_price=trn_skeleton_with_zones_cluster_filtered.groupBy('BU','category_desc','tempe_product_key','item_name','rank','group','cluster','price_zones','trn_transaction_date').\
agg(avg('avg_trn_item_unit_price').alias('avg_price_in_cluster_zone'),stddev('avg_trn_item_unit_price').alias('stddev_price_in_cluster_zone')).\
withColumn('CoV_Price',round(col('stddev_price_in_cluster_zone')/col('avg_price_in_cluster_zone')*100,2))
#display(average_price)

#join this back with summary5 to get cov
summary6=summary5.join(average_price,on=['BU','category_desc','tempe_product_key','item_name','rank','group','cluster','price_zones','trn_transaction_date']).\
         withColumn('key',concat(col('tempe_product_key'),col('cluster'),col('price_zones')))
display(summary6)

# COMMAND ----------

# DBTITLE 1,Issue Cases- No. of days with multiple price points across stores in an item*zone
date_summary=summary6.withColumn('price_pts',when(col('distinct_price_pts_in_cluster_zone')==1,'single_price_pt').otherwise('multiple_price_pts')).\
groupBy('BU','category_desc','tempe_product_key','item_name','rank','cluster','price_zones').pivot('price_pts').agg(countDistinct('trn_transaction_date'))
display(date_summary)

# COMMAND ----------

# define issue cases 
date_summary2=date_summary.na.fill(value=0,subset=["multiple_price_pts","single_price_pt"])
#calculate % issue cases based on these metrics
date_summary3=date_summary2.withColumn("Issue_Flag",when(col("single_price_pt")/(col("multiple_price_pts")+col("single_price_pt"))>=0.8,"Good").otherwise("Issue"))
display(date_summary3)

# COMMAND ----------

#get the "Good/Issue" Flag from above and join with summary6 at PF*cluster*Zone level 
flag_added=summary6.join(date_summary3,on=['BU','category_desc','tempe_product_key','item_name','rank','cluster','price_zones'],how='left')
display(flag_added)

# COMMAND ----------

# DBTITLE 1,Identify Issue Cases 
#Issue Cases,based on no. of distinct price pts >1 and CoV_basis and price live at more than 10% stores in zone
issue_cases=flag_added.filter((col('distinct_price_pts_in_cluster_zone')>1) & (col('CoV_Price')>1)&(col("%stores at given price pt")>10) & (col("Issue_Flag")=="Issue"))
display(issue_cases)

# COMMAND ----------

#calculating distinct price points in each cluster_zone again after removing price pts that were live in less than 10% stores
issue_cases_new_distinct_price_pts=issue_cases.groupBy('BU','category_desc','tempe_product_key','item_name','rank','cluster','price_zones','trn_transaction_date','key').agg(countDistinct('avg_trn_item_unit_price').alias('new_distinct_price_pts_in_cluster_zone'))
summary7=issue_cases.join(issue_cases_new_distinct_price_pts,on=['BU','category_desc','tempe_product_key','item_name','rank','cluster','price_zones','trn_transaction_date','key'])
display(summary7)

# COMMAND ----------

#filter final issue cases
summary8=summary7.filter(col('new_distinct_price_pts_in_cluster_zone')>1).\
         withColumn('key',concat(col('tempe_product_key'),col('cluster'),col('price_zones')))

# COMMAND ----------

# DBTITLE 1,Final Summary at Category Level
Total_Item_Zones=summary6.groupBy('BU','category_desc').agg(countDistinct('key').alias('total_no_of_item_zones'))
Issue_Item_Zones=summary8.groupBy('BU','category_desc').agg(countDistinct('key').alias('issue_item_zones'))
Final_summary=Total_Item_Zones.join(Issue_Item_Zones,on=['BU','category_desc']).\
              withColumn('%Issue_Cases',round(col('issue_item_zones')/col('total_no_of_item_zones')*100,2))
display(Final_summary)

# COMMAND ----------

# DBTITLE 1,Get Item*Store*Week Level data for Issue Cases for Template Creation
#finally filter out these issue cases in the original dataset at item*store*date level and filter 6 months of data for each issue case
issue_cases_data_final=trn_skeleton_with_zones_cluster.\
                       filter(col('trn_transaction_date')>='2021-06-01').\
                       join(summary8.select('BU','category_desc','tempe_product_key','item_name','cluster','price_zones'),on=['BU','category_desc','tempe_product_key','item_name','cluster','price_zones'],how='inner')

#roll up at weekly level for template
issue_cases_data_final_for_template=issue_cases_data_final.withColumn("week_no",dayofweek("trn_transaction_date")).\
withColumn("adj_factor", col("week_no") - 4).\
withColumn("adj_factor", when(col("adj_factor") < 0, 7+col("adj_factor")).otherwise(col("adj_factor"))).\
withColumn("week_start_date", expr("date_sub(trn_transaction_date, adj_factor)")).\
withColumn('sales_x_price', col('trn_gross_amount') * col('avg_trn_item_unit_price')).\
groupBy(['category_desc','tempe_product_key','item_name','trn_site_sys_id','site_state_id','week_start_date']).\
agg(sum('trn_gross_amount').alias('rev'),sum('trn_sold_quantity').alias('qty'),sum('sales_x_price').alias('sales_x_price')).\
withColumn('reg_price',round(col('sales_x_price')/col('rev'),2))



display(issue_cases_data_final_for_template)

# COMMAND ----------

issue_cases_data_final_for_template.count()

# COMMAND ----------


