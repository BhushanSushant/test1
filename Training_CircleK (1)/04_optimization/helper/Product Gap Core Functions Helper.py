# Databricks notebook source
!pip unintsall xlrd
!pip install xlrd==1.2.0
!pip install xlsxwriter
#!pip install pandas --upgrade
!pip3 install pyxlsb

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
from dateutil.relativedelta import relativedelta
from datetime import *
import datetime, pandas as pd, xlrd, xlsxwriter,pyxlsb
from pyxlsb import open_workbook
import re
import math
import statistics

# COMMAND ----------

class Setup:
  
  def __init__(self,bu_abrev,refresh):
    self.bu = bu_abrev
    self.refresh = refresh
    
  
  def readFiles(self):
    #Format of file will be changed - Make sure to change this once we have the final format
    
    prod_gap = pd.read_excel("/dbfs/Phase3_extensions/Optimization/"+self.refresh+"/inputs/"+self.bu.lower()+"/"+self.bu.upper()+"_Optimization_Rules_Input_Template_Final.xlsx", sheet_name="Product Gap Rules", skiprows=7,usecols="B:H")
    
    pf = pd.read_excel("/dbfs/Phase3_extensions/Optimization/"+self.refresh+"/inputs/"+self.bu.lower()+"/"+self.bu.upper()+"_Optimization_Rules_Input_Template_Final.xlsx", sheet_name="Price Family", skiprows=4,usecols="B:Q")
    
    pf.rename({'Item Name':'Article_Name'},inplace=True,axis=1)
    
    return [prod_gap,pf]
    
    
  def createBasicFormat(self):
    
    prod_gap = self.readFiles()[0]
    pf = self.readFiles()[1]
    
    def GetDepCategory(dep):
      #dep = dep.strip()
      if pf[(pf['Price Family']==dep) | (pf['Article_Name']==dep)].shape[0] > 0:
        dep_cat = pf[(pf['Price Family']==dep) | (pf['Article_Name']==dep)]['Category Name'].iloc[0]
        return dep_cat
      elif pf[(pf['Price Family']==dep.strip()) | (pf['Article_Name']==dep.strip())].shape[0] > 0:
        dep_cat = pf[(pf['Price Family']==dep.strip()) | (pf['Article_Name']==dep.strip())]['Category Name'].iloc[0]
        return dep_cat
      else:
        print("Dependent PF not found, please check: ",dep)
  
  
    def GetIndCategory(ind):
      #ind = ind.strip()
      if pf[(pf['Price Family']==ind) | (pf['Article_Name']==ind)].shape[0] > 0:
        ind_cat = pf[(pf['Price Family']==ind) | (pf['Article_Name']==ind)]['Category Name'].iloc[0]
        return ind_cat
      elif pf[(pf['Price Family']==ind.strip()) | (pf['Article_Name']==ind.strip())].shape[0] > 0:
        ind_cat = pf[(pf['Price Family']==ind.strip()) | (pf['Article_Name']==ind.strip())]['Category Name'].iloc[0]
        return ind_cat
      else:
        print("Independent PF not found, please check: ",ind)
    
    
    prod_gap['Dependent Category'] = prod_gap['Dependent Name'].apply(GetDepCategory)
    prod_gap['Independent Category'] = prod_gap['Independent Name'].apply(GetIndCategory)
    
    if prod_gap.isna().sum()[7]!=0:
      raise ValueError("Dependent Category is missing, check Price Family Sheet")
    if prod_gap.isna().sum()[8]!=0:
      raise ValueError("Independent Category is missing, check Price Family Sheet")
    
    
    prod_gap_v1 = prod_gap.copy()
    
    prod_gap = pd.DataFrame()
    for i in prod_gap_v1.values:
      hash_map = {}
      hash_map['Category.Name.Dep'] = i[7]
      #hash_map['Category.Name.Ind'] = i[8]

      rel_type = i[1].strip()

      if rel_type == 'Family to Family':
        hash_map['Category.Dep'] = 'PF'
        hash_map['Category.Indep'] = 'PF'
      elif rel_type == 'Item to Item':
        hash_map['Category.Dep'] = 'Item'
        hash_map['Category.Indep'] = 'Item'
      elif rel_type == 'Family to Item':
        hash_map['Category.Dep'] = 'PF'
        hash_map['Category.Indep'] = 'Item'
      elif rel_type == 'Item to Family':
        hash_map['Category.Dep'] = 'Item'
        hash_map['Category.Indep'] = 'PF'

      hash_map['Dependent'] = i[5]
      hash_map['Independent'] = i[3]
      hash_map['Initial.Rule'] = i[6]

      prod_gap = prod_gap.append(hash_map,ignore_index=True)

    #prod_gap = prod_gap[['Category.Name.Dep','Category.Name.Ind','Category.Dep','Category.Indep','Dependent','Independent','Initial.Rule']]
    prod_gap = prod_gap[['Category.Name.Dep','Category.Dep','Category.Indep','Dependent','Independent','Initial.Rule']]
    
    prod_gap['Initial.Rule'] = prod_gap['Initial.Rule'].str.strip()
    
    return [prod_gap,pf,prod_gap_v1]
  
  
  

# COMMAND ----------


lst_10_lower = ['At minimum 10% lower retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% more penny profit than competing NB item']

lst_10_higher = ['At minimum 10% higher retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% less penny profit than competing NB item']

lst_expensive = ['More expensive']

lst_LessExpensive = ['Less expensive']

lst_HMargin = ['higher margin%']

lst_LMargin = ['lower margin%']

lst_moreExpensive_cheaper = ['More expensive, cheaper per liter','More expensive, cheaper per ML','More expensive, cheaper per ounce','More expensive, cheaper per oz','More expensive, cheaper per unit','More expensive, cheaper per lb']

lst_lessExpensive_expensive = ['Less expensive, expensive per liter','Less expensive, expensive per ML','Less expensive, expensive per ounce','Less expensive, expensive per oz','Less expensive, expensive per unit','Less expensive, expensive per lb']

lst_higher_margin = ['Less expensive, higher margin%, expensive per unit']

lst_lower_margin = ['More expensive, lower margin%, cheaper per unit']

lst_higher_priceMargin = ['More expensive, higher margin%']

lst_lower_priceMargin = ['Less expensive, lower margin%']

cig_multipacks = ['Single packs should have lower total retail and higher per-unit retail than multipacks']

lst_rule_no_weight = lst_10_lower + lst_10_higher + ['Same retail','within $0.10']


# COMMAND ----------

class Swapping:
  
  def __init__(self,prod_gap):
    self.prod_gap = prod_gap
    
  
  def InitiateSwapping(self):
    #Swap dependent and independent if more than one independent for a given dependent
#     r = SetRules()
    prod_gap_new = pd.DataFrame()
    lst_columns = prod_gap.columns

    for i in prod_gap['Dependent'].unique():

      data = prod_gap[prod_gap['Dependent'] == i]

      #No swapping of dependent, independent and rule if one dependent has only one independent
      if data.shape[0] == 1:
        count = 0
        hash_map = {}
        for j in data.values:
          hash_map = {}
          for count in range(0,len(j)):
            hash_map[lst_columns[count]] = j[count]
        prod_gap_new = prod_gap_new.append(hash_map,ignore_index=True)
      #Swap if more than one independent    
      elif data.shape[0] > 1:
        for k in data.values:
          hash_map = {}
          for count in range(0,len(k)):
            hash_map[lst_columns[count]] = k[count]

          #Swapping of dependent and independent using a temporary variable
          temp = hash_map['Dependent']
          hash_map['Dependent'] = hash_map['Independent']
          hash_map['Independent'] = temp

          #Swapping of rule by referencing the above declared lists
          if str(hash_map['Initial.Rule']) in lst_moreExpensive_cheaper:
            index = lst_moreExpensive_cheaper.index(hash_map['Initial.Rule'])
            hash_map['Initial.Rule'] = lst_lessExpensive_expensive[index]

          elif str(hash_map['Initial.Rule']) in lst_lessExpensive_expensive:
            index =  lst_lessExpensive_expensive.index(hash_map['Initial.Rule'])
            hash_map['Initial.Rule'] = lst_moreExpensive_cheaper[index]

          elif str(hash_map['Initial.Rule']) in lst_higher_margin:
            hash_map['Initial.Rule'] = lst_lower_margin[0]

          elif str(hash_map['Initial.Rule']) in lst_lower_margin:
            hash_map['Initial.Rule'] = lst_higher_margin[0]

          elif str(hash_map['Initial.Rule']) in lst_10_lower:
            hash_map['Initial.Rule'] = lst_10_higher[0]

          elif str(hash_map['Initial.Rule']) in lst_expensive:
            hash_map['Initial.Rule'] = lst_LessExpensive[0]

          elif str(hash_map['Initial.Rule']) in lst_higher_priceMargin:
            hash_map['Initial.Rule'] = lst_lower_priceMargin[0]

          elif str(hash_map['Initial.Rule']) in lst_lower_priceMargin:
            hash_map['Initial.Rule'] = lst_higher_priceMargin[0]

          elif str(hash_map['Initial.Rule']) in lst_HMargin:
            hash_map['Initial.Rule'] = lst_LMargin[0]

          elif str(hash_map['Initial.Rule']) in lst_LMargin:
            hash_map['Initial.Rule'] = lst_HMargin[0]

          elif "Dependent family must be" in str(hash_map['Initial.Rule']):
            flip_rule = str(hash_map['Initial.Rule'])
            if 'lower' in flip_rule.lower():
              rule_lst = flip_rule.split()
              rule_lst[-1] = 'Higher'
              hash_map['Initial.Rule'] = " ".join(rule_lst)
            elif 'higher' in flip_rule.lower():
              rule_lst = flip_rule.split()
              rule_lst[-1] = 'Lower'
              hash_map['Initial.Rule'] = " ".join(rule_lst)
            else:
              print("No higher/lower in the price rule")


          prod_gap_new = prod_gap_new.append(hash_map,ignore_index=True)
          prod_gap_new = prod_gap_new[[i for i in lst_columns]]
          
    return prod_gap_new

# COMMAND ----------

# Setting up the rules
#Placeholders to assign the rules

no_of_split = 0
rule_classification = []
side = []
types = []
min_max = []
weight_split = 0


def set_gap_rules(rule):
  
  global no_of_split, rule_classification, side, types, min_max, weight_split
  
  if rule in lst_moreExpensive_cheaper:
    no_of_split = 2
    rule_classification = ['Price','Value']
    side = ['Higher','Higher']
    types = ['Percentage','Percentage']
    min_max = ['0.00001','9999']
    
  elif rule in lst_lessExpensive_expensive:
    no_of_split = 2
    rule_classification = ['Price','Value']
    side = ['Lower','Lower']
    types = ['Percentage','Percentage']
    min_max = ['0.00001','9999']
    
  elif rule in lst_lower_margin:
    no_of_split = 3
    rule_classification = ['Margin Percentage','Price','Value']
    side = ['Lower','Higher','Higher']
    types = ['Percentage','Percentage','Percentage']
    min_max = ['0.00001','9999']
    
  elif rule in lst_higher_margin:
    no_of_split = 3
    rule_classification = ['Margin Percentage','Price','Value']
    side = ['Higher','Lower','Lower']
    types = ['Percentage','Percentage','Percentage']
    min_max = ['0.00001','9999']
    
  elif rule in lst_10_lower:
    no_of_split = 2
    rule_classification = ['Price','Margin']
    side = ['Lower','Higher']
    types = ['Percentage','Percentage']
    min_max = ['0.1','9999']
    
  elif rule in lst_10_higher:
    no_of_split = 2
    rule_classification = ['Price','Margin']
    side = ['Higher','Lower']
    types = ['Percentage','Percentage']
    min_max = ['0.1','9999']
    
  elif rule == 'Same retail':
    no_of_split = 1
    rule_classification = ['Price']
    side = ['Same']
    types = ['Percentage']
    min_max = ['0','0']
    
  elif "within $0.10" in rule:
    no_of_split = 1
    rule_classification = ['Price']
    side = ['Within']
    types = ['Absolute']
    min_max = ['0','0.1']
    
#   elif rule in item_cheaper_than_dep:
#     no_of_split = 1
#     rule_classification = ['Price']
#     side = ['Lower']
#     types = ['Percentage']
#     min_max = ['0.00001','9999']
    
#   elif rule in item_never_cheaper_than_dep:
#     no_of_split = 1
#     rule_classification = ['Price']
#     side = ['Lower']
#     types = ['Percentage']
#     min_max = ['0','9999']
  
  elif rule in lst_expensive:
    no_of_split = 1
    rule_classification = ['Price']
    side = ['Higher']
    types = ['Percentage']
    min_max = ['0','9999']
    
  elif rule in lst_LessExpensive:
    no_of_split = 1
    rule_classification = ['Price']
    side = ['Lower']
    types = ['Percentage']
    min_max = ['0','9999']
  
  elif rule in lst_higher_priceMargin:
    no_of_split = 2
    rule_classification = ['Price','Margin Percentage']
    side = ['Higher','Higher']
    types = ['Percentage','Percentage']
    min_max = ['0.00001','9999']

  elif rule in lst_lower_priceMargin:
    no_of_split = 2
    rule_classification = ['Price','Margin Percentage']
    side = ['Lower','Lower']
    types = ['Percentage','Percentage']
    min_max = ['0.00001','9999']
   
  #Dependent family must be minimum/maximum/exact $0.10 Lower
  
  elif rule in lst_HMargin:
    no_of_split = 1
    rule_classification = ['Margin Percentage']
    side = ['Higher']
    types = ['Percentage']
    min_max = ['0.00001','9999']
    
  elif rule in lst_LMargin:
    no_of_split = 1
    rule_classification = ['Margin Percentage']
    side = ['Lower']
    types = ['Percentage']
    min_max = ['0.00001','9999']
  
  
  elif "Dependent family must be" in rule:
    if len(rule.split(',') == 1):
      no_of_split = 1
      rule_classification = ['Price']

      rule_lst = rule.split()
      if 'lower' in rule.lower():
        side = ['Lower']
      elif 'higher' in rule.lower():
        side = ['Higher']

      if rule_lst[-2][-1] == '%':
        types = ['Percentage']
        value = rule_lst[-2][:-1]
        value = float(value)/100
      else:
        types = ['Absolute']
        value = rule_lst[-2][1:]


      if "exact" in rule:
        min_max = [value,value]
      elif "minimum" in rule:
        min_max = [value,'9999']
      elif "maximum" in rule:
        min_max = ['0.00001',value]



# COMMAND ----------

#unit conversion

def getConversion(unit):
  unit = unit.strip()
  if unit.lower() == 'oz':
    return 29.5735
  elif unit.lower() == 'ltr' or 'l' or 'lt':
    return 1000

def convertMetric(unit,value):
  if unit.lower() != 'ml':
    metric_value = getConversion(unit)
    val = metric_value * value
    return int(val)
  else:
    return value  
  

  
def pk_oz_z_ml_ONLY(item):   
  match2 = re.search('[+-]?([0-9]*[.])?[0-9]+((?:z| z))',item.lower())
  match1 = re.search('[+-]?([0-9]*[.])?[0-9]+((?:pk|oz|ml| pk| oz| ml))',item.lower())
  if match2:
    return float(item[match2.span()[0]:match2.span()[1]][:-1])
  elif match1:
    return float(item[match1.span()[0]:match1.span()[1]][:-2])
  else:
    return 0
  
  
def ltr_ONLY(item):
  match1 = re.search('[+-]?([0-9]*[.])?[0-9]+((?:ltr| ltr))',item.lower())
  match2 = re.search('[+-]?([0-9]*[.])?[0-9]+((?:lt| lt))',item.lower())
  match3 = re.search('[+-]?([0-9]*[.])?[0-9]+((?:l| l))',item.lower())
  if match3:
    return float(item[match3.span()[0]:match3.span()[1]][:-1])
  elif match2:
    return float(item[match2.span()[0]:match2.span()[1]][:-2])
  elif match1:
    return float(item[match1.span()[0]:match1.span()[1]][:-3])
  else:
    return 0
    
def pk_ONLY(item):
  match = re.search('[+-]?([0-9]*[.])?[0-9]+((?:pk))',item.lower())
  if match:
    return float(item[match.span()[0]:match.span()[1]][:-2])
  else:
    return 0
  
def oz_ONLY(item):
  match1 = re.search('[+-]?([0-9]*[.])?[0-9]+((?:oz))',item.lower())
  match2 = re.search('[+-]?([0-9]*[.])?[0-9]+((?:z))',item.lower())
  if match2:
    return float(item[match2.span()[0]:match2.span()[1]][:-1])
  elif match1:
    return float(item[match1.span()[0]:match1.span()[1]][:-2])
  else:
    return 0

def ml_ONLY(item):
    match = re.search('[+-]?([0-9]*[.])?[0-9]+((?:ml| ml))',item.lower())
    if match:
      return float(item[match.span()[0]:match.span()[1]][:-2])
    else:
      return 0

  

# COMMAND ----------

#extract weight from dep and ind and convert unit to the same scale

def getFromMultipleWeight(dependent_lst,independent_lst):

#   define variables used repeatedly first
  dep_lst = str(dependent_lst[0]).lower()
  ind_lst = str(independent_lst[0]).lower()

  match_dep = pk_ONLY(dep_lst)
  match_ind = pk_ONLY(ind_lst)
  
  #Handling 1PK scenario, when there's no pk in item description, mark item weight as 1
  if match_dep and not match_ind and len(dependent_lst) == 1 and len(independent_lst) == 1:
    dep_wt = match_dep
    ind_wt = 1
    return([str(dep_wt),str(ind_wt)])
  elif not match_dep and match_ind and len(dependent_lst) == 1 and len(independent_lst) == 1:
    dep_wt = 1
    ind_wt = match_ind
    return([str(dep_wt),str(ind_wt)])
  
# when there are pk/z/oz in dependent/independent item description, following script will extract both pk and o/oz, when the list contains multiple items, it will use the average pk and o/oz  
  if (pk_ONLY(dep_lst) or oz_ONLY(dep_lst)) and (pk_ONLY(ind_lst) or oz_ONLY(ind_lst)):

    #PK and OZ combination (No conversion seen yet)
    if len(dependent_lst) == 1:
      match = pk_ONLY(dep_lst)
      if match:
        dependent_pk = float(pk_ONLY(dep_lst))

      match = oz_ONLY(dep_lst)
      if match:
        dependent_oz = float(oz_ONLY(dep_lst))
        
    elif len(dependent_lst) > 1:
      dependent_pk = 0
      for i in range(len(dependent_lst)):
        dependent_pk += float(np.where(pk_ONLY(dependent_lst[i]) == 0, 1,pk_ONLY(dependent_lst[i])))
#         dependent_pk += float(pk_ONLY(dependent_lst[i]))
      dependent_pk = dependent_pk/len(dependent_lst)
      
      dependent_oz = 0
      for i in range(len(dependent_lst)):
        dependent_oz += float(oz_ONLY(dependent_lst[i]))
      dependent_oz = dependent_oz/len(dependent_lst)
    
    
    if len(independent_lst) == 1:
      match = pk_ONLY(ind_lst)
      if match:
        independent_pk = float(pk_ONLY(ind_lst))

      match = oz_ONLY(ind_lst)
      if match:
        independent_oz = float(oz_ONLY(ind_lst))
        
    elif len(independent_lst) > 1:
      independent_pk = 0
      for i in range(len(independent_lst)):
        independent_pk += float(np.where(pk_ONLY(independent_lst[i]) == 0, 1,pk_ONLY(independent_lst[i])))
#         independent_pk += float(pk_ONLY(independent_lst[i]))
      independent_pk = independent_pk/len(independent_lst)
      
      independent_oz = 0
      for i in range(len(independent_lst)):
        independent_oz += float(oz_ONLY(independent_lst[i]))
      independent_oz = independent_oz/len(independent_lst)
# when there are both pk and oz in item description, will pick whichever the value is different, if both are different, then pick based on dependent item, whichever unit has the highest value    
    flag_dep_ind = ' '
          
    if dependent_pk == independent_pk:
      flag_dep_ind = 'pk'
    elif dependent_oz == independent_oz:
      flag_dep_ind = 'oz'
    elif (dependent_pk == independent_pk) and (dependent_oz == independent_oz):
      flag_dep_ind = 'largest'
    else:
      flag_dep_ind = 'largest'


    if flag_dep_ind == 'pk':
      return [str(dependent_oz),str(independent_oz)]
    elif flag_dep_ind == 'oz': 
      return [str(dependent_pk),str(independent_pk)]
    else:
      if dependent_oz > dependent_pk:
        return [str(dependent_oz),str(independent_oz)]
      else:
        return [str(dependent_pk),str(independent_pk)]
        
        
  
  
  
#  first we will extract dep and ind unit and values 
  dependent_wt_uc = 0
  dependent_wt = 0
  sum_dep = 0
  dep_unit = ''
  ind_unit = ''
  if oz_ONLY(dep_lst):
    dep_unit = 'oz'
    if (len(dependent_lst) == 1):
      dependent_wt_uc = oz_ONLY(dep_lst)
    elif (len(dependent_lst) > 1):
      for i in range(len(dependent_lst)):
        sum_dep += float(oz_ONLY(dependent_lst[i]))
      dependent_wt_uc = sum_dep/len(dependent_lst)
  elif ltr_ONLY(dep_lst):
    dep_unit = 'ltr'
    if (len(dependent_lst) == 1):
      dependent_wt_uc = ltr_ONLY(dep_lst)
    elif (len(dependent_lst) > 1):
      for i in range(0,len(dependent_lst)):
        sum_dep += float(ltr_ONLY(dependent_lst[i]))
      dependent_wt_uc = sum_dep/len(dependent_lst)
  elif ml_ONLY(dep_lst):
    dep_unit = 'ml'
    if (len(dependent_lst) == 1):
      dependent_wt_uc = ml_ONLY(dep_lst)
    elif (len(dependent_lst) > 1):
      for i in range(0,len(dependent_lst)):
        sum_dep += float(ml_ONLY(dependent_lst[i]))
      dependent_wt_uc = sum_dep/len(dependent_lst)
  
  independent_wt_uc = 0
  independent_wt = 0
  sum_ind = 0
  if oz_ONLY(ind_lst):
    ind_unit = 'oz'
    if (len(independent_lst) == 1):
      independent_wt_uc = oz_ONLY(ind_lst)
    elif (len(independent_lst) > 1):
      for i in range(0,len(independent_lst)):
        sum_ind += float(oz_ONLY(independent_lst[i]))
      independent_wt_uc = sum_ind/len(independent_lst)
  elif ltr_ONLY(ind_lst):
    ind_unit = 'ltr'
    if (len(independent_lst) == 1):
      independent_wt_uc = ltr_ONLY(ind_lst)
    elif (len(independent_lst) > 1):
      for i in range(0,len(independent_lst)):
        sum_ind += float(ltr_ONLY(independent_lst[i]))
      independent_wt_uc = sum_ind/len(independent_lst)
  elif ml_ONLY(ind_lst):
    ind_unit = 'ml'
    if (len(independent_lst) == 1):
      independent_wt_uc = ml_ONLY(ind_lst)
    elif (len(independent_lst) > 1):
      for i in range(0,len(independent_lst)):
        sum_ind += float(ml_ONLY(independent_lst[i]))
      independent_wt_uc = sum_ind/len(independent_lst)

# handeling scenarios where dep unit and ind units are the same, we do not do the unit conversion, otherwise convert the unit oz and ltr both to ml

  if dep_unit == ind_unit:
    dependent_wt = dependent_wt_uc
    independent_wt = independent_wt_uc
  else:
    dependent_wt = convertMetric(dep_unit,dependent_wt_uc)
    independent_wt = convertMetric(ind_unit,independent_wt_uc)
    
  return [str(dependent_wt),str(independent_wt)]

# COMMAND ----------

class FinalOutput:
  
  def __init__(self,prod_gap):
    self.prod_gap = prod_gap
  
  def output_final_prod_gap(self):

    temp_df = pd.DataFrame()
    lst_final_cols = ['Category.Name.Dep','Category.Dep','Category.Indep','Dependent','Independent','Initial.Rule','Rule.Classification','Side','Type','Min','Max','Weight.Dep','Weight.Indep']

    for i in prod_gap.values:

      if i[5] != " ":  
        set_gap_rules(i[5])

        for j in range(0,no_of_split):
          hash_map = {}
          hash_map['Rule.Classification'] = rule_classification[j]
          hash_map['Side'] = side[j]
          hash_map['Type'] = types[j]
          hash_map['Min'] = min_max[0]
          hash_map['Max'] = min_max[1]
          if j == no_of_split-1:
            if i[5] in lst_rule_no_weight:
              hash_map['Weight.Dep'] = " "
              hash_map['Weight.Indep'] = " "
            else:

              dependent_lst = list(pf[pf['Price Family']==str(i[3]).strip()]['Article_Name'])
              independent_lst = list(pf[pf['Price Family']==str(i[4]).strip()]['Article_Name'])  
              try:
                lst_weights = getFromMultipleWeight(dependent_lst,independent_lst)

              except Exception as e:
                break


              hash_map['Weight.Dep'] = lst_weights[0]
              hash_map['Weight.Indep'] = lst_weights[1]


          hash_map['Category.Name.Dep'] = i[0]
          hash_map['Category.Dep'] = i[1]
          hash_map['Category.Indep'] = i[2]
          hash_map['Dependent'] = i[3]
          hash_map['Independent'] = i[4]
          hash_map['Initial.Rule'] = i[5]

          temp_df = temp_df.append(hash_map,ignore_index=True)
    temp_df = temp_df[[i for i in lst_final_cols]]
    
#     for relation type is item, use the product key instead of the price family name for mapping

    def GetPkDep(rel,dep):
      if rel == 'Item':
        try:
          return prod_gap_v1[prod_gap_v1['Dependent Name'] == dep]['Dependent Product Key'].iloc[0]
        except:
          try:
            return prod_gap_v1[prod_gap_v1['Independent Name'] == dep]['Independent Product Key'].iloc[0]
          except:
            print("Dependent:",rel,dep)
      else:
        return dep

    def GetPkInd(rel,ind):
      if rel == 'Item':
        try:
          return prod_gap_v1[prod_gap_v1['Independent Name'] == ind]['Independent Product Key'].iloc[0]
        except:
          try:
            return prod_gap_v1[prod_gap_v1['Dependent Name'] == ind]['Dependent Product Key'].iloc[0]
          except:
            print("Independent:",rel,ind)
      else:
        return ind
      
    temp_df['Dependent'] = temp_df.apply(lambda x:GetPkDep(x['Category.Dep'],x['Dependent']),axis=1)
    temp_df['Independent'] = temp_df.apply(lambda x:GetPkInd(x['Category.Indep'],x['Independent']),axis=1)

    return temp_df
  
