# Databricks notebook source
import pandas as pd
import re
from collections import defaultdict
import numpy as np

# COMMAND ----------

class ProdGapLoader:
    def __init__(self, bu, prodgap_fpath):
        self.bu = bu
        self.prodgap_fpath = prodgap_fpath
        self._rule_mapping = {'Min 10% Lower': 'At minimum 10% lower retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% more penny profit than competing NB item',
                            'More Exp': 'More expensive',
                            'Higher Mrgn%': 'Higher margin%',
                            'Same Retail': 'Same Retail',
                            'More Exp;cheap per ML': 'More expensive;cheaper per ML',
                            'More Exp;cheap per unit': 'More expensive;cheaper per unit',
                            'More Exp;cheap per oz': 'More expensive;cheaper per oz',
                            'More Exp;cheap per liter': 'More expensive;cheaper per liter',
                            'More Exp;lower mrgn%;cheap per unit': 'More expensive;lower margin%;cheaper per unit',
                            'More Exp;higher mrgn%': 'More expensive;higher margin%',
                            'Other': 'Other'}
        self.prodgap_raw = self._readProdGap()
        self.scope = self._readScope()
        self.prodgap = self._formatProdGap()
    
    def _readProdGap(self):
        prod_gap = pd.read_excel(self.prodgap_fpath, sheet_name='Product Gap Rules', skiprows=5, usecols='A:S')
        prod_gap = prod_gap.iloc[1:, :].dropna(axis='index', how='all')
        prod_gap['Relationship/Rule'] = prod_gap['Relationship/Rule'].apply(lambda x: self._rule_mapping[x])
        
        # DW Note - Drop any 'Other' rules, those has to be handled manually
        prod_gap['Relationship/Rule'] = prod_gap['Other Rules'].where(prod_gap['Relationship/Rule'] == 'Other', prod_gap['Relationship/Rule'])
        if prod_gap.isna().sum()[2] != 0:
            raise ValueError("Dependent Category is missing, check Prod Gap Rule Sheet provided")
        if prod_gap.isna().sum()[9] != 0:
            raise ValueError("Independent Category is missing, check Prod Gap Rule Sheet provided")
            
        return prod_gap
    
    def _readScope(self):
        scope = spark.sql("SELECT * FROM circlek_db.lp_dashboard_items_scope WHERE BU = '{}'".format(self.bu)).toPandas()
        scope = scope.fillna(-1)
        scope[['item_number', 'upc', 'product_key']] = scope[['item_number', 'upc', 'product_key']].astype('int64')
        return scope
        
    def _formatProdGap(self):
        def _format(row):
            cat_name_dep = row['Dep.Category']
            initial_rule = row['Relationship/Rule']
            rule = row['Relationship Type'].strip().split()
            cat_dep, cat_ind = rule[0], rule[-1]
            if cat_dep not in {'PF', 'Item'} or cat_ind not in {'PF', 'Item'}:
                raise ValueError("Prod Gap Rule Sheet contains invalid Relationship Type")
            dep = row['Dep.PF.Name'] if cat_dep == 'PF' else row['Dep.Item.Name']
            ind = row['Ind.PF.Name'] if cat_ind == 'PF' else row['Ind.Item.Name']
            return cat_name_dep, cat_dep, cat_ind, dep, ind, initial_rule
        
        output = pd.DataFrame()
        output[['Category.Name.Dep','Category.Dep','Category.Indep','Dependent','Independent','Initial.Rule']] = self.prodgap_raw.apply(_format, axis='columns', result_type='expand')
        return output

# COMMAND ----------

class ProdGapSwapper(ProdGapLoader):
    def __init__(self, bu, prodgap_fpath):
        super().__init__(bu, prodgap_fpath)
    
    @property
    def _rule_swapper(self):
        # DW TODO: when revisit this for development, clean up the reserve rule (word choice & case) for regex matching instead of explicitly list everything out
        # build a two-way dict for rule mapping rule when swapping
        mapp = {'At minimum 10% lower retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% more penny profit than competing NB item': 'At minimum 10% higher retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% less penny profit than competing NB item',
                'More expensive': 'Less expensive',
                'Higher margin%': 'lower margin%',
                'More expensive;cheaper per liter': 'Less expensive;expensive per liter',
                'More expensive;cheaper per ML': 'Less expensive;expensive per ML',
                'More expensive;cheaper per ounce': 'Less expensive;expensive per ounce',
                'More expensive;cheaper per oz': 'Less expensive;expensive per oz',
                'More expensive;cheaper per unit': 'Less expensive;expensive per unit',
                'More expensive;cheaper per lb': 'Less expensive;expensive per lb',
                'Less expensive;higher margin%;expensive per unit': 'More expensive;lower margin%;cheaper per unit',
                'More expensive;higher margin%': 'Less expensive;lower margin%',
                'Same Retail': 'Same Retail'
        }
        mapp.update(dict(reversed(item) for item in mapp.items()))
        return mapp
    
    def _swap_rule(self, rule, mapp):
        # helper function for swapping rule, used in swap function
        new_rule = rule
        if mapp.get(rule):
            new_rule = mapp.get(rule)
        # DW TODO: inherit derectly from Kartik's legacy code, need more thought
        elif "Dependent family must be" in rule:
            rule_lower = rule.lower()
            if 'lower' in rule_lower:
                rule_lst = rule.split()
                rule_lst[-1] = 'Higher'
                new_rule = " ".join(rule_lst)
            elif 'higher' in rule_lower:
                rule_lst = rule.split()
                rule_lst[-1] = 'Lower'
                new_rule = " ".join(rule_lst)
            else:
                print("No higher/lower in the price rule")
        else:
            # add a flag to tell if non-standardized rule has been swapped
            new_rule += '-swapped'
        return new_rule
    
    def swap(self):
        prodgap_swap = []
        # init swapper dict
        mapp = self._rule_swapper
        for dep in self.prodgap['Dependent'].unique():
            data = self.prodgap[self.prodgap['Dependent'] == dep].copy()
            nrow = data.shape[0]
            if nrow > 1:
                # swap ind and dep when 1 dependent is mapped to multiple independent
                # copy is necessary here
                data['Dependent'], data['Independent'] = data['Independent'], data['Dependent'].copy()
                data['Category.Dep'], data['Category.Indep'] = data['Category.Indep'], data['Category.Dep'].copy()
                # swap rule
                swapped_rules = []
                for rule in data['Initial.Rule']:
                    new_rule = self._swap_rule(rule, mapp)
                    swapped_rules.append(new_rule)
                data['Initial.Rule'] = pd.Series(swapped_rules, index=data.index)
            prodgap_swap.append(data)
        return pd.concat(prodgap_swap).sort_index()

# COMMAND ----------

class ProdGapWeightHandler(ProdGapSwapper):
    def __init__(self, bu, prodgap_fpath):
        super().__init__(bu, prodgap_fpath)
        # this corresponds to the _rule_swapper atrribute in ProdGapSwapper class
        # DW TODO: develop a rule/text based process instead of one to one mapping
        # DW TODO: Urgent direction seems skeptical
        self._rule_parser = {
                # more exp, cheaper
                'More expensive;cheaper per liter': 
                (['Price', 'Value'], ['Higher', 'Higher'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'More expensive;cheaper per ML': 
                (['Price', 'Value'], ['Higher', 'Higher'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'More expensive;cheaper per ounce': 
                (['Price', 'Value'], ['Higher', 'Higher'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'More expensive;cheaper per oz': 
                (['Price', 'Value'], ['Higher', 'Higher'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'More expensive;cheaper per unit': 
                (['Price', 'Value'], ['Higher', 'Higher'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'More expensive;cheaper per lb': 
                (['Price', 'Value'], ['Higher', 'Higher'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                # less exp, exp
                'Less expensive;expensive per liter': 
                (['Price', 'Value'], ['Lower', 'Lower'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'Less expensive;expensive per ML': 
                (['Price', 'Value'], ['Lower', 'Lower'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'Less expensive;expensive per ounce': 
                (['Price', 'Value'], ['Lower', 'Lower'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'Less expensive;expensive per oz': 
                (['Price', 'Value'], ['Lower', 'Lower'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'Less expensive;expensive per unit': 
                (['Price', 'Value'], ['Lower', 'Lower'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                'Less expensive;expensive per lb': 
                (['Price', 'Value'], ['Lower', 'Lower'], ['Percentage', 'Percentage'], ['0.00001', '9999']),
                # more exp, margin, unit
                'More expensive;lower margin%;cheaper per unit': 
                (['Margin Percentage', 'Price', 'Value'], ['Lower', 'Higher', 'Higher'],
                 ['Percentage', 'Percentage', 'Percentage'], ['0.00001', '9999']),
            
                'Less expensive;higher margin%;expensive per unit': 
                (['Margin Percentage', 'Price', 'Value'], ['Higher', 'Lower', 'Lower'],
                 ['Percentage', 'Percentage', 'Percentage'], ['0.00001', '9999']),
                'At minimum 10% lower retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% more penny profit than competing NB item': 
                (['Price', 'Margin'], ['Lower', 'Higher'], ['Percentage', 'Percentage'], ['0.1', '9999']), 
                
                'At minimum 10% higher retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% less penny profit than competing NB item': 
                (['Price', 'Margin'], ['Higher', 'Lower'], ['Percentage', 'Percentage'], ['0.1', '9999']), 
                
                'Same Retail': (['Price'], ['Same'], ['Percentage'], ['0', '0']),
            
                'More expensive': (['Price'], ['Higher'], ['Percentage'], ['0', '9999']),
                
                'Less expensive': (['Price'], ['Lower'], ['Percentage'], ['0', '9999']),
                
                'More expensive;higher margin%': 
                (['Price', 'Margin Percentage'], ['Higher', 'Higher'], ['Percentage', 'Percentage'],
                 ['0.00001', '9999']),
                'Less expensive;lower margin%': 
                (['Price', 'Margin Percentage'], ['Lower', 'Lower'], ['Percentage','Percentage'],
                 ['0.00001', '9999']),
                
                'Higher margin%': (['Margin Percentage'], ['Higher'], ['Percentage'], ['0.00001', '9999']),
                'lower margin%': (['Margin Percentage'], ['Lower'], ['Percentage'], ['0.00001', '9999'])}
        self._no_weight_rules = {'At minimum 10% lower retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% more penny profit than competing NB item', 'At minimum 10% higher retail than the NB equivalent in the sub-category tier while delivering a minimum of 10% less penny profit than competing NB item', 'Same Retail','within $0.10'}
        self._mapp_to_gram = {'lt': 1000,
                             'oz': 29.5735,
                             'ml': 1,
                             'cl': 10,
                             'dl': 100,
                             'kg': 1000,
                             'gr': 1}
    
    def _parse_rule(self, rule):
        if self._rule_parser.get(rule):
            return self._rule_parser.get(rule)
        # DW TODO:the following part was gotten from Kartik's latest code, need more thought
        elif "within $0.10" in rule:
            rule_classification = ['Price']
            sides = ['Within']
            types = ['Absolute']
            min_max = ['0', '0.1']
        elif "Dependent family must be" in rule:
            if len(rule.split(',') == 1):
                rule_classification = ['Price']

                rule_lst = rule.split()
                if 'lower' in rule.lower():
                    sides = ['Lower']
                elif 'higher' in rule.lower():
                    sides = ['Higher']

                if rule_lst[-2][-1] == '%':
                    types = ['Percentage']
                    value = rule_lst[-2][:-1]
                    value = float(value)/100
                else:
                    types = ['Absolute']
                    value = rule_lst[-2][1:]
                    
                if "exact" in rule:
                    min_max = [value, value]
                elif "minimum" in rule:
                    min_max = [value, '9999']
                elif "maximum" in rule:
                    min_max = ['0.00001', value]
        else:
            rule_classification = ['Other']
            sides = ['']
            types = ['']
            min_max = ['', '']
        return rule_classification, sides, types, min_max
    
    def _match_pack(self, item_desc):
        match = re.search('([+-]?([0-9]*[.,])?[0-9]+)[\s]?[-]?(pk|pak|p|x|pack)', item_desc)
        if match:
            return float(match.group(1).replace(',', '.'))
        return 0
    
    # currently we're only considering oz as volumn
    def _match_volumn(self, item_desc):
        # this function return 2 values
        ######## the number part after converted to gram and
        # the original matched unit, standardized to it full form, e.g. lt to ltr
        match = re.search('([+-]?([0-9]*[.,])?[0-9]+)[\s]?[-]?(ltr|lt|l|oz|z|ml|cl|dl)', item_desc)
        if match:
            unit_raw = match.group(3)
            unit = 'lt' if unit_raw in {'ltr', 'lt', 'l'} else unit_raw
            unit = 'oz' if unit_raw in {'oz', 'z'} else unit
            num_part = match.group(1).replace(',', '.')
            return (self._to_gram(float(num_part), unit), unit)
        return 0
    
    # ideally this should only be used for EU, not taken from Kartik's code
    def _match_weight(self, item_desc):
        match = re.search('([+-]?([0-9]*[.,])?[0-9]+)[\s]?[-]?(g|gr|kg)', item_desc)
        if match:
            unit_raw = match.group(3)
            unit = 'gr' if unit_raw in {'gr', 'g'} else unit_raw
            num_part = match.group(1).replace(',', '.')
            return (self._to_gram(float(num_part), unit), unit)
        return 0
    
    # will only need this when need arises, piece is seen in european BUs
    def _match_piece(self, item_desc):
        pass
    
    # helpers for unit conversion
    def _to_gram(self, number, unit):
        if self._mapp_to_gram.get(unit):
            return self._mapp_to_gram.get(unit) * number
        else:
            raise KeyError(f'Unknown unit: {unit}')
            
    def _gram_to(self, number, unit):
        if self._mapp_to_gram.get(unit):
            return round(number / self._mapp_to_gram.get(unit), 2)
        else:
            raise KeyError(f'Unknown unit: {unit}')
            
    def _getPFWeight(self, dependents, independents):
        # elements in both lists needed to be converted to lower case 
        # DW TODO: Logic taken from Kartik's work directly, need more thoughts
        # the biggest problem of this function is that it only looks at the first item in a pf for determining
        # the underlying logic, thus, ordering of items matters, 
        # also, if the first item does not contain relevent pack/weight info, this code won't work
        
        # first item from each list
        dep_item = dependents[0]
        ind_item = independents[0]
    
        # early termination
        # if both item does not contain any number, terminate
        if sum([1 if char.isdigit() else 0 for char in dep_item + ind_item]) == 0:
            return '0', '0'
        
        # pack match will be used several times in this function
        dep_pack_match = self._match_pack(dep_item)
        ind_pack_match = self._match_pack(ind_item)
        # early termination
        # DW TODO: taken from Kartik's logic of this needed to be revisited
        # Handling 1PK scenario, when there's no pk in item description, mark item weight as 1
        if len(dependents) == len(independents) == 1:
            if not (dep_pack_match and ind_pack_match) and (dep_pack_match or ind_pack_match):
                return str(dep_pack_match) if dep_pack_match else '1', str(ind_pack_match) if ind_pack_match else '1'
        
        # DW TODO: repeated works warning, wrap in helpers when have time
        dep_vol_match = self._match_volumn(dep_item)
        ind_vol_match = self._match_volumn(ind_item)
        dep_wei_match = self._match_weight(dep_item)
        ind_wei_match = self._match_weight(ind_item)
        
        # early termination
        if not (dep_vol_match or ind_vol_match or dep_wei_match or ind_wei_match):
            return '0', '0'

        dep_pks = [self._match_pack(item) if self._match_pack(item) else 1 for item in dependents]
        ind_pks = [self._match_pack(item) if self._match_pack(item) else 1 for item in independents]
        dep_vols = [self._match_volumn(item)[0] for item in dependents if self._match_volumn(item)]
        ind_vols = [self._match_volumn(item)[0] for item in independents if self._match_volumn(item)]
        dep_weis = [self._match_weight(item)[0] for item in dependents if self._match_weight(item)]
        ind_weis = [self._match_weight(item)[0] for item in independents if self._match_weight(item)]
        
        dep_avg_pks = round(sum(dep_pks) / len(dep_pks), 2) if dep_pks else 0
        ind_avg_pks = round(sum(ind_pks) / len(ind_pks), 2) if ind_pks else 0
        dep_avg_vol = round(sum(dep_vols) / len(dep_vols), 2) if dep_vols else 0
        ind_avg_vol = round(sum(ind_vols) / len(ind_vols), 2) if ind_vols else 0
        dep_avg_wei = round(sum(dep_weis) / len(dep_weis), 2) if dep_weis else 0
        ind_avg_wei = round(sum(ind_weis) / len(ind_weis), 2) if ind_weis else 0

        # DW TODO: taken from Kartik's logic of this needed to be revisited
        # the only two changes:
        # 1. the first condition to avoid ValueError in the last few conditions
        # when there are pk and volumn presents in dependent/independent item description, 
        # 2. when both pack and volumne are presented and the values are not the same across the pf
        # return pack * volumn
        # following script will extract both pk and volumn, 
        # when there are both pk and oz in item description, 
        # will pick whichever the value is different, 
        # if both are different, then pick based on dependent item, whichever unit has the highest value    
        # when the list contains multiple items, it will use the average pk and volumn  
        # returned value will be converted back to the unit used in the first dependent item's item description
        if dep_pack_match and dep_vol_match and ind_pack_match and ind_vol_match:
            if dep_avg_pks == ind_avg_pks:
                dep_avg_vol = self._gram_to(dep_avg_vol, dep_vol_match[1])
                ind_avg_vol = self._gram_to(ind_avg_vol, dep_vol_match[1])
                return str(dep_avg_vol), str(ind_avg_vol)
            elif dep_avg_vol == ind_avg_vol:
                return str(dep_avg_pks), str(ind_avg_pks)
            else:
                dep_avg_vol = self._gram_to(dep_avg_vol, dep_vol_match[1])
                ind_avg_vol = self._gram_to(ind_avg_vol, dep_vol_match[1])
                return str(dep_avg_vol * dep_avg_pks), str(ind_avg_vol * ind_avg_pks)
#             return str(dep_avg_pks), str(ind_avg_pks)
        
        # volumn has the priority over weight
        # Kartik: handling scenarios where dep unit and ind units are the same, 
        # we do not do the unit conversion, otherwise convert the unit oz and ltr both to ml
        dep_output_unit = (dep_vol_match or dep_wei_match)[1] if (dep_vol_match or dep_wei_match) else 'gr'
        ind_output_unit = (ind_vol_match or ind_wei_match)[1] if (ind_vol_match or ind_wei_match) else 'gr'
        
        if dep_output_unit == ind_output_unit:
            dep_val = self._gram_to(dep_avg_vol or dep_avg_wei, dep_output_unit)
            ind_val = self._gram_to(ind_avg_vol or ind_avg_wei, ind_output_unit)
            return str(dep_val), str(ind_val)
        else:
            return str(dep_avg_vol or dep_avg_wei), str(ind_avg_vol or ind_avg_wei)
        
        return '0', '0'
    
    def createOutput(self, swapped_prodgap):
        def _get_pf(row):
            dependents, independents = [row['Dependent'].lower()], [row['Independent'].lower()]
            if row['Category.Dep'] == 'PF':
                dependents = [name.lower() for name in self.scope.loc[self.scope['price_family']==row['Dependent'].upper(),
                                                                      'item_name'].values]
            if row['Category.Indep'] == 'PF':
                independents = [name.lower() for name in
                                self.scope.loc[self.scope['price_family']==row['Independent'].upper(), 'item_name'].values]
            return dependents, independents
        def _get_product_id(item_name):
            data = self.scope.loc[self.scope['item_name'] == item_name, ['product_key', 'upc',
                                                                         'modelling_level']].drop_duplicates()
            if data.shape[0] > 1:
                raise ValueError(f'Duplicate item name found in scope: {item_name}')
            product_key, upc, modelling_level = data.values[0]
            if modelling_level == 'EAN':
                return str(int(upc))
            else:
                return str(int(product_key))
            
        
        outputs = defaultdict(list)
        for _, row in swapped_prodgap.iterrows():
            rule_classification, sides, types, min_max = self._parse_rule(row['Initial.Rule'])
            dep, ind = row['Dependent'], row['Independent']
            if row['Category.Dep'] == 'Item':
                dep = _get_product_id(row['Dependent'])
            if row['Category.Indep'] == 'Item':
                ind = _get_product_id(row['Independent'])
                
            for index, (classy, side, ty) in enumerate(zip(rule_classification, sides, types)):
                outputs['Category.Name.Dep'].append(row['Category.Name.Dep'])
                outputs['Category.Dep'].append(row['Category.Dep'])
                outputs['Category.Indep'].append(row['Category.Indep'])
                outputs['Dependent'].append(dep)
#                 outputs['Dependent_org'].append(row['Dependent'])
                outputs['Independent'].append(ind)
#                 outputs['Independent_org'].append(row['Independent'])
                outputs['Initial.Rule'].append(row['Initial.Rule'])
                outputs['Rule.Classification'].append(classy)
                outputs['Side'].append(side)
                outputs['Type'].append(ty)
                weight_dep, weight_ind = np.nan, np.nan
                if index == len(rule_classification) - 1 and row['Initial.Rule'] not in self._no_weight_rules:
                    dependents, independents = _get_pf(row)
                    weight_dep, weight_ind = self._getPFWeight(dependents, independents)
                outputs['Weight.Dep'].append(weight_dep)
                outputs['Weight.Indep'].append(weight_ind)
                outputs['Min'].append(min_max[0])
                outputs['Max'].append(min_max[1])
        return pd.DataFrame(outputs)
                

# COMMAND ----------

# wrapper class - this is only for naming concern
class ProdGapHandler(ProdGapWeightHandler):
    pass

# COMMAND ----------

# EU Handler
# The only difference between these two regions is that EU BU can provide us with SOX data
# SOX data is a product description data associates with the JDE system which contains accurate weight/size info
# Therefore, for each rule that requires weight/size info, we'll first try to get it from SOX prior to regex parsing
class ProdGapHandlerEU(ProdGapHandler):
    def __init__(self, bu, prodgap_fpath, sox_fpath):
        super().__init__(bu, prodgap_fpath)
        self._sox_fpath = sox_fpath
        self._sox = self._readSox()
        self.scope_w_weight = self.scope.merge(self._sox, how='left', on=['item_number', 'upc'])
        
    def _readSox(self):
        sox = pd.read_excel(self._sox_fpath)
        sox.rename(mapper=lambda colname: '_'.join(substr.lower() for substr in re.split('\.|_| ', colname)),
                   axis='columns', inplace=True)
        sox = sox[['short_item_number', 'net_content',
                   'net_content_uom', 'number_type', 'um', 'outer_size', 'unique_product_number']]
        sox = sox.loc[sox['number_type'] == 'EAN', ]
        sox.rename(mapper={'short_item_number': 'item_number',
                           'unique_product_number': 'upc'}, axis='columns', inplace=True)
        sox = sox.dropna(subset=['item_number', 'upc', 'net_content',
                   'net_content_uom', 'number_type', 'um', 'outer_size'], how='any', axis='index')
        sox[['item_number', 'upc']] = sox[['item_number', 'upc']].astype('int64')
        return sox
    
    def _getSoxWeight():
        pass
    
    def createOutput(self, swapped_prodgap):
        def _get_pf(row):
            dependents, independents = [row['Dependent'].lower()], [row['Independent'].lower()]
            if row['Category.Dep'] == 'PF':
                dependents = [name.lower() for name in self.scope.loc[self.scope['price_family']==row['Dependent'].upper(),
                                                                      'item_name'].values]
            if row['Category.Indep'] == 'PF':
                independents = [name.lower() for name in
                                self.scope.loc[self.scope['price_family']==row['Independent'].upper(), 'item_name'].values]
            return dependents, independents
        
        def _get_SOX_weights(row):
            # only KG, LT and PC are seen in SOX for Norway, Sweden and Denmark as of Nov21Refresh
            # hence, unit conversion is not necessary for this refresh
            # DW TODO: when have time, implement unit conversion interface for Europe SOX data 
            # folowing similar logic used in _getPFWeight helper function in NA
            dep_col = 'item_name' if row['Category.Dep'] == 'Item' else 'price_family'
            ind_col = 'item_name' if row['Category.Indep'] == 'Item' else 'price_family'
            
            dep_weight_size = self.scope_w_weight.loc[self.scope_w_weight[dep_col] == row['Dependent'],
                                                          ['net_content', 'net_content_uom', 'outer_size',
                                                           'um']].dropna().drop_duplicates()
            ind_weight_size = self.scope_w_weight.loc[self.scope_w_weight[ind_col] == row['Independent'],
                                                          ['net_content', 'net_content_uom', 'outer_size',
                                                           'um']].dropna().drop_duplicates()
            if len(dep_weight_size) > 1:
                if dep_col == 'item_name':
                    item_name = row['Dependent']
                    raise ValueError(f'Mutiple items with the same item name exist in scope: {item_name}')
                
            if len(ind_weight_size) > 1:
                if ind_col == 'item_name':
                    item_name = row['Independent']
                    raise ValueError(f'Mutiple items with the same item name exist in scope: {item_name}')
                    
            dep_dom_uom = dep_weight_size['net_content_uom'].mode().values[0]
            dep_dom_um = dep_weight_size['um'].mode().values[0]
            dep_avg_weight = dep_weight_size.loc[dep_weight_size['net_content_uom'] == dep_dom_uom]['net_content'].mean()
            dep_avg_pks = dep_weight_size.loc[dep_weight_size['um'] == dep_dom_um]['outer_size'].mean()
                
            ind_dom_uom = ind_weight_size['net_content_uom'].mode().values[0]
            ind_dom_um = ind_weight_size['um'].mode().values[0]
            ind_avg_weight = ind_weight_size.loc[ind_weight_size['net_content_uom'] == ind_dom_uom]['net_content'].mean()
            ind_avg_pks = ind_weight_size.loc[ind_weight_size['um'] == ind_dom_um]['outer_size'].mean()
            
            if dep_avg_pks == ind_avg_pks:
                return str(dep_avg_weight), str(ind_avg_weight)
            elif dep_avg_weight == ind_avg_weight:
                return str(dep_avg_pks), str(ind_avg_pks)
            elif dep_avg_vol > dep_avg_pks:
                return str(dep_avg_weight), str(ind_avg_weight)
            return None, None
        
        def _get_product_id(item_name):
            data = self.scope.loc[self.scope['item_name'] == item_name, ['product_key', 'upc',
                                                                         'modelling_level']].drop_duplicates()
            if data.shape[0] > 1:
                raise ValueError(f'Duplicate item name found in scope: {item_name}')
            product_key, upc, modelling_level = data.values[0]
            if modelling_level == 'EAN':
                return str(int(upc))
            else:
                return str(int(product_key))
            
        
        outputs = defaultdict(list)
        for _, row in swapped_prodgap.iterrows():
            rule_classification, sides, types, min_max = self._parse_rule(row['Initial.Rule'])
            dep, ind = row['Dependent'], row['Independent']
            if row['Category.Dep'] == 'Item':
                dep = _get_product_id(row['Dependent'])
            if row['Category.Indep'] == 'Item':
                ind = _get_product_id(row['Independent'])
                
            for index, (classy, side, ty) in enumerate(zip(rule_classification, sides, types)):
                outputs['Category.Name.Dep'].append(row['Category.Name.Dep'])
                outputs['Category.Dep'].append(row['Category.Dep'])
                outputs['Category.Indep'].append(row['Category.Indep'])
                outputs['Dependent'].append(dep)
#                 outputs['Dependent_org'].append(row['Dependent'])
                outputs['Independent'].append(ind)
#                 outputs['Independent_org'].append(row['Independent'])
                outputs['Initial.Rule'].append(row['Initial.Rule'])
                outputs['Rule.Classification'].append(classy)
                outputs['Side'].append(side)
                outputs['Type'].append(ty)
                weight_dep, weight_ind = np.nan, np.nan
                if index == len(rule_classification) - 1 and row['Initial.Rule'] not in self._no_weight_rules:
                    # change to get weight/pack from scope_with_weight
                    # tuple, list of lost item names
                    weight_dep, weight_ind = _get_SOX_weights(row)
                    if not weight_dep or not weight_ind:
                        dependents, independents = _get_pf(row)
                        weight_dep, weight_ind = self._getPFWeight(dependents, independents)
                outputs['Weight.Dep'].append(weight_dep)
                outputs['Weight.Indep'].append(weight_ind)
                outputs['Min'].append(min_max[0])
                outputs['Max'].append(min_max[1])
        return pd.DataFrame(outputs)
                

# COMMAND ----------

# testing/example usecase - NA
# fpath = '/dbfs/Phase3_extensions/Optimization/Nov21_Refresh/inputs/cc/CC_Opti_Input_Format_test.xlsx'
# temp = ProdGapHandlerNA('1600 - Coastal Carolina Division', fpath)

# testing/example usecase - EU
# fpath = '/dbfs/FileStore/Darren/Phase3_Extension/Nov21_Refresh/prod_gap/SW_Opti_Input_Test.xlsx'
# sox_fpath = '/dbfs/FileStore/Darren/Phase3_Extension/Nov21_Refresh/prod_gap/SOX_Vecka_1_2022.xlsx'
# temp = ProdGapHandlerEU('Sweden', fpath, sox_fpath)
# temp = ProdGapHandler('Sweden', fpath)

# COMMAND ----------

# temp.prodgap

# COMMAND ----------

# temp.createOutput(temp.swap())

# COMMAND ----------

# temp.prodgap

# COMMAND ----------


