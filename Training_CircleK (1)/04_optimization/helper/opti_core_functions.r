# Databricks notebook source
PF_Across_Classification <- function(merged_data,weight_map){
  
  PF_issue <- merged_data %>% select(Category.Name, Category.Special.Classification, Price.Family) %>%
  filter(Price.Family != "No_Family") %>%
  unique() %>%
  left_join(weight_map %>% select(-Margin.Weight, -Revenue.Weight)) %>%
  group_by(Category.Name, Price.Family) %>% 
  summarise(count = n_distinct(Optimization.Strategy)) %>% 
  ungroup() %>% 
  filter(count>1)
  
  return(PF_issue)
  
}

PF_Across_Categories <- function(merged_data){
  
  PF_issue <- merged_data %>% select(Category.Name, SubCategory.Name, Price.Family) %>%
  filter(Price.Family != "No_Family") %>%
  unique() %>% 
  group_by(Price.Family) %>% 
  summarise(count = n_distinct(Category.Name)) %>% 
  ungroup() %>% 
  filter(count>1)
  
  return(PF_issue)
  
}

price_fam_alert_function <- function(merged_data, price_col){
  
  price_alert <- merged_data %>%
  select(c(Category.Name, Item.Name, Sys.ID.Join, Price.Family, Opti_Key, Cluster, price_col)) %>% 
  filter(Price.Family != "No_Family") %>%
  group_by(Category.Name, Price.Family, Cluster) %>%
  mutate(max_fam_price = max(get(price_col), na.rm =T),
         fam_price = mean(get(price_col), na.rm =T)) %>% 
  ungroup() %>%
  mutate(price_issue = ifelse((fam_price != get(price_col) | max_fam_price != get(price_col)), "Issue", "Good"),
         abs_price_diff_from_fam_avg = round(abs((get(price_col)-fam_price)/fam_price),4)) %>%
  distinct() %>%
  select(-max_fam_price, -fam_price) %>%
  arrange(Category.Name, Price.Family, Cluster) %>%
  group_by(Category.Name, Price.Family, Cluster) %>%
  mutate(max_diff = max(abs_price_diff_from_fam_avg, na.rm=T)) %>% 
  ungroup() %>%
  filter(price_issue == "Issue")

  return(price_alert)
  
}

prod_gap_function_as_is <- function(item_data, prod_gap){
  
  item_data <- item_data %>% mutate(Sys.ID.Join =  as.character(Sys.ID.Join),
                                   Regular.Price = as.numeric(Regular.Price),
                                   Per.Unit.Cost = as.numeric(Per.Unit.Cost))   ## dash dtype
  
  prod_gap_weights <- prod_gap %>% 
    filter(!is.na(Weight.Dep)) %>% 
    select(Category.Name.Dep,Category.Dep,Category.Indep,Dependent,Independent,Initial.Rule,Weight.Dep,Weight.Indep) %>% 
    unique() %>% mutate(Dependent = as.character(Dependent), 
                        Independent = as.character(Independent),
                        Weight.Dep = as.numeric(Weight.Dep),
                        Weight.Indep = as.numeric(Weight.Indep)) ## dash dtype
  
 
  prod_gap <- prod_gap %>% select(-Weight.Dep,-Weight.Indep) %>% mutate(Dependent = as.character(Dependent), 
                                                                        Independent = as.character(Independent)) %>%   ## dash dtype
  left_join(prod_gap_weights)

  # Getting the Price and Cost Data from the Item Level Data

  # Getting the Rel Items and Rel Price Families having Prod Gap Rules
  rel_PF_dep <- unique(prod_gap$Dependent[prod_gap$Category.Dep == "PF"])
  rel_PF_indep <- unique(prod_gap$Independent[prod_gap$Category.Indep == "PF"])
  rel_PF <- union(rel_PF_dep, rel_PF_indep)
  rel_items_dep <- unique(prod_gap$Dependent[prod_gap$Category.Dep == "Item"])
  rel_items_indep <- unique(prod_gap$Independent[prod_gap$Category.Indep == "Item"])
  rel_items <- union(rel_items_dep, rel_items_indep)
  

  item_data$`Vat%`[is.na(item_data$`Vat%`)] <- 0

  # Subsetting the Price Families having the Prod Gap Rules
  item_data_pf <- item_data %>%
    select(Cluster, Price.Family, Regular.Price, Per.Unit.Cost, "Vat%") %>%
    filter(Price.Family %in% rel_PF) %>%
    group_by( Cluster, Price.Family) %>%
    summarise_all(funs(mean(.,na.rm=T))) %>%
    ungroup()

  # Subsetting the Items having the Prod Gap Rules
  #The below line was commented out becuase it was throwing an error on "weig"
  #item_data_item <- item_data %>%weig
  item_data_item <- item_data %>%
    select(Sys.ID.Join, Cluster, Regular.Price, Per.Unit.Cost, "Vat%") %>%
    filter(Sys.ID.Join %in% rel_items)

  # Joining the Price and Cost Metrics in the Prod Gap File for PF to PF Rule
  prod_gap_joined_1 <- prod_gap %>%
    filter((Category.Dep == "PF") & (Category.Indep == "PF")) %>%
    left_join(item_data_pf, c("Dependent" = "Price.Family")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_pf, c("Independent" = "Price.Family", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  
  # Joining the Price and Cost Metrics in the Prod Gap File for PF to Item Rule
  prod_gap_joined_2 <- prod_gap %>%
    filter((Category.Dep == "PF") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) %>% ## dash dtype
    left_join(item_data_pf, c("Dependent" = "Price.Family")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_item, c("Independent" = "Sys.ID.Join", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Joining the Price and Cost Metrics in the Prod Gap File for Item to PF Rule
  prod_gap_joined_3 <- prod_gap %>%
    filter((Category.Dep == "Item") & (Category.Indep == "PF")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%
    left_join(item_data_item, c("Dependent" = "Sys.ID.Join")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_pf, c("Independent" = "Price.Family", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))
 

  # Joining the Price and Cost Metrics in the Prod Gap File for Item to Item Rule
  prod_gap_joined_4 <- prod_gap %>%
    filter((Category.Dep == "Item") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),   ## dash dtype
           Independent = as.character(Independent)) %>%   ## dash dtype
    left_join(item_data_item, c("Dependent" = "Sys.ID.Join")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_item, c("Independent" = "Sys.ID.Join", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Appending all the Cases
  prod_gap_merged <- bind_rows(prod_gap_joined_1, prod_gap_joined_2, prod_gap_joined_3, prod_gap_joined_4) %>%
    mutate(Value.Dep = Weight.Dep/Price.Dep,
           Value.Indep = Weight.Indep/Price.Indep,
           Margin.Dep = (Price.Dep/(1+Vat.Dep) - Cost.Dep),
           Margin.Indep = (Price.Indep/(1+Vat.Indep) - Cost.Indep),
           Margin.Dep.Perc = ((Price.Dep/(1+Vat.Dep) - Cost.Dep)/(Price.Dep/(1+Vat.Dep))),
           Margin.Indep.Perc = ((Price.Indep/(1+Vat.Indep) - Cost.Indep)/(Price.Indep/(1+Vat.Indep))))

  # Dividing the Product Gap Rules based the 3 Types: Price, Value and Margin and then we check Breaches

  # Price Rule Breach
  prod_gap_rules_price <- prod_gap_merged %>%
    filter(Rule.Classification == "Price") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Price.Dep>Price.Indep, "Good",
                                 ifelse(Side == "Lower" & Price.Dep<Price.Indep, "Good",
                                        ifelse(Side == "Within", "Good",
                                               ifelse(Side == "Same" & abs(Price.Dep-Price.Indep)<=0.01, "Good", "Issue"))))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Price.Dep/Price.Indep-1), round(abs(Price.Dep-Price.Indep),2))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Price Breach"),
                              ifelse(((difference>Max | difference<Min) & (Side != "Same")), paste0(Side, " Price Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Value Rule Breach
  prod_gap_rules_value <- prod_gap_merged %>%
    filter(Rule.Classification == "Value") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Value.Dep>Value.Indep, "Good",
                                 ifelse(Side == "Lower" & Value.Dep<Value.Indep, "Good",
                                        ifelse(Side == "Same" & Value.Dep==Value.Indep, "Good", "Issue")))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Value.Dep/Value.Indep-1), abs(Value.Dep-Value.Indep))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Value Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Value Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Margin Rule Breach
  prod_gap_rules_margin <- prod_gap_merged %>%
    filter(Rule.Classification == "Margin") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Margin.Dep>Margin.Indep, "Good",
                                 ifelse(Side == "Lower" & Margin.Dep<Margin.Indep, "Good",
                                        ifelse(Side == "Same" & Margin.Dep==Margin.Indep, "Good", "Issue")))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Margin.Dep/Margin.Indep-1), abs(Margin.Dep-Margin.Indep))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Margin Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Margin Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Margin Percentage Rule Breach  
  prod_gap_rules_margin_perc <- prod_gap_merged %>%
    filter((Rule.Classification == "Margin Percentage")) %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Margin.Dep.Perc>Margin.Indep.Perc, "Good",
                                 ifelse(Side == "Lower" & Margin.Dep.Perc<Margin.Indep.Perc, "Good",
                                        ifelse(Side == "Same" & Margin.Dep.Perc==Margin.Indep.Perc, "Good", "Issue")))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Margin.Dep.Perc/Margin.Indep.Perc-1), abs(Margin.Indep.Perc-Margin.Dep.Perc))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Margin Percentage Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Margin Percentage Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # All Product Gap Breaches
  all_rules_breach <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin, prod_gap_rules_margin_perc) %>%
                        arrange(Dependent) %>% filter(grepl("Breach", is.Breach))

  all_rules_breach_report <- all_rules_breach %>% 
                               select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Independent, Cluster, 
                                      Initial.Rule, Rule.Classification, is.Breach) %>% 
                               unique() %>% mutate(Rule.Classification = paste0(Rule.Classification, "_Breach")) %>%
                               spread(Rule.Classification, is.Breach)

  all_rules_breach_final <- all_rules_breach %>% select(-Rule.Classification, -Side, -Type, -Min, -Max, -difference, -is.Breach) %>% unique() %>% 
                               left_join(all_rules_breach_report)

  all_rules_ratio <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin) %>%
    mutate(penny_flag = ifelse(((Rule.Classification == "Margin") & (Side == "Same")), "Y", "N"),
           same_retail_flag = ifelse(((Rule.Classification == "Price") & (Side == "Same")), "Y", "N")) %>%
    arrange(Dependent) %>%
    select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Cluster, Independent, 
           Initial.Rule, Price.Dep, Price.Indep, Cost.Dep, Cost.Indep, penny_flag, same_retail_flag) %>%
    unique() %>%
    mutate(Ratio = Price.Dep/Price.Indep) %>%
    filter(!is.na(Ratio)) %>%
    mutate(Ratio = ifelse(same_retail_flag == "Y", 1, 
                          ifelse(penny_flag == "N", (Price.Dep/Price.Indep), 0)))

  # Either Dependent/Independent not in scope for the Current Wave
  not_in_data <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin) %>%
    arrange(Dependent) %>%
    left_join(all_rules_breach %>% select(Dependent, Cluster) %>% unique() %>% mutate(Breach = "Y")) %>%
    filter(is.na(Breach)) %>% select(-Breach) %>%
    select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Cluster, Independent, Price.Dep, Price.Indep) %>%
    unique() %>%
    mutate(Ratio = Price.Dep/Price.Indep) %>%
    filter(is.na(Ratio))

  return(list(all_rules_breach_final, not_in_data, all_rules_ratio))
  
}


prod_gap_function_opt <- function(item_data, prod_gap, prod_gap_alerts_as_is){
  
  prod_gap_weights <- prod_gap %>% 
    filter(!is.na(Weight.Dep)) %>% 
    select(Category.Name.Dep,Category.Dep,Category.Indep,Dependent,Independent,Initial.Rule,Weight.Dep,Weight.Indep) %>% 
    unique()

  prod_gap <- prod_gap %>% select(-Weight.Dep,-Weight.Indep) %>% left_join(prod_gap_weights)

  # Getting the Price and Cost Data from the Item Level Data

  # Getting the Rel Items and Rel Price Families having Prod Gap Rules
  rel_PF_dep <- unique(prod_gap$Dependent[prod_gap$Category.Dep == "PF"])
  rel_PF_indep <- unique(prod_gap$Independent[prod_gap$Category.Indep == "PF"])
  rel_PF <- union(rel_PF_dep, rel_PF_indep)
  rel_items_dep <- unique(prod_gap$Dependent[prod_gap$Category.Dep == "Item"])
  rel_items_indep <- unique(prod_gap$Independent[prod_gap$Category.Indep == "Item"])
  rel_items <- union(rel_items_dep, rel_items_indep)

  item_data$`Vat%`[is.na(item_data$`Vat%`)] <- 0

  # Subsetting the Price Families having the Prod Gap Rules
  item_data_pf <- item_data %>%
    select(Cluster, Price.Family, OPTIMIZED_PRICE_ROUNDED, Per.Unit.Cost, "Vat%") %>%
    filter(Price.Family %in% rel_PF) %>%
    group_by( Cluster, Price.Family) %>%
    summarise_all(funs(mean(.,na.rm=T))) %>%
    ungroup()

  # Subsetting the Items having the Prod Gap Rules
  item_data_item <- item_data %>%
    select(Sys.ID.Join, Cluster, OPTIMIZED_PRICE_ROUNDED, Per.Unit.Cost, "Vat%") %>%
    filter(Sys.ID.Join %in% rel_items)

  # Joining the Price and Cost Metrics in the Prod Gap File for PF to PF Rule
  prod_gap_joined_1 <- prod_gap %>%
    filter((Category.Dep == "PF") & (Category.Indep == "PF")) %>%
    left_join(item_data_pf, c("Dependent" = "Price.Family")) %>%
    rename(Price.Dep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_pf, c("Independent" = "Price.Family", "Cluster")) %>%
    rename(Price.Indep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Joining the Price and Cost Metrics in the Prod Gap File for PF to Item Rule
  prod_gap_joined_2 <- prod_gap %>%
    filter((Category.Dep == "PF") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) %>%  ## dash dtype
    left_join(item_data_pf, c("Dependent" = "Price.Family")) %>%
    rename(Price.Dep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_item, c("Independent" = "Sys.ID.Join", "Cluster")) %>%
    rename(Price.Indep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Joining the Price and Cost Metrics in the Prod Gap File for Item to PF Rule
  prod_gap_joined_3 <- prod_gap %>%
    filter((Category.Dep == "Item") & (Category.Indep == "PF")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%
    left_join(item_data_item, c("Dependent" = "Sys.ID.Join")) %>%
    rename(Price.Dep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_pf, c("Independent" = "Price.Family", "Cluster")) %>%
    rename(Price.Indep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Joining the Price and Cost Metrics in the Prod Gap File for Item to Item Rule
  prod_gap_joined_4 <- prod_gap %>%
    filter((Category.Dep == "Item") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%  ## dash dtype
    left_join(item_data_item, c("Dependent" = "Sys.ID.Join")) %>%
    rename(Price.Dep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_item, c("Independent" = "Sys.ID.Join", "Cluster")) %>%
    rename(Price.Indep = OPTIMIZED_PRICE_ROUNDED, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Appending all the Cases
  prod_gap_merged <- bind_rows(prod_gap_joined_1, prod_gap_joined_2, prod_gap_joined_3, prod_gap_joined_4) %>%
    mutate(Value.Dep = Weight.Dep/Price.Dep,
           Value.Indep = Weight.Indep/Price.Indep,
           Margin.Dep = (Price.Dep/(1+Vat.Dep) - Cost.Dep),
           Margin.Indep = (Price.Indep/(1+Vat.Indep) - Cost.Indep),
           Margin.Dep.Perc = ((Price.Dep/(1+Vat.Dep) - Cost.Dep)/(Price.Dep/(1+Vat.Dep))),
           Margin.Indep.Perc = ((Price.Indep/(1+Vat.Indep) - Cost.Indep)/(Price.Indep/(1+Vat.Indep))))

  # Dividing the Product Gap Rules based the 3 Types: Price, Value and Margin and then we check Breaches

  # Price Rule Breach
  prod_gap_rules_price <- prod_gap_merged %>%
    filter(Rule.Classification == "Price") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & round(Price.Dep,2) >= round(Price.Indep,2), "Good",
                                 ifelse(Side == "Lower" & round(Price.Dep,2) <= round(Price.Indep,2), "Good",
                                       ifelse(Side == "Within", "Good",
                                               ifelse(Side == "Same" & abs(Price.Dep-Price.Indep)<=0.01, "Good", "Issue"))))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Price.Dep/Price.Indep-1), round(abs(Price.Dep-Price.Indep),2))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Price Breach"),
                              ifelse(((difference>Max | difference<Min) & (Side != "Same")), paste0(Side, " Price Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Value Rule Breach
  prod_gap_rules_value <- prod_gap_merged %>%
    filter(Rule.Classification == "Value") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Value.Dep>Value.Indep, "Good",
                                 ifelse(Side == "Lower" & Value.Dep<Value.Indep, "Good",
                                        ifelse(Side == "Same" & Value.Dep==Value.Indep, "Good", "Issue")))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Value.Dep/Value.Indep-1), abs(Value.Dep-Value.Indep))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Value Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Value Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Margin Rule Breach
  prod_gap_rules_margin <- prod_gap_merged %>%
    filter(Rule.Classification == "Margin") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Margin.Dep>Margin.Indep, "Good",
                                 ifelse(Side == "Lower" & Margin.Dep<Margin.Indep, "Good",
                                        ifelse(Side == "Within", "Good",
                                               ifelse(Side == "Same" & Margin.Dep==Margin.Indep, "Good", "Issue"))))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Margin.Dep/Margin.Indep-1), abs(Margin.Dep-Margin.Indep))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Margin Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Margin Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Margin Percentage Rule Breach  
  prod_gap_rules_margin_perc <- prod_gap_merged %>%
    filter((Rule.Classification == "Margin Percentage")) %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Margin.Dep.Perc>Margin.Indep.Perc, "Good",
                                 ifelse(Side == "Lower" & Margin.Dep.Perc<Margin.Indep.Perc, "Good",
                                        ifelse(Side == "Within", "Good",
                                               ifelse(Side == "Same" & Margin.Dep.Perc==Margin.Indep.Perc, "Good", "Issue"))))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Margin.Dep.Perc/Margin.Indep.Perc-1), abs(Margin.Indep.Perc-Margin.Dep.Perc))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Margin Percentage Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Margin Percentage Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # All Product Gap Breaches
  all_rules_breach <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin, prod_gap_rules_margin_perc) %>%
                        arrange(Dependent) %>% filter(grepl("Breach", is.Breach))

  all_rules_breach_report <- all_rules_breach %>% 
                               select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Independent, Cluster, 
                                      Initial.Rule, Rule.Classification, is.Breach) %>% 
                               unique() %>% mutate(Rule.Classification = paste0(Rule.Classification, "_Breach")) %>%
                               spread(Rule.Classification, is.Breach)

  all_rules_breach_final <- all_rules_breach %>% select(-Rule.Classification ,-Side, -Type, -Min, -Max, -difference, -is.Breach) %>% unique() %>% 
                               left_join(all_rules_breach_report) %>%
                               mutate(Dependent = as.character(Dependent),
                                      Independent = as.character(Independent)) %>%
                               left_join(prod_gap_alerts_as_is %>% 
                                         select(Category.Name.Dep,Category.Dep,Category.Indep,Dependent,Independent,Cluster) %>%
                                         mutate(Dependent = as.character(Dependent),
                                                Independent = as.character(Independent)) %>%
                                         mutate(Rule_Failed_AS_IS = "Y")) %>% 
                               mutate(Rule_Failed_AS_IS = ifelse(is.na(Rule_Failed_AS_IS), "N", Rule_Failed_AS_IS))

  return(all_rules_breach_final)
  
}

PF_Remapping_func <- function(input_data){
  
  rel_fams <- unique(PF_Issue_Classification$Price.Family)

  rel_data <- input_data %>%
    select(Price.Family, Category.Special.Classification, Sys.ID.Join, sales_amount) %>% 
    unique() %>%
    filter(Price.Family %in% rel_fams) %>%
    group_by(Price.Family, Category.Special.Classification) %>%
    summarise(sales = sum(sales_amount, na.rm = T)) %>%
    ungroup() %>%
    arrange(Price.Family, desc(sales)) %>%
    group_by(Price.Family) %>%
    mutate(Rank = row_number()) %>%
    ungroup() %>%
    filter(Rank == 1) %>%
    select(Price.Family, Category.Special.Classification) %>%
    unique() %>%
    rename(Category.Special.Classification_2 = Category.Special.Classification)

  merged_data <- input_data %>%
    left_join(rel_data, "Price.Family") %>%
    mutate(Category.Special.Classification = ifelse(is.na(Category.Special.Classification_2),
                                                    Category.Special.Classification, Category.Special.Classification_2)) %>%
    select(-Category.Special.Classification_2)
  
  return(merged_data)
  
}

ending_number_summary_function <- function(merged_data){
  
  ending_number_summary <- merged_data %>%
  select(Category.Name, SubCategory.Name, Sys.ID.Join, Cluster, Old.Regular.Price) %>%
  mutate(Old.Regular.Price = as.character(Old.Regular.Price)) %>%
  mutate(Ending_Number = str_sub(Old.Regular.Price, -1)) %>%
  group_by(Category.Name) %>%
  mutate(Cat_Item_Cluster_Count = n()) %>%
  ungroup() %>%
  group_by(Category.Name, Ending_Number) %>%
  summarise(Item_Cluster_Count = n(),
            Cat_Item_Cluster_Count = mean(Cat_Item_Cluster_Count, na.rm = T)) %>%
  ungroup() %>%
  mutate(Perc_item_clusters = round((100*Item_Cluster_Count/Cat_Item_Cluster_Count),2)) %>%
  arrange(Category.Name, -Perc_item_clusters) %>%
  group_by(Category.Name) %>%
  mutate(Ending_Number_Rank = row_number()) %>%
  ungroup() %>%
  filter(Ending_Number_Rank <=3) %>%
  select(Category.Name, Ending_Number, Perc_item_clusters, Item_Cluster_Count, Ending_Number_Rank)

  return(ending_number_summary)
  
}


breach_report_function <- function(data, column_to_be_checked){
  
  # Defining the Columns affecting Lower Bounds and Upper Bounds, respectively
  LB_Cols <- c("LB_Cat", "LB_min_mar", "Promo_price_floor", "LB_Comp")
  UB_Cols <- c("UB_Cat", "UB_Promo", "Price_Ceil", "UB_qty", "UB_Comp")
  
  # Creating a Flag if any of the Lower Bound is Breached
  for(lb_col in LB_Cols){

    varname <- paste0(lb_col,"_check")
    data <- data %>%
      mutate(!!varname := ifelse(get(column_to_be_checked)<get(lb_col),
                                 paste0(lb_col, "_Breach"), "No"))
    
  }
  
   # Creating a Flag if any of the Upper Bound is Breached
  for(ub_col in UB_Cols){
    
    varname <- paste0(ub_col,"_check")
    data <- data %>%
      mutate(!!varname := ifelse(get(column_to_be_checked)>get(ub_col),
                                 paste0(ub_col, "_Breach"), "No"))
    
  }
  
  check_cols <- grep("check", colnames(data), value = T)
  
  # Collapsing all the Breach Flag Columns to create a single Column "Breach_Summary" and filtering the items with any Breach in the Bounds
  breach_report <- data %>%
    unite(Breach_Summary, check_cols, sep = ",") %>%
    filter(grepl("Breach", Breach_Summary)) %>%
    mutate(Breach_Summary = gsub("No,|,No", "", Breach_Summary))
  
  # Creating the Final Breach Report
  final_breach_report <- data %>%
    select(Category.Name, SubCategory.Name, Item.Name, Sys.ID.Join, Cluster, Price.Family, Regular.Price, Old.Regular.Price, 
           Per.Unit.Cost, Opti_Key, LB_Cols, UB_Cols, Final.Price.LB.Family2, Final.Price.UB.Family2) %>%
    filter(Opti_Key %in% unique(breach_report$Opti_Key)) %>%
    left_join(breach_report %>% select(Sys.ID.Join, Cluster, Breach_Summary)) %>%
    arrange(Opti_Key) %>%
    filter(!is.na(Breach_Summary))
  
  return(final_breach_report)
  
}

prod_gap_merge_function <- function(item_data, all_rules_ratio, prod_gap_alerts_as_is, prod_gap, prod_gap_function_opt){
  
  # Summarising the Bounds and Optimized Price at Price Family Level
  rel_items_data_PF <- item_data %>%
    select(Category.Name, Cluster, Price.Family, Final.Price.LB.Family2, Final.Price.UB.Family2, OPTIMIZED_PRICE_ROUNDED) %>%
    group_by(Category.Name, Cluster, Price.Family) %>%
    summarise_all(funs(mean(.,na.rm=T))) %>%
    ungroup()
  
  # Selecting the Bounds and Optimized Price at Item Level
  rel_items_data_item <- item_data %>%
    select(Category.Name, Sys.ID.Join, Cluster, Final.Price.LB.Family2, Final.Price.UB.Family2, OPTIMIZED_PRICE_ROUNDED)
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Family to Family Relationship
  rel_PF_PF <- all_rules_ratio %>% 
    filter((Category.Dep == "PF") & (Category.Indep == "PF")) %>%
    left_join(rel_items_data_PF %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Price.Family")) %>%
    left_join(rel_items_data_PF %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Price.Family")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED)
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Family to Item Relationship
  rel_PF_Item <- all_rules_ratio %>% 
    filter((Category.Dep == "PF") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) %>%  ## dash dtype
    left_join(rel_items_data_PF %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Price.Family")) %>%
    left_join(rel_items_data_item %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Sys.ID.Join")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) 
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Items to Family Relationship
  rel_Item_PF <- all_rules_ratio %>% 
    filter((Category.Dep == "Item") & (Category.Indep == "PF")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%
    left_join(rel_items_data_item %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Sys.ID.Join")) %>%
    left_join(rel_items_data_PF %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Price.Family")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) 
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Items to Item Relationship
  rel_Item_Item <- all_rules_ratio %>% 
    filter((Category.Dep == "Item") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%  ## dash dtype
    left_join(rel_items_data_item %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Sys.ID.Join")) %>%
    left_join(rel_items_data_item %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Sys.ID.Join")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) 
  
  # Checking if the Product Gap Rules are followed in the Free-Flow Optimization or not. If not, then we apply AS-IS Price Ratios
  prod_gap_alerts_opt <- prod_gap_function_opt(item_data, prod_gap, prod_gap_alerts_as_is)
  
  # Appending with the Types
  final_opti_price_prod_gap <- bind_rows(rel_PF_PF, rel_PF_Item, rel_Item_PF, rel_Item_Item) %>%
    left_join(prod_gap_alerts_opt %>% select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Independent, Cluster, Rule_Failed_AS_IS) %>% 
              mutate(Rule_Failed_OPT = "Y")) %>%
    filter(Rule_Failed_OPT == "Y" & Rule_Failed_AS_IS == "N") %>%
    mutate(OPTIMIZED_PRICE_ROUNDED_DEP = ifelse(penny_flag == "N", 
                                                OPTIMIZED_PRICE_ROUNDED_INDEP*Ratio, (Cost.Dep+(OPTIMIZED_PRICE_ROUNDED_INDEP-Cost.Indep)))) %>%
    mutate(out_of_bounds_flag=ifelse(((OPTIMIZED_PRICE_ROUNDED_DEP<Final.Price.LB.Family2.Dep)|(OPTIMIZED_PRICE_ROUNDED_DEP>Final.Price.UB.Family2.Dep)),
                                  "Y", "N"))
  
  # Updating the Stage 1 Output with the New Optimized Prices of the Dependent Items
  items_data_final <- item_data %>%
    mutate(Sys.ID.Join = as.character(Sys.ID.Join)) %>%
    left_join(final_opti_price_prod_gap %>% select(Category.Name.Dep, Dependent, Cluster, OPTIMIZED_PRICE_ROUNDED_DEP),
              c("Category.Name"="Category.Name.Dep", "Sys.ID.Join"="Dependent", "Cluster")) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED_DEP = ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP>Final.Price.UB.Family2)), 
                                                Final.Price.UB.Family2,
                                                ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP<Final.Price.LB.Family2)),
                                                       Final.Price.LB.Family2, OPTIMIZED_PRICE_ROUNDED_DEP))) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(OPTIMIZED_PRICE_ROUNDED_DEP), OPTIMIZED_PRICE_ROUNDED, OPTIMIZED_PRICE_ROUNDED_DEP)) %>%
    select(-OPTIMIZED_PRICE_ROUNDED_DEP) %>%
    left_join(final_opti_price_prod_gap %>% select(Category.Name.Dep, Dependent, Cluster, OPTIMIZED_PRICE_ROUNDED_DEP),
              c("Category.Name"="Category.Name.Dep", "Price.Family"="Dependent", "Cluster")) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED_DEP = ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP>Final.Price.UB.Family2)),
                                                Final.Price.UB.Family2,
                                                ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP<Final.Price.LB.Family2)),
                                                       Final.Price.LB.Family2, OPTIMIZED_PRICE_ROUNDED_DEP))) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(OPTIMIZED_PRICE_ROUNDED_DEP), OPTIMIZED_PRICE_ROUNDED, OPTIMIZED_PRICE_ROUNDED_DEP)) %>%
    select(-OPTIMIZED_PRICE_ROUNDED_DEP) %>%
#     left_join(final_opti_price_prod_gap %>% select(Independent, Cluster, out_of_bounds_flag) %>% unique(),
#             c("Sys.ID.Join"="Independent", "Cluster")) %>%
#     mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(out_of_bounds_flag), OPTIMIZED_PRICE_ROUNDED,
#                                             ifelse(out_of_bounds_flag == "N", OPTIMIZED_PRICE_ROUNDED, Regular.Price))) %>%
#     select(-out_of_bounds_flag) %>%
#     left_join(final_opti_price_prod_gap %>% select(Independent, Cluster, out_of_bounds_flag) %>% unique(),
#             c("Price.Family"="Independent", "Cluster")) %>%
#     mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(out_of_bounds_flag), OPTIMIZED_PRICE_ROUNDED,
#                                             ifelse(out_of_bounds_flag == "N", OPTIMIZED_PRICE_ROUNDED, Regular.Price))) %>%
#     select(-out_of_bounds_flag) %>%
    mutate(Sys.ID.Join = as.character(Sys.ID.Join))  ## dash dtype
  
  return(list(items_data_final, final_opti_price_prod_gap, prod_gap_alerts_opt))
  
  
}
  
round_off_optimization_price <- function(optimized_data, round_off, map_to, round_off_function){
  
  optimized_data <- optimized_data %>% mutate(Sys.ID.Join = as.character(Sys.ID.Join))       ## dash dtype 
  
  if(nrow(optimized_data %>% filter(Issue_Flag == "No_Issue")) == 0){
    
     optimized_data <- optimized_data %>% 
                        mutate(opti_units = Base.Units + ((Base.Units)*((OPTIMIZED_PRICE_ROUNDED/Regular.Price)-1)*(Base.Price.Elasticity)),
                               opti_rev = opti_units*(OPTIMIZED_PRICE_ROUNDED/(1+`Vat%`)),
                               opti_margin = opti_units*(OPTIMIZED_PRICE_ROUNDED/(1+`Vat%`) - Per.Unit.Cost),
                               opti_units_tot = opti_units*Test_Stores,
                               opti_rev_tot = opti_rev*Test_Stores,
                               opti_margin_tot = opti_margin*Test_Stores)
    
    return(optimized_data)
    
  }else{
      
  round_off_opt_price <- optimized_data %>%
    filter(Issue_Flag == "No_Issue") %>%
    select(Category.Name, Sys.ID.Join, Cluster, OPTIMIZED_PRICE_ROUNDED) %>%
    left_join(round_off)

  # Rounding Off Lower Bound
  round_off_opt_price$initial_price_point <- round_off_opt_price$OPTIMIZED_PRICE_ROUNDED
  round_off_opt_price$map_to <- map_to               # "Upper", "Lower", "Closest"
  round_off_opt_price$Opti_Rounded <- mapply(round_off_function,round_off_opt_price$initial_price_point, 
                                              round_off_opt_price$initial_round_decimal_point, round_off_opt_price$range,
                                              round_off_opt_price$range_1, round_off_opt_price$range_2, round_off_opt_price$range_3,
                                              round_off_opt_price$range_4, round_off_opt_price$ending_number, 
                                              round_off_opt_price$ending_number_1, round_off_opt_price$ending_number_2, 
                                              round_off_opt_price$ending_number_3, round_off_opt_price$ending_number_4,
                                              round_off_opt_price$subtraction, round_off_opt_price$map_to)

  # Updating the Optimized Price in the Optimized Data and re-calculating the Optimized Metrics
  final_optimization_data <- optimized_data %>%
    left_join(round_off_opt_price %>% select(Category.Name, Sys.ID.Join, Cluster, Opti_Rounded), 
              c("Category.Name", "Sys.ID.Join", "Cluster")) %>%
     mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(Opti_Rounded), OPTIMIZED_PRICE_ROUNDED, Opti_Rounded)) %>%
     select(-Opti_Rounded) %>%
     mutate(opti_units = Base.Units + ((Base.Units)*((OPTIMIZED_PRICE_ROUNDED/Regular.Price)-1)*(Base.Price.Elasticity)),
            opti_rev = opti_units*(OPTIMIZED_PRICE_ROUNDED/(1+`Vat%`)),
            opti_margin = opti_units*(OPTIMIZED_PRICE_ROUNDED/(1+`Vat%`) - Per.Unit.Cost)) %>%
     mutate(opti_units_tot = opti_units*Test_Stores,
            opti_rev_tot = opti_rev*Test_Stores,
            opti_margin_tot = opti_margin*Test_Stores)

  return(final_optimization_data)
    
  }
  
}



generate_opti_summary_all <- function(opti_res_adj_final, weight_map){
 
  # optimization Summary at Sub-Category Level  
  opti_summary_1 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Category.Name) %>%
    mutate(Total_Units= sum(Base.Units_tot, na.rm = T),
           Total_Rev= sum(base_rev_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name, Category.Special.Classification) %>%
    summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
              Base.Units = sum(Base.Units_tot, na.rm = T),
              base_rev = sum(base_rev_tot, na.rm = T),
              base_margin = sum(base_margin_tot, na.rm = T),
              OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
              opti_units = sum(opti_units_tot, na.rm = T),
              opti_rev = sum(opti_rev_tot, na.rm = T),
              opti_margin = sum(opti_margin_tot, na.rm = T),
              Total_Units = mean(Total_Units, na.rm = T),
              Total_Rev = mean(Total_Rev, na.rm = T),
              Price_Inc_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Dec_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n(),
              count_item_x_clusters = n()) %>% 
    ungroup() %>%
    mutate(margin_lift = (opti_margin/base_margin-1)*100,
           rev_lift = (opti_rev/base_rev-1)*100,
           unit_lift = (opti_units/Base.Units-1)*100,
           price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100,
           Units_Share = Base.Units/Total_Units,
           Rev_Share = base_rev/Total_Rev)
  
  # optimization Summary at Category Level 
  opti_summary_2 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Category.Name) %>%
    mutate(Total_Units= sum(Base.Units_tot, na.rm = T),
           Total_Rev= sum(base_rev_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name) %>%
    summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
              Base.Units = sum(Base.Units_tot, na.rm = T),
              base_rev = sum(base_rev_tot, na.rm = T),
              base_margin = sum(base_margin_tot, na.rm = T),
              OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
              opti_units = sum(opti_units_tot, na.rm = T),
              opti_rev = sum(opti_rev_tot, na.rm = T),
              opti_margin = sum(opti_margin_tot, na.rm = T),
              Total_Units = mean(Total_Units, na.rm = T),
              Total_Rev = mean(Total_Rev, na.rm = T),
              Price_Inc_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Dec_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n(),
              count_item_x_clusters = n()) %>% 
    ungroup() %>%
    mutate(margin_lift = (opti_margin/base_margin-1)*100,
           rev_lift = (opti_rev/base_rev-1)*100,
           unit_lift = (opti_units/Base.Units-1)*100,
           price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100,
           Units_Share = Base.Units/Total_Units,
           Rev_Share = base_rev/Total_Rev) %>%
    mutate(Category.Special.Classification = "Overall")
  
  # Weighted Elasticity at Sub-Category Level
  summary_elasticity_1 <- opti_res_adj_final %>%
    group_by(Category.Name, Category.Special.Classification, Sys.ID.Join) %>%
    summarise(Weighted_Elasticity = weighted.mean(Base.Price.Elasticity, Base.Units_tot),
              Units = sum(Base.Units_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name, Category.Special.Classification) %>%
    summarise(Weighted_Elasticity_Final = weighted.mean(Weighted_Elasticity, Units)) %>%
    ungroup()
  
  # Weighted Elasticity at Category Level
  summary_elasticity_2 <- opti_res_adj_final %>%
    group_by(Category.Name, Category.Special.Classification, Sys.ID.Join) %>%
    summarise(Weighted_Elasticity = weighted.mean(Base.Price.Elasticity, Base.Units_tot),
              Units = sum(Base.Units_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name, Category.Special.Classification) %>%
    summarise(Weighted_Elasticity_Final = weighted.mean(Weighted_Elasticity, Units),
              Units = sum(Units, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name) %>%
    summarise(Weighted_Elasticity_Final = weighted.mean(Weighted_Elasticity_Final, Units)) %>%
    ungroup() %>%
    mutate(Category.Special.Classification = "Overall")
  
  # Combining all the Summaries
  final_opti_summary <- bind_rows(opti_summary_1, opti_summary_2) %>% unique() %>%
    left_join(min_margin_file %>% unique()) %>%
    mutate(Average.Category.Minimum.Margin = ifelse(is.na(Average.Category.Minimum.Margin), 0, Average.Category.Minimum.Margin)) %>%
    mutate(avg_cat_mar_base = base_margin/base_rev,
           avg_cat_mar_opt = opti_margin/opti_rev) %>%
    mutate(cat_mar_flag = ifelse(avg_cat_mar_base >= Average.Category.Minimum.Margin & avg_cat_mar_opt < Average.Category.Minimum.Margin, 
                                 "Avg Cat Margin Fail","Avg Cat Margin Pass"))%>%
    left_join(weight_map) %>%
    left_join(bind_rows(summary_elasticity_1, summary_elasticity_2) %>% unique()) %>%
    rename(Optimization_Strategy = Optimization.Strategy) %>%
    select(Category.Name,Category.Special.Classification,Regular.Price,Base.Units,base_rev,base_margin,OPTIMIZED_PRICE_ROUNDED,Units_Share,Rev_Share,
           opti_units,opti_rev,opti_margin,Price_Inc_Perc,Price_Dec_Perc,No_Price_Change_Perc,count_item_x_clusters,Optimization_Strategy,margin_lift,
           rev_lift,unit_lift,price_lift,Average.Category.Minimum.Margin,avg_cat_mar_base,avg_cat_mar_opt,cat_mar_flag,Margin.Weight,Revenue.Weight,Unit.Weight,
           Weighted_Elasticity_Final)
  
  # Cluster Level Summaries
  cluster_level_summary <- opti_res_adj_final %>% 
    group_by(Category.Name, Cluster) %>%
    summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
              Base.Units = sum(Base.Units_tot, na.rm = T),
              base_rev = sum(base_rev_tot, na.rm = T),
              base_margin = sum(base_margin_tot, na.rm = T),
              OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
              opti_units = sum(opti_units_tot, na.rm = T),
              opti_rev = sum(opti_rev_tot, na.rm = T),
              opti_margin = sum(opti_margin_tot, na.rm = T)) %>% 
    ungroup() %>%
    mutate(margin_lift = (opti_margin/base_margin-1)*100,
           rev_lift = (opti_rev/base_rev-1)*100,
           unit_lift = (opti_units/Base.Units-1)*100,
           price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100) %>%
    select(Category.Name, Cluster, margin_lift, rev_lift, unit_lift, price_lift)
  
  price_by_cluster_1 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Category.Name, Cluster) %>%
    summarise(Price_Increase_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Decrease_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n(),
              Total_Items = n()) %>% 
    ungroup()
  
  price_by_cluster_2 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Cluster) %>%
    summarise(Price_Increase_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Decrease_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n()) %>% 
    ungroup()
  
    # Price Family x Cluster Level Summaries
    pf_cluster_level_summary <- opti_res_adj_final %>% 
      group_by(Category.Name, Opti_Key, Price.Family, Cluster) %>%
      summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
                Base.Units = sum(Base.Units_tot, na.rm = T),
                base_rev = sum(base_rev_tot, na.rm = T),
                base_margin = sum(base_margin_tot, na.rm = T),
                OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
                opti_units = sum(opti_units_tot, na.rm = T),
                opti_rev = sum(opti_rev_tot, na.rm = T),
                opti_margin = sum(opti_margin_tot, na.rm = T)) %>% 
      ungroup() %>%
      mutate(margin_lift = (opti_margin/base_margin-1)*100,
             rev_lift = (opti_rev/base_rev-1)*100,
             unit_lift = (opti_units/Base.Units-1)*100,
             price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100) %>%
      select(Category.Name, Opti_Key, Price.Family, Cluster, margin_lift, rev_lift, unit_lift, price_lift)
  
  return(list(final_opti_summary, cluster_level_summary, price_by_cluster_1, price_by_cluster_2, pf_cluster_level_summary))
   
}
    
final_opti_summary_function <- function(opti_summary, price_alerts_as_is, prod_alerts_as_is, bound_breach_report_as_is,
                                        price_alerts_opt, prod_alerts_opt, bound_breach_report_opt){
  
  # Creating Breach Summary for each of these Reports and then merging them in "opti_summary" for the final view
  
  price_alerts_as_is_summary <- price_alerts_as_is %>%
    group_by(Category.Name) %>%
    summarise(PF_Breach_Count_AS_IS = n_distinct(Opti_Key)) %>%
    ungroup()
  
  price_alerts_opt_summary <- price_alerts_opt %>%
    group_by(Category.Name) %>%
    summarise(PF_Breach_Count_OPT = n_distinct(Opti_Key)) %>%
    ungroup()
  
  prod_alerts_as_is_summary <- prod_alerts_as_is %>%
    rename(Category.Name = Category.Name.Dep) %>%
    group_by(Category.Name) %>%
    summarise(Prod_Gap_Breach_Count_AS_IS = n_distinct(Dependent, Cluster)) %>%
    ungroup()
  
  prod_alerts_opt_summary <- prod_alerts_opt %>%
    rename(Category.Name = Category.Name.Dep) %>%
    group_by(Category.Name) %>%
    summarise(Prod_Gap_Breach_Count_OPT = n_distinct(Dependent, Cluster)) %>%
    ungroup()

  bound_breach_report_as_is_summary <- bound_breach_report_as_is %>%
    group_by(Category.Name) %>%
    summarise(Bound_Breach_Count_AS_IS = n_distinct(Opti_Key)) %>%
    ungroup()
  
  bound_breach_report_opt_summary <- bound_breach_report_opt %>%
    group_by(Category.Name) %>%
    summarise(Bound_Breach_Count_OPT = n_distinct(Opti_Key)) %>%
    ungroup()
  
  final_opti_summary <- opti_summary %>%
    left_join(price_alerts_as_is_summary) %>%
    left_join(price_alerts_opt_summary) %>%
    left_join(prod_alerts_as_is_summary) %>%
    left_join(prod_alerts_opt_summary) %>%
    left_join(bound_breach_report_as_is_summary) %>%
    left_join(bound_breach_report_opt_summary)
  
  return(final_opti_summary)
  
}
  

eq_rules_merged <- function(opti_res_prod_gap_merged_round, eq_rules){
  
  avg_pf_prices <-  opti_res_prod_gap_merged_round %>% 
    mutate(item_or_pf = ifelse(Price.Family == "No_Family", Sys.ID.Join, Price.Family)) %>%
    mutate(item_or_pf = as.character(item_or_pf)) %>%
    group_by(item_or_pf, Cluster) %>%
    summarise(avg_indep_price = mean(OPTIMIZED_PRICE_ROUNDED , na.rm = T)) %>%
    ungroup()

  dep_eq_prices <- eq_rules %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) %>%
    left_join(avg_pf_prices, c("Independent"="item_or_pf")) %>%
    mutate(avg_dep_price = round(((Multiplicative.Component*avg_indep_price)+Additive.Component),2)) %>%
    select(Dependent, Cluster, avg_dep_price)

  opti_res_prod_gap_merged_round_eq_merged <- opti_res_prod_gap_merged_round %>%
    mutate(item_or_pf = ifelse(Price.Family == "No_Family", Sys.ID.Join, Price.Family)) %>%
    mutate(item_or_pf = as.character(item_or_pf)) %>%
    left_join(dep_eq_prices, c("item_or_pf"="Dependent", "Cluster")) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(avg_dep_price), OPTIMIZED_PRICE_ROUNDED, avg_dep_price)) %>%
    select(-item_or_pf,-avg_dep_price)

  return(opti_res_prod_gap_merged_round_eq_merged)

}

# COMMAND ----------

optimal_weights_function <- function(opti_summary_all_weights){
  
  max_margin_margin_lift_bound =   5                              #Margin Lift Upper Bound for Max Margin Strategy
  max_margin_rev_lift_bound =      0                              #Revenue Lift Lower Bound for Max Margin Strategy
  grow_margin_rev_lift_bound =     0                              #Revenue Lift Lower Bound for Grow Margin Strategy
  units_lift_cutoff  =            -1                              #Optimal Weight Function Parameter
  distance_cutoff_margin =         2                              #Optimal Weight Function Parameter
  distance_cutoff_rev   =          2                              #Optimal Weight Function Parameter
  balanced_dist_cutoff  =          1                              #Optimal Weight Function Parameter

  max_rev_rev_lift_bound = max_margin_margin_lift_bound
  max_rev_margin_lift_bound = max_margin_rev_lift_bound
  grow_rev_margin_lift_bound = grow_margin_rev_lift_bound
  
  weights_all <- opti_summary_all_weights %>%
    select(SubCategory.Name, Optimization_Strategy, margin_lift, rev_lift, unit_lift, Margin.Weight, Revenue.Weight, Unit.Weight) %>%
    mutate(Abs_Diff = abs(margin_lift-rev_lift),
           Incr_Margin = margin_lift-rev_lift,
           Incr_Rev = rev_lift-margin_lift)
  colnames(weights_all)
  
  # Defining the Intersection Function
  intersection_func <- function(data, lift_cutoff){
    
    if(nrow(data) == 0){
      
      out <- data.frame(SubCategory.Name = c("Zero"), Type_2 = c("Zero"))
      out <- out[0,]
      return(out)
      
    }else{
      
      out <- data %>%
        select(SubCategory.Name, margin_lift, rev_lift) %>%
        mutate(pos_margin = ifelse(margin_lift>lift_cutoff,1,0),
               pos_revenue = ifelse(rev_lift>lift_cutoff,1,0),
               pos_sum = pos_margin + pos_revenue) %>%
        group_by(SubCategory.Name) %>%
        summarise(max_sum = max(pos_sum, na.rm = T)) %>%
        ungroup() %>%
        mutate(Type_2 = ifelse(max_sum == 2, "Intersection_Above", "Intersection_Below")) %>%
        select(-max_sum)
      
      return(out)
      
    }
    
  }
  
  ############# Identifying the Type #############
  
  sub_cat_type <- weights_all %>%
    select(SubCategory.Name, margin_lift, rev_lift, Margin.Weight) %>%
    filter(Margin.Weight %in% c(0,1)) %>%
    mutate("ML>RL" = margin_lift>rev_lift,
           Margin.Weight = paste0("Margin_Weight_", Margin.Weight)) %>%
    select(-rev_lift, -margin_lift) %>%
    spread(Margin.Weight, "ML>RL") %>%
    mutate(Type = ifelse((Margin_Weight_0 == TRUE & Margin_Weight_1 == TRUE), "Margin_Lift Higher Throughout",
                         ifelse((Margin_Weight_0 == FALSE & Margin_Weight_1 == FALSE), "Rev_Lift Higher Throughout", "Lifts Intersect"))) %>%
    select(-Margin_Weight_0, -Margin_Weight_1)
  
  full_weights_data <- weights_all %>%
    left_join(sub_cat_type, "SubCategory.Name")
  
  
  ############ Balancing Strategy ###############
  
  balancing_strategy_1 <- full_weights_data %>%
    filter(Optimization_Strategy == "Balanced",
           Type != "Lifts Intersect") 
  
  balancing_strategy_2 <- full_weights_data %>%
    filter(Optimization_Strategy == "Balanced",
           Type == "Lifts Intersect")
  
  if(nrow(balancing_strategy_1) == 0){
    
    balancing_strategy_1_rank <- NULL
    
  }else{
    
    balancing_strategy_1_rank <- balancing_strategy_1 %>%
      group_by(SubCategory.Name) %>%
      arrange(Abs_Diff) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
    
  }
  
  if(nrow(balancing_strategy_2) == 0){
    
    balancing_strategy_2_rank_above <- NULL
    balancing_strategy_2_rank_below <- NULL
    
  }else{
    
    intersect_dir <- intersection_func(balancing_strategy_2, lift_cutoff = 0)
    
    intersect_dir_above <- intersect_dir %>% filter(Type_2 == "Intersection_Above")
    intersect_dir_below <- intersect_dir %>% filter(Type_2 == "Intersection_Below")
    
    if(nrow(intersect_dir_above) == 0){
      
      balancing_strategy_2_rank_above <- NULL
      
    }else{
      
      extra_check <- balancing_strategy_2 %>%
        left_join(intersect_dir_above, "SubCategory.Name") %>%
        filter(!is.na(Type_2)) %>%
        select(-Type_2) %>%
        filter(margin_lift > 0 & rev_lift > 0 & margin_lift < 5 & rev_lift < 5,
               margin_lift > rev_lift) %>%
        group_by(SubCategory.Name) %>%
        arrange(Abs_Diff) %>%
        mutate(Rank = row_number()) %>%
        ungroup() %>%
        arrange(SubCategory.Name) %>%
        filter(Rank<=2) %>%
        group_by(SubCategory.Name) %>%
        mutate(Lag_Margin_Lift = lag(margin_lift,1),
               Lag_Rev_Lift = lag(rev_lift,1)) %>%
        filter(!is.na(Lag_Margin_Lift)) %>%
        mutate(distance = (margin_lift/Lag_Margin_Lift-1)-abs((rev_lift/Lag_Rev_Lift-1))) %>%
        ungroup() %>%
        filter(distance > balanced_dist_cutoff)
      
      balancing_strategy_2_rank_above_1 <- balancing_strategy_2 %>%
        left_join(intersect_dir_above, "SubCategory.Name") %>%
        filter(!is.na(Type_2)) %>%
        select(-Type_2) %>%
        filter(margin_lift > 0 & rev_lift > 0) %>%
        filter(!SubCategory.Name %in% extra_check$SubCategory.Name) %>%
        group_by(SubCategory.Name) %>%
        arrange(Abs_Diff) %>%
        mutate(Rank = row_number()) %>%
        ungroup() %>%
        arrange(SubCategory.Name)
      
      balancing_strategy_2_rank_above_2 <- balancing_strategy_2 %>%
        left_join(intersect_dir_above, "SubCategory.Name") %>%
        filter(!is.na(Type_2)) %>%
        select(-Type_2) %>%
        filter(margin_lift > 0 & rev_lift > 0) %>%
        filter(SubCategory.Name %in% extra_check$SubCategory.Name) %>%
        group_by(SubCategory.Name) %>%
        arrange(Abs_Diff) %>%
        mutate(Rank = row_number()) %>%
        ungroup() %>%
        arrange(SubCategory.Name) %>%
        filter(Rank == 2) %>%
        mutate(Rank = 1)
      
      balancing_strategy_2_rank_above <- bind_rows(balancing_strategy_2_rank_above_1, 
                                                   balancing_strategy_2_rank_above_2)
      
    }
    
    if(nrow(intersect_dir_below) == 0) {
      
      balancing_strategy_2_rank_below <- NULL
      
    }else{
      
      balancing_strategy_2_rank_below_1 <- balancing_strategy_2 %>%
        left_join(intersect_dir_below, "SubCategory.Name") %>%
        filter(!is.na(Type_2)) %>%
        select(-Type_2) %>%
        filter(margin_lift > 0 & margin_lift <= 0.5 & rev_lift > -1) %>%
        group_by(SubCategory.Name) %>%
        arrange(desc(Abs_Diff)) %>%
        mutate(Rank = row_number()) %>%
        ungroup() %>%
        arrange(SubCategory.Name)
      
      subcats_remained <- setdiff(intersect_dir_below$SubCategory.Name, 
                                  balancing_strategy_2_rank_below_1$SubCategory.Name)
      
      if(length(subcats_remained) ==0){
        
        balancing_strategy_2_rank_below_2 <- NULL
        
      }else{
        
        balancing_strategy_2_rank_below_2 <- balancing_strategy_2 %>%
          left_join(intersect_dir_below, "SubCategory.Name") %>%
          filter(!is.na(Type_2)) %>%
          select(-Type_2) %>%
          filter(SubCategory.Name %in% subcats_remained) %>%
          filter(margin_lift > 0 & rev_lift > -1) %>%
          group_by(SubCategory.Name) %>%
          arrange(Abs_Diff) %>%
          mutate(Rank = row_number()) %>%
          ungroup() %>%
          arrange(SubCategory.Name)
        
      }
      
      balancing_strategy_2_rank_below <- bind_rows(balancing_strategy_2_rank_below_1,
                                                   balancing_strategy_2_rank_below_2)
      
    }
    
  }
  
  ############# Maximise Margin and Grow Margin Strategy ##############
  
  ### "Margin_Lift Higher Throughout", "Rev_Lift Higher Throughout", "Lifts Intersect"
  
  margin_strategy_1 <- full_weights_data %>%
    filter(Optimization_Strategy %in% c("Maximize Margin", "Grow Margin"),
           Type == "Rev_Lift Higher Throughout")
  
  margin_strategy_2 <- full_weights_data %>%
    filter(Optimization_Strategy %in% c("Maximize Margin", "Grow Margin"),
           Type == "Margin_Lift Higher Throughout")
  
  margin_strategy_3 <- full_weights_data %>%
    filter(Optimization_Strategy %in% c("Maximize Margin", "Grow Margin"),
           Type == "Lifts Intersect")
  
  
  # Ranking those cases where we use desc(Incr_Margin) as Decision Metric
  if(nrow(margin_strategy_1) == 0){
    
    margin_strategy_1_rank <- NULL
    
  }else{
    
    margin_strategy_1_rank <- margin_strategy_1 %>%
      group_by(SubCategory.Name) %>%
      arrange(desc(Incr_Margin)) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
    
  }
  
  # Ranking those cases where we use asc(Incr_Margin) as Decision Metric
  if(nrow(bind_rows(margin_strategy_2, margin_strategy_3)) == 0){
    
    margin_strategy_2_rank <- NULL
    
  }else{
    
    # Filtering Classes such that either of the Bound is Breached
    rel_subcats_strategy_2 <- margin_strategy_2 %>%
      mutate(Margin_Bound = max_margin_margin_lift_bound,
             Rev_Bound = ifelse(Optimization_Strategy == "Maximize Margin", 
                                max_margin_rev_lift_bound, grow_margin_rev_lift_bound)) %>%
      filter(Margin.Weight == 0) %>%
      filter(!(margin_lift<Margin_Bound & rev_lift>Rev_Bound))
    
    # Finding Sub-Category Directions for Strategy 3
    rel_subcats_strategy_3 <- intersection_func(margin_strategy_3, lift_cutoff = max_margin_margin_lift_bound) %>% 
      filter(Type_2 == "Intersection_Above")
    
    rel_subcats <- c(unique(rel_subcats_strategy_2$SubCategory.Name), 
                     unique(rel_subcats_strategy_3$SubCategory.Name))
    
    margin_strategy_2_rank <- bind_rows(margin_strategy_2, margin_strategy_3) %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      filter(Incr_Margin > 0) %>%
      group_by(SubCategory.Name) %>%
      arrange(Incr_Margin) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
    
  }
  
  # Ranking those cases where we use desc(Incr_Margin) as Decision Metric
  if(nrow(bind_rows(margin_strategy_2, margin_strategy_3)) == 0){
    
    margin_strategy_3_rank_1 <- NULL
    
  }else{
    
    # Filtering Classes such that either of the Bound is Breached
    rel_subcats_strategy_2 <- margin_strategy_2 %>%
      mutate(Margin_Bound = max_margin_margin_lift_bound,
             Rev_Bound = ifelse(Optimization_Strategy == "Maximize Margin", 
                                max_margin_rev_lift_bound, grow_margin_rev_lift_bound)) %>%
      filter(Margin.Weight == 0) %>%
      filter(margin_lift<Margin_Bound & rev_lift>Rev_Bound)
    
    # Finding Sub-Category Directions for Strategy 3
    rel_subcats_strategy_3_1 <- intersection_func(margin_strategy_3 %>% filter(Optimization_Strategy == "Maximize Margin"), 
                                                  lift_cutoff = max_margin_rev_lift_bound) %>% filter(Type_2 == "Intersection_Above")
    rel_subcats_strategy_3_2 <- intersection_func(margin_strategy_3 %>% filter(Optimization_Strategy == "Grow Margin"), 
                                                  lift_cutoff = grow_margin_rev_lift_bound) %>% filter(Type_2 == "Intersection_Above")
    rel_subcats_strategy_3_3 <- intersection_func(margin_strategy_3, lift_cutoff = max_margin_margin_lift_bound) %>% 
      filter(Type_2 == "Intersection_Below")
    
    rel_subcats_strategy_3 <- intersect(union(rel_subcats_strategy_3_1$SubCategory.Name, 
                                              rel_subcats_strategy_3_2$SubCategory.Name),
                                        rel_subcats_strategy_3_3$SubCategory.Name)
    
    rel_subcats <- c(unique(rel_subcats_strategy_2$SubCategory.Name), rel_subcats_strategy_3)
    
    margin_strategy_3_rank_1_GM <- bind_rows(margin_strategy_2, margin_strategy_3) %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      mutate(Margin_Bound = max_margin_margin_lift_bound,
             Rev_Bound = ifelse(Optimization_Strategy == "Maximize Margin", 
                                max_margin_rev_lift_bound, grow_margin_rev_lift_bound)) %>%
      filter(Optimization_Strategy == "Grow Margin",
             margin_lift<Margin_Bound & rev_lift>Rev_Bound,
             margin_lift>rev_lift) %>%
      mutate(distance = abs((margin_lift/rev_lift-1)-distance_cutoff_margin)) %>%
      group_by(SubCategory.Name) %>%
      arrange(distance) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name) %>%
      select(-Margin_Bound, -Rev_Bound, -distance)
    
    units_check <- bind_rows(margin_strategy_2, margin_strategy_3) %>%
      filter(Optimization_Strategy == "Maximize Margin",
             SubCategory.Name %in% rel_subcats) %>%
      group_by(SubCategory.Name) %>%
      summarise(max_unit_lift = max(unit_lift, na.rm = T),
                max_above_unit_bound = ifelse(max_unit_lift>units_lift_cutoff, "Yes", "No")) %>%
      ungroup() %>%
      filter(max_above_unit_bound == "No")
    
    margin_strategy_3_rank_1_MM_1 <- bind_rows(margin_strategy_2, margin_strategy_3) %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      mutate(Margin_Bound = max_margin_margin_lift_bound,
             Rev_Bound = ifelse(Optimization_Strategy == "Maximize Margin", 
                                max_margin_rev_lift_bound, grow_margin_rev_lift_bound)) %>%
      filter(!SubCategory.Name %in% units_check$SubCategory.Name) %>%
      filter(Optimization_Strategy == "Maximize Margin",
             margin_lift<Margin_Bound & rev_lift>Rev_Bound & unit_lift>units_lift_cutoff) %>%
      group_by(SubCategory.Name) %>%
      arrange(desc(Incr_Margin)) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name) %>%
      select(-Margin_Bound, -Rev_Bound)
    
    margin_strategy_3_rank_1_MM_2 <- bind_rows(margin_strategy_2, margin_strategy_3) %>%
      filter(SubCategory.Name %in% units_check$SubCategory.Name) %>%
      group_by(SubCategory.Name) %>%
      arrange(Margin.Weight) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
    
    margin_strategy_3_rank_1 <- bind_rows(margin_strategy_3_rank_1_GM, margin_strategy_3_rank_1_MM_1,
                                          margin_strategy_3_rank_1_MM_2)
    
  }
  
  # Ranking those cases intersection is below the Rev Bound
  if(nrow(margin_strategy_3) == 0){
    
    margin_strategy_3_rank_2 <- NULL
    
  }else{
    
    
    # Finding Sub-Category Directions for Strategy 3
    rel_subcats_strategy_3_1 <- intersection_func(margin_strategy_3 %>% filter(Optimization_Strategy == "Maximize Margin"), 
                                                  lift_cutoff = max_margin_rev_lift_bound) %>% filter(Type_2 == "Intersection_Below")
    rel_subcats_strategy_3_2 <- intersection_func(margin_strategy_3 %>% filter(Optimization_Strategy == "Grow Margin"), 
                                                  lift_cutoff = grow_margin_rev_lift_bound) %>% filter(Type_2 == "Intersection_Below")
    
    rel_subcats_strategy_3 <- union(rel_subcats_strategy_3_1$SubCategory.Name,
                                    rel_subcats_strategy_3_2$SubCategory.Name)
    
    rel_subcats <- c(rel_subcats_strategy_3)
    
    margin_strategy_3_rank_2 <- margin_strategy_3 %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      filter(Incr_Margin>0,
             rev_lift > -1) %>%
      group_by(SubCategory.Name) %>%
      arrange(desc(Incr_Margin)) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
  }
  
  
  ############# Maximise Revenue and Grow Revenue Strategy ##############
  
  ### "Rev_Lift Higher Throughout", "Margin_Lift Higher Throughout", "Lifts Intersect"
  
  rev_strategy_1 <- full_weights_data %>%
    filter(Optimization_Strategy %in% c("Maximize Revenue", "Grow Revenue"),
           Type == "Margin_Lift Higher Throughout")
  
  rev_strategy_2 <- full_weights_data %>%
    filter(Optimization_Strategy %in% c("Maximize Revenue", "Grow Revenue"),
           Type == "Rev_Lift Higher Throughout")
  
  rev_strategy_3 <- full_weights_data %>%
    filter(Optimization_Strategy %in% c("Maximize Revenue", "Grow Revenue"),
           Type == "Lifts Intersect")
  
  
  # Ranking those cases where we use desc(Incr_Rev) as Decision Metric
  if(nrow(rev_strategy_1) == 0){
    
    rev_strategy_1_rank <- NULL
    
  }else{
    
    rev_strategy_1_rank <- rev_strategy_1 %>%
      group_by(SubCategory.Name) %>%
      arrange(desc(Incr_Rev)) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
    
  }
  
  # Ranking those cases where we use asc(Incr_Rev) as Decision Metric
  if(nrow(bind_rows(rev_strategy_2, rev_strategy_3)) == 0){
    
    rev_strategy_2_rank <- NULL
    
  }else{
    
    # Filtering Classes such that either of the Bound is Breached
    rel_subcats_strategy_2 <- rev_strategy_2 %>%
      mutate(Rev_Bound = max_rev_rev_lift_bound,
             Margin_Bound = ifelse(Optimization_Strategy == "Maximize Revenue", 
                                   max_rev_margin_lift_bound, grow_rev_margin_lift_bound)) %>%
      filter(Revenue.Weight == 0) %>%
      filter(!(rev_lift<Rev_Bound & margin_lift>Margin_Bound))
    
    # Finding Sub-Category Directions for Strategy 3
    rel_subcats_strategy_3 <- intersection_func(rev_strategy_3, lift_cutoff = max_rev_rev_lift_bound) %>% 
      filter(Type_2 == "Intersection_Above")
    
    rel_subcats <- c(unique(rel_subcats_strategy_2$SubCategory.Name), 
                     unique(rel_subcats_strategy_3$SubCategory.Name))
    
    rev_strategy_2_rank <- bind_rows(rev_strategy_2, rev_strategy_3) %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      filter(Incr_Rev > 0) %>%
      group_by(SubCategory.Name) %>%
      arrange(Incr_Rev) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
    
  }
  
  # Ranking those cases where we use desc(Incr_Rev) as Decision Metric
  if(nrow(bind_rows(rev_strategy_2, rev_strategy_3)) == 0){
    
    rev_strategy_3_rank_1 <- NULL
    
  }else{
    
    # Filtering Classes such that either of the Bound is Breached
    rel_subcats_strategy_2 <- rev_strategy_2 %>%
      mutate(Rev_Bound = max_rev_rev_lift_bound,
             Margin_Bound = ifelse(Optimization_Strategy == "Maximize Revenue", 
                                   max_rev_margin_lift_bound, grow_rev_margin_lift_bound)) %>%
      filter(Revenue.Weight == 0) %>%
      filter(rev_lift<Rev_Bound & margin_lift>Margin_Bound)
    
    # Finding Sub-Category Directions for Strategy 3
    rel_subcats_strategy_3_1 <- intersection_func(rev_strategy_3 %>% filter(Optimization_Strategy == "Maximize Revenue"), 
                                                  lift_cutoff = max_rev_margin_lift_bound) %>% filter(Type_2 == "Intersection_Above")
    rel_subcats_strategy_3_2 <- intersection_func(rev_strategy_3 %>% filter(Optimization_Strategy == "Grow Revenue"), 
                                                  lift_cutoff = grow_rev_margin_lift_bound) %>% filter(Type_2 == "Intersection_Above")
    rel_subcats_strategy_3_3 <- intersection_func(rev_strategy_3, lift_cutoff = max_rev_rev_lift_bound) %>% filter(Type_2 == "Intersection_Below")
    
    rel_subcats_strategy_3 <- intersect(union(rel_subcats_strategy_3_1$SubCategory.Name, 
                                              rel_subcats_strategy_3_2$SubCategory.Name),
                                        rel_subcats_strategy_3_3$SubCategory.Name)
    
    rel_subcats <- c(unique(rel_subcats_strategy_2$SubCategory.Name), rel_subcats_strategy_3)
    
    rev_strategy_3_rank_1_GR <- bind_rows(rev_strategy_2, rev_strategy_3) %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      mutate(Rev_Bound = max_rev_rev_lift_bound,
             Margin_Bound = ifelse(Optimization_Strategy == "Maximize Revenue", 
                                   max_rev_margin_lift_bound, grow_rev_margin_lift_bound)) %>%
      filter(Optimization_Strategy == "Grow Revenue",
             rev_lift<Rev_Bound & margin_lift>Margin_Bound,
             rev_lift>margin_lift) %>%
      mutate(distance = abs((rev_lift/margin_lift-1)-distance_cutoff_rev)) %>%
      group_by(SubCategory.Name) %>%
      arrange(distance) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name) %>%
      select(-Rev_Bound, -Margin_Bound, -distance)
    
    rev_strategy_3_rank_1_MR <- bind_rows(rev_strategy_2, rev_strategy_3) %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      mutate(Rev_Bound = max_rev_rev_lift_bound,
             Margin_Bound = ifelse(Optimization_Strategy == "Maximize Revenue", 
                                   max_rev_margin_lift_bound, grow_rev_margin_lift_bound)) %>%
      filter(Optimization_Strategy == "Maximize Revenue",
             rev_lift<Rev_Bound & margin_lift>Margin_Bound) %>%
      group_by(SubCategory.Name) %>%
      arrange(desc(Incr_Rev)) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name) %>%
      select(-Rev_Bound, -Margin_Bound)
    
    rev_strategy_3_rank_1 <- bind_rows(rev_strategy_3_rank_1_GR, rev_strategy_3_rank_1_MR)
    
  }
  
  # Ranking those cases intersection is below the Rev Bound
  if(nrow(rev_strategy_3) == 0){
    
    rev_strategy_3_rank_2 <- NULL
    
  }else{
    
    
    # Finding Sub-Category Directions for Strategy 3
    rel_subcats_strategy_3_1 <- intersection_func(rev_strategy_3 %>% filter(Optimization_Strategy == "Maximize Revenue"), 
                                                  lift_cutoff = max_rev_margin_lift_bound) %>% filter(Type_2 == "Intersection_Below")
    rel_subcats_strategy_3_2 <- intersection_func(rev_strategy_3 %>% filter(Optimization_Strategy == "Grow Revenue"), 
                                                  lift_cutoff = grow_rev_margin_lift_bound) %>% filter(Type_2 == "Intersection_Below")
    
    rel_subcats_strategy_3 <- union(rel_subcats_strategy_3_1$SubCategory.Name,
                                    rel_subcats_strategy_3_2$SubCategory.Name)
    
    rel_subcats <- c(rel_subcats_strategy_3)
    
    rev_strategy_3_rank_2 <- rev_strategy_3 %>%
      filter(SubCategory.Name %in% rel_subcats) %>%
      filter(Incr_Rev>0,
             margin_lift > -1) %>%
      group_by(SubCategory.Name) %>%
      arrange(desc(Incr_Rev)) %>%
      mutate(Rank = row_number()) %>%
      ungroup() %>%
      arrange(SubCategory.Name)
    
  }
  
  final_weights <- bind_rows(balancing_strategy_1_rank,
                             balancing_strategy_2_rank_above,
                             balancing_strategy_2_rank_below,
                             margin_strategy_1_rank,
                             margin_strategy_2_rank,
                             margin_strategy_3_rank_1,
                             margin_strategy_3_rank_2,
                             rev_strategy_1_rank,
                             rev_strategy_2_rank,
                             rev_strategy_3_rank_1,
                             rev_strategy_3_rank_2) %>%
    filter(Rank == 1) %>%
    select(SubCategory.Name, Optimization_Strategy, margin_lift, rev_lift, unit_lift, Margin.Weight, Revenue.Weight, Unit.Weight) %>%
    rename(Margin.Weight.Opti = Margin.Weight, 
           Revenue.Weight.Opti = Revenue.Weight,
           Unit.Weight.Opti = Unit.Weight)
  
  
  return(final_weights)
  
}


# COMMAND ----------

cat_wise_opt_cons <- function(key){
  
  print(key)
  
  opti_data <- Decis_vars_Final%>%
                  filter(Opti_Key == key & Category.Name == x)

  x0_curr <- as.vector(opti_data$Regular.Price)            #Regular.Price
  x0_mean <- as.vector(opti_data$start_mean)               #start mean
  LB_data <- as.vector(opti_data$Final.Price.LB.Family2)
  UB_data <- as.vector(opti_data$Final.Price.UB.Family2)

  print(LB_data)
  print(UB_data)

  optimization_data <- LB_UB_Final %>%
            filter(Opti_Key == key & Category.Name == x) %>%
            left_join(weight_map)
  
  units_lift_cutoff = mean(optimization_data$Units_Constraint)

  eval_f1 <- function(bp_adj) {

    optimization_data$BASE_PRICE_ADJUSTED = bp_adj

    optimization_func_data <- optimization_data %>%
            mutate(Base.Price.Elasticity = as.numeric(Base.Price.Elasticity)) %>%
            mutate(BASE_PRICE_ADJUSTED = round(BASE_PRICE_ADJUSTED,2)) %>%
            mutate(base_rev = Base.Units*(Regular.Price/(1+`Vat%`)),
                   base_margin = Base.Units*(Regular.Price/(1+`Vat%`) - Per.Unit.Cost),
                   opti_units = Base.Units + ((Base.Units)*((BASE_PRICE_ADJUSTED/Regular.Price)-1)*(Base.Price.Elasticity)),
                   opti_rev = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`)),
                   opti_margin = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`) - Per.Unit.Cost)) %>%
            mutate(Objective = Margin.Weight*opti_margin + Revenue.Weight*opti_rev)

    opti_index <- sum(optimization_func_data$Objective)

    return(-(opti_index))

  }
  
  eval_ineq_cons <- function(bp_adj){

      optimization_data$BASE_PRICE_ADJUSTED = bp_adj

      optimization_cons_data <- optimization_data %>%
              mutate(Base.Price.Elasticity = as.numeric(Base.Price.Elasticity)) %>%
              mutate(BASE_PRICE_ADJUSTED = round(BASE_PRICE_ADJUSTED,2)) %>%
              mutate(base_rev = Base.Units*(Regular.Price/(1+`Vat%`)),
                     base_margin = Base.Units*(Regular.Price/(1+`Vat%`) - Per.Unit.Cost),
                     opti_units = Base.Units + ((Base.Units)*((BASE_PRICE_ADJUSTED/Regular.Price)-1)*(Base.Price.Elasticity)),
                     opti_rev = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`)),
                     opti_margin = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`) - Per.Unit.Cost)) %>%
              rename(OPTIMIZED_PRICE_ROUNDED = BASE_PRICE_ADJUSTED) %>%
              left_join(test_stores_count) %>%
              mutate(Base.Units_tot = Base.Units*Test_Stores,
                     base_rev_tot = base_rev*Test_Stores,
                     base_margin_tot = base_margin*Test_Stores,
                     opti_units_tot = opti_units*Test_Stores,
                     opti_rev_tot = opti_rev*Test_Stores,
                     opti_margin_tot = opti_margin*Test_Stores)

      units_lift = sum(optimization_cons_data$opti_units_tot)/sum(optimization_cons_data$Base.Units_tot) - 1

    constr <- rbind(units_lift_cutoff - units_lift)

    return(constr)

  }

  opts <- list("algorithm"= "NLOPT_LN_COBYLA",   #"NLOPT_LD_SLSQP","NLOPT_LN_COBYLA","NLOPT_GN_DIRECT_L_NOSCAL"
                 "xtol_rel"=1.0e-6,
                 "ftol_rel"=1.0e-6,
                 "print_level"=3,
                 "maxeval"=1000
                 )

#   print(x0_curr)

#   opti_curr <-  nloptr( x0=x0_curr, 
#                  eval_f=eval_f1,
#                  lb=LB_data,
#                  ub=UB_data,
#                  opts=opts
#                 )

  print(x0_mean)

  opti_mean <-  nloptr(x0=x0_mean,
                       eval_f=eval_f1,
                       eval_g_ineq = eval_ineq_cons,
                       lb=LB_data,
                       ub=UB_data,
                       opts=opts
                      )

  print(opti_mean$solution)
  optimization_data$BASE_PRICE_ADJUSTED <- as.vector(opti_mean$solution)

  #calculate metrics based on optimized prices
  optimization_data_final <- optimization_data %>%
    mutate(Opti_Key = ifelse((Price.Family != "No_Family"), paste0(Price.Family,"_", Cluster), Opti_Key)) %>%
    select(-Margin.Weight,-Revenue.Weight) %>%
    mutate(Base.Price.Elasticity = as.numeric(Base.Price.Elasticity)) %>%
    mutate(BASE_PRICE_ADJUSTED = round(BASE_PRICE_ADJUSTED,2)) %>%
    mutate(base_rev = Base.Units*(Regular.Price/(1+`Vat%`)),
           base_margin = Base.Units*(Regular.Price/(1+`Vat%`) - Per.Unit.Cost),
           opti_units = Base.Units + ((Base.Units)*((BASE_PRICE_ADJUSTED/Regular.Price)-1)*(Base.Price.Elasticity)),
           opti_rev = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`)),
           opti_margin = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`) - Per.Unit.Cost))%>%
    rename(OPTIMIZED_PRICE_ROUNDED = BASE_PRICE_ADJUSTED) %>%
    left_join(test_stores_count) %>%
    mutate(Base.Units_tot = Base.Units*Test_Stores,
           base_rev_tot = base_rev*Test_Stores,
           base_margin_tot = base_margin*Test_Stores,
           opti_units_tot = opti_units*Test_Stores,
           opti_rev_tot = opti_rev*Test_Stores,
           opti_margin_tot = opti_margin*Test_Stores) %>%
    select(-Units_Constraint)

  print(optimization_data_final$OPTIMIZED_PRICE_ROUNDED) 
  
  return(optimization_data_final)
  
}




# COMMAND ----------

## Adding a unit based parameter for Unit weights in optimization

cat_wise_opt_cons_units_beta <- function(key){
  
  
  
  opti_data <- Decis_vars_Final%>%
                  filter(Opti_Key == key & Category.Name == x)%>%mutate(start_mean = as.numeric(start_mean), Regular.Price = as.numeric(Regular.Price)) 

  x0_curr <- as.vector(opti_data$Regular.Price)            #Regular.Price
  x0_mean <- as.vector(opti_data$start_mean)               #start mean
  LB_data <- as.vector(opti_data$Final.Price.LB.Family2)
  UB_data <- as.vector(opti_data$Final.Price.UB.Family2)

#   print(LB_data)
#   print(UB_data)

  optimization_data <- LB_UB_Final %>%
            filter(Opti_Key == key & Category.Name == x) %>%
            left_join(weight_map)
  
  units_lift_cutoff = mean(optimization_data$Units_Constraint)

  eval_f1 <- function(bp_adj) {

    optimization_data$BASE_PRICE_ADJUSTED = bp_adj

    optimization_func_data <- optimization_data %>%
            mutate(Base.Price.Elasticity = ifelse(is.na(Base.Price.Elasticity), -1.01, as.numeric(Base.Price.Elasticity))) %>%  ## dash temp NA fix for Elasticity
            mutate(BASE_PRICE_ADJUSTED = round(BASE_PRICE_ADJUSTED,2)) %>%
            mutate(base_rev = Base.Units*(Regular.Price/(1+`Vat%`)),
                   Base.Units = Base.Units,                                                    ## dash new col
                   base_margin = Base.Units*(Regular.Price/(1+`Vat%`) - Per.Unit.Cost),
                   opti_units = Base.Units + ((Base.Units)*((BASE_PRICE_ADJUSTED/Regular.Price)-1)*(Base.Price.Elasticity)),
                   opti_rev = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`)),
                   opti_margin = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`) - Per.Unit.Cost)) %>%
    #Adding unit weight in the objective function
            mutate(Objective = Margin.Weight*opti_margin + Revenue.Weight*opti_rev + Unit.Weight*opti_units)

    opti_index <- sum(optimization_func_data$Objective)

    return(-(opti_index))

  }
  
  eval_ineq_cons <- function(bp_adj){

      optimization_data$BASE_PRICE_ADJUSTED = bp_adj

      optimization_cons_data <- optimization_data %>%
              mutate(Base.Price.Elasticity = ifelse(is.na(Base.Price.Elasticity), -1.01, as.numeric(Base.Price.Elasticity))) %>%  ## dash temp NA fix for Elasticity
              mutate(BASE_PRICE_ADJUSTED = round(BASE_PRICE_ADJUSTED,2)) %>%
              mutate(base_rev = Base.Units*(Regular.Price/(1+`Vat%`)),
                     Base.Units = Base.Units,                                                    ## dash new col
                     base_margin = Base.Units*(Regular.Price/(1+`Vat%`) - Per.Unit.Cost),
                     opti_units = Base.Units + ((Base.Units)*((BASE_PRICE_ADJUSTED/Regular.Price)-1)*(Base.Price.Elasticity)),
                     opti_rev = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`)),
                     opti_margin = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`) - Per.Unit.Cost)) %>%
              rename(OPTIMIZED_PRICE_ROUNDED = BASE_PRICE_ADJUSTED) %>% select (-site_state_id) %>%  ## dash site_state_id
              left_join(test_stores_count, c("Sys.ID.Join", "Cluster", "contract")) %>%           ## dash join key
              mutate(Test_Stores = ifelse(is.na(Test_Stores), 1, Test_Stores)) %>%               ## dash temp to avoid NA errors
              mutate(Base.Units_tot = Base.Units*Test_Stores,
                     base_rev_tot = base_rev*Test_Stores,
                     base_margin_tot = base_margin*Test_Stores,
                     opti_units_tot = opti_units*Test_Stores,
                     opti_rev_tot = opti_rev*Test_Stores,
                     opti_margin_tot = opti_margin*Test_Stores)

      units_lift = (sum(optimization_cons_data$opti_units_tot) + 0.0001)/(sum(optimization_cons_data$Base.Units_tot) + 0.0001) - 1

    constr <- rbind(units_lift_cutoff - units_lift)

    return(constr)

  }

  opts <- list("algorithm"= "NLOPT_LN_COBYLA",   #"NLOPT_LD_SLSQP","NLOPT_LN_COBYLA","NLOPT_GN_DIRECT_L_NOSCAL"
                 "xtol_rel"=1.0e-6,
                 "ftol_rel"=1.0e-6,
                 "print_level"=3,
                 "maxeval"=10000
                 )

#   print(x0_curr)

  ## dash - not part of current model - experiental parameter
#   opti_curr <-  nloptr( x0=x0_curr, 
#                  eval_f=eval_f1,
#                  lb=LB_data,
#                  ub=UB_data,
#                  opts=opts
#                 )

  print(x0_mean)

  opti_mean <-  nloptr(x0=x0_mean,
                       eval_f=eval_f1,
                       eval_g_ineq = eval_ineq_cons,
                       lb=LB_data,
                       ub=UB_data,
                       opts=opts
                      )

  #print(opti_mean$solution)
  optimization_data$BASE_PRICE_ADJUSTED <- as.vector(opti_mean$solution)

  #calculate metrics based on optimized prices
  optimization_data_final <- optimization_data %>%
    mutate(Opti_Key = ifelse((Price.Family != "No_Family"), paste0(Price.Family,"_", Cluster), Opti_Key)) %>%
    select(-Margin.Weight,-Revenue.Weight) %>%
    mutate(Base.Price.Elasticity = ifelse(is.na(Base.Price.Elasticity), -1.01, as.numeric(Base.Price.Elasticity))) %>%  ## dash temp NA fix for Elasticity
    mutate(BASE_PRICE_ADJUSTED = round(BASE_PRICE_ADJUSTED,2)) %>%
    mutate(base_rev = Base.Units*(Regular.Price/(1+`Vat%`)),
           Base.Units = Base.Units,                                                    ## dash new col
           base_margin = Base.Units*(Regular.Price/(1+`Vat%`) - Per.Unit.Cost),
           opti_units = Base.Units + ((Base.Units)*((BASE_PRICE_ADJUSTED/Regular.Price)-1)*(Base.Price.Elasticity)),
           opti_rev = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`)),
           opti_margin = opti_units*(BASE_PRICE_ADJUSTED/(1+`Vat%`) - Per.Unit.Cost))%>%
    rename(OPTIMIZED_PRICE_ROUNDED = BASE_PRICE_ADJUSTED) %>% select (-site_state_id) %>%  ## dash site_state_id
    left_join(test_stores_count, c("Sys.ID.Join", "Cluster", "contract")) %>%           ## dash join key
    mutate(Test_Stores = ifelse(is.na(Test_Stores), 1, Test_Stores)) %>%               ## dash temp to avoid NA errors
    mutate(Base.Units_tot = Base.Units*Test_Stores,
           base_rev_tot = base_rev*Test_Stores,
           base_margin_tot = base_margin*Test_Stores,
           opti_units_tot = opti_units*Test_Stores,
           opti_rev_tot = opti_rev*Test_Stores,
           opti_margin_tot = opti_margin*Test_Stores) %>%
    select(-Units_Constraint)

  #print(optimization_data_final$OPTIMIZED_PRICE_ROUNDED) 
  
  return(optimization_data_final)
  
}

# COMMAND ----------


round_off_function <- function(initial_price_point, initial_round_decimal_point, range, range_1, range_2, range_3, range_4, ending_number, 
                               ending_number_1, ending_number_2, ending_number_3, ending_number_4, subtraction, map_to){
  
  
  if(is.na(subtraction)){subtraction = 0}
  
  if(((range == "N") & (is.na(ending_number)) & (is.na(ending_number_1)))){
    
    if(initial_round_decimal_point == 0){
      
      round_off_price = round(initial_price_point, initial_round_decimal_point) - subtraction
      
      if((map_to == "Upper") & (round_off_price<initial_price_point)){
        return(round_off_price+1)
      }else if((map_to == "Lower") & (round_off_price>initial_price_point)){
        return(round_off_price-1)
      }else{
        return(round_off_price)
      }
        
    }else{
      return(round(initial_price_point, initial_round_decimal_point) - subtraction) 
    }
    
  }else if(range == "Selective"){
    
    ending_numbers_allowed <- as.numeric(str_split(ending_number,",")[[1]])
    round_off_price = round(initial_price_point, 0) - subtraction
    possible_price_points = unique(c((round_off_price + ending_numbers_allowed), (round_off_price - (1-ending_numbers_allowed))))
    if(map_to == "Upper"){
      return(min(possible_price_points[possible_price_points>=initial_price_point]))
    }else if(map_to == "Lower"){
      return(max(possible_price_points[possible_price_points<=initial_price_point]))
    }else{
      return(possible_price_points[which.min(abs(possible_price_points-initial_price_point))])
    }
      
  }else if(range == "Range and Selective"){
  
    if(initial_price_point < range_1){

      ending_number = ending_number_1

    }else if(initial_price_point < range_2){

      ending_number = ending_number_2

    }else if(initial_price_point < range_3){

      ending_number = ending_number_3

    }else if(initial_price_point < range_4){

      ending_number = ending_number_4

    }else{

      stop("Something is wrong with the ranges, check before proceeding!")

    }

    if(is.na(ending_number)){

      round_off_price = round(initial_price_point, 0) - subtraction

      if((map_to == "Upper") & (round_off_price<initial_price_point)){
        return(round_off_price+1)
      }else if((map_to == "Lower") & (round_off_price>initial_price_point)){
        return(round_off_price-1)
      }else{
        return(round_off_price)
      }

    }else{

      ending_numbers_allowed <- as.numeric(str_split(ending_number,",")[[1]])
      #ending_numbers_allowed <- as.numeric(str_extract_all(ending_number,"\\d")[[1]])
      round_off_price = round(initial_price_point, 0)
      possible_price_points = unique(c((round_off_price + ending_numbers_allowed), (round_off_price - (1-ending_numbers_allowed))))
      if(map_to == "Upper"){
        return(min(possible_price_points[possible_price_points>=initial_price_point]))
      }else if(map_to == "Lower"){
        return(max(possible_price_points[possible_price_points<=initial_price_point]))
      }else{
        return(possible_price_points[which.min(abs(possible_price_points-initial_price_point))])
      }

    }

  }else{
    
  # Initial Operations
  initial_price_point <- round(initial_price_point, initial_round_decimal_point)
  
  # Mapping the Correct Ending Numbers for cases where Ending Numbers are different for different ranges of Prices
  if(range == "Y"){
    
    if(initial_price_point < range_1){
      
      ending_number = ending_number_1
      
    }else if(initial_price_point < range_2){
      
      ending_number = ending_number_2
      
    }else if(initial_price_point < range_3){
      
      ending_number = ending_number_3
      
    }else if(initial_price_point < range_4){
      
      ending_number = ending_number_4
      
    }else{
      
      stop("Something is wrong with the ranges, check before proceeding!")
      
    }
    
  }
  
  # If No Ending Rule is Defined
  if(is.na(ending_number)){
    
    return(round((initial_price_point-subtraction),initial_round_decimal_point))
    
  }else{
    
    # Creating the 10 Possible Price Points on the Upper and Lower Side of the given Price Point
    df_upper <- data.frame(Upper_Side = double(10), stringsAsFactors = F)
    df_lower <- data.frame(Lower_Side = double(10), stringsAsFactors = F)
    
    decimal_scale = 1/10^initial_round_decimal_point
    ending_numbers_allowed <- as.numeric(str_extract_all(ending_number,"\\d")[[1]])
    
    for(i in c(1:10)){
      df_upper$Upper_Side[i] = initial_price_point + (i-1)*decimal_scale
      df_lower$Lower_Side[i] = initial_price_point - (i-1)*decimal_scale
    }
    
    # Deriving the Ending Number- Units Place Digit
    df_upper$ending_number <- as.numeric(str_sub(format(df_upper$Upper_Side), -1))
    df_lower$ending_number <- as.numeric(str_sub(format(df_lower$Lower_Side), -1))
    
    # Filtering the set of Ending Numbers as given
    df_updated <- df_upper %>%
      left_join(df_lower, "ending_number") %>%
      select(ending_number, Upper_Side, Lower_Side) %>%
      mutate(abs_diff_upper = abs(Upper_Side- initial_price_point),
             abs_diff_lower = abs(Lower_Side- initial_price_point)) %>%
      filter(ending_number %in% ending_numbers_allowed)
    
    # Choosing the Best Price Point on the Upper Side, Lower Side or Closest of the Both
    closest_upper <- df_updated$Upper_Side[which.min(df_updated$abs_diff_upper)]
    closest_lower <- df_updated$Lower_Side[which.min(df_updated$abs_diff_lower)]
    min_diff <- min(df_updated$abs_diff_upper[which.min(df_updated$abs_diff_upper)],
                    df_updated$abs_diff_lower[which.min(df_updated$abs_diff_lower)])
    closest_overall <- max(df_updated$Upper_Side[df_updated$abs_diff_upper == min_diff],
                           df_updated$Lower_Side[df_updated$abs_diff_lower == min_diff])
    
    
    # Returning the Final Price Point based on the "map_to" Strategy
    if(map_to == "Upper"){
      final_price_point = closest_upper-subtraction
    }else if(map_to == "Lower"){
      final_price_point = closest_lower-subtraction
    }else{
      final_price_point = closest_overall-subtraction
    }
    
    return(round(final_price_point,initial_round_decimal_point))
     
  }
    
 }
  
}

# COMMAND ----------

prod_gap_function_as_is_beta <- function(item_data, prod_gap){
  
  item_data <- item_data %>% mutate(Sys.ID.Join =  as.character(Sys.ID.Join),
                                   Regular.Price = as.numeric(Regular.Price),
                                   Per.Unit.Cost = as.numeric(Per.Unit.Cost))   ## dash dtype
  
  prod_gap_weights <- prod_gap %>% 
    filter(!is.na(Weight.Dep)) %>% 
    select(Category.Name.Dep,Category.Dep,Category.Indep,Dependent,Independent,Initial.Rule,Weight.Dep,Weight.Indep) %>% 
    unique() %>% mutate(Dependent = as.character(Dependent), 
                        Independent = as.character(Independent),
                        Weight.Dep = as.numeric(Weight.Dep),
                        Weight.Indep = as.numeric(Weight.Indep)) ## dash dtype
  
 
  prod_gap <- prod_gap %>% select(-Weight.Dep,-Weight.Indep) %>% mutate(Dependent = as.character(Dependent), 
                                                                        Independent = as.character(Independent)) %>%   ## dash dtype
  left_join(prod_gap_weights)
  
  
  # Getting the Price and Cost Data from the Item Level Data

  # Getting the Rel Items and Rel Price Families having Prod Gap Rules
  rel_PF_dep <- unique(prod_gap$Dependent[prod_gap$Category.Dep == "PF"])
  rel_PF_indep <- unique(prod_gap$Independent[prod_gap$Category.Indep == "PF"])
  rel_PF <- union(rel_PF_dep, rel_PF_indep)
  rel_items_dep <- unique(prod_gap$Dependent[prod_gap$Category.Dep == "Item"])
  rel_items_indep <- unique(prod_gap$Independent[prod_gap$Category.Indep == "Item"])
  rel_items <- union(rel_items_dep, rel_items_indep)

  item_data$`Vat%`[is.na(item_data$`Vat%`)] <- 0

  # Subsetting the Price Families having the Prod Gap Rules
  item_data_pf <- item_data %>%
    select(Cluster, Price.Family, Regular.Price, Per.Unit.Cost, "Vat%") %>%
    filter(Price.Family %in% rel_PF) %>%
    group_by( Cluster, Price.Family) %>%
    summarise_all(funs(mean(.,na.rm=T))) %>%
    ungroup()

  # Subsetting the Items having the Prod Gap Rules
  item_data_item <- item_data %>%
    select(Sys.ID.Join, Cluster, Regular.Price, Per.Unit.Cost, "Vat%") %>%
    filter(Sys.ID.Join %in% rel_items)

  # Joining the Price and Cost Metrics in the Prod Gap File for PF to PF Rule
  prod_gap_joined_1 <- prod_gap %>%
    filter((Category.Dep == "PF") & (Category.Indep == "PF")) %>%
    left_join(item_data_pf, c("Dependent" = "Price.Family")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_pf, c("Independent" = "Price.Family", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Joining the Price and Cost Metrics in the Prod Gap File for PF to Item Rule
  prod_gap_joined_2 <- prod_gap %>%
    filter((Category.Dep == "PF") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) %>% ## dash dtype
    left_join(item_data_pf, c("Dependent" = "Price.Family")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_item, c("Independent" = "Sys.ID.Join", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Joining the Price and Cost Metrics in the Prod Gap File for Item to PF Rule
  prod_gap_joined_3 <- prod_gap %>%
    filter((Category.Dep == "Item") & (Category.Indep == "PF")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%
    left_join(item_data_item, c("Dependent" = "Sys.ID.Join")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_pf, c("Independent" = "Price.Family", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Joining the Price and Cost Metrics in the Prod Gap File for Item to Item Rule
  prod_gap_joined_4 <- prod_gap %>%
    filter((Category.Dep == "Item") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%  ## dash dtype
    left_join(item_data_item, c("Dependent" = "Sys.ID.Join")) %>%
    rename(Price.Dep = Regular.Price, 
           Cost.Dep = Per.Unit.Cost, 
           Vat.Dep = "Vat%") %>%
    left_join(item_data_item, c("Independent" = "Sys.ID.Join", "Cluster")) %>%
    rename(Price.Indep = Regular.Price, 
           Cost.Indep = Per.Unit.Cost, 
           Vat.Indep = "Vat%") %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent))

  # Appending all the Cases
  prod_gap_merged <- bind_rows(prod_gap_joined_1, prod_gap_joined_2, prod_gap_joined_3, prod_gap_joined_4) %>%
    mutate(Value.Dep = Weight.Dep/Price.Dep,
           Value.Indep = Weight.Indep/Price.Indep,
           Margin.Dep = (Price.Dep/(1+Vat.Dep) - Cost.Dep),
           Margin.Indep = (Price.Indep/(1+Vat.Indep) - Cost.Indep),
           Margin.Dep.Perc = ((Price.Dep/(1+Vat.Dep) - Cost.Dep)/(Price.Dep/(1+Vat.Dep))),
           Margin.Indep.Perc = ((Price.Indep/(1+Vat.Indep) - Cost.Indep)/(Price.Indep/(1+Vat.Indep))))

  # Dividing the Product Gap Rules based the 3 Types: Price, Value and Margin and then we check Breaches

  # Price Rule Breach
  prod_gap_rules_price <- prod_gap_merged %>%
    filter(Rule.Classification == "Price") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Price.Dep>Price.Indep, "Good",
                                 ifelse(Side == "Lower" & Price.Dep<Price.Indep, "Good",
                                        ifelse(Side == "Within", "Good",
                                               ifelse(Side == "Same" & abs(Price.Dep-Price.Indep)<=0.01, "Good", "Issue"))))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Price.Dep/Price.Indep-1), round(abs(Price.Dep-Price.Indep),2))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Price Breach"),
                        ifelse(((difference>Max | difference<Min) & (Side != "Same") & (Type != "Absolute")), paste0(Side, " Price Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Value Rule Breach
  prod_gap_rules_value <- prod_gap_merged %>%
    filter(Rule.Classification == "Value") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Value.Dep>Value.Indep, "Good",
                                 ifelse(Side == "Lower" & Value.Dep<Value.Indep, "Good",
                                        ifelse(Side == "Same" & Value.Dep==Value.Indep, "Good", "Issue")))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Value.Dep/Value.Indep-1), abs(Value.Dep-Value.Indep))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Value Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Value Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Margin Rule Breach
  prod_gap_rules_margin <- prod_gap_merged %>%
    filter(Rule.Classification == "Margin") %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Margin.Dep>Margin.Indep, "Good",
                                 ifelse(Side == "Lower" & Margin.Dep<Margin.Indep, "Good",
                                        ifelse(Side == "Within", "Good",
                                               ifelse(Side == "Same" & Margin.Dep==Margin.Indep, "Good", "Issue"))))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Margin.Dep/Margin.Indep-1), abs(Margin.Dep-Margin.Indep))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Margin Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Margin Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # Margin Percentage Rule Breach  
  prod_gap_rules_margin_perc <- prod_gap_merged %>%
    filter((Rule.Classification == "Margin Percentage")) %>%
    mutate(Comment_Side = ifelse(Side == "Higher" & Margin.Dep.Perc>Margin.Indep.Perc, "Good",
                                 ifelse(Side == "Lower" & Margin.Dep.Perc<Margin.Indep.Perc, "Good",
                                        ifelse(Side == "Within", "Good",
                                                ifelse(Side == "Same" & Margin.Dep.Perc==Margin.Indep.Perc, "Good", "Issue"))))) %>%
    mutate(difference = ifelse(Type == "Percentage", abs(Margin.Dep.Perc/Margin.Indep.Perc-1), abs(Margin.Indep.Perc-Margin.Dep.Perc))) %>%
    mutate(is.Breach = ifelse(Comment_Side == "Issue", paste0(Side, " Margin Percentage Breach"),
                              ifelse((difference>Max | difference<Min), paste0(Side, " Margin Percentage Breach"), "Good"))) %>%
    select(-Comment_Side) %>%
    mutate(is.Breach = as.character(is.Breach))

  # All Product Gap Breaches
  all_rules_breach <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin, prod_gap_rules_margin_perc) %>%
                        arrange(Dependent) %>% filter(grepl("Breach", is.Breach))

  all_rules_breach_report <- all_rules_breach %>% 
                               select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Independent, Cluster, 
                                      Initial.Rule, Rule.Classification, is.Breach) %>% 
                               unique() %>% mutate(Rule.Classification = paste0(Rule.Classification, "_Breach")) %>%
                               spread(Rule.Classification, is.Breach)

  all_rules_breach_final <- all_rules_breach %>% select(-Rule.Classification, -Side, -Type, -Min, -Max, -difference, -is.Breach) %>% unique() %>% 
                               left_join(all_rules_breach_report)
  
  absolute_rule_flag <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin, prod_gap_rules_margin_perc) %>%
    group_by(Dependent, Cluster, Independent, Initial.Rule) %>%
    mutate(count_rows = n()) %>%
    ungroup() %>%
    filter(count_rows == 1) %>%
    filter((Rule.Classification == "Price") & (Type == "Absolute")) %>%
    mutate(absolute_flag = "Y") %>%
    mutate(addition = ifelse(Side == "Higher", Min, -Min)) %>%
    select(Dependent, Cluster, Independent, Initial.Rule, absolute_flag, addition)
    

  all_rules_ratio <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin, prod_gap_rules_margin_perc) %>%
    mutate(penny_flag = ifelse(((Rule.Classification == "Margin") & (Side == "Same")), "Y", "N"),
           same_retail_flag = ifelse(((Rule.Classification == "Price") & (Side == "Same")), "Y", "N")) %>%
    arrange(Dependent) %>%
    select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Cluster, Independent, 
           Initial.Rule, Price.Dep, Price.Indep, Cost.Dep, Cost.Indep, penny_flag, same_retail_flag) %>%
    unique() %>%
    mutate(Ratio = Price.Dep/Price.Indep) %>%
    filter(!is.na(Ratio)) %>%
    mutate(Ratio = ifelse(same_retail_flag == "Y", 1, 
                          ifelse(penny_flag == "N", (Price.Dep/Price.Indep), 0))) %>%
    left_join(absolute_rule_flag) %>%
    mutate(absolute_flag = ifelse(is.na(absolute_flag), "N", absolute_flag),
           Ratio = ifelse(is.na(addition), Ratio, addition)) %>%
    select(-addition)

  # Either Dependent/Independent not in scope for the Current Wave
  not_in_data <- bind_rows(prod_gap_rules_price, prod_gap_rules_value, prod_gap_rules_margin, prod_gap_rules_margin_perc) %>%
    arrange(Dependent) %>%
    left_join(all_rules_breach %>% select(Dependent, Cluster) %>% unique() %>% mutate(Breach = "Y")) %>%
    filter(is.na(Breach)) %>% select(-Breach) %>%
    select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Cluster, Independent, Price.Dep, Price.Indep) %>%
    unique() %>%
    mutate(Ratio = Price.Dep/Price.Indep) %>%
    filter(is.na(Ratio))

  return(list(all_rules_breach_final, not_in_data, all_rules_ratio))
  
}

prod_gap_merge_function_beta <- function(item_data, all_rules_ratio, prod_gap_alerts_as_is, prod_gap, prod_gap_function_opt){
  
  # Summarising the Bounds and Optimized Price at Price Family Level
  rel_items_data_PF <- item_data %>%
    select(Category.Name, Cluster, Price.Family, Final.Price.LB.Family2, Final.Price.UB.Family2, OPTIMIZED_PRICE_ROUNDED) %>%
    group_by(Category.Name, Cluster, Price.Family) %>%
    summarise_all(funs(mean(.,na.rm=T))) %>%
    ungroup()
  
  # Selecting the Bounds and Optimized Price at Item Level
  rel_items_data_item <- item_data %>%
    select(Category.Name, Sys.ID.Join, Cluster, Final.Price.LB.Family2, Final.Price.UB.Family2, OPTIMIZED_PRICE_ROUNDED)
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Family to Family Relationship
  rel_PF_PF <- all_rules_ratio %>% 
    filter((Category.Dep == "PF") & (Category.Indep == "PF")) %>%
    left_join(rel_items_data_PF %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Price.Family")) %>%
    left_join(rel_items_data_PF %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Price.Family")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED)
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Family to Item Relationship
  rel_PF_Item <- all_rules_ratio %>% 
    filter((Category.Dep == "PF") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) %>% ## dash dtype
    left_join(rel_items_data_PF %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Price.Family")) %>%
    left_join(rel_items_data_item %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Sys.ID.Join")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) 
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Items to Family Relationship
  rel_Item_PF <- all_rules_ratio %>% 
    filter((Category.Dep == "Item") & (Category.Indep == "PF")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%
    left_join(rel_items_data_item %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Sys.ID.Join")) %>%
    left_join(rel_items_data_PF %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Price.Family")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) 
  
  # Joining the Bounds of the Dependent and Optimized Price of the Independent for Items to Item Relationship
  rel_Item_Item <- all_rules_ratio %>% 
    filter((Category.Dep == "Item") & (Category.Indep == "Item")) %>%
    mutate(Dependent = as.character(Dependent),  ## dash dtype
           Independent = as.character(Independent)) %>%  ## dash dtype
    left_join(rel_items_data_item %>% select(-OPTIMIZED_PRICE_ROUNDED), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Dependent" = "Sys.ID.Join")) %>%
    left_join(rel_items_data_item %>% select(-Final.Price.LB.Family2, -Final.Price.UB.Family2), 
              c("Category.Name.Dep" = "Category.Name", "Cluster", "Independent" = "Sys.ID.Join")) %>%
    rename(Final.Price.LB.Family2.Dep = Final.Price.LB.Family2,
           Final.Price.UB.Family2.Dep = Final.Price.UB.Family2,
           OPTIMIZED_PRICE_ROUNDED_INDEP = OPTIMIZED_PRICE_ROUNDED) %>%
    mutate(Dependent = as.character(Dependent),
           Independent = as.character(Independent)) 
  
  # Checking if the Product Gap Rules are followed in the Free-Flow Optimization or not. If not, then we apply AS-IS Price Ratios
  prod_gap_alerts_opt <- prod_gap_function_opt(item_data, prod_gap, prod_gap_alerts_as_is)
  
  # Appending with the Types
  final_opti_price_prod_gap <- bind_rows(rel_PF_PF, rel_PF_Item, rel_Item_PF, rel_Item_Item) %>%
    left_join(prod_gap_alerts_opt %>% select(Category.Name.Dep, Category.Dep, Category.Indep, Dependent, Independent, Cluster, Rule_Failed_AS_IS) %>% 
              mutate(Rule_Failed_OPT = "Y")) %>%
    filter(Rule_Failed_OPT == "Y" & Rule_Failed_AS_IS == "N") %>%
    mutate(OPTIMIZED_PRICE_ROUNDED_DEP = ifelse(penny_flag == "Y", 
                                                (Cost.Dep+(OPTIMIZED_PRICE_ROUNDED_INDEP-Cost.Indep)), 
                                                ifelse(absolute_flag == "Y", (OPTIMIZED_PRICE_ROUNDED_INDEP+Ratio), 
                                                       OPTIMIZED_PRICE_ROUNDED_INDEP*Ratio))) %>%
    mutate(out_of_bounds_flag=ifelse(((OPTIMIZED_PRICE_ROUNDED_DEP<Final.Price.LB.Family2.Dep)|(OPTIMIZED_PRICE_ROUNDED_DEP>Final.Price.UB.Family2.Dep)),
                                  "Y", "N"))
  
  # Updating the Stage 1 Output with the New Optimized Prices of the Dependent Items
  items_data_final <- item_data %>%
    mutate(Sys.ID.Join = as.character(Sys.ID.Join)) %>%
    left_join(final_opti_price_prod_gap %>% select(Category.Name.Dep, Dependent, Cluster, OPTIMIZED_PRICE_ROUNDED_DEP),
              c("Category.Name"="Category.Name.Dep", "Sys.ID.Join"="Dependent", "Cluster")) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED_DEP = ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP>Final.Price.UB.Family2)), 
                                                Final.Price.UB.Family2,
                                                ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP<Final.Price.LB.Family2)),
                                                       Final.Price.LB.Family2, OPTIMIZED_PRICE_ROUNDED_DEP))) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(OPTIMIZED_PRICE_ROUNDED_DEP), OPTIMIZED_PRICE_ROUNDED, OPTIMIZED_PRICE_ROUNDED_DEP)) %>%
    select(-OPTIMIZED_PRICE_ROUNDED_DEP) %>%
    left_join(final_opti_price_prod_gap %>% select(Category.Name.Dep, Dependent, Cluster, OPTIMIZED_PRICE_ROUNDED_DEP),
              c("Category.Name"="Category.Name.Dep", "Price.Family"="Dependent", "Cluster")) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED_DEP = ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP>Final.Price.UB.Family2)),
                                                Final.Price.UB.Family2,
                                                ifelse(((!is.na(OPTIMIZED_PRICE_ROUNDED_DEP)) & (OPTIMIZED_PRICE_ROUNDED_DEP<Final.Price.LB.Family2)),
                                                       Final.Price.LB.Family2, OPTIMIZED_PRICE_ROUNDED_DEP))) %>%
    mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(OPTIMIZED_PRICE_ROUNDED_DEP), OPTIMIZED_PRICE_ROUNDED, OPTIMIZED_PRICE_ROUNDED_DEP)) %>%
    select(-OPTIMIZED_PRICE_ROUNDED_DEP) %>%
#     left_join(final_opti_price_prod_gap %>% select(Independent, Cluster, out_of_bounds_flag) %>% unique(),
#             c("Sys.ID.Join"="Independent", "Cluster")) %>%
#     mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(out_of_bounds_flag), OPTIMIZED_PRICE_ROUNDED,
#                                             ifelse(out_of_bounds_flag == "N", OPTIMIZED_PRICE_ROUNDED, Regular.Price))) %>%
#     select(-out_of_bounds_flag) %>%
#     left_join(final_opti_price_prod_gap %>% select(Independent, Cluster, out_of_bounds_flag) %>% unique(),
#             c("Price.Family"="Independent", "Cluster")) %>%
#     mutate(OPTIMIZED_PRICE_ROUNDED = ifelse(is.na(out_of_bounds_flag), OPTIMIZED_PRICE_ROUNDED,
#                                             ifelse(out_of_bounds_flag == "N", OPTIMIZED_PRICE_ROUNDED, Regular.Price))) %>%
#     select(-out_of_bounds_flag) %>%
    mutate(Sys.ID.Join = as.character(Sys.ID.Join))  ### dash dtype
  
  return(list(items_data_final, final_opti_price_prod_gap, prod_gap_alerts_opt))
  
  
}

# COMMAND ----------

generate_opti_summary_all_beta <- function(opti_res_adj_final, weight_map){
 
  opti_res_adj_final$product_key <- as.character(opti_res_adj_final$product_key)   ## dash dtype
  opti_res_adj_final$Sys.ID.Join <- as.character(opti_res_adj_final$Sys.ID.Join)   ## dash dtype
  
 
  # optimization Summary at Sub-Category Level  
  opti_summary_1 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Category.Name) %>%
    mutate(Total_Units= sum(Base.Units_tot, na.rm = T),
           Total_Rev= sum(base_rev_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name, Category.Special.Classification) %>%
    summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
              Base.Units = sum(Base.Units_tot, na.rm = T),
              base_rev = sum(base_rev_tot, na.rm = T),
              base_margin = sum(base_margin_tot, na.rm = T),
              OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
              opti_units = sum(opti_units_tot, na.rm = T),
              opti_rev = sum(opti_rev_tot, na.rm = T),
              opti_margin = sum(opti_margin_tot, na.rm = T),
              Total_Units = mean(Total_Units, na.rm = T),
              Total_Rev = mean(Total_Rev, na.rm = T),
              Price_Inc_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Dec_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n(),
              count_item_x_clusters = n()) %>% 
    ungroup() %>%
    mutate(margin_lift = (opti_margin/base_margin-1)*100,
           rev_lift = (opti_rev/base_rev-1)*100,
           unit_lift = (opti_units/Base.Units-1)*100,
           price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100,
           Units_Share = Base.Units/Total_Units,
           Rev_Share = base_rev/Total_Rev)
  
  # optimization Summary at Category Level 
  opti_summary_2 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Category.Name) %>%
    mutate(Total_Units= sum(Base.Units_tot, na.rm = T),
           Total_Rev= sum(base_rev_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name) %>%
    summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
              Base.Units = sum(Base.Units_tot, na.rm = T),
              base_rev = sum(base_rev_tot, na.rm = T),
              base_margin = sum(base_margin_tot, na.rm = T),
              OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
              opti_units = sum(opti_units_tot, na.rm = T),
              opti_rev = sum(opti_rev_tot, na.rm = T),
              opti_margin = sum(opti_margin_tot, na.rm = T),
              Total_Units = mean(Total_Units, na.rm = T),
              Total_Rev = mean(Total_Rev, na.rm = T),
              Price_Inc_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Dec_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n(),
              count_item_x_clusters = n()) %>% 
    ungroup() %>%
    mutate(margin_lift = (opti_margin/base_margin-1)*100,
           rev_lift = (opti_rev/base_rev-1)*100,
           unit_lift = (opti_units/Base.Units-1)*100,
           price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100,
           Units_Share = Base.Units/Total_Units,
           Rev_Share = base_rev/Total_Rev) %>%
    mutate(Category.Special.Classification = "Overall")
  
  # Weighted Elasticity at Sub-Category Level
  summary_elasticity_1 <- opti_res_adj_final %>%
    group_by(Category.Name, Category.Special.Classification, Sys.ID.Join) %>%
    summarise(Weighted_Elasticity = weighted.mean(Base.Price.Elasticity, Base.Units_tot),
              Units = sum(Base.Units_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name, Category.Special.Classification) %>%
    summarise(Weighted_Elasticity_Final = weighted.mean(Weighted_Elasticity, Units)) %>%
    ungroup()
  
  # Weighted Elasticity at Category Level
  summary_elasticity_2 <- opti_res_adj_final %>%
    group_by(Category.Name, Category.Special.Classification, Sys.ID.Join) %>%
    summarise(Weighted_Elasticity = weighted.mean(Base.Price.Elasticity, Base.Units_tot),
              Units = sum(Base.Units_tot, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name, Category.Special.Classification) %>%
    summarise(Weighted_Elasticity_Final = weighted.mean(Weighted_Elasticity, Units),
              Units = sum(Units, na.rm = T)) %>%
    ungroup() %>%
    group_by(Category.Name) %>%
    summarise(Weighted_Elasticity_Final = weighted.mean(Weighted_Elasticity_Final, Units)) %>%
    ungroup() %>%
    mutate(Category.Special.Classification = "Overall")
  
  # Combining all the Summaries
  final_opti_summary <- bind_rows(opti_summary_1, opti_summary_2) %>% unique() %>%
    left_join(min_margin_file %>% unique()) %>%
    mutate(Average.Category.Minimum.Margin = ifelse(is.na(Average.Category.Minimum.Margin), 0, Average.Category.Minimum.Margin)) %>%
    mutate(avg_cat_mar_base = base_margin/base_rev,
           avg_cat_mar_opt = opti_margin/opti_rev) %>%
    mutate(cat_mar_flag = ifelse(avg_cat_mar_base >= Average.Category.Minimum.Margin & avg_cat_mar_opt < Average.Category.Minimum.Margin, 
                                 "Avg Cat Margin Fail","Avg Cat Margin Pass"))%>%
    left_join(weight_map) %>%
    left_join(bind_rows(summary_elasticity_1, summary_elasticity_2) %>% unique()) %>%
    rename(Optimization_Strategy = Optimization.Strategy) %>%
    select(Category.Name,Category.Special.Classification,Regular.Price,Base.Units,base_rev,base_margin,OPTIMIZED_PRICE_ROUNDED,Units_Share,Rev_Share,
           opti_units,opti_rev,opti_margin,Price_Inc_Perc,Price_Dec_Perc,No_Price_Change_Perc,count_item_x_clusters,Optimization_Strategy,margin_lift,
           rev_lift,unit_lift,price_lift,Average.Category.Minimum.Margin,avg_cat_mar_base,avg_cat_mar_opt,cat_mar_flag,Margin.Weight,Revenue.Weight,Unit.Weight,
           Weighted_Elasticity_Final)
  
  # Cluster Level Summaries
  cluster_level_summary <- opti_res_adj_final %>% 
    group_by(Category.Name, Cluster) %>%
    summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
              Base.Units = sum(Base.Units_tot, na.rm = T),
              base_rev = sum(base_rev_tot, na.rm = T),
              base_margin = sum(base_margin_tot, na.rm = T),
              OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
              opti_units = sum(opti_units_tot, na.rm = T),
              opti_rev = sum(opti_rev_tot, na.rm = T),
              opti_margin = sum(opti_margin_tot, na.rm = T)) %>% 
    ungroup() %>%
    mutate(margin_lift = (opti_margin/base_margin-1)*100,
           rev_lift = (opti_rev/base_rev-1)*100,
           unit_lift = (opti_units/Base.Units-1)*100,
           price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100) %>%
    select(Category.Name, Cluster, margin_lift, rev_lift, unit_lift, price_lift)
  
  price_by_cluster_1 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Category.Name, Cluster) %>%
    summarise(Price_Increase_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Decrease_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n(),
              Total_Items = n()) %>% 
    ungroup()
  
  price_by_cluster_2 <- opti_res_adj_final %>% 
    mutate(Price_Inc = ifelse(OPTIMIZED_PRICE_ROUNDED > Regular.Price, 1, 0),
           Price_Dec = ifelse(OPTIMIZED_PRICE_ROUNDED < Regular.Price, 1, 0),
           No_Price_Change = ifelse(OPTIMIZED_PRICE_ROUNDED == Regular.Price, 1, 0)) %>%
    group_by(Cluster) %>%
    summarise(Price_Increase_Perc = 100*sum(Price_Inc, na.rm = T)/n(),
              Price_Decrease_Perc = 100*sum(Price_Dec, na.rm = T)/n(),
              No_Price_Change_Perc = 100*sum(No_Price_Change, na.rm = T)/n()) %>% 
    ungroup()
  
    # Price Family x Cluster Level Summaries
    pf_cluster_level_lifts <- opti_res_adj_final %>% 
      mutate(item_or_pf = ifelse(Price.Family == "No_Family", Sys.ID.Join, Price.Family)) %>% 
      group_by(Category.Name, item_or_pf, Price.Family, Cluster) %>%
      summarise(Regular.Price = weighted.mean(Regular.Price, base_rev_tot, na.rm = T),
                Base.Units = sum(Base.Units_tot, na.rm = T),
                base_rev = sum(base_rev_tot, na.rm = T),
                base_margin = sum(base_margin_tot, na.rm = T),
                OPTIMIZED_PRICE_ROUNDED = weighted.mean(OPTIMIZED_PRICE_ROUNDED, opti_rev_tot, na.rm = T),
                opti_units = sum(opti_units_tot, na.rm = T),
                opti_rev = sum(opti_rev_tot, na.rm = T),
                opti_margin = sum(opti_margin_tot, na.rm = T)) %>% 
      ungroup() %>%
      mutate(margin_lift = (opti_margin/base_margin-1)*100,
             rev_lift = (opti_rev/base_rev-1)*100,
             unit_lift = (opti_units/Base.Units-1)*100,
             price_lift = (OPTIMIZED_PRICE_ROUNDED/Regular.Price-1)*100) %>%
      select(Category.Name, item_or_pf, Price.Family, Cluster, margin_lift, rev_lift, unit_lift, price_lift)
  
  pf_cluster_level_summary <- opti_res_adj_final %>% 
    select(BU, department_desc, Category.Name, SubCategory.Name, Category.Special.Classification, Manufacturer.Name, sell_unit_qty,
           Item.Name, Sys.ID.Join, Cluster, Price.Family, Opti_Key, Promo_price_floor, LB_min_mar, LB_Cat, Price_Ceil, UB_Cat, Item_LB, Item_UB,
           Final.Price.LB.Family, Final.Price.UB.Family,
           Issue_Flag, Final.Price.LB.Family2, Final.Price.UB.Family2, Base.Units, Base.Price.Elasticity, Per.Unit.Cost, Regular.Price,
           OPTIMIZED_PRICE_ROUNDED) %>%
    mutate(item_or_pf = ifelse(Price.Family == "No_Family", Sys.ID.Join, Price.Family)) %>% 
    left_join(pf_cluster_level_lifts) %>% select(-item_or_pf)
  
  return(list(final_opti_summary, cluster_level_summary, price_by_cluster_1, price_by_cluster_2, pf_cluster_level_summary))
   
}

# COMMAND ----------

# prod_gap_identity_merge <- function(opti_res_adj_final, prod_gap_ratios){
  
#   identity_view <- prod_gap_ratios %>%
#     select(Cluster, Independent) %>% unique() %>%
#     arrange(Cluster, Independent) %>%
#     mutate(Identity = row_number())

#   pair_number <- prod_gap_ratios %>%
#     select(Cluster, Independent, Dependent) %>% unique() %>%
#     left_join(identity_view)

#   final_identity <- pair_number %>% select(Cluster, Identity, Independent) %>% rename(Item_Family = Independent) %>%
#     bind_rows(pair_number %>% select(Cluster, Identity, Dependent) %>% rename(Item_Family = Dependent)) %>%
#     unique() %>%
#     arrange(Cluster, Identity, Item_Family)

#   identity_mapping <- prod_gap_ratios %>% select(Cluster, Category.Indep, Independent) %>% rename(Cat = Category.Indep, Item_Family=Independent) %>%
#     bind_rows(prod_gap_ratios %>% select(Cluster, Category.Dep, Dependent) %>% rename(Cat = Category.Dep, Item_Family=Dependent)) %>%
#     unique() %>%
#     arrange(Cluster, Item_Family) %>%
#     left_join(final_identity) %>%
#     mutate(prod_gap_identity = paste0("Prod_Gap_Pair_",Identity)) %>%
#     select(-Identity)

#   identity_map_item <- identity_mapping %>% filter(Cat == "Item") %>% mutate(Item_Family = as.numeric(Item_Family)) %>% select(-Cat)
#   identity_map_PF <- identity_mapping %>% filter(Cat == "PF") %>% select(-Cat)

#   prod_identity_merged <- opti_res_adj_final %>%
#     left_join(identity_map_PF %>% rename(Price.Family = Item_Family)) %>%
#     left_join(identity_map_item %>% rename(Sys.ID.Join = Item_Family)) %>%
#     mutate(prod_gap_identity = ifelse(is.na(prod_gap_identity), Price.Family, prod_gap_identity)) %>%
#     mutate(prod_gap_identity = ifelse(prod_gap_identity == "No_Family", row_number(), prod_gap_identity)) 
  
#   return(prod_identity_merged)
# }
