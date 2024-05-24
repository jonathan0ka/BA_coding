library(dplyr)
library(tidyverse)

df <- read_csv("/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/BA_Thesis/BA_coding/datasets/eikon_data/variable_data/monthly_panel_v1.csv")

head(df)

df_test <- df %>% filter(stock_RIC == "NESN.S" & date == as.Date("2023-11-30")) 
print(df_test[, c("stock_RIC", "date", "stock_value_held", "ETF_ownership", "FUND_ownership")])
