
################################### exisiting code
import pandas as pd
import os

relevant_df = pd.read_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/cleaned_merged_data_sets/cleaned_overview_euro_stoxx_600.csv')

# Specify the directory containing the 5000 CSV files
directory = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/raw_download_data/stoxx_europe_600_downloads'

# List of unique names to filter by
relevant_names = relevant_df['consistent_name'].unique()

# Initialize an empty list to store the data frames
data_frames = []

# Loop through all the CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        # Extract the date from the filename
        date_str = filename[-12:-4]
        # Construct the full path to the file
        file_path = os.path.join(directory, filename)
        # Read the current CSV file
        current_df = pd.read_csv(file_path, header=2)
        # Add the date to the current DataFrame
        current_df['date'] = pd.to_datetime(date_str, format='%Y%m%d')
        # Filter to keep only rows with names that are in the relevant_names list
        current_df = current_df[current_df['Name'].isin(relevant_names)]
        # Append the filtered DataFrame to the list
        data_frames.append(current_df)

# Concatenate all data frames
collected_data = pd.concat(data_frames, ignore_index=True)

# Save the collected data to a new CSV file in long format
collected_data.to_csv('/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_600_merge_raw.csv', index=False)



####################################################### R code
raw_data <- read.csv("/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/stoxx_europe_600_merge_raw.csv")

####### rename columns
colnames(raw_data) = c("ticker", "name", "sector", "asset_class","market_cap", "weight", "nominal_value", "nominal", "price", "country", "stock_exchange","currency","date")

raw_data$weight[raw_data$weight == ""] <- NA
raw_data <- na.omit(raw_data, subset = "weight")

cleaned_data <- raw_data[, c("ticker", "name", "weight", "price", "date")]

####### data
cleaned_data$date <- as.Date(as.character(cleaned_data$date))
cleaned_data$weight <- as.numeric(gsub(",", ".", cleaned_data$weight))
cleaned_data$price <- as.numeric(gsub(",", ".", cleaned_data$price))
#cleaned_data$market_cap <- as.numeric(gsub(",", ".", cleaned_data$market_cap))

#### zero weights
cleaned_data <- filter(cleaned_data, weight != 0)

# Function to replace the name
replace_name <- function(name) {
  for (consistent_name in names(name_mapping)) {
    if (name %in% name_mapping[[consistent_name]]) {
      return(consistent_name)
    }
  }
  return(name)
}

################ export large data set
cleaned_data$consistent_name <- sapply(cleaned_data$name, replace_name)