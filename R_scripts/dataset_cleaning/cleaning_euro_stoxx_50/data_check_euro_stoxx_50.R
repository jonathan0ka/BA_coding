
raw_data <- read.csv("/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/merged_data_sets/euro_stoxx_50_merge.csv")

####### rename columns
colnames(raw_data) = c("ticker", "name", "weight", "price", "date")

###### NAs
raw_data <- na.omit(raw_data, "weight")

#### zero weights
raw_data <- filter(raw_data, weight != 0)

####### data
raw_data$date <- as.Date(as.character(raw_data$date), format = "%Y%m%d")

##### test plotting
library(ggplot2)
library(dplyr)

ggplot(data = raw_data, aes(x = date, y = weight, color = name)) +
  geom_point(size = 0.35) +
  theme_minimal() +
  theme(legend.position = "none") +
  labs(title = "Stock Weights Over Time - iShares Euro STOXX 50 ETF", x = "Date", y = "Weight")

##### take subsets
data_sorted <- raw_data[order(-raw_data$weight), ]

# Step 2: Divide the data into four equal-sized subsets
subset_size <- ceiling(nrow(data_sorted) / 4)  # Calculate the size of each subset (rounding up)

# Create the subsets
subset1 <- data_sorted[1:subset_size, ]
subset2 <- data_sorted[(subset_size + 1):(subset_size * 2), ]
subset3 <- data_sorted[(subset_size * 2 + 1):(subset_size * 3), ]
subset4 <- data_sorted[(subset_size * 3 + 1):nrow(data_sorted), ]

p1 <- ggplot(subset1, aes(x = date, y = weight, group = name, color = name)) +
  geom_point() +
  ggtitle("Subset 1") +
  theme(legend.position = "none")
print(p1)








