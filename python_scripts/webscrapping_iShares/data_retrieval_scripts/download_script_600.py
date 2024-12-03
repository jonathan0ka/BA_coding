import csv
import requests

def download_file(url, destination_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(destination_path, 'wb') as f:
            f.write(response.content)
    else:
        print(f"Failed to download {url}")

# Path to your CSV file
csv_file_path = 'stoxx_europe_600_links.csv'    

# Folder where you want to save the downloaded files
download_folder = '/Users/jonathanzeh/Library/CloudStorage/OneDrive-Personal/RStudio/BA_Thesis/stoxx_europe_600_downloads'

with open(csv_file_path, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        url = row[0]
        # Extracting file name from URL
        file_name = url.split('/')[-1]
         # Extract the last 8 digits from the URL to get the date
        date_str = url[-8:]
        # Construct the file name using the date
        file_name = f"stoxx_europe_600_{date_str}.csv"
        destination_path = f"{download_folder}/{file_name}"
        download_file(url, destination_path)
        print(f"Downloaded {file_name}")
