
import argparse
import requests
import pandas as pd
from tqdm import tqdm
import os

# Function to find place_id
def find_place_id(company_name, api_key):
    company_name_encoded = requests.utils.quote(company_name)
    find_place_url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={company_name_encoded}&inputtype=textquery&fields=place_id&key={api_key}"
    response = requests.get(find_place_url)
    if response.status_code == 200 and response.json()['status'] == 'OK':
        return response.json()['candidates'][0]['place_id']
    return None

# Function to find website using place_id
def find_website(place_id, api_key):
    if place_id:
        place_details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=website&key={api_key}"
        response = requests.get(place_details_url)
        if response.status_code == 200 and response.json()['status'] == 'OK':
            return response.json()['result'].get('website', None)
    return None

def process_companies(input_df, output_df, start_index=0):
    for index, row in tqdm(input_df[start_index:].iterrows(), initial=start_index, total=input_df.shape[0], desc="Processing companies"):
        if index % 5000 == 0 and index > start_index:
            # Save the progress every 5000 iterations
            output_df.to_csv('output_new4.csv', index=False)
        
        company_name = row['ENTP_NM']
        # Process if the record has not been processed yet
        if index >= len(output_df):
            uen = row['UEN']  # Extract UEN from the row
            company_name = row['ENTP_NM']  # Extract ENTP_NM from the row
            reg_pd = row['REG_PD']  # Extract REG_PD from the row
            place_id = find_place_id(company_name, api_key)
            website_url = find_website(place_id, api_key)
            # Append new result to output_df
            output_df = output_df.append({
                'UEN': uen,
                'ENTP_NM': company_name,
                'REG_PD': reg_pd,
                'website_url': website_url
            }, ignore_index=True)

    # Save final progress
    output_df.to_csv('output_new4.csv', index=False)

# Set up command-line argument parsing
parser = argparse.ArgumentParser(description="Process a CSV file of companies to find website URLs.")
parser.add_argument("input_file", help="Path to the input CSV file")
args = parser.parse_args()

# Your Google API key - replace with your own key
api_key = 'ABCDEFG1234567'

# Read the input CSV file
input_df = pd.read_csv(args.input_file)
if not 'website_url' in input_df.columns:
    input_df['website_url'] = None  # Add a new column for website URLs

# Check if output.csv exists
if os.path.exists('output_new4.csv'):
    # Load from output.csv
    output_df = pd.read_csv('output_new4.csv')
    # Set the starting index as the length of the output DataFrame
    start_index = len(output_df)
else:
    # Initialize an empty DataFrame with the same columns as input_df
    output_df = pd.DataFrame(columns=input_df.columns)
    start_index = 0

# Process the DataFrame
process_companies(input_df, output_df, start_index)
