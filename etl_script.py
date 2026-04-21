#Q1.
Etl_script.py

import boto3
import pandas as pd

# CONFIG
bucket_name = "exam-sales-data-bucket025"
input_file_key = "sales_file.csv"
output_file_key = "output/filtered_sales.csv"

# Initialize S3 client
s3 = boto3.client('s3')

# Download file from S3
s3.download_file(bucket_name, input_file_key, 'sales.csv')

# Read CSV
df = pd.read_csv('sales.csv')

# Filter records where amount > 1000
filtered_df = df[df['amount'] > 1000]

# Save filtered data
filtered_df.to_csv('filtered_sales.csv', index=False)

# Upload back to S3
s3.upload_file('filtered_sales.csv', bucket_name, output_file_key)

print("ETL process completed successfully!")
