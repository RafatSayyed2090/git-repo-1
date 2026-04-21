import boto3
import pandas as pd
import pymysql
from datetime import datetime

s3 = boto3.client('s3')
sns = boto3.client('sns')

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:905418280390:student-pipeline-topic"

RDS_HOST = "RDS_HOST"
RDS_USER = "RDS_USER"
RDS_PASSWORD = "RDS_PASSWORD"
RDS_DB = "RDS_DB"

def lambda_handler(event, context):
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    local_file = '/tmp/input.csv'
    s3.download_file(bucket, key, local_file)
    
    df = pd.read_csv(local_file)
    
    df['processed_timestamp'] = datetime.now()
    
    output_file = '/tmp/processed.csv'
    df.to_csv(output_file, index=False)
    
    output_key = 'processed/processed.csv'
    s3.upload_file(output_file, bucket, output_key)
    
    # Insert into RDS
    conn = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DB
    )
    
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO students VALUES (%s,%s,%s,%s)",
            (row['id'], row['name'], row['marks'], str(row['processed_timestamp']))
        )
    
    conn.commit()
    conn.close()
    
    # Send SNS
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Success",
        Message="File processed successfully"
    )
    
    return "Done"
