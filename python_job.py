import boto3
import pandas as pd
import json
import pandas

class S3Reader:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key
        self.s3_client = boto3.client('s3',
                                       aws_access_key_id=self.access_key,
                                       aws_secret_access_key=self.secret_key)
    
    def read_csv_from_s3(self, bucket_name, file_name):
      
        response = self.s3_client.get_object(Bucket=bucket_name, Key=file_name)
        csv_data = response['Body']
        df_raw = pd.read_csv(csv_data, low_memory=False)
        json_data = self.transform_csv_json(df_raw)
        json_key = 'transformed_json/chandigarh-streetlight-location-statics.json'
        
        if self.check_file_exists(bucket_name, json_key):
            print(f"File '{json_key}' already exists in the S3 bucket.")
        
        else:
            self.upload_json_to_s3(bucket_name, json_key, json_data)
            print(f"File '{json_key}' file pushed in the S3 bucket.")
    
    def transform_csv_json(self, df_raw):
        df = df_raw[['CCMS', 'Latitude', 'Longitude', 'Address']].dropna()
        df.drop_duplicates(inplace=True)
        
        chandigarh_dict = list(
            map(
                lambda row: {
                    "deviceID": df.iloc[row, 0],
                    "address": df.iloc[row, 3],
                    "location": {
                        "type": "Point",
                        "coordinates": [
                            round(float(df.iloc[row, 2]), 6),
                            round(float(df.iloc[row, 1]), 6),
                        ],
                    },
                },
                range(1, df.shape[0]),
            )
        )
        
        return json.dumps(chandigarh_dict, indent=4)
    
    def upload_json_to_s3(self, bucket_name, key, json_data):
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json_data,
            ContentType='application/json'
        )
    
    def check_file_exists(self, bucket_name, file_name):
        try:
            self.s3_client.head_object(Bucket=bucket_name, Key=file_name)
            return True
        except Exception as e:
            return False

access_key = 'AKIA6KFM5H3WDUTXQTJ7'
secret_key = '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'
bucket_name = 'project-folder'
file_name = 'chandigarh-streetlight-location-static.csv'

s3_reader = S3Reader(access_key, secret_key)
s3_reader.read_csv_from_s3(bucket_name, file_name)
