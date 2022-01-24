import os
import boto3
from pprint import pprint as pp
import json
import pandas as pd
import csv

class FishMarket():

    def __init__(self, bucket_name,  prefix):
        self.s3_client = boto3.client("s3") # in here we can also put any aws cileint
        self.s3_resource = boto3.resource("s3")

        self.bucket_list = self.s3_client.list_buckets()
        self.bucket_name = bucket_name
        self.s3_resource.Bucket(bucket_name)
        self.prefix = prefix


    def extract_csv(self):
        bucket_contents = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.prefix)
        list_csv = []
        for object in bucket_contents['Contents']:
            if 'fish-market' in object['Key']:
                file = object['Key']
                file_csv = self.s3_client.get_object(Bucket=self.bucket_name, Key=file)
                df = pd.read_csv(file_csv['Body'])
                list_csv.append(df)
        combined_data = pd.concat(list_csv, join="outer", ignore_index=True)
        return combined_data

    def transformation(self,):
        data = self.extract_csv()
        output = data.groupby('Species').mean()
        file_csv = output.to_csv("AllanE_Avg.csv",index=False)
        return file_csv

    def loading(self):
        extracted_data = self.transformation()
        self.s3_client.upload_file(Filename=extracted_data, Bucket=self.bucket_name, Key="Data26/Test/AllanE_FishMarket.csv")