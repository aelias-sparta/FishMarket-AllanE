from FishMarket import FishMarket
from mongoImport import MongoImport
import boto3
import json
from pprint import pprint as pp
import pymongo

if __name__ == '__main__':
    # declaring the bucket names and objects
    bucket_name = "data-eng-resources"
    prefix = 'python'

    # creating an instance
    obj = FishMarket(bucket_name, prefix)
    # calling the loader function to load the data back into the s3 storage
    #obj.data_loader()
    # file, df_avg = obj.transformation()
    # print(df_avg)
    client = pymongo.MongoClient("mongodb://18.196.80.188:27017/Sparta")
    db = client['AllanFish-Market']

    # loading files into mongodb both local and EC2
    filename ="AllanE_FishMarket.csv"
    collection = "FishMarket"
    Ip = "18.196.80.188"
    mongoObj = MongoImport(filename=filename, collection=collection, Ip=Ip)
    mongoObj.load_localhost()
    mongoObj.load_EC2()

    # checking files if exist in the mongodb
    client = pymongo.MongoClient(f"mongodb://{Ip}:27017/Sparta")
    db = client['AllanFish-Market']
    for data in db.FishMarket.find({}):
        pp(data)

