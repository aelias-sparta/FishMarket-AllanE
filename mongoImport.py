from FishMarket import FishMarket
import boto3
import json
import pymongo
import requests
from pprint import pprint as pp
import csv
#client = pymongo.MongoReplicaSetClient("mongodb://localhost:27017/Sparta")

class MongoImport():

    def __init__(self, filename, collection, Ip):
        self.filename = filename
        self.collection = collection
        self.Ip = Ip

    def load_localhost(self):
        client = pymongo.MongoClient()
        db = client['AllanFish-Market']

        db.drop_collection(self.collection)
        db.create_collection(self.collection)
        with open(self.filename, 'r') as f:
            for line in csv.DictReader(f):
                db.FishMarket.insert_one(line)

    def load_EC2(self):
        client = pymongo.MongoClient(f"mongodb://{self.Ip}:27017/Sparta")
        db = client['AllanFish-Market']

        db.drop_collection(self.collection)
        db.create_collection(self.collection)
        with open(self.filename, 'r') as f:
            for line in csv.DictReader(f):
                db.FishMarket.insert_one(line)
