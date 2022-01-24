from FishMarket import FishMarket

bucket_name = "data-eng-resources"
file = 0
prefix = 'python'
obj = FishMarket(bucket_name, prefix)
print(obj.transformation())