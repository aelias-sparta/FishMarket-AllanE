from FishMarket import FishMarket


if __name__ == '__main__':
    # declaring the bucket names and objects
    bucket_name = "data-eng-resources"
    prefix = 'python'
    # creating an instance
    obj = FishMarket(bucket_name, prefix)
    # calling the loader function to load the data back into the s3 storage
    obj.data_loader()
    file, df_avg = obj.transformation()
    print(df_avg)


