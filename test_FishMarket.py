import unittest
import csv
import pandas as pd
from FishMarket import FishMarket
import boto3
from pandas.testing import assert_frame_equal
s3_client = boto3.client("s3")
test_obj = FishMarket(bucket_name="data-eng-resources", prefix='python')

class TestFishMarket(unittest.TestCase):

# tsting the extraction function

    def test_extract_csv(self):
        s3_object = s3_client.get_object(Bucket="data-eng-resources", Key='python/fish-market-mon.csv')
        expected = pd.read_csv(s3_object["Body"])
        assert test_obj.extract_csv is not None
        assert isinstance(test_obj.extract_csv(), pd.DataFrame)
        # this check the contents of fish-market-mon exist in combine output file
        assert_frame_equal(expected, test_obj.extract_csv().iloc[0:159,:])

# testing the transformation function
    def test_transformation(self):
        file, df_avg = test_obj.transformation()
        with open(file, 'r') as csv_file:
            reader = csv.reader(csv_file, dialect='excel')
            self.assertEqual(next(reader), ["Species","Weight","Length1","Length2","Length3","Height","Width"])
            # self.assertEqual(next(reader), ["Bream",621.0342592219048,33.30624040942857,36.63864104609524,41.38783637542857,
            #                                 18.699551565333334,6.175619729380953])
        assert df_avg.shape == (7, 6) # the averahe dataframe should be 7 columns by 6 rows

if __name__ == '__main__':
    unittest.main()
