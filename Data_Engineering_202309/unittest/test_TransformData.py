import unittest

from Data_Engineering_202309.ReadDb import ReadDb
from Data_Engineering_202309.TransformData import TransformData


class TransformDataTest(unittest.TestCase):

    def test_build_customer_journeys(self):
        db = ReadDb()
        session_sources, conversions, session_costs = db.query_all_data_from_database(
            '/Users/zgolli/PycharmProjects/challenge.db')
        print(session_sources.head())
        tf = TransformData()
        customer_journeys = tf.query_and_build_customer_journeys(session_sources, conversions)
        print(customer_journeys)
        assert True


if __name__ == '__main__':
    unittest.main()