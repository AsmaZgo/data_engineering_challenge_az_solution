import unittest

from Data_Engineering_202309.APIAccess import APIAccess
from Data_Engineering_202309.ReadDb import ReadDb
from Data_Engineering_202309.TransformData import TransformData


class APIAccessTest(unittest.TestCase):
    def test_send_data_to_ihc_api(self):
        db = ReadDb()
        session_sources, conversions, session_costs = db.query_all_data_from_database(
            '/Users/zgolli/PycharmProjects/challenge.db')

        tf = TransformData()
        customer_journeys = tf.query_and_build_customer_journeys(session_sources, conversions)
        api=APIAccess()
        attribution_results = api.send_data_to_ihc_api(customer_journeys)
        print(attribution_results)
        assert True


if __name__ == '__main__':
    unittest.main()