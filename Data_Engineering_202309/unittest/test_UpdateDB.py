import unittest

from Data_Engineering_202309.api.APIAccess import APIAccess
from Data_Engineering_202309.data.access.ReadDb import ReadDb
from Data_Engineering_202309.data.transformation.TransformData import TransformData
from Data_Engineering_202309.data.access.UpdateDB import UpdateDb


class UpdateDBTest(unittest.TestCase):
    def test_write(self):
        db = ReadDb()
        session_sources, conversions, session_costs = db.query_all_data_from_database(
            '/Users/zgolli/PycharmProjects/challenge.db')

        tf = TransformData()
        customer_journeys = tf.query_transform_and_build_customer_journeys(session_sources, conversions)
        api = APIAccess()
        attribution_results = api.send_data_to_ihc_api(customer_journeys)
        writer=UpdateDb()
        writer.WriteDFtoDB('/Users/zgolli/PycharmProjects/challenge.db',attribution_results,'attribution_customer_journey')
        assert True


if __name__ == '__main__':
    unittest.main()
