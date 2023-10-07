import unittest

from Data_Engineering_202309.data.access.ReadDb import ReadDb
from Data_Engineering_202309.data.transformation.TransformData import TransformData


class TransformDataTest(unittest.TestCase):

    def test_build_customer_journeys(self):
        db = ReadDb('/Users/zgolli/PycharmProjects/challenge.db')
        session_sources, conversions, session_costs = db.query_all_data_from_database()
        print(session_sources.head())
        db.close_connection()
        tf = TransformData()
        customer_journeys = tf.query_and_build_customer_journeys(session_sources, conversions)
        print(customer_journeys)

        assert True

    def test_build_channel_report(self):
        db = ReadDb('/Users/zgolli/PycharmProjects/challenge.db')
        session_sources, conversions, session_costs = db.query_all_data_from_database()
        attribute_cust_journey = db.query_data_from_database('attribution_customer_journey')
        db.close_connection()
        tf = TransformData()
        channel_report = tf.fill_channel_reporting(session_sources, session_costs, conversions, attribute_cust_journey)

        assert True

    def test_calculate_metrics_channel_report(self):
        db = ReadDb('/Users/zgolli/PycharmProjects/challenge.db')

        channel_report = db.query_data_from_database('channel_reporting')

        tf = TransformData()
        channel_report = tf.create_aggregated_metrics(channel_report)

        assert True

    def test_write_channel_report(self):
        db = ReadDb('/Users/zgolli/PycharmProjects/challenge.db')

        channel_report = db.query_data_from_database('channel_reporting')
        db.close_connection()
        tf = TransformData()
        channel_report = tf.create_aggregated_metrics(channel_report)
        tf.write_channel_reporting_to_csv(channel_report, "channel_report.csv")
        assert True


if __name__ == '__main__':
    unittest.main()
