# Main function to orchestrate the pipeline
import argparse

from Data_Engineering_202309.api.APIAccess import APIAccess
from Data_Engineering_202309.data.access.ReadDb import ReadDb
from Data_Engineering_202309.data.access.UpdateDB import UpdateDb
from Data_Engineering_202309.data.transformation.TransformData import TransformData


def pipeline(start_date, end_date):
    db = ReadDb('/Users/zgolli/PycharmProjects/challenge.db')

    # Query and build customer journeys out of the two first tables
    sessions = db.getFilteredSessions(start_date,end_date)
    conversions=db.query_data_from_database("conversions")
    session_costs=db.query_data_from_database("session_costs")




    # Transform customer journey into list(s) of dictionaries and send to IHC API
    tf = TransformData()
    customer_journeys = tf.query_transform_and_build_customer_journeys(sessions, conversions)
    api = APIAccess()
    attribution_results = api.send_data_to_ihc_api(customer_journeys)
    # Write the data received back from the IHC API to the table attribution_customer_journey
    writer = UpdateDb()
    writer.connect(db.conn)
    writer.WriteDFtoDBFromConn(attribution_results,
                       'attribution_customer_journey')

    # Fill the table channel_reporting by querying the now filled four tables

    attribute_cust_journey = db.query_data_from_database('attribution_customer_journey')


    db.close_connection()
    channel_report = tf.fill_channel_reporting(sessions, session_costs, conversions, attribute_cust_journey)
    # Create a .csv file of channel_reporting
    channel_report = tf.create_aggregated_metrics(channel_report)
    tf.write_channel_reporting_to_csv(channel_report, "channel_report.csv")


    print("Data exported to CSV: {}".format("channel_report.csv"))

if __name__ == "__main__":
    # Parse command-line arguments for start and end dates
    parser = argparse.ArgumentParser(description="Simple Data Engineering Pipeline")
    parser.add_argument("--start_date", type=str, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", type=str, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()

    # Call the main function with specified time range
    pipeline(args.start_date, args.end_date)
