import pandas as pd

from Data_Engineering_202309.data.access.UpdateDB import UpdateDb


class TransformData:
    def __init__(self):
        pass
    def query_and_build_customer_journeys(self, session_sources, conversions):
        # Merge session_sources and conversions on user_id
        merged_data = pd.merge(session_sources, conversions, on='user_id', how='inner')

        # Filter sessions that happened before the conversion timestamp
        merged_data = merged_data[merged_data['event_date'] < merged_data['conv_date']]

        # Group by conv_id and aggregate sessions into lists of dictionaries
        customer_journeys = merged_data.groupby('conv_id').apply(lambda x: x.to_dict(orient='records')).to_dict()

        return customer_journeys

    def query_transform_and_build_customer_journeys(self, session_sources, conversions):
        # Merge session_sources and conversions on user_id
        merged_data = pd.merge(session_sources, conversions, on='user_id', how='inner')

        merged_data['timestamp']=merged_data['conv_date']+" "+merged_data['conv_time']

        merged_data['event_timestamp']=merged_data['event_date']+" "+merged_data['event_time']
        # Filter sessions that happened before the conversion timestamp
        merged_data = merged_data[merged_data['event_timestamp'] < merged_data['timestamp']]
        print(merged_data.head())
        merged_data=merged_data.rename(columns={"conv_id": "conversion_id","channel_name":"channel_label"
            #,"revenue":"conversion"
                                                })

        list_to_pop = ['event_timestamp', 'event_time', 'event_date', 'conv_time', 'conv_date', 'user_id']
        [merged_data.pop(col) for col in list_to_pop]
        # From the following hint: for each conv_id you need to get all sessions for the given user_id that happened before the conversion timestamp
        # we understand that a conversation always happened thus the attribute conversation equal to 1
        merged_data['conversion']=1
        # Group by conv_id and aggregate sessions into lists of dictionaries
        customer_journeys = merged_data.groupby('conversion_id').apply(lambda x: x.to_dict(orient='records')).to_dict()

        return customer_journeys

    def fill_channel_reporting(self,session_sources, session_costs, conversions, attribution_customer_journey):

        channel_reporting = pd.merge(session_sources, session_costs, on='session_id', how='inner')
        channel_reporting = pd.merge(channel_reporting, conversions, on='user_id', how='inner')
        channel_reporting = pd.merge(channel_reporting, attribution_customer_journey, on='conv_id', how='inner')
        writer = UpdateDb()
        writer.WriteDFtoDB('/Users/zgolli/PycharmProjects/challenge.db', channel_reporting,
                           'channel_reporting')

        return channel_reporting

    def create_aggregated_metrics(self,channel_reporting):
        # Group by channel_name and event_date, aggregate data and calculate CPO and ROAS
        channel_reporting = channel_reporting.groupby(['channel_name', 'event_date']).agg({
            'cost': 'sum',
            'ihc': 'sum',
            'revenue': 'sum'
        }).reset_index()

        channel_reporting['CPO'] = channel_reporting['cost'] / channel_reporting['ihc']
        channel_reporting['ROAS'] = channel_reporting['revenue'] / channel_reporting['cost']

        return channel_reporting

    def write_channel_reporting_to_csv(self,channel_reporting, file_path):
        channel_reporting.to_csv(file_path, index=False)