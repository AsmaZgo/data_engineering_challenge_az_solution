import pandas as pd


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
        merged_data2=merged_data.copy()
        print(merged_data2.head())
        merged_data = merged_data[merged_data['event_timestamp'] < merged_data['timestamp']]
        print(merged_data.head())
        merged_data=merged_data.rename(columns={"conv_id": "conversion_id","channel_name":"channel_label"
            #,"revenue":"conversion"
                                                })

        list_to_pop = ['event_timestamp', 'event_time', 'event_date', 'conv_time', 'conv_date', 'user_id']
        [merged_data.pop(col) for col in list_to_pop]
        # Group by conv_id and aggregate sessions into lists of dictionaries
        customer_journeys = merged_data.groupby('conversion_id').apply(lambda x: x.to_dict(orient='records')).to_dict()

        return customer_journeys