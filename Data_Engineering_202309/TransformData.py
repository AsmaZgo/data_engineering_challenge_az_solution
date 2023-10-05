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