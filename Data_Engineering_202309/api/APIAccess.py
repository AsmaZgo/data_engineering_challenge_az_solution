import json

import pandas as pd
import requests


class APIAccess:
    def __init__(self):
        pass

    import requests

    def send_data_to_ihc_api(self, customer_journeys):
        ## Insert API Key here
        api_key = 'ae3e7602-f3e9-47c0-8917-796940dbc28f'

        ## Insert Conversion Type ID here
        conv_type_id = 'data_engineering_challenge'

        api_url = "https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}".format(
            conv_type_id=conv_type_id)

        # Placeholder for attribution results
        attribution_results = pd.DataFrame()

        for conv_id, sessions in customer_journeys.items():
            # Send data to IHC API in chunks
            chunk_size = 3000  # Adjusted the chunk size based on API limits, 3000 sessions per request
            chunks = [sessions[i:i + chunk_size] for i in range(0, len(sessions), chunk_size)]

            for chunk in chunks:

                redistribution_parameter = {
                    'initializer': {
                        'direction': 'earlier_sessions_only',
                        'receive_threshold': 0,
                        'redistribution_channel_labels': ['Direct', 'Email_Newsletter'],
                    },
                    'holder': {
                        'direction': 'any_session',
                        'receive_threshold': 0,
                        'redistribution_channel_labels': ['Direct', 'Email_Newsletter'],
                    },
                    'closer': {
                        'direction': 'later_sessions_only',
                        'receive_threshold': 0.1,
                        'redistribution_channel_labels': ['SEO - Brand'],
                    }
                }
                # Make API call
                body = {
                    'customer_journeys': chunk,
                    'redistribution_parameter': redistribution_parameter
                }

                response = requests.post(
                    api_url,
                    data=json.dumps(body),
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': api_key
                    }
                )
                results = response.json()

                # Handle the response and update attribution results
                # Here, you should parse the response and update attribution_results with the received data
                print("Status Code: " + str(results['statusCode']))

                print("-" * 30)

                print("Partial Failure Errors:" + str(results['partialFailureErrors']))

                print("-" * 30)

                print (results['value'])
                df = pd.DataFrame.from_dict(results['value'])
                if attribution_results.size == 0:
                    attribution_results = df
                else:
                    attribution_results = pd.concat([attribution_results, df])

        return attribution_results[['conversion_id', 'session_id', 'ihc']].rename(columns={"conversion_id": "conv_id"})
