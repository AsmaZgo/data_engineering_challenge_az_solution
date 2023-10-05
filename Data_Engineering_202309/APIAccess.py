import requests


class APIAccess:
    def __init__(self):
        pass

    import requests

    def send_data_to_ihc_api(self,customer_journeys):
        ## Insert API Key here
        api_key = 'ae3e7602-f3e9-47c0-8917-796940dbc28f'

        ## Insert Conversion Type ID here
        conv_type_id = 'data_engineering_challenge'

        api_url = "https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}".format(
            conv_type_id=conv_type_id)



        # Placeholder for attribution results
        attribution_results = {}

        for conv_id, sessions in customer_journeys.items():
            # Send data to IHC API in chunks
            chunk_size = 3000  # Adjusted the chunk size based on API limits, 3000 sessions per request
            chunks = [sessions[i:i + chunk_size] for i in range(0, len(sessions), chunk_size)]

            for chunk in chunks:
                payload = {
                    "api_key": api_key,
                    "conv_type_id": conv_type_id,
                    "conv_id": conv_id,
                    "sessions": chunk
                }

                # Make API call
                response = requests.post(api_url, json=payload)
                print(response.text)
                # Handle the response and update attribution results
                # Here, you should parse the response and update attribution_results with the received data

        return attribution_results



