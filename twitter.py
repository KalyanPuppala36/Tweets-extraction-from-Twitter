import time
import requests
import pandas as pd
import json
import config

bearer_token = config.Bearer_token
query = '(#KingKohli) OR (#ViratKohli)'
#out_file = 'raw.txt'

stream_url = "https://api.twitter.com/2/tweets/search/stream/rules"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
query_params = {'query': query,
                'tweet.fields': 'author_id,public_metrics,source',
                'user.fields': 'username,public_metrics',
                'expansions': 'author_id',
                'max_results': 100
                }


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers, params, next_token=None):
    if next_token:
        params['next_token'] = next_token
    response = requests.request("GET", url, headers=headers, params=params)
    time.sleep(3.1)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_tweets(num_tweets, output_fh):
    next_token = None
    tweets_stored = 0
    while tweets_stored < num_tweets:
        headers = create_headers(bearer_token)
        json_response = connect_to_endpoint(stream_url, headers, query_params, next_token)
        if json_response['meta']['result_count'] == 0:
            break
        author_dict = {x['id']: x['username'] for x in json_response['includes']['users']}
        for tweet in json_response['data']:
            try:
                tweet['username'] = author_dict[tweet['author_id']]
            except KeyError:
                print(f"No data for {tweet['author_id']}")
            output_fh.write(json.dumps(tweet) + '\n')
            tweets_stored += 1
        try:
            next_token = json_response['meta']['next_token']
            print(next_token)
        except KeyError:
            break
    return None


def main():
    with open(out_file, 'a') as f:
        get_tweets(100000, f)


main()

tweets = []
with open(out_file, 'r') as f:
    for row in f.readlines():
        tweet = json.loads(row)
        tweets.append(tweet)

p = pd.DataFrame(tweets)
k = (pd.json_normalize(json.loads(p.to_json(orient='records'))))
k.to_excel(r"C:\Users\KALYAN\OneDrive\Desktop\Filtered Streams.xlsx")
