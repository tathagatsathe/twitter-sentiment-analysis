from kafka import KafkaProducer
import os, requests
from dotenv import load_dotenv

load_dotenv()
bearer_token = os.getenv('2TWITTER_BEARER_TOKEN')

producer = KafkaProducer(bootstrap_servers='localhost:9092')

rules_url = "https://api.twitter.com/2/tweets/search/stream/rules"
url = 'https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at'

headers = {'Authorization': f'Bearer {bearer_token}'}

def delete_all_rules():
    response = requests.get(rules_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f'Cannot get rules (HTTP {response.status_code}): {response.text}')
    rules = response.json()
    ids = [r['id'] for r in rules['data']]
    payload = {'delete': {'ids': ids}}
    response = requests.post(rules_url, headers=headers, json=payload)
    if response.status_code != 200:
        raise Exception(f'Cannot delete rules (HTTP {response.status_code}): {response.text}')

delete_all_rules()

data = {
    "add": [
        # {"value": "Apple OR #AAPL lang:en -is:retweet", "tag":"Apple"},
        # {"value": "Microsoft OR #MSFT lang:en -is:retweet", "tag":"Microsoft"},
        # {"value": "Google OR #GOOG lang:en -is:retweet", "tag":"Google"},
        # {"value": "Amazon OR #AMZN lang:en -is:retweet", "tag":"Amazon"},
        # {"value": "Berkshire Hathaway OR #BRK-B lang:en -is:retweet", "tag":"Berkshire Hathaway"},
        {"value": "Tesla OR #TSLA lang:en -is:retweet", "tag":"Tesla"},
        # {"value": "Nvidia OR #NVDA lang:en -is:retweet", "tag":"NVIDIA"},
        # {"value": "IBM lang:en -is:retweet", "tag":"IBM"}
    ]}

response = requests.post(rules_url,headers=headers,json=data)

if response.status_code !=201:
    print(f"Error {response.status_code}: {response.text}")
else:
    print("Rules added Successfully.")


# send GET request to API endpoint
response = requests.get(url, headers=headers, stream=True)
# parse JSON data from response and publish to Kafka topic
for line in response.iter_lines():
    if line:
        # data = json.loads(line)
        print(line)
        producer.send('twitter', value=line)

producer.close()

# Delete all the existing rules



