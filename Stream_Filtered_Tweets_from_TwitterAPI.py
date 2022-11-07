import requests
import os
import json
from time import sleep
from json import dumps
from kafka import KafkaProducer 
import sys

# To set your enviornment variables, in your terminal, run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'   # run this on the Teminal to set the bearer token used in the next line
bearer_token = os.environ.get("BEARER_TOKEN")   # get something from OS using bearer token for authantication


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = "Bearer " + bearer_token
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()

# 
def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))
    
def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
                { "value": "(@ClimateChangeAI OR @ClimateChangeG2 OR @media_climate OR @EINWarming OR @globalwarmingg OR @globalwarmingt OR #ClimateChange OR @ThePlanetEarth OR #ClimateAction OR #ClimateEmergency OR #GlobalWarming OR \"climate change\" OR \"global warming\")", "tag": "OTHER" }, 
        {"value": "climate change OR #climatechange", "tag": "climate change text only"},
        {"value": "(climate change OR #climatechange) is:verified", "tag": "climate change text plus verified"},
        {"value": "(climate change OR #climatechange) #HappeningNow", "tag": "climate change plus happening now hashtag"},
        {"value": "(climate change OR #climatechange) (@Greenpeace OR @World_Wildlife OR @earthisland OR @RAN OR @foe_us OR @EndOvershoot OR @NRDC OR @RnfrstAlliance OR @Earthjustice OR @EnvDefenseFund)", \
        "tag": "climate change plus non profit mentions"}
    ]
    
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))

def setup_kafka_producer():
    producer = KafkaProducer(bootstrap_servers='kafka:29092')
    return producer

def get_stream(set, producer):
    response = requests.get(
         "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at,public_metrics&expansions=geo.place_id,author_id&user.fields=name,username,location&place.fields=country,country_code,full_name,geo,id,name,place_type",auth=bearer_oauth, stream=True,

    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
        
    for num, response_line in enumerate(response.iter_lines()):
        if response_line:
            print(response_line)                                  # print out raw data from twitter in binary     
            producer.send('climateChange', value=response_line)
            print('num value:', num)
            
            if num >= 5000:           
                print("Collected " + str(num) + " tweets into Producer. Exiting the program!")
                sys.exit()      
                
def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    producer = setup_kafka_producer()    # new
    get_stream(set, producer)
    sleep(10)

if __name__ == "__main__":
    main()