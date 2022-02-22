#imports
import requests
import json
import boto3
from datetime import datetime

#Global objects
now = datetime.now()
client = boto3.client('dynamodb')

#Globals
categories = ['chinese', 'french', 'mexican', 'sushi', 'thai', 'spanish','southern','burgers']
NUM_CATEGORIES = len(categories)
                     
#Yelp API Info
url="https://api.yelp.com/v3/businesses/search"
api_key="GwqhCEhInWtmshuieG1o50BH2G0ce5FHW7nBrayGp6Wtass12ZPsh1bP6qeXD4MeKJymKp_1JEgLHPhO_9EqiLm39jf3zx62_x7d8-Ph_gO8YvY4lX7dooTwJQERYnYx" #Not hidden but okay
headers = {
    'Authorization': 'Bearer %s' % api_key,
}
parameters = {
    'location': 'Manhattan',
    'term': 'Resturant',
    'radius': 40000,
    'limit': 50
}

#API Call for Each Category
def get_businesses():
    data = []
    for i in range(0, NUM_CATEGORIES):
        for offset in range(0, 1000, 50):
            params = { **parameters, 'offset': offset, 'category': categories[i]}
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data += (response.json()['businesses'])
            elif response.status_code == 400:
                print('400 Bad Request')
                break
    return data

def format(restaurants):
    data = []
    for r in restaurants:
        data.append({
            "id": r['id'],
            "location": r['location']['address1'],
            "categories": r['categories']
        })
    return data

def write(data):
    for restaurant in data:
        client.put_item(
            TableName='yelp_restaurants',
            Item={
                'business_id': {'S' : restaurant['id']},
                'name': {'S' : restaurant['name']},
                'address': {'S': restaurant['location']['address1'] },
                'coordinates': {'M' : 
                        { 'latitude': 
                            { 'N': str(restaurant['coordinates']['latitude']) },
                           'longitude':
                            { 'N': str(restaurant['coordinates']['longitude']) }
                        }
                },
                'num_reviews': { 'N' : str(restaurant['review_count']) },
                'rating': {'N' : str(restaurant['rating'])},
                'zip_code':{'S': restaurant['location']['zip_code']},
                'inserted_at_timestamp': {'S': now.strftime("%d/%m/%Y %H:%M:%S")}
            }
        )

def main():
    data = get_businesses()
    write(data)
    extract = format(data)
    print(json.dumps(extract)) #Pipe output to a JSON file so we can import to opensearch 
main()