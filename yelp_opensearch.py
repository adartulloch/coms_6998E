from calendar import weekday
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json

# This loop iterates through the JSON Object containing our restaurant ID and address
def process_json():
    id = 1
    bulk_file = ''
    f = open('opensearch_db.json')
    restaurants = json.load(f)

    # Now we iterate over all of the dict elements to format in the correct way, before forming OpenSearch index
    for restaurant in restaurants:

        # Document portion of bulk file
        index = { 'id': restaurant['id'], 'location' : restaurant['location'], 'categories': restaurant['categories']}
        
        #Action and metadata portions
        bulk_file += '{ "index" : { "_index" : "restaurants", "_type" : "_doc", "_id" : "' + str(id) + '" } }\n'

        # The optional_document portion of the bulk file
        bulk_file += json.dumps(index) + '\n'

        id += 1

    return bulk_file

host = 'search-restaurants-2wjvub6neyby3znk4yquei7r5u.us-east-1.es.amazonaws.com/'
region = 'us-east-1'

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)

search = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

search.bulk(process_json())