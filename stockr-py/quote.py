import _conf as cf
import json
import pprint
import sseclient

import urllib3
import requests

def with_urllib3(url):
    """Get a streaming response for the given event feed using urllib3."""
    http = urllib3.PoolManager()
    return http.request('GET', url, preload_content=False)

def with_requests(url):
    """Get a streaming response for the given event feed using requests."""
    return requests.get(url, stream=True)

url = cf.url_quote_sse
response = with_urllib3(url)  # or with_requests(url)
client = sseclient.SSEClient(response)
for event in client.events():
    pprint.pprint(json.loads(event.data))

