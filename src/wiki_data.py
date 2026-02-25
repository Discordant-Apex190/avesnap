import requests

search_query = 'anhinga'
number_of_results = 3
endpoint = 'search/page'
base_url = 'https://en.wikipedia.org/w/rest.php/v1/'

headers = {'User-Agent': 'MediaWiki REST API docs examples/0.1 (https://meta.wikimedia.org/wiki/User:APaskulin_(WMF))'}

url = base_url + endpoint
response = requests.get(url, headers=headers, params={'q': search_query, 'limit': number_of_results})
data = response.json()

for page in data['pages']:
  print(page['title'])
  print('https://en.wikipedia.org/wiki/' + page['key'])
  print()