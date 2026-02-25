import requests

search_query = 'Anhinga'
number_of_results = 2
endpoint = 'search/page'
base_url = 'https://en.wikipedia.org/w/rest.php/v1/'

headers = {'User-Agent': 'Avesnap- My birding data website'}

url = base_url + endpoint
response = requests.get(url, headers=headers, params={'q': search_query, 'limit': number_of_results})
data = response.json()

for page in data['pages']:
  print(page['title'])
  print(page['excerpt'])
  print('https://en.wikipedia.org/wiki/' + page['key'])
  if page['description'] is not None:
    print(page['description'])
  print()
  thumbnail_url = 'https:' + page['thumbnail']['url']
  print(thumbnail_url)