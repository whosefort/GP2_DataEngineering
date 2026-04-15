import requests
url = "https://api.reccobeats.com/v1/track/recommendation&size=100"

payload = {}
headers = {
  'Accept': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)