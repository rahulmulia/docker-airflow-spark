import requests
import ast
import json




def generate_token():
  url = "https://accounts.spotify.com/api/token"

  payload = 'grant_type=client_credentials'
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Authorization': 'Basic <fill_your_token>'
  }

  response = requests.request("POST", url, headers=headers, data=payload)
  dictResult = ast.literal_eval(response.text)
  with open("token.json", 'w') as fi:
    json.dump(dictResult, fi, indent=2)

  return

# generate_token()