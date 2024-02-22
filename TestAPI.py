import requests
import json

def random_user():
    res = requests.get("https://randomuser.me/api/")
    data = res.json()
    print(json.dumps(data, indent=3))

random_user()

