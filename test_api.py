"""Quick API test script - following Sam's email exactly."""
import os
from dotenv import load_dotenv
from pathlib import Path
import requests

# Load env
load_dotenv(Path(".env"))
client_id = os.getenv('MOATA_CLIENT_ID')
client_secret = os.getenv('MOATA_CLIENT_SECRET')

print(f"Client ID: {client_id[:15]}...")

# Get token - EXACTLY as Sam's email
print("\n1. Getting token (Sam's method)...")
token_url = 'https://moata.b2clogin.com/moata.onmicrosoft.com/B2C_1A_CLIENTCREDENTIALSFLOW/oauth2/v2.0/token'

# Sam uses requests.get with data and params!
r = requests.get(
    token_url,
    data={
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    },
    params={
        'scope': 'https://moata.onmicrosoft.com/moata.io/.default'
    },
    verify=False
)
print(f"Token status: {r.status_code}")
if r.status_code != 200:
    print(f"Error: {r.text[:300]}")
    exit(1)
    
token = r.json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

# Test rain gauge (asset type 100 - from Sam's email!)
print("\n2. Rain Gauge assets (assetType=100)...")
url1 = 'https://api.moata.io/ae/v1/projects/594/assets?assetTypeId=100'
r1 = requests.get(url1, headers=headers, verify=False)
print(f"   Status: {r1.status_code}")
if r1.status_code == 200:
    data = r1.json()
    items = data if isinstance(data, list) else data.get('items', [])
    print(f"   Found: {len(items)} rain gauges")
    if items:
        print(f"   First: {items[0].get('name', 'unknown')}")

# Test radar catchment (asset type 3541)
print("\n3. Radar Catchment assets (assetType=3541)...")
url2 = 'https://api.moata.io/ae/v1/projects/594/assets?assetTypeId=3541'
r2 = requests.get(url2, headers=headers, verify=False)
print(f"   Status: {r2.status_code}")
if r2.status_code == 200:
    data = r2.json()
    items = data.get('items', [])
    print(f"   Found: {len(items)} catchments")
