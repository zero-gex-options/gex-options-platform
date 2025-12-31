"""
TradeStation OAuth Token Generator

Interactive script to get OAuth tokens from TradeStation.
This must be run manually during the initial setup of the application.

This process reads the TradeStation API client ID and secret from your
local .env file, prints out the authorization URL to access via web
browser, waits for you to input the callback URL provided from the web
browser session, then exchanges for tokens and saves the refresh token
to your local .env file.

For more detailed information, review the TradeStation API
documentation for Auth Code Flow:
https://api.tradestation.com/docs/fundamentals/authentication/auth-code
"""

import os
import requests
from urllib.parse import urlencode, urlparse, parse_qs
from dotenv import load_dotenv

print("\n" + "="*60)
print("TradeStation OAuth Setup")
print("="*60)

# Load .env file
load_dotenv()

# Configuration
CLIENT_ID = os.getenv('TRADESTATION_CLIENT_ID')
CLIENT_SECRET = os.getenv('TRADESTATION_CLIENT_SECRET')
REDIRECT_URI = "http://localhost:3000"
USE_SANDBOX = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'

# Check that client ID and secret are specified in .env
if not CLIENT_ID or not CLIENT_SECRET:
    print("❌ Error: TRADESTATION_CLIENT_ID and TRADESTATION_CLIENT_SECRET must be set in .env file")
    print("\nPlease add these lines to /home/ubuntu/gex-options-platform/.env:")
    print("TRADESTATION_CLIENT_ID=your_client_id_here")
    print("TRADESTATION_CLIENT_SECRET=your_client_secret_here")
    exit(1)

print(f"✅ Loaded credentials from .env file")
print(f"   Client ID: {CLIENT_ID[:20]}...")

# OAuth URLS
AUTH_URL = "https://signin.tradestation.com/authorize"
TOKEN_URL = "https://signin.tradestation.com/oauth/token"
if USE_SANDBOX:
    AUTH_URL = "https://sim-signin.tradestation.com/authorize"
    TOKEN_URL = "https://sim-signin.tradestation.com/oauth/token"

# Generate authorization URL
params = {
    'response_type': 'code',
    'client_id': CLIENT_ID,
    'audience' : 'https://api.tradestation.com',
    'redirect_uri': REDIRECT_URI,
    'state' : 'gex',
    'scope': 'openid offline_access profile MarketData ReadAccount Trade OptionSpreads'
}

auth_url = f"{AUTH_URL}?{urlencode(params)}"

print("\nSTEP 1: Visit this URL in your browser:\n")
print(auth_url)

print("\n\nSTEP 2: After authorizing, you'll be redirected to a URL like:")
print("http://localhost:3000/callback?code=XXXXX")
print("\nThe page won't load (that's OK). Just copy the ENTIRE URL from your browser.")

callback_url = input("\n\nPaste the callback URL here: ").strip()

# Extract code
parsed = urlparse(callback_url)
params = parse_qs(parsed.query)

if 'code' not in params:
    print("❌ No authorization code found in URL")
    exit(1)

auth_code = params['code'][0]
print(f"\n✅ Authorization code: {auth_code[:20]}...")

# Exchange for tokens
print("\n🔄 Exchanging code for tokens...")

data = {
    'grant_type': 'authorization_code',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'code': auth_code,
    'redirect_uri': REDIRECT_URI
}

response = requests.post(TOKEN_URL, data=data)

if response.status_code == 200:

    # Parse tokens from JSON response
    tokens = response.json()

    # Validate that tokens were successfully
    # parsed from JSON response
    token_types = ["access_token", "refresh_token", "expires_in"]
    if all(key in tokens for key in token_types):
        access_token = tokens.get('access_token')
        refresh_token = tokens.get('refresh_token')
        expires_in = tokens.get('expires_in')
        print("✅ Tokens received!")
        print(f"\nAccess Token: {access_token[:20]}...")
        print(f"Refresh Token: {refresh_token[:20]}...")
        print(f"Expires in: {expires_in} seconds")

    # Save to .env
    env_path = '/home/ubuntu/gex-options-platform/.env'
    
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    # Remove old token lines
    lines = [l for l in lines if not l.startswith('TRADESTATION_ACCESS_TOKEN=') 
             and not l.startswith('TRADESTATION_REFRESH_TOKEN=')]
    
    # Add new tokens
    lines.append(f"TRADESTATION_ACCESS_TOKEN={tokens['access_token']}\n")
    lines.append(f"TRADESTATION_REFRESH_TOKEN={tokens['refresh_token']}\n")
    
    with open(env_path, 'w') as f:
        f.writelines(lines)
    
    print(f"\n💾 Tokens saved to {env_path}")
    print("\n✅ Done! You can now start your services:")
    print("   sudo systemctl start gex-ingestion")
    print("   sudo systemctl start gex-scheduler")
    
else:
    print(f"❌ Failed: {response.status_code}")
    print(response.text)
