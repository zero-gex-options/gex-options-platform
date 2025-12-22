"""
TradeStation OAuth Token Generator

Interactive script to get OAuth tokens from TradeStation.
"""

import requests
import webbrowser
from urllib.parse import urlencode, parse_qs, urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
CLIENT_ID = os.getenv('TRADESTATION_CLIENT_ID')
CLIENT_SECRET = os.getenv('TRADESTATION_CLIENT_SECRET')
REDIRECT_URI = 'http://localhost:3000'

# OAuth URLs
AUTH_URL = 'https://signin.tradestation.com/authorize'
TOKEN_URL = 'https://signin.tradestation.com/oauth/token'

# For sandbox
USE_SANDBOX = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
if USE_SANDBOX:
    AUTH_URL = 'https://sim-signin.tradestation.com/authorize'
    TOKEN_URL = 'https://sim-signin.tradestation.com/oauth/token'

# Global to store authorization code
auth_code = None


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler to receive OAuth callback"""
    
    def do_GET(self):
        """Handle GET request with authorization code"""
        global auth_code
        
        # Parse the callback URL
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        
        if 'code' in params:
            auth_code = params['code'][0]
            
            # Send success page
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = """
            <html>
            <head><title>TradeStation OAuth</title></head>
            <body style="font-family: Arial; text-align: center; margin-top: 100px;">
                <h1>✅ Authorization Successful!</h1>
                <p>You can close this window and return to the terminal.</p>
            </body>
            </html>
            """
            self.wfile.write(html.encode())
        else:
            # Send error page
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = """
            <html>
            <head><title>TradeStation OAuth Error</title></head>
            <body style="font-family: Arial; text-align: center; margin-top: 100px;">
                <h1>❌ Authorization Failed</h1>
                <p>No authorization code received.</p>
            </body>
            </html>
            """
            self.wfile.write(html.encode())
    
    def log_message(self, format, *args):
        """Suppress default logging"""
        pass


def start_callback_server():
    """Start HTTP server to receive OAuth callback"""
    server = HTTPServer(('localhost', 3000), OAuthCallbackHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server


def get_authorization_code():
    """Get authorization code via browser flow"""
    global auth_code
    
    print("\n" + "="*60)
    print("Step 1: Get Authorization Code")
    print("="*60)
    
    # Start local server
    print("\n1. Starting local callback server on port 3000...")
    server = start_callback_server()
    
    # Build authorization URL
    auth_params = {
        'response_type': 'code',
        'client_id': CLIENT_ID,
        'audience' : 'https://api.tradestation.com',
        'redirect_uri': REDIRECT_URI,
        'state' : 'gex',
        'scope': 'openid offline_access profile MarketData ReadAccount Trade OptionSpreads'
    }
    
    auth_url = f"{AUTH_URL}?{urlencode(auth_params)}"
    
    print("2. Opening browser for TradeStation login...")
    print(f"\nIf browser doesn't open, visit this URL manually:")
    print(f"{auth_url}\n")
    
    # Open browser
    webbrowser.open(auth_url)
    
    # Wait for callback
    print("3. Waiting for authorization...")
    print("   (Please login and authorize in your browser)")
    
    import time
    timeout = 120  # 2 minutes
    elapsed = 0
    
    while auth_code is None and elapsed < timeout:
        time.sleep(1)
        elapsed += 1
        if elapsed % 10 == 0:
            print(f"   Still waiting... ({elapsed}s)")
    
    server.shutdown()
    
    if auth_code is None:
        print("\n❌ Timeout waiting for authorization")
        return None
    
    print(f"\n✅ Authorization code received!")
    print(f"Code: {auth_code[:20]}...")
    
    return auth_code


def exchange_code_for_tokens(code):
    """Exchange authorization code for access and refresh tokens"""
    
    print("\n" + "="*60)
    print("Step 2: Exchange Code for Tokens")
    print("="*60)
    
    data = {
        'grant_type': 'authorization_code',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'code': code,
        'redirect_uri': REDIRECT_URI
    }
    
    print("\nRequesting tokens from TradeStation...")
    
    try:
        response = requests.post(TOKEN_URL, data=data)
        response.raise_for_status()
        
        tokens = response.json()
        
        print("\n✅ Tokens received!")
        
        return tokens
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Failed to get tokens: {e}")
        if hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        return None


def save_tokens_to_env(tokens):
    """Save tokens to .env file"""
    
    print("\n" + "="*60)
    print("Step 3: Save Tokens")
    print("="*60)
    
    refresh_token = tokens.get('refresh_token')
    access_token = tokens.get('access_token')
    expires_in = tokens.get('expires_in')
    
    print(f"\nAccess Token: {access_token[:20]}...")
    print(f"Refresh Token: {refresh_token[:20]}...")
    print(f"Expires in: {expires_in} seconds")
    
    # Update .env file
    env_path = '.env'
    
    print(f"\nUpdating {env_path}...")
    
    # Read existing .env
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
    
    # Update refresh token
    env_vars['TRADESTATION_REFRESH_TOKEN'] = refresh_token
    
    # Write back
    with open(env_path, 'w') as f:
        f.write("# TradeStation API\n")
        for key, value in env_vars.items():
            if key.startswith('TRADESTATION'):
                f.write(f"{key}={value}\n")
        
        f.write("\n# Database\n")
        for key, value in env_vars.items():
            if key.startswith('DB_'):
                f.write(f"{key}={value}\n")
        
        f.write("\n# Application Settings\n")
        for key, value in env_vars.items():
            if not key.startswith('TRADESTATION') and not key.startswith('DB_'):
                f.write(f"{key}={value}\n")
    
    print(f"✅ Refresh token saved to {env_path}")


def main():
    """Main function"""
    
    print("\n" + "="*60)
    print("TradeStation OAuth Token Generator")
    print("="*60)
    
    if not CLIENT_ID or not CLIENT_SECRET:
        print("\n❌ Error: TRADESTATION_CLIENT_ID and TRADESTATION_CLIENT_SECRET")
        print("   must be set in .env file first!")
        return
    
    print(f"\nClient ID: {CLIENT_ID[:20]}...")
    print(f"Environment: {'SANDBOX' if USE_SANDBOX else 'PRODUCTION'}")
    print(f"Redirect URI: {REDIRECT_URI}")
    
    input("\nPress ENTER to start the OAuth flow...")
    
    # Step 1: Get authorization code
    code = get_authorization_code()
    
    if not code:
        print("\n❌ Failed to get authorization code")
        return
    
    # Step 2: Exchange for tokens
    tokens = exchange_code_for_tokens(code)
    
    if not tokens:
        print("\n❌ Failed to get tokens")
        return
    
    # Step 3: Save tokens
    save_tokens_to_env(tokens)
    
    print("\n" + "="*60)
    print("✅ OAuth flow complete!")
    print("="*60)
    print("\nYou can now run the ingestion engine:")
    print("  python src/ingestion/tradestation_streaming_ingestion_engine.py")
    print()


if __name__ == '__main__':
    main()
