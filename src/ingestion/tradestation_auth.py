"""
TradeStation OAuth 2.0 Authentication Handler

Manages access tokens with automatic refresh.
"""

import requests
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TradeStationAuth:
    """Handle TradeStation OAuth 2.0 authentication and token refresh"""
    
    TOKEN_URL = "https://signin.tradestation.com/oauth/token"
    SANDBOX_TOKEN_URL = "https://sim-signin.tradestation.com/oauth/token"
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize authentication handler
        
        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: OAuth refresh token from initial authorization
            sandbox: Use sandbox environment (default False)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.sandbox = sandbox
        
        self.token_url = self.SANDBOX_TOKEN_URL if sandbox else self.TOKEN_URL
        
        self.access_token = None
        self.token_expiry = None
        
        logger.info(f"TradeStation Auth initialized (sandbox={sandbox})")
    
    def get_access_token(self) -> str:
        """
        Get valid access token, refreshing if necessary
        
        Returns:
            Valid access token
        """
        # Check if we need to refresh
        if self.access_token is None or self._is_token_expired():
            logger.info("Access token missing or expired, refreshing...")
            self._refresh_access_token()
        
        return self.access_token
    
    def _is_token_expired(self) -> bool:
        """Check if current token is expired or will expire soon"""
        if self.token_expiry is None:
            return True
        
        # Refresh if less than 5 minutes remaining
        return datetime.now() >= (self.token_expiry - timedelta(minutes=5))
    
    def _refresh_access_token(self):
        """Refresh the access token using refresh token"""
        
        logger.info("Requesting new access token...")
        
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }
        
        try:
            response = requests.post(self.token_url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            
            self.access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 1200)  # Default 20 minutes
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            # Update refresh token if provided
            if 'refresh_token' in token_data:
                self.refresh_token = token_data['refresh_token']
                logger.info("Refresh token updated")
            
            logger.info(f"✅ Access token obtained, expires in {expires_in} seconds")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise
    
    def get_headers(self) -> dict:
        """
        Get HTTP headers with valid authorization
        
        Returns:
            Dictionary of headers for API requests
        """
        token = self.get_access_token()
        
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def revoke_token(self):
        """Revoke the current access token"""
        # TradeStation doesn't provide token revocation endpoint
        # Just clear local tokens
        self.access_token = None
        self.token_expiry = None
        logger.info("Tokens cleared")


# Test function
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logging.basicConfig(level=logging.INFO)
    
    print("Testing TradeStation Authentication")
    print("="*60)
    
    client_id = os.getenv('TRADESTATION_CLIENT_ID')
    client_secret = os.getenv('TRADESTATION_CLIENT_SECRET')
    refresh_token = os.getenv('TRADESTATION_REFRESH_TOKEN')
    sandbox = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    
    if not all([client_id, client_secret, refresh_token]):
        print("❌ Missing credentials in .env file")
        exit(1)
    
    # Test authentication
    auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)
    
    try:
        # Get access token
        token = auth.get_access_token()
        print(f"✅ Access token obtained: {token[:20]}...")
        
        # Get headers
        headers = auth.get_headers()
        print(f"✅ Headers generated")
        print(f"   Authorization: Bearer {headers['Authorization'][7:27]}...")
        
        print("\n" + "="*60)
        print("✅ Authentication test passed!")
        
    except Exception as e:
        print(f"❌ Authentication test failed: {e}")
        exit(1)
