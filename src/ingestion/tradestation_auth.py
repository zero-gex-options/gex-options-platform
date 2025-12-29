"""
TradeStation Authentication Manager

Handles OAuth2 authentication with TradeStation API.
"""

import os
import requests
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

load_dotenv()

# Get and validate logging level from environment
log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
valid_levels = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

if log_level_str in valid_levels:
    log_level = valid_levels[log_level_str]
else:
    log_level = logging.INFO
    print(f"Warning: Invalid LOG_LEVEL '{log_level_str}', defaulting to INFO. Valid options: {', '.join(valid_levels.keys())}")

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TradeStationAuth:
    """Manage TradeStation API authentication"""

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize auth manager

        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: Refresh token for obtaining access tokens
            sandbox: Use sandbox environment (default False)
        """
        logger.debug(f"Initializing TradeStationAuth (sandbox={sandbox})")

        if not client_id or not client_secret or not refresh_token:
            logger.critical("Missing required authentication credentials!")
            raise ValueError("Client ID, Client Secret, and Refresh Token are required")

        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.sandbox = sandbox

        self.token_url = "https://signin.tradestation.com/oauth/token"
        self.access_token = None
        self.token_expiry = None

        logger.info(f"TradeStation auth initialized for {'sandbox' if sandbox else 'production'}")

    def get_access_token(self) -> str:
        """
        Get valid access token, refreshing if necessary

        Returns:
            Valid access token
        """
        logger.debug("Checking access token validity")

        if self.access_token and self.token_expiry:
            time_until_expiry = (self.token_expiry - datetime.now()).total_seconds()
            logger.debug(f"Token expires in {time_until_expiry:.0f} seconds")

            if datetime.now() < self.token_expiry:
                logger.debug("Using cached access token")
                return self.access_token
            else:
                logger.info("Access token expired, refreshing...")
        else:
            logger.info("No cached token, obtaining new access token...")

        return self._refresh_access_token()

    def _refresh_access_token(self) -> str:
        """
        Refresh access token using refresh token

        Returns:
            New access token
        """
        logger.debug(f"Requesting new access token from {self.token_url}")

        payload = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }

        try:
            response = requests.post(self.token_url, data=payload, timeout=10)

            logger.debug(f"Token request status code: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"Token refresh failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                response.raise_for_status()

            data = response.json()

            self.access_token = data['access_token']
            expires_in = data.get('expires_in', 1200)  # Default 20 minutes

            # Set expiry with 60 second buffer
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)

            logger.info(f"✅ Access token refreshed successfully (expires in {expires_in}s)")
            logger.debug(f"Token expiry set to: {self.token_expiry}")

            return self.access_token

        except requests.exceptions.Timeout:
            logger.error("Token refresh request timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Token refresh request failed: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected token response format, missing key: {e}")
            logger.debug(f"Response data: {data}")
            raise
        except Exception as e:
            logger.critical(f"Unexpected error during token refresh: {e}", exc_info=True)
            raise

    def get_headers(self) -> dict:
        """
        Get authorization headers for API requests

        Returns:
            Dictionary with Authorization header
        """
        token = self.get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        logger.debug("Generated authorization headers")
        return headers


# Test
if __name__ == '__main__':
    logger.info("Testing TradeStation authentication...")

    auth = TradeStationAuth(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    )

    try:
        token = auth.get_access_token()
        logger.info(f"✅ Auth test successful! Token: {token[:20]}...")
    except Exception as e:
        logger.error(f"❌ Auth test failed: {e}")
