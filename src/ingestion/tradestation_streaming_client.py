"""
TradeStation Streaming Client

Handles HTTP streaming connections to TradeStation API for real-time options chain data.
Uses HTTP/1.1 chunked transfer encoding.
"""

import aiohttp
import json
import os
import argparse
import asyncio
from datetime import datetime
from typing import Callable, Optional
from tradestation_auth import TradeStationAuth
from src.utils import get_logger

# Initialize logger
logger = get_logger(__name__)

class TradeStationStreamingClient:
    """HTTP streaming client for TradeStation options chain data"""

    BASE_URL = "https://api.tradestation.com/v3/marketdata/stream"
    SANDBOX_URL = "https://sim-api.tradestation.com/v3/marketdata/stream"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize streaming client

        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: Refresh token for obtaining access tokens
            sandbox: Use sandbox environment (default False)
        """
        logger.debug("Initializing TradeStationStreamingClient...")

        self.base_url = self.SANDBOX_URL if sandbox else self.BASE_URL
        self.auth = TradeStationAuth(client_id, client_secret, refresh_token, sandbox)
        self.session: Optional[aiohttp.ClientSession] = None
        
        if sandbox:
            logger.warning(f"Using SANDBOX environment [{self.base_url}] - data may not be real-time")
        else:
            logger.info(f"Using PRODUCTION environment [{self.base_url}]")

        logger.info(f"TradeStationStreamingClient initialized for {'sandbox' if sandbox else 'production'}")

    async def __aenter__(self):
        """Async context manager entry"""
        if not self.session:
            self.session = aiohttp.ClientSession()
            logger.debug("Created aiohttp session")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def close(self):
        """Close HTTP session and cleanup"""
        logger.info("Closing HTTP streaming session...")

        if self.session and not self.session.closed:
            await self.session.close()
            logger.debug("aiohttp session closed")

        logger.info("✅ Session closed")

    async def stream_options_chain(
        self, 
        callback: Callable[[dict], None],
        underlying: str,
        expiration: Optional[str] = None,
        strike_proximity: Optional[int] = None
    ):
        """
        Stream real-time options chain data using HTTP streaming

        Args:
            callback: Async or sync function to call with each update
            underlying: Underlying symbol (e.g., 'SPY')
            expiration: Optional option expiration date (DD-MM-YYYY format)
            strike_proximity: Optional number of strikes above/below spot to stream
        """

        logger.info(f"🔄 Starting options chain stream for {underlying} {expiration}")
        
        # Construct full URL and params for API call
        url = f"{self.base_url}/options/chains/{underlying}"
        params = {'expiration': expiration} if expiration else None

        # Set strike proximity param if specified
        if strike_proximity:
            params['strikeProximity'] = strike_proximity
            logger.debug(f"Filtering to {strike_proximity} strikes above/below spot")

        # Get fresh access token
        headers = self.auth.get_headers()
        headers['Content-Type'] = 'application/json'
        headers['Accept'] = 'application/vnd.tradestation.streams.v2+json'

        if not self.session:
            self.session = aiohttp.ClientSession()

        try:
            logger.debug(f"Connecting to stream: {url} with params {params}...")
            
            async with self.session.get(
                url, 
                headers=headers, 
                params=params,
                timeout=aiohttp.ClientTimeout(total=None)  # No timeout for streaming
            ) as response:
                
                if response.status != 200:
                    logger.error(f"Stream connection failed with status {response.status}")
                    logger.error(f"Response: {await response.text()}")
                    return

                logger.info(f"✅ Options chain stream connected for {underlying} {expiration}")

                # Process streaming chunks
                await self._process_http_stream(response, callback)

        except asyncio.CancelledError:
            logger.info("Stream cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in options chain stream: {e}", exc_info=True)
            raise

    async def _process_http_stream(self, response, callback: Callable[[dict], None]):
        """
        Process HTTP chunked transfer stream

        TradeStation streams use HTTP/1.1 chunked encoding where:
        - Multiple JSON objects may be in one chunk
        - One JSON object may span multiple chunks
        - Each complete JSON object ends with newline

        Args:
            response: aiohttp response object
            callback: Function to call with each parsed JSON object
        """
        logger.debug("Starting HTTP stream processing...")
        
        buffer = ""
        chunk_count = 0
        object_count = 0
        
        try:
            async for chunk in response.content.iter_chunked(8192):
                chunk_count += 1
                
                # Decode chunk
                try:
                    text = chunk.decode('utf-8')
                    buffer += text
                except UnicodeDecodeError as e:
                    logger.warning(f"Failed to decode chunk #{chunk_count}: {e}")
                    continue

                # Process complete JSON objects (delimited by newlines)
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    # Try to parse JSON
                    try:
                        data = json.loads(line)
                        object_count += 1
                        
                        if object_count % 100 == 0:
                            logger.debug(f"Processed {object_count} objects from {chunk_count} chunks")
                        
                        # Call user's callback
                        if asyncio.iscoroutinefunction(callback):
                            await callback(data)
                        else:
                            callback(data)
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON object: {e}")
                        logger.debug(f"Problematic line: {line[:200]}...")

        except asyncio.CancelledError:
            logger.info(f"Stream processing cancelled (processed {object_count} objects)")
            raise
        except Exception as e:
            logger.error(f"Error processing stream: {e}", exc_info=True)
            raise
        finally:
            logger.info(f"Stream ended. Total: {object_count} objects from {chunk_count} chunks")


def parse_arguments():
    """
    Parse command-line arguments for TradeStation streaming client operations.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        prog='tradestation_streaming_client.py',
        description='TradeStation Streaming API Client - Stream options chain data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s --underlying SPY --expiration 01-31-2026
  %(prog)s --underlying SPY --expiration 01-31-2026 --strike-proximity 10
  %(prog)s --underlying AAPL --expiration 01-31-2026

For more help, use -h or --help
        '''
    )

    parser.add_argument('--underlying', type=str, default="SPY", help='Underlying symbol (default: SPY)')
    parser.add_argument('--expiration', type=str, help='Expiration date in DD-MM-YYYY format (optional)')
    parser.add_argument('--strike-proximity', type=int, help='Number of strikes above/below spot to stream (optional)')
    parser.add_argument('--duration', type=int, default=60, help='How long to stream in seconds (default: 60)')
    
    return parser.parse_args()


async def main():
    """Main entry point"""
    
    print("\n" + "="*60)
    print("TradeStation Streaming API Client")
    print("="*60 + "\n")

    args = parse_arguments()

    # Initialize client
    async with TradeStationStreamingClient(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    ) as client:
        
        print(f"Streaming options chain for {args.underlying} exp {args.expiration}")
        if args.strike_proximity:
            print(f"Strike proximity: {args.strike_proximity}")
        print(f"Duration: {args.duration} seconds")
        print("-" * 60)
        print()
        
        update_count = 0
        
        async def option_handler(data):
            nonlocal update_count
            update_count += 1
            
            # Print first 5 updates with full data
            if update_count <= 5:
                print(f"Update #{update_count}:")
                print(json.dumps(data, indent=2))
                print()
            # Then just count
            elif update_count % 50 == 0:
                print(f"  ... received {update_count} updates so far")
        
        # Stream for specified duration
        stream_task = asyncio.create_task(
            client.stream_options_chain(
                option_handler,
                args.underlying, 
                args.expiration, 
                args.strike_proximity
            )
        )
        
        await asyncio.sleep(args.duration)
        stream_task.cancel()
        
        try:
            await stream_task
        except asyncio.CancelledError:
            pass
        
        print()
        print("="*60)
        print(f"✅ Stream complete! Received {update_count} updates in {args.duration} seconds")
        print("="*60)
        print()


if __name__ == '__main__':
    asyncio.run(main())
