import requests
import json
import csv
import base64
import time
import os
import argparse
from datetime import datetime
from typing import Optional, List, Tuple
from urllib.parse import quote
import struct

# Multiple URLs for redundancy - if one fails, try the next
RPC_URLS = [
    "https://namada-rpc.wavefive.xyz",
    "https://rpc.namada-archive.citizenweb3.com",
    "https://namada-archive.tm.p2p.org",
    "https://rpc.namada.tududes.com",
    "https://namada-rpc.publicnode.com",
]

INDEXER_URLS = [
    "https://indexer.namada.tududes.com",
    "https://namada-indexer.wavefive.xyz",
    "https://namada-api.sproutstake.space",
    "https://namada-mainnet-indexer.mellifera.network",
]

MASP_EPOCH_MULTIPLIER = 4

def try_multiple_urls(urls: List[str], endpoint: str, **kwargs) -> Optional[requests.Response]:
    """
    Try multiple URLs for a given endpoint, returning the first successful response.
    
    Args:
        urls: List of base URLs to try
        endpoint: The endpoint path to append to each URL
        **kwargs: Additional arguments to pass to requests.get
        
    Returns:
        The first successful response, or None if all URLs fail
    """
    for i, base_url in enumerate(urls):
        try:
            url = f"{base_url}{endpoint}"
            print(f"Trying URL {i+1}/{len(urls)}: {url}")
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            print(f"✓ Success with URL {i+1}: {base_url}")
            return response
        except Exception as e:
            print(f"✗ Failed with URL {i+1} ({base_url}): {e}")
            if i == len(urls) - 1:  # Last URL
                print(f"All URLs failed for endpoint: {endpoint}")
                return None
            continue
    return None

# Get the most recent block from the node
def get_start_height() -> int:
    """Get the current block height from the RPC node."""
    response = try_multiple_urls(RPC_URLS, "/block")
    if response is None:
        raise Exception("Failed to get start height from all RPC URLs")
    
    try:
        data = response.json()
        height = int(data["result"]["block"]["header"]["height"])
        print(f"Current block height: {height}")
        return height
    except Exception as e:
        print(f"Error parsing start height response: {e}")
        raise

def get_token_list() -> List[str]:
    """Get token list from the indexer API."""
    response = try_multiple_urls(INDEXER_URLS, "/api/v1/chain/token")
    if response is None:
        print("Failed to get token list from all indexer URLs, using fallback")
        # Fallback to NAM token if all indexers fail
        return ["tnam1q9gr66cvu4hrzm0sd5kmlnjje82gs3xlfg3v6nu7"]
    
    try:
        tokens = response.json()
        
        # Extract addresses from the token list
        addresses = [token["address"] for token in tokens]
        print(f"Found {len(addresses)} tokens")
        return addresses
    except Exception as e:
        print(f"Error parsing token list response: {e}")
        # Fallback to NAM token if parsing fails
        return ["tnam1q9gr66cvu4hrzm0sd5kmlnjje82gs3xlfg3v6nu7"]

# Calculating the query heights
# If we assume 7s block time, 10000 blocks will be roughly 20 hrs
# Therefore we should get at least one query per masp epoch (which is 24hrs)
def do_historical_queries(start_height: int, end_height: int, end_masp_epoch: Optional[int], csv_writer, token_addresses: List[str]) -> List[int]:
    """Query historical data at regular intervals and write to CSV."""
    queried_heights = []
    interval = 10000  # Query every 10000 blocks
    current_height = start_height
    
    # Track seen MASP epochs to avoid duplicates
    seen_masp_epochs = set()
    
    while current_height >= end_height:
        try:
            print(f"Querying height: {current_height}")
            result = query_at_height(current_height, token_addresses)
            
            if result and result[2] not in seen_masp_epochs:  # result[2] is masp_epoch
                # Check if we've reached the end MASP epoch
                if end_masp_epoch is not None and result[2] <= end_masp_epoch:
                    print(f"✓ Reached end MASP epoch {end_masp_epoch}, stopping data collection")
                    break
                
                # Write base row
                base_row = {
                    'height': result[0],
                    'timestamp': result[1],
                    'masp_epoch': result[2]
                }
                
                # Add token-specific data
                for token_addr, inflation, locked in result[3]:
                    row = base_row.copy()
                    row['token_address'] = token_addr
                    row['last_inflation'] = inflation
                    row['last_locked'] = locked
                    csv_writer.writerow(row)
                
                seen_masp_epochs.add(result[2])
                queried_heights.append(current_height)
                print(f"✓ Data written for height {current_height}, MASP epoch {result[2]}")
            else:
                print(f"⚠ Skipped height {current_height} (duplicate MASP epoch or no data)")
                
        except Exception as e:
            print(f"✗ Error querying height {current_height}: {e}")
        
        # Add delay between queries to respect rate limits
        time.sleep(1)  # 1 second delay between heights
        
        current_height -= interval
    
    return queried_heights

def query_at_height(height: int, token_addresses: List[str]) -> Optional[Tuple[int, str, int, List[Tuple[str, int, int]]]]:
    """Query all data for a specific height."""
    try:
        timestamp = query_block_timestamp(height)
        masp_epoch = query_and_decode_masp_epoch(height)
        token_data = query_all_tokens_data(height, token_addresses)
        
        return (height, timestamp, masp_epoch, token_data)
    except Exception as e:
        print(f"Error querying height {height}: {e}")
        return None

def query_block_timestamp(height: int) -> str:
    """Get the timestamp from block header."""
    response = try_multiple_urls(RPC_URLS, f"/block?height={height}")
    if response is None:
        raise Exception(f"Failed to get timestamp for height {height} from all RPC URLs")
    
    try:
        data = response.json()
        timestamp = data["result"]["block"]["header"]["time"]
        return timestamp
    except Exception as e:
        print(f"Error parsing timestamp response for height {height}: {e}")
        raise

def query_and_decode_masp_epoch(height: int) -> int:
    """Get and decode the MASP epoch value."""
    path = f"/shell/epoch_at_height/{height}"
    encoded_path = quote(f'"{path}"')
    response = try_multiple_urls(RPC_URLS, f"/abci_query?path={encoded_path}")
    if response is None:
        print(f"Failed to get MASP epoch for height {height} from all RPC URLs")
        return 0
    
    try:
        data = response.json()
        
        if data["result"]["response"]["code"] == 0:
            value = data["result"]["response"]["value"]
            epoch = decode_abci_option_epoch(value)
            print(f"epoch: {epoch}")
            # MASP epoch is the epoch divided by MASP_EPOCH_MULTIPLIER, discarding remainder
            return epoch // MASP_EPOCH_MULTIPLIER
        else:
            print(f"No MASP epoch data at height {height}")
            return 0
    except Exception as e:
        print(f"Error parsing MASP epoch response for height {height}: {e}")
        return 0

def query_all_tokens_data(height: int, token_addresses: List[str]) -> List[Tuple[str, int, int]]:
    """Query last inflation and locked data for all tokens."""
    token_data = []
    
    for token_addr in token_addresses:
        try:
            last_inflation = query_and_decode_last_inflation(height, token_addr)
            last_locked = query_and_decode_last_locked(height, token_addr)
            token_data.append((token_addr, last_inflation, last_locked))
        except Exception as e:
            print(f"Error querying token {token_addr} at height {height}: {e}")
            # Add default values for failed queries
            token_data.append((token_addr, 0, 0))
    
    return token_data

def query_and_decode_last_inflation(height: int, asset_address: str) -> int:
    """Get and decode the last inflation value for a specific asset."""
    path = f"/shell/value/#tnam1pyqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqej6juv/#{asset_address}/parameters/last_inflation"
    encoded_path = quote(f'"{path}"')
    response = try_multiple_urls(RPC_URLS, f"/abci_query?path={encoded_path}&height={height}")
    if response is None:
        print(f"Failed to get last inflation for {asset_address} at height {height} from all RPC URLs")
        return 0
    
    try:
        data = response.json()
        
        if data["result"]["response"]["code"] == 0:
            value = data["result"]["response"]["value"]
            return decode_abci_int(value)
        else:
            print(f"No last inflation data for {asset_address} at height {height}")
            return 0
    except Exception as e:
        print(f"Error parsing last inflation response for {asset_address} at height {height}: {e}")
        return 0

def query_and_decode_last_locked(height: int, asset_address: str) -> int:
    """Get and decode the last locked amount value for a specific asset."""
    path = f"/shell/value/#tnam1pyqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqej6juv/#{asset_address}/parameters/last_locked_amount"
    encoded_path = quote(f'"{path}"')
    response = try_multiple_urls(RPC_URLS, f"/abci_query?path={encoded_path}&height={height}")
    if response is None:
        print(f"Failed to get last locked for {asset_address} at height {height} from all RPC URLs")
        return 0
    
    try:
        data = response.json()
        
        if data["result"]["response"]["code"] == 0:
            value = data["result"]["response"]["value"]
            return decode_abci_int(value)
        else:
            print(f"No last locked data for {asset_address} at height {height}")
            return 0
    except Exception as e:
        print(f"Error parsing last locked response for {asset_address} at height {height}: {e}")
        return 0

def decode_abci_int(base64_str: str) -> int:
    """Decode a base64 encoded integer from ABCI response."""
    try:
        if not base64_str:
            return 0
        
        # Decode base64
        decoded_bytes = base64.b64decode(base64_str)
        
        # Convert bytes to integer (little-endian)
        result = 0
        for i, byte in enumerate(decoded_bytes):
            result += byte * (256 ** i)
        
        return result
    except Exception as e:
        print(f"Error decoding base64 string '{base64_str}': {e}")
        return 0

def decode_abci_option_epoch(base64_str: str) -> int:
    """
    Decode a base64 encoded Option<Epoch> from ABCI response.

    Args:
        base64_str: Base64 encoded string from ABCI response
        
    Returns:
        The epoch number as an integer, or None if the option was None
    """
    try:
        if not base64_str:
            return None
        
        # Decode base64
        decoded_bytes = base64.b64decode(base64_str)
        
        if len(decoded_bytes) == 0:
            return None
        
        # Option<Epoch> encoding:
        # - 1 byte discriminator: 0 = None, 1 = Some
        # - 8 bytes for u64 epoch value (little-endian)
        
        if len(decoded_bytes) < 1:
            return None
            
        discriminator = decoded_bytes[0]
        
        if discriminator == 0:
            # None
            return None
        elif discriminator == 1:
            # Some
            if len(decoded_bytes) < 9:  # 1 byte discriminator + 8 bytes u64
                return None
            
            # Extract the u64 epoch value (little-endian)
            epoch_bytes = decoded_bytes[1:9]
            epoch_value = struct.unpack('<Q', epoch_bytes)[0]  # '<Q' = little-endian u64
            
            return epoch_value
        else:
            print(f"Unknown discriminator value: {discriminator}")
            return None
            
    except Exception as e:
        print(f"Error decoding base64 string '{base64_str}': {e}")
        return None


def main():
    """Main function to orchestrate the data collection."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch historical MASP rewards data')
    parser.add_argument('--start-height', type=int, help='Starting block height (default: current height)')
    parser.add_argument('--end-height', type=int, default=0, help='Ending block height (default: 0)')
    parser.add_argument('--end-masp-epoch', type=int, help='Ending MASP epoch (optional, stops when reached)')
    args = parser.parse_args()
    
    print("Starting historical rewards data collection...")
    
    # Get token list
    token_addresses = get_token_list()
    print(f"Tokens to query: {token_addresses}")
    
    # Prepare CSV writer
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    # Ensure csv directory exists
    os.makedirs("csv", exist_ok=True)
    
    filename = f"csv/{date_str}.csv"
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['height', 'timestamp', 'masp_epoch', 'token_address', 'last_inflation', 'last_locked']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        try:
            # Get start height
            if args.start_height:
                start_height = args.start_height
                print(f"Using provided start height: {start_height}")
            else:
                start_height = get_start_height()
                print(f"Using current block height: {start_height}")
            
            end_height = args.end_height
            end_masp_epoch = args.end_masp_epoch
            print(f"End height: {end_height}")
            print(f"End MASP epoch: {end_masp_epoch if end_masp_epoch is not None else 'None (no limit)'}")
            
            # Do historical queries
            queried_heights = do_historical_queries(start_height, end_height, end_masp_epoch, writer, token_addresses)
            
            print(f"\nData collection complete!")
            print(f"Results saved to: {filename}")
            print(f"Queried {len(queried_heights)} heights")
            
        except Exception as e:
            print(f"Error in main execution: {e}")
    
    print("CSV file closed.")

if __name__ == "__main__":
    main()
