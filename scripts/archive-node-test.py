import requests
import json
from typing import Dict, List
import concurrent.futures
from urllib.parse import urljoin
import logging
import sys
import base64

# ANSI color codes
GREEN = '\033[92m'
YELLOW = '\033[93m'
RESET = '\033[0m'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout
)

RPC_LIST_URL = "https://raw.githubusercontent.com/Luminara-Hub/namada-ecosystem/refs/heads/main/user-and-dev-tools/mainnet/rpc.json"
ABCI_QUERY_STRING = "/abci_query?path=%22/shell/value/%23tnam1pyqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqej6juv/%23tnam1q9gr66cvu4hrzm0sd5kmlnjje82gs3xlfg3v6nu7/balance/minted%22&height=1"

def is_base64(s: str) -> bool:
    """Check if a string is base64 encoded."""
    try:
        # Try to decode the string
        base64.b64decode(s)
        # Check if the string only contains valid base64 characters
        return all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=' for c in s)
    except Exception:
        return False

def get_rpc_list() -> List[Dict]:
    """Fetch the list of RPC endpoints."""
    logging.info("Fetching RPC list from %s", RPC_LIST_URL)
    response = requests.get(RPC_LIST_URL)
    response.raise_for_status()
    rpc_list = response.json()
    logging.info("Found %d RPC endpoints to test", len(rpc_list))
    return rpc_list

def test_endpoint(rpc_info: Dict) -> Dict:
    """Test a single RPC endpoint and return the result."""
    base_url = rpc_info["RPC Address"]
    full_url = urljoin(base_url, ABCI_QUERY_STRING)
    
    logging.info("Testing endpoint: %s", base_url)
    try:
        response = requests.get(full_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Check for successful response with base64 value
        response_data = data.get("result", {}).get("response", {})
        if response_data.get("code") == 0 and response_data.get("value"):
            value = response_data["value"]
            if is_base64(value):
                logging.info("✓ Successfully tested %s - Block limit: All", base_url)
                return {
                    "url": base_url,
                    "block_limit": float('inf'),  # Use infinity for sorting
                    "status": "success",
                    "limit_type": "all"
                }
        
        # Check for error message with block limit
        error_info = response_data.get("info", "")
        if "Cannot query more than" in error_info:
            # Extract the number from the error message
            import re
            match = re.search(r'Cannot query more than (\d+) blocks', error_info)
            if match:
                block_limit = int(match.group(1))
                logging.info("✓ Successfully tested %s - Block limit: %d", base_url, block_limit)
                return {
                    "url": base_url,
                    "block_limit": block_limit,
                    "status": "success",
                    "limit_type": "limited"
                }
        
        logging.warning("✗ Could not extract block limit from %s", base_url)
        return {
            "url": base_url,
            "block_limit": None,
            "status": "error",
            "error": "Could not extract block limit"
        }
    except Exception as e:
        logging.error("✗ Error testing %s: %s", base_url, str(e))
        return {
            "url": base_url,
            "block_limit": None,
            "status": "error",
            "error": str(e)
        }

def main():
    # Get the list of RPC endpoints
    rpc_list = get_rpc_list()
    
    # Test all endpoints concurrently
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_rpc = {executor.submit(test_endpoint, rpc): rpc for rpc in rpc_list}
        for future in concurrent.futures.as_completed(future_to_rpc):
            result = future.result()
            results.append(result)
    
    # Sort results by block limit (None values at the end)
    results.sort(key=lambda x: (x["block_limit"] is None, -(x["block_limit"] or 0)))
    
    # Print final results
    print("\nFinal Results:")
    print("-" * 80)
    for result in results:
        if result["status"] == "success":
            if result.get("limit_type") == "all":
                # Green for "All"
                print(f"{GREEN}URL: {result['url']}")
                print(f"Block Height Limit: All{RESET}")
            elif result["block_limit"] > 500000:
                # Yellow for high limits
                print(f"{YELLOW}URL: {result['url']}")
                print(f"Block Height Limit: {result['block_limit']}{RESET}")
            else:
                # White (default) for others
                print(f"URL: {result['url']}")
                print(f"Block Height Limit: {result['block_limit']}")
        else:
            print(f"URL: {result['url']}")
            print(f"Error: {result['error']}")
        print("-" * 80)

if __name__ == "__main__":
    main()