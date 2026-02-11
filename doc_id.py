import re
import aiohttp
import asyncio
import json

async def extract_marketplace_doc_id(page_content, headers, doc_type="search"):
    """
    Extract Facebook Marketplace GraphQL doc_id (async).
    doc_type: "search" for CometMarketplaceSearchContentPaginationQuery or "pdp" for MarketplacePDPContainerQuery
    Returns doc_id or None.
    """
    js_urls = re.findall(
        r'https?://static\.xx\.fbcdn\.net/rsrc\.php/[^"\s]+\.js',
        page_content
    )

    if not js_urls:
        return None

    # Select pattern based on doc_type
    if doc_type == "pdp":
        pattern = re.compile(
            r'MarketplacePDPContainerQuery_facebookRelayOperation'
            r'.*?a\.exports\s*=\s*"(\d+)"'
        )
    else:  # default to search
        pattern = re.compile(
            r'CometMarketplaceSearchContentPaginationQuery_facebookRelayOperation'
            r'.*?a\.exports\s*=\s*"(\d+)"'
        )

    timeout = aiohttp.ClientTimeout(total=3)  # Reduced timeout

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # Concurrent processing of JS files for better performance
        tasks = []
        for js_url in js_urls[:20]:  # Reduced from 40 to 20 for speed
            tasks.append(check_js_file(session, js_url, pattern))

        # Wait for first successful result
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                if result:
                    return result
            except Exception:
                continue

    return None

async def check_js_file(session, js_url, pattern):
    """Check a single JS file for the doc_id pattern."""
    try:
        async with session.get(js_url) as resp:
            if resp.status != 200:
                return None
            text = await resp.text()
            match = pattern.search(text)
            return match.group(1) if match else None
    except Exception:
        return None

async def extract_and_print_doc_id(url, doc_type="search"):
    """
    Extract and print the doc_id from a Facebook Marketplace URL.

    Args:
        url (str): Facebook Marketplace URL
        doc_type (str): Type of doc_id to extract ("search" or "pdp")
    """
    cookies = {

    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,bn;q=0.7',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.facebook.com',
        'priority': 'u=1, i',
        'sec-ch-prefers-color-scheme': 'light',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="143.0.7499.147", "Chromium";v="143.0.7499.147", "Not A(Brand";v="24.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"19.0.0"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-asbd-id': '359341',
        'x-fb-friendly-name': 'CometMarketplaceSearchContentPaginationQuery',
        'x-fb-lsd': 'afhYRGhhryXZKwJ1B5Lwqk',
    }

    timeout = aiohttp.ClientTimeout(total=10)

    try:
        async with aiohttp.ClientSession(headers=headers, cookies=cookies, timeout=timeout) as session:
            print(f"Fetching page content from: {url}")
            async with session.get(url) as response:
                response.raise_for_status()
                page_content = await response.text()

            print(f"Extracting {doc_type} doc_id...")
            doc_id = await extract_marketplace_doc_id(page_content, headers, doc_type)

            if doc_id:
                print(f"Successfully extracted {doc_type} doc_id: {doc_id}")
                return doc_id
            else:
                print(f"Failed to extract {doc_type} doc_id")
                return None

    except Exception as e:
        print(f"Error extracting doc_id: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Example URL - replace with your marketplace URL
    url = "https://www.facebook.com/marketplace/108479165840750/search/?query=phone&exact=false"

    print("Facebook Marketplace Doc ID Extractor")
    print("=" * 40)

    # Extract search doc_id
    asyncio.run(extract_and_print_doc_id(url, "search"))

    # Extract PDP doc_id (would need a listing URL)
    # listing_url = "https://www.facebook.com/marketplace/item/123456789/"
    # asyncio.run(extract_and_print_doc_id(listing_url, "pdp"))
