import aiohttp
import json
import asyncio
from urllib.parse import urlparse, parse_qs
from extractor import extract_marketplace_listings
from helper import extract_marketplace_doc_id, extract_browse_params

# Configuration constants
LOCATION_ID = "112922070389398"  # Default location ID
HEADERS = {
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
    'x-fb-friendly-name': 'CometMarketplaceSearchContentPaginationQuery',
}
COOKIES = {
    # Add Facebook cookies here if needed, e.g., 'c_user': 'your_id', 'xs': 'your_token'
}

proxy = {
    "use_proxy": True,  # Set to True to enable proxy on Render
    "username": "groups-RESIDENTIAL",
    "password": "apify_proxy_z2faOHkbVrgtIWY62TM4a8vRkAuyGB1sgh5Y",
    "hostname": "proxy.apify.com",
    "port": 8000
}

def get_proxy_url(for_aiohttp=False):
    """Generate proxy URL. For aiohttp, returns the URL directly. For requests, returns dict format."""
    if not proxy["use_proxy"]:
        return None

    proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['hostname']}:{proxy['port']}"

    if for_aiohttp:
        return proxy_url
    else:
        return {"http": proxy_url, "https": proxy_url}

async def scrape_listings(query, max_items=50):
    """
    Scrape Facebook Marketplace listings for a given query.

    Args:
        query (str): The search query.
        max_items (int): Maximum number of listings to fetch.

    Returns:
        list: List of extracted listings.
    """
    # Construct the URL
    url = f"https://www.facebook.com/marketplace/{LOCATION_ID}/search?query={query.replace(' ', '%20')}"

    print(f"Processing URL: {url}")
    print(f"Search query: '{query}' (Max items: {max_items})")

    # Update headers with current URL as referer
    current_headers = HEADERS.copy()
    current_headers['referer'] = url

    # Get marketplace page HTML
    print("Fetching marketplace page...")
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        proxy_url = get_proxy_url(for_aiohttp=True)
        connector = aiohttp.TCPConnector() if proxy_url is None else aiohttp.TCPConnector()

        async with aiohttp.ClientSession(timeout=timeout, cookies=COOKIES, connector=connector) as session:
            async with session.get(url, headers=current_headers, proxy=proxy_url) as response:
                print(f"Page fetch status: {response.status}")
                response.raise_for_status()
                page_content = await response.text()
                print(f"Page content length: {len(page_content)}")
                # Accept any response that has reasonable content
                if len(page_content) < 100:
                    print("Page content too short")
                    return []
    except Exception as e:
        print(f"Error fetching page: {e}")
        return []

    # Extract doc_id
    print("Extracting doc_id...")
    cached_doc_id = await extract_marketplace_doc_id(page_content, current_headers)

    if not cached_doc_id:
        print("Failed to extract doc_id")
        return []

    print(f"Extracted doc_id: {cached_doc_id}")

    print(f"Using doc_id: {cached_doc_id}")

    # Extract browse parameters
    print("Extracting browse parameters...")
    browse_params = extract_browse_params(page_content)

    if not browse_params:
        return []

    print(f"Location: {browse_params['filter_location_latitude']}, {browse_params['filter_location_longitude']} (Radius: {browse_params['filter_radius_km']}km)")

    # Initialize variables for pagination
    all_listings = []
    cursor = None
    items_per_page = 24

    timeout = aiohttp.ClientTimeout(total=8)
    proxy_url = get_proxy_url(for_aiohttp=True)
    connector = aiohttp.TCPConnector() if proxy_url is None else aiohttp.TCPConnector()

    async with aiohttp.ClientSession(timeout=timeout, cookies=COOKIES, connector=connector) as session:
        while len(all_listings) < max_items:

            # Prepare GraphQL variables
            if cursor:
                # Use cursor for pagination
                variables = {
                    "count": items_per_page,
                    "cursor": cursor,
                    "params": {
                        "bqf": {"callsite": "COMMERCE_MKTPLACE_WWW", "query": query},
                        "browse_request_params": browse_params,
                        "custom_request_params": {
                            "browse_context": None,
                            "contextual_filters": [],
                            "referral_code": None,
                            "referral_ui_component": None,
                            "saved_search_strid": None,
                            "search_vertical": "C2C",
                            "seo_url": None,
                            "serp_landing_settings": {"virtual_category_id": ""},
                            "surface": "SEARCH",
                            "virtual_contextual_filters": []
                        }
                    },
                    "scale": 1
                }
            else:
                # First page - use the original cursor
                variables = {
                    "count": items_per_page,
                    "cursor": "{\"pg\":1,\"b2c\":{\"br\":\"\",\"it\":0,\"hmsr\":false,\"tbi\":0},\"c2c\":{\"br\":\"AbrOVXU1wQAxZIwDA-LNo4zTOGyNjx6X9DVtPnVeCddFbXL-ibLXjBbVbJl8tONW4FuuQG_4tZ75a_TZzSFXeuYzvIyQShXoCQ3NjpMY66g32rVt4XIAi4cHMsW4PyjnHY74qnaPiIwY3NxSzGYD-0ob0-8GFJdYRK7tJd1d9m7W2WiLU-uTU5Jzk_5xRXbXdkzNbqmRvFIxfDCeVfsiEqdGN1h3Ihp6D8Hxjx3L2Aasxg_qTPYAJwJj8VuchU0YgwGN0oyIyCLrX-T6njFV7xeOG3QOwOILiv7xAaYgHUWhBm8S0TPoOdXwYTnYqwAxZrJzwS8cXrau9JSs2QJCHP7A3szCZGbM2wxbIbmubzB4wvitgrdXKWZFfMy03dOLGlaNWbZzat2ODzOFExvigvmNVUN9S2Pb3pA_HIuDs18YLWl-BdFhpRhB_uBMkqrOQ-yRHlm6D28h-aUEUviPu-s0dLQib3LSoVUFwbJIrinZpN2s7S-fRGHUktAkNatxlGb5vI2eHI2Sl9QlFtxetLZKmRF5Igmxh8eSY4oKJH5NWpNxeIsYdue9WmWmQdGJ_pp0tls2cL3AZ39t-9rGhdPu7EHtTBOkPrnPn_dqHjWNLucKQmzuy9lFJ__T6b85I_TymDQljNo5rVSKkLMaE_v3QGOpAXj1ojUGTRtJXkKmWQ\",\"it\":20,\"rpbr\":\"\",\"rphr\":false,\"rmhr\":false},\"ads\":{\"items_since_last_ad\":20,\"items_retrieved\":20,\"ad_index\":0,\"ad_slot\":0,\"dynamic_gap_rule\":0,\"counted_organic_items\":0,\"average_organic_score\":0,\"is_dynamic_gap_rule_set\":false,\"first_organic_score\":0,\"is_dynamic_initial_gap_set\":false,\"iterated_organic_items\":0,\"top_organic_score\":0,\"feed_slice_number\":0,\"feed_retrieved_items\":0,\"ad_req_id\":855487502,\"refresh_ts\":0,\"cursor_id\":28097,\"mc_id\":0,\"ad_index_e2e\":0,\"seen_ads\":{\"ad_ids\":[],\"page_ids\":[],\"campaign_ids\":[]},\"has_ad_index_been_reset\":false,\"is_reconsideration_ads_dropped\":false},\"irr\":false,\"serp_cta\":false,\"rui\":[],\"mpid\":[],\"ubp\":null,\"ncrnd\":1,\"irsr\":false,\"bmpr\":[],\"bmpeid\":[],\"nmbmp\":false,\"skrr\":false,\"ioour\":false,\"ise\":false,\"sms_cursor\":{\"page_index\":0,\"blended_ad_index\":0,\"organics_since_last_ad\":0,\"page_organic_count\":0,\"blended_organic_index\":0,\"returned_ad_index\":0,\"total_index\":0}}",
                    "params": {
                        "bqf": {"callsite": "COMMERCE_MKTPLACE_WWW", "query": query},
                        "browse_request_params": browse_params,
                        "custom_request_params": {
                            "browse_context": None,
                            "contextual_filters": [],
                            "referral_code": None,
                            "referral_ui_component": None,
                            "saved_search_strid": None,
                            "search_vertical": "C2C",
                            "seo_url": None,
                            "serp_landing_settings": {"virtual_category_id": ""},
                            "surface": "SEARCH",
                            "virtual_contextual_filters": []
                        }
                    },
                    "scale": 1
                }

            # Prepare data
            data = {
                'server_timestamps': 'true',
                'variables': json.dumps(variables),
                'doc_id': cached_doc_id,
            }

            # Make GraphQL request
            try:
                async with session.post('https://www.facebook.com/api/graphql/',
                                       headers=HEADERS,
                                       data=data) as response:

                    if response.status != 200:
                        break

                    response_text = await response.text()
                    response_data = json.loads(response_text)

            except Exception:
                break

            # Extract listings
            page_listings = extract_marketplace_listings(response_data, search_query=query, location="")

            if not page_listings:
                break

            # Add to collection
            all_listings.extend(page_listings)

            # Check if enough
            if len(all_listings) >= max_items:
                all_listings = all_listings[:max_items]
                break

            # Extract cursor
            try:
                page_info = response_data.get("data", {}).get("marketplace_search", {}).get("feed_units", {}).get("page_info", {})
                if not page_info.get("has_next_page", False):
                    break

                cursor = page_info.get("end_cursor")
                if not cursor:
                    break

            except Exception:
                break

            await asyncio.sleep(0.2)

    return all_listings
