import aiohttp
import json
import asyncio
from urllib.parse import urlparse, parse_qs
from extractor import extract_marketplace_listings
from helper import extract_marketplace_doc_id, extract_marketplace_pdp_doc_id, extract_browse_params


# Configuration
config = {
    "urls": [
        # "https://www.facebook.com/marketplace/108479165840750/search/?query=phone&exact=false",
        "https://www.facebook.com/marketplace/113520048658655/search?query=clothe"
    ],
    "deepScrape": False,  # If true, will visit details page and extract more information
    "count": 100
}

proxy = {
    "use_proxy": False,  # Set to False to disable proxy
    "username": "tanvirdipt0",
    "password": "Hyc7XRGgZNh2nZznOt16",
    "hostname": "core-residential.evomi.com",
    "port": 1000
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

def extract_listing_from_pdp_response(detailed_data):
    """Optimized extraction of marketplace listings from PDP response."""
    extracted_listings = []

    def recursive_search(node):
        if isinstance(node, dict):
            # Check if this node is a marketplace listing
            if ("id" in node and "listing_price" in node and "location" in node):
                extracted_listings.append(node)
                return
            # Continue searching nested objects
            for value in node.values():
                recursive_search(value)
        elif isinstance(node, list):
            for item in node:
                recursive_search(item)

    recursive_search(detailed_data)
    return extracted_listings

async def get_detailed_listing_data(session, listing_id, pdp_doc_id, proxy_url=None):
    """Get detailed data for a specific listing using shared aiohttp session."""
    try:
        # Use the exact variables from the original working deep.py
        variables = {
            "enableJobEmployerActionBar": False,
            "enableJobSeekerActionBar": False,
            "feedbackSource": 56,
            "feedLocation": "MARKETPLACE_MEGAMALL",
            "referralCode": "null",
            "scale": 1,
            "targetId": listing_id,
            "useDefaultActor": False,
            "__relay_internal__pv__CometUFIShareActionMigrationrelayprovider": True,
            "__relay_internal__pv__CometUFIReactionsEnableShortNamerelayprovider": False,
            "__relay_internal__pv__CometUFICommentAvatarStickerAnimatedImagerelayprovider": False,
            "__relay_internal__pv__IsWorkUserrelayprovider": False,
            "__relay_internal__pv__GHLShouldChangeSponsoredDataFieldNamerelayprovider": False,
            "__relay_internal__pv__GHLShouldChangeAdIdFieldNamerelayprovider": False,
            "__relay_internal__pv__CometUFI_dedicated_comment_routable_dialog_gkrelayprovider": False
        }

        data = {
            'fb_api_req_friendly_name': 'MarketplacePDPContainerQuery',
            'server_timestamps': 'true',
            'variables': json.dumps(variables),
            'doc_id': pdp_doc_id,  # Use dynamically extracted doc_id
        }

        # Use shared session for better performance
        async with session.post('https://www.facebook.com/api/graphql/',
                               headers=headers,
                               data=data,
                               proxy=proxy_url) as response:
            if response.status == 200:
                # Facebook sometimes returns JSON with text/html content-type
                response_text = await response.text()
                try:
                    detailed_data = json.loads(response_text)
                    return detailed_data
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON response for listing {listing_id}: {response_text[:200]}...")
                    return None
            else:
                print(f"HTTP {response.status} for listing {listing_id}")
                return None

    except Exception as e:
        print(f"Exception getting detailed data for listing {listing_id}: {e}")
        return None

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
    'x-fb-friendly-name': 'CometMarketplaceSearchContentPaginationQuery',
}

async def process_single_url(url, cached_doc_id, max_items=None):
    """Process a single marketplace URL and return its listings."""

    # Extract query from URL
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    search_query = query_params.get('query', ['shirt'])[0]

    print(f"Processing URL: {url}")
    print(f"Search query: '{search_query}' (Max items: {max_items if max_items is not None else 'Unlimited'})")

    # Update headers with current URL as referer
    current_headers = headers.copy()
    current_headers['referer'] = url

    # Get marketplace page HTML
    print("Fetching marketplace page...")
    try:
        timeout = aiohttp.ClientTimeout(total=8)  # Reduced timeout
        proxy_url = get_proxy_url(for_aiohttp=True)
        connector = aiohttp.TCPConnector() if proxy_url is None else aiohttp.TCPConnector()

        async with aiohttp.ClientSession(timeout=timeout, cookies=cookies, connector=connector) as session:
            async with session.get(url, headers=current_headers, proxy=proxy_url) as response:
                response.raise_for_status()
                page_content = await response.text()
                # Accept any response that has reasonable content
                if len(page_content) < 100:
                    return []  # Too short, probably an error
    except Exception:
        return []  # Fail silently, will be retried

    # Use cached doc_id instead of extracting again
    doc_id = cached_doc_id
    print(f"Using cached doc_id: {doc_id}")

    # Extract browse parameters from the page (location-specific)
    print("Extracting browse parameters...")
    browse_params = extract_browse_params(page_content)

    if not browse_params:
        return []

    print(f"Location: {browse_params['filter_location_latitude']}, {browse_params['filter_location_longitude']} (Radius: {browse_params['filter_radius_km']}km)")

    # Initialize variables for pagination
    all_listings = []
    cursor = None
    items_per_page = 24

    timeout = aiohttp.ClientTimeout(total=8)  # Optimized timeout

    async with aiohttp.ClientSession(timeout=timeout, cookies=cookies) as session:
        while max_items is None or len(all_listings) < max_items:

            # Prepare GraphQL variables
            if cursor:
                # Use cursor for pagination
                variables = {
                    "count": items_per_page,
                    "cursor": cursor,
                    "params": {
                        "bqf": {"callsite": "COMMERCE_MKTPLACE_WWW", "query": search_query},
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
                        "bqf": {"callsite": "COMMERCE_MKTPLACE_WWW", "query": search_query},
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

            # Prepare data with dynamic doc_id
            data = {
                'server_timestamps': 'true',
                'variables': json.dumps(variables),
                'doc_id': cached_doc_id,
            }

            # Make GraphQL request using aiohttp for better performance
            try:
                proxy_url = get_proxy_url(for_aiohttp=True)
                async with session.post('https://www.facebook.com/api/graphql/',
                                       headers=headers,
                                       data=data,
                                       proxy=proxy_url) as response:

                    if response.status != 200:
                        break

                    response_text = await response.text()
                    response_data = json.loads(response_text)

            except Exception:
                break

            # Extract marketplace listings from this page
            page_listings = extract_marketplace_listings(response_data, search_query=search_query, location="")

            if not page_listings:
                break

            # Add listings from this page to our collection
            all_listings.extend(page_listings)

            # Check if we have enough listings or if there's no next page
            if max_items is not None and len(all_listings) >= max_items:
                # Trim to max_items if we exceeded
                all_listings = all_listings[:max_items]
                break

            # Extract cursor for next page
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

async def process_url_with_retry(url, cached_doc_id, max_retries=3, max_items=None):
    """Process a single URL with retry logic."""
    for attempt in range(max_retries):
        if attempt > 0:
            print(f"Retry {attempt}/{max_retries - 1} for {url}")
            await asyncio.sleep(2)  # Reduced delay

        listings = await process_single_url(url, cached_doc_id, max_items)
        if listings:
            return url, listings

    return url, []

async def main():
    """Main async function to scrape Facebook Marketplace with optional deep scraping."""

    urls = config["urls"]
    deep_scrape = config["deepScrape"]
    count_value = config.get("count", 50)
    # Handle empty string or None for unlimited scraping
    if count_value == "" or count_value is None:
        max_items = None  # None means unlimited
    else:
        max_items = int(count_value) if isinstance(count_value, str) else count_value

    if not urls:
        print("No URLs provided in config")
        return

    # Show configuration
    print("Facebook Marketplace Scraper")
    print(f"URLs: {len(urls)}")
    print(f"Deep Scrape: {deep_scrape}")
    print(f"Target Count: {max_items if max_items is not None else 'All'}")
    print(f"Proxy: {'Enabled' if proxy['use_proxy'] else 'Disabled'}")
    print("-" * 50)

    if deep_scrape:
        # Deep scraping mode
        print("Starting DEEP SCRAPING mode...")

        # First, get listing IDs from search results
        print("Phase 1: Collecting listing IDs from search results...")

        # Extract doc_id once and cache it for all URLs
        print("Initializing scraper...")
        first_url = urls[0]

        # Extract doc_id from the first URL
        current_headers = headers.copy()
        current_headers['referer'] = first_url

        try:
            timeout = aiohttp.ClientTimeout(total=8)
            proxy_url = get_proxy_url(for_aiohttp=True)
            connector = aiohttp.TCPConnector()

            async with aiohttp.ClientSession(timeout=timeout, cookies=cookies, connector=connector) as session:
                async with session.get(first_url, headers=current_headers, proxy=proxy_url) as response:
                    response.raise_for_status()
                    page_content = await response.text()
        except Exception as e:
            print(f"❌ Error fetching marketplace page for doc_id: {e}")
            return

        cached_doc_id = await extract_marketplace_doc_id(page_content, current_headers)

        if not cached_doc_id:
            print("❌ Failed to extract doc_id")
            return

        print(f"Cached search doc_id: {cached_doc_id}")

        # For deep scraping, we need the PDP doc_id from a listing page
        print("Extracting PDP doc_id from a listing page...")

        # First get a sample listing ID to visit its page
        sample_listings = await process_single_url(urls[0], cached_doc_id, max_items=1)
        if not sample_listings:
            print("Warning: Could not get sample listings for PDP doc_id extraction")
            cached_pdp_doc_id = "33071634612482224"  # Fallback to known working doc_id
        else:
            sample_listing_id = sample_listings[0].get("id")
            if sample_listing_id:
                # Visit the listing page to extract PDP doc_id
                listing_url = f"https://www.facebook.com/marketplace/item/{sample_listing_id}/"
                print(f"Visiting listing page: {listing_url}")

                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8), cookies=cookies) as listing_session:
                        async with listing_session.get(listing_url, headers=current_headers) as listing_response:
                            listing_response.raise_for_status()
                            listing_page_content = await listing_response.text()

                            cached_pdp_doc_id = await extract_marketplace_pdp_doc_id(listing_page_content, current_headers)
                            if cached_pdp_doc_id:
                                print(f"Successfully extracted PDP doc_id: {cached_pdp_doc_id}")
                            else:
                                print("Warning: Could not extract PDP doc_id from listing page, using fallback")
                                cached_pdp_doc_id = "33071634612482224"  # Fallback to known working doc_id
                except Exception as e:
                    print(f"Error visiting listing page for PDP doc_id: {e}")
                    cached_pdp_doc_id = "33071634612482224"  # Fallback to known working doc_id
            else:
                print("Warning: No listing ID found for PDP doc_id extraction")
                cached_pdp_doc_id = "33071634612482224"  # Fallback to known working doc_id

        # Collect listing IDs from search results
        listing_ids = set()
        for url in urls:
            print(f"Processing search URL: {url}")
            listings = await process_single_url(url, cached_doc_id, max_items)

            for listing in listings:
                if listing.get("id"):
                    listing_ids.add(listing["id"])

            print(f"Found {len(listings)} listings in this search")

        print(f"Total unique listing IDs collected: {len(listing_ids)}")

        # Phase 2: Get detailed data for each listing
        print("Phase 2: Fetching detailed data for each listing...")

        # Create optimized session for concurrent PDP requests
        timeout = aiohttp.ClientTimeout(total=12)
        proxy_url = get_proxy_url(for_aiohttp=True)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20, ttl_dns_cache=30)
        shared_session = aiohttp.ClientSession(timeout=timeout, cookies=cookies, connector=connector)

        detailed_listings = []
        target_listing_ids = list(listing_ids)[:max_items] if max_items else list(listing_ids)

        try:
            # Create concurrent tasks for all PDP requests
            tasks = []
            for listing_id in target_listing_ids:
                task = get_detailed_listing_data(shared_session, listing_id, cached_pdp_doc_id, proxy_url)
                tasks.append(task)

            print(f"Processing {len(tasks)} listings concurrently...")

            # Execute all requests concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for i, result in enumerate(results):
                listing_id = target_listing_ids[i]
                if isinstance(result, Exception):
                    print(f"Warning: Failed to get detailed data for listing {listing_id}: {result}")
                    continue

                if result:
                    # Optimized extraction logic
                    extracted_listings = extract_listing_from_pdp_response(result)
                    if extracted_listings:
                        detailed_listings.extend(extracted_listings)
                    else:
                        print(f"Warning: No listings extracted from PDP response for {listing_id}")

        finally:
            await shared_session.close()

        # Group by query for consistency with regular mode
        all_results = {}
        for listing in detailed_listings:
            # Try to determine query from the listing data or use default
            query = "deep_scraped"  # Default category for deep scraped listings
            if query not in all_results:
                all_results[query] = []
            all_results[query].append(listing)

    else:
        # Regular scraping mode (existing logic)
        print("Starting REGULAR SCRAPING mode...")

        # Extract doc_id once and cache it for all URLs
        print("Initializing scraper...")
        first_url = urls[0]

        # Extract doc_id from the first URL
        current_headers = headers.copy()
        current_headers['referer'] = first_url

        try:
            timeout = aiohttp.ClientTimeout(total=8)
            proxy_url = get_proxy_url(for_aiohttp=True)
            connector = aiohttp.TCPConnector()

            async with aiohttp.ClientSession(timeout=timeout, cookies=cookies, connector=connector) as session:
                async with session.get(first_url, headers=current_headers, proxy=proxy_url) as response:
                    response.raise_for_status()
                    page_content = await response.text()
        except Exception as e:
            print(f"❌ Error fetching marketplace page for doc_id: {e}")
            return

        cached_doc_id = await extract_marketplace_doc_id(page_content, current_headers)

        if not cached_doc_id:
            print("❌ Failed to extract doc_id")
            return

        print(f"Cached doc_id: {cached_doc_id}")
        print(f"Processing {len(urls)} URLs concurrently...")

        # Process all URLs concurrently
        tasks = [process_url_with_retry(url, cached_doc_id, max_retries=3, max_items=max_items) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and group by query
        all_results = {}
        total_listings = 0

        for result in results:
            if isinstance(result, Exception):
                print(f"❌ Error in concurrent processing: {result}")
                continue

            url, listings = result
            total_listings += len(listings)

            # Extract query from URL for grouping
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            search_query = query_params.get('query', ['unknown'])[0]

            # Group by query
            if search_query not in all_results:
                all_results[search_query] = []

            all_results[search_query].extend(listings)

            # Minimal progress indicator
            print(f"{search_query}: {len(listings)} listings")

    # Output results
    total_listings = sum(len(listings) for listings in all_results.values())
    print(f"\nScraping Complete!")
    print(f"Total: {total_listings} listings from {len(urls)} URLs")
    print(f"Mode: {'Deep Scraping' if deep_scrape else 'Regular Scraping'}")

    # Save results
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print("Results saved to output.json")

    # Summary by query
    for query, listings in all_results.items():
        print(f"{query}: {len(listings)} listings")

if __name__ == "__main__":
    asyncio.run(main())
