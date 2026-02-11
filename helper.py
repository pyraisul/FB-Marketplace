import re
import aiohttp
import asyncio
import json

# Pre-compiled regex patterns for better performance
BROWSE_PARAM_PATTERNS = [
    re.compile(r'browse_request_params["\s]*:[\s]*({[^}]*})', re.IGNORECASE | re.DOTALL),
    re.compile(r'browse_request_params\s*:\s*({[^}]*})', re.IGNORECASE | re.DOTALL),
    re.compile(r'"browse_request_params"\s*:\s*({[^}]*})', re.IGNORECASE | re.DOTALL),
    re.compile(r'browse_request_params"\s*:\s*\{([^}]*)\}', re.IGNORECASE | re.DOTALL),
]

LAT_PATTERNS = [
    re.compile(r'filter_location_latitude["\s]*:[\s]*([0-9.-]+)', re.IGNORECASE),
    re.compile(r'latitude["\s]*:[\s]*([0-9.-]+)', re.IGNORECASE),
    re.compile(r'"lat"\s*:\s*([0-9.-]+)', re.IGNORECASE),
]

LON_PATTERNS = [
    re.compile(r'filter_location_longitude["\s]*:[\s]*([0-9.-]+)', re.IGNORECASE),
    re.compile(r'longitude["\s]*:[\s]*([0-9.-]+)', re.IGNORECASE),
    re.compile(r'"lng"\s*:\s*([0-9.-]+)', re.IGNORECASE),
]

RADIUS_PATTERNS = [
    re.compile(r'filter_radius_km["\s]*:[\s]*([0-9]+)', re.IGNORECASE),
    re.compile(r'radius["\s]*:[\s]*([0-9]+)', re.IGNORECASE),
]

def extract_browse_params(page_content):
    """Extract browse_request_params from the marketplace page HTML using pre-compiled patterns."""
    # Try multiple patterns to find the browse_request_params
    for pattern in BROWSE_PARAM_PATTERNS:
        matches = pattern.findall(page_content)
        for match in matches:
            try:
                # Try to parse as JSON
                params_str = '{' + match + '}'
                params = json.loads(params_str)

                # Check if it has the expected structure
                if 'filter_location_latitude' in params and 'filter_location_longitude' in params:
                    return params
            except (json.JSONDecodeError, TypeError):
                continue

    # Try to find individual location parameters using pre-compiled patterns
    latitude = None
    longitude = None
    radius = 500

    for pattern in LAT_PATTERNS:
        match = pattern.search(page_content)
        if match:
            latitude = float(match.group(1))
            break

    for pattern in LON_PATTERNS:
        match = pattern.search(page_content)
        if match:
            longitude = float(match.group(1))
            break

    for pattern in RADIUS_PATTERNS:
        match = pattern.search(page_content)
        if match:
            radius = int(match.group(1))
            break

    if latitude is not None and longitude is not None:
        return {
            "commerce_enable_local_pickup": True,
            "commerce_enable_shipping": True,
            "commerce_search_and_rp_available": True,
            "commerce_search_and_rp_category_id": [],
            "commerce_search_and_rp_condition": None,
            "commerce_search_and_rp_ctime_days": None,
            "filter_location_latitude": latitude,
            "filter_location_longitude": longitude,
            "filter_price_lower_bound": 0,
            "filter_price_upper_bound": 214748364700,
            "filter_radius_km": radius
        }

    return None

async def extract_marketplace_doc_id(page_content, headers, doc_type="search", proxy_url=None):
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

    print(f"Found {len(js_urls)} JS urls for doc_id extraction")

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

    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # Concurrent processing of JS files for better performance
        tasks = []
        for js_url in js_urls[:20]:  # Reduced from 40 to 20 for speed
            tasks.append(check_js_file(session, js_url, pattern, proxy_url=proxy_url))

        # Wait for first successful result
        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                if result:
                    return result
            except Exception:
                continue

    return None

async def check_js_file(session, js_url, pattern, proxy_url=None):
    """Check a single JS file for the doc_id pattern."""
    try:
        async with session.get(js_url, proxy=proxy_url) as resp:
            if resp.status != 200:
                print(f"JS fetch failed {resp.status} for {js_url}")
                return None
            text = await resp.text()
            match = pattern.search(text)
            return match.group(1) if match else None
    except Exception:
        return None

# Backward compatibility
async def extract_marketplace_pdp_doc_id(page_content, headers, proxy_url=None):
    """Backward compatibility wrapper for PDP doc_id extraction."""
    return await extract_marketplace_doc_id(page_content, headers, doc_type="pdp", proxy_url=proxy_url)
