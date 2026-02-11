import json
from typing import Dict, List, Any, Optional

def extract_marketplace_listings(response_data: Dict[str, Any], search_query: str = "t-shirt", location: str = "dhaka") -> List[Dict[str, Any]]:
    """
    Extract marketplace listings from Facebook GraphQL response data.

    Args:
        response_data: The JSON response data from Facebook Marketplace API
        search_query: The search query used (for constructing facebookUrl)
        location: The location used in search (for constructing facebookUrl)

    Returns:
        List of extracted listing data in the desired format
    """
    listings = []

    try:
        # Navigate to the edges containing the listings
        edges = response_data.get("data", {}).get("marketplace_search", {}).get("feed_units", {}).get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            listing = node.get("listing", {})

            if not listing:
                continue

            # Extract the listing data
            listing_id = listing.get("id")
            if not listing_id:
                continue

            # Construct URLs
            facebook_url = f"https://www.facebook.com/marketplace/{location}/search/?query={search_query.replace(' ', '%20')}"
            listing_url = f"https://www.facebook.com/marketplace/item/{listing_id}"

            # Extract primary photo
            primary_photo = listing.get("primary_listing_photo", {})
            primary_listing_photo = {
                "__typename": "ProductImage",
                "image": {
                    "uri": primary_photo.get("image", {}).get("uri", "")
                },
                "id": primary_photo.get("id", "")
            } if primary_photo else None

            # Extract price information
            listing_price = listing.get("listing_price", {})

            # Extract location
            location_data = listing.get("location", {})

            # Build the final listing object
            extracted_listing = {
                "facebookUrl": facebook_url,
                "listingUrl": listing_url,
                "id": listing_id,
                "primary_listing_photo": primary_listing_photo,
                "listing_price": listing_price,
                "strikethrough_price": listing.get("strikethrough_price"),
                "comparable_price": listing.get("comparable_price"),
                "comparable_price_type": listing.get("comparable_price_type"),
                "location": location_data,
                "is_hidden": listing.get("is_hidden", False),
                "is_live": listing.get("is_live", True),
                "is_pending": listing.get("is_pending", False),
                "is_sold": listing.get("is_sold", False),
                "is_viewer_seller": listing.get("is_viewer_seller", False),
                "min_listing_price": listing.get("min_listing_price"),
                "max_listing_price": listing.get("max_listing_price"),
                "marketplace_listing_category_id": listing.get("marketplace_listing_category_id"),
                "marketplace_listing_title": listing.get("marketplace_listing_title"),
                "custom_title": listing.get("custom_title"),
                "custom_sub_titles_with_rendering_flags": listing.get("custom_sub_titles_with_rendering_flags", []),
                "origin_group": listing.get("origin_group"),
                "listing_video": listing.get("listing_video"),
                "parent_listing": listing.get("parent_listing"),
                "marketplace_listing_seller": listing.get("marketplace_listing_seller"),
                "delivery_types": listing.get("delivery_types", [])
            }

            listings.append(extracted_listing)

    except Exception as e:
        print(f"Error extracting listings: {e}")
        return []

    return listings

def main():
    """
    Main function to demonstrate the extraction.
    """
    # Load the response data
    try:
        with open("response.json", "r", encoding="utf-8") as f:
            response_data = json.load(f)
    except FileNotFoundError:
        print("response.json file not found!")
        return
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return

    # Extract listings
    listings = extract_marketplace_listings(response_data, search_query="t-shirt", location="dhaka")

    # Print results
    print(f"Extracted {len(listings)} listings")

    # Save to file (avoid console encoding issues)
    with open("extracted_listings.json", "w", encoding="utf-8") as f:
        json.dump(listings, f, indent=2, ensure_ascii=False)

    print("Extracted listings saved to extracted_listings.json")

if __name__ == "__main__":
    main()
