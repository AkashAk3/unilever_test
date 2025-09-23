import requests
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
import re

def extract_urls_from_sitemap(sitemap_url):
    """
    Extract all URLs from an XML sitemap
    """
    try:
        # Fetch the sitemap
        print(f"Fetching sitemap: {sitemap_url}")
        response = requests.get(sitemap_url)
        response.raise_for_status()
        
        # Parse the XML
        root = ET.fromstring(response.content)
        
        # Find all URLs - handle different XML namespaces
        urls = []
        
        # Try common sitemap namespaces
        namespaces = [
            {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'},
            {'ns': ''},  # No namespace
        ]
        
        for ns in namespaces:
            if ns['ns']:
                # With namespace
                loc_elements = root.findall('.//ns:loc', {'ns': ns['ns']})
            else:
                # Without namespace
                loc_elements = root.findall('.//loc')
            
            if loc_elements:
                for loc in loc_elements:
                    url = loc.text.strip() if loc.text else None
                    if url:
                        urls.append(url)
                break
        
        # If no URLs found with standard methods, try regex as fallback
        if not urls:
            print("Standard parsing failed, trying regex...")
            content = response.text
            url_pattern = r'<loc>(.*?)</loc>'
            urls = re.findall(url_pattern, content)
        
        return urls
        
    except requests.RequestException as e:
        print(f"Error fetching sitemap: {e}")
        return []
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []

def main():
    sitemap_url = "https://www.unilever.com/sitemap.xml"
    
    # Extract URLs
    urls = extract_urls_from_sitemap(sitemap_url)
    
    if urls:
        print(f"\nFound {len(urls)} URLs:")
        print("-" * 50)
        
        for i, url in enumerate(urls, 1):
            print(f"{i}. {url}")
        
        # Save to file
        with open('unilever_urls.txt', 'w', encoding='utf-8') as f:
            for url in urls:
                f.write(url + '\n')
        
        print(f"\nURLs saved to 'unilever_urls.txt'")
        
    else:
        print("No URLs found in the sitemap.")

if __name__ == "__main__":
    main()