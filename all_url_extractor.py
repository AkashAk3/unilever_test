
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_internal_urls(url, base_domain):
    """Extract all internal URLs from a given URL"""
    try:
        # response = requests.get(url, timeout=10)
        response = requests.get(url, timeout=10, verify=False)

        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        internal_urls = set()
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Convert relative URLs to absolute
            full_url = urljoin(url, href)
            
            # Check if URL belongs to the same domain
            parsed_url = urlparse(full_url)
            if parsed_url.netloc == base_domain:
                if full_url.lower().endswith(('.pdf', '.ppt', '.pptx', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.mp4', '.avi', '.mp3')):
                    continue
                # Clean URL (remove fragments)
                clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                if parsed_url.query:
                    clean_url += f"?{parsed_url.query}"
                internal_urls.add(clean_url)
        
        return internal_urls
    
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return set()

def recursive_url_extraction(start_url):
    """Recursively extract internal URLs with stopping condition"""
    
    # Get base domain
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc
    
    all_urls = set()
    to_process = set([start_url])
    processed = set()
    
    # Track last 5 iterations for stopping condition
    last_5_counts = []
    
    print(f"Starting extraction from: {start_url}")
    print(f"Base domain: {base_domain}")
    print("-" * 50)
    
    iteration = 0
    while to_process:
        iteration += 1
        current_batch = to_process.copy()
        to_process.clear()
        
        print(f"Iteration {iteration}: Processing {len(current_batch)} URLs")
        
        new_urls_this_iteration = set()
        
        for url in current_batch:
            if url in processed:
                continue
                
            print(f"  Processing: {url}")
            processed.add(url)
            
            # Get internal URLs from current URL
            internal_urls = get_internal_urls(url, base_domain)
            
            # Find truly new URLs
            new_urls = internal_urls - all_urls
            new_urls_this_iteration.update(new_urls)
            
            # Add new URLs to process next
            to_process.update(new_urls)
            
            # Add to total collection
            all_urls.update(internal_urls)
            
            # Small delay to be respectful
            time.sleep(0.5)
        
        # Track count of new URLs found
        new_count = len(new_urls_this_iteration)
        last_5_counts.append(new_count)
        
        print(f"  Found {new_count} new URLs this iteration")
        print(f"  Total unique URLs so far: {len(all_urls)}")
        
        # Keep only last 5 counts
        if len(last_5_counts) > 5:
            last_5_counts.pop(0)
        
        # Check stopping condition: last 5 iterations found no new URLs
        if len(last_5_counts) >= 5 and sum(last_5_counts) == 0:
            print("\nStopping condition met: No new URLs found in last 5 iterations")
            break
            
        print("-" * 30)
    
    return all_urls

def main():
    # Get URL from user
    start_url = "https://www.unilever.com/news/news-search/2025/unilevers-100-accelerator-partnership-unlocks-ai-innovation-across-supply-chain"
    
    if not start_url.startswith(('http://', 'https://')):
        start_url = 'https://' + start_url
    
    print(f"\nStarting recursive URL extraction...")
    
    # Extract URLs
    all_internal_urls = recursive_url_extraction(start_url)
    
    # Print results
    print("\n" + "="*50)
    print("EXTRACTION COMPLETE")
    print("="*50)
    print(f"Total unique internal URLs found: {len(all_internal_urls)}")
    print("\nAll Internal URLs:")
    print("-" * 30)
    
    for i, url in enumerate(sorted(all_internal_urls), 1):
        print(f"{i:3d}. {url}")
    
    return all_internal_urls

if __name__ == "__main__":
    urls = main()