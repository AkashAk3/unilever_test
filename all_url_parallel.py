# all url parallel
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import deque

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ParallelURLExtractor:
    def __init__(self, max_workers=10, delay=0.1):
        self.max_workers = max_workers
        self.delay = delay
        self.session = requests.Session()
        self.session.verify = False
        
        # Thread-safe collections
        self.all_urls = set()
        self.processed = set()
        self.lock = threading.Lock()
        
    def get_internal_urls(self, url, base_domain):
        """Extract all internal URLs from a given URL"""
        try:
            print("before request",url)
            headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        # Add other relevant headers from your browser
    }
            response = self.session.get(url, headers=headers ,timeout=500)
            response.raise_for_status()
            print("at line 30",response)
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
            
            # Small delay to be respectful
            if self.delay > 0:
                time.sleep(self.delay)
                
            return internal_urls
        
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return set()

    def process_batch(self, urls_batch, base_domain):
        """Process a batch of URLs in parallel"""
        new_urls = set()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all URLs for processing
            future_to_url = {
                executor.submit(self.get_internal_urls, url, base_domain): url 
                for url in urls_batch
            }
            
            # Process completed futures
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    urls_found = future.result()
                    
                    # Thread-safe update of collections
                    with self.lock:
                        # Mark as processed
                        self.processed.add(url)
                        
                        # Find truly new URLs
                        truly_new = urls_found - self.all_urls
                        new_urls.update(truly_new)
                        
                        # Update all_urls
                        self.all_urls.update(urls_found)
                    
                    print(f"✓ Processed: {url} ({len(urls_found)} URLs found)")
                    
                except Exception as e:
                    print(f"✗ Failed: {url} - {e}")
                    with self.lock:
                        self.processed.add(url)  # Mark as processed even if failed
        
        return new_urls

    def parallel_url_extraction(self, start_url, batch_size=20):
        """Extract URLs using parallel processing with batching"""
        
        # Get base domain
        parsed_start = urlparse(start_url)
        base_domain = parsed_start.netloc
        
        # Initialize
        to_process = deque([start_url])
        last_5_counts = []
        
        print(f"Starting parallel extraction from: {start_url}")
        print(f"Base domain: {base_domain}")
        print(f"Max workers: {self.max_workers}, Batch size: {batch_size}")
        print("-" * 60)
        
        iteration = 0
        start_time = time.time()
        
        while to_process:
            iteration += 1
            
            # Create batch from to_process queue
            current_batch = []
            batch_count = min(batch_size, len(to_process))
            
            for _ in range(batch_count):
                if to_process:
                    url = to_process.popleft()
                    if url not in self.processed:
                        current_batch.append(url)
            
            if not current_batch:
                break
                
            print(f"\nIteration {iteration}: Processing batch of {len(current_batch)} URLs")
            iteration_start = time.time()
            
            # Process batch in parallel
            new_urls = self.process_batch(current_batch, base_domain)
            
            # Add new URLs to processing queue
            for url in new_urls:
                if url not in self.processed:
                    to_process.append(url)
            
            # Track statistics
            new_count = len(new_urls)
            last_5_counts.append(new_count)
            
            iteration_time = time.time() - iteration_start
            
            print(f"Batch completed in {iteration_time:.2f}s")
            print(f"New URLs found: {new_count}")
            print(f"Total unique URLs: {len(self.all_urls)}")
            print(f"URLs in queue: {len(to_process)}")
            
            # Keep only last 5 counts
            if len(last_5_counts) > 5:
                last_5_counts.pop(0)
            
            # Stopping condition
            if len(last_5_counts) >= 3 and sum(last_5_counts[-3:]) == 0:
                print("\nStopping: No new URLs found in last 3 iterations")
                break
            
            # Safety limit
            if len(self.all_urls) > 10000:
                print(f"\nSafety limit reached: {len(self.all_urls)} URLs found")
                break
        
        total_time = time.time() - start_time
        print(f"\nExtraction completed in {total_time:.2f}s")
        return self.all_urls

def main():
    # start_url = "https://www.unilever.com/"
    # start_url = "https://www.degreedeodorant.com/us/en/home.html"
    # start_url="http://quebrandobarreiras.rexona.com.br/"
    start_url= "https://www.tanyapepsodent.com/"
    # start_url ="https://breakinglimits.degreedeodorant.com/en-US"
    # Configuration
    max_workers = 15      # Number of parallel threads
    batch_size = 25       # URLs to process per batch
    delay = 0.05         # Delay between requests (seconds)
    
    print("🚀 Parallel URL Extractor")
    print("=" * 50)
    
    # Create extractor
    extractor = ParallelURLExtractor(max_workers=max_workers, delay=delay)
    
    # Extract URLs
    all_urls = extractor.parallel_url_extraction(start_url, batch_size=batch_size)
    
    # Results
    print("\n" + "="*60)
    print("🎉 EXTRACTION COMPLETE")
    print("="*60)
    print(f"Total unique URLs found: {len(all_urls)}")
    print(f"Processing speed: ~{len(all_urls)/(time.time()):.1f} URLs per second")
    
    # Save to file
    with open('extracted_urls.txt', 'w', encoding='utf-8') as f:
        for url in sorted(all_urls):
            f.write(url + '\n')
    
    print(f"URLs saved to 'extracted_urls.txt'")
    
    # Show sample URLs
    print(f"\nSample URLs (showing first 20):")
    print("-" * 40)
    for i, url in enumerate(sorted(all_urls)[:20], 1):
        print(f"{i:2d}. {url}")
    
    if len(all_urls) > 20:
        print(f"... and {len(all_urls) - 20} more URLs")
    
    return all_urls

if __name__ == "__main__":
    urls = main()