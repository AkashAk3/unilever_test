# combined_scraper.py - URL Extraction + Deep Content Scraping
import asyncio
import json
import logging
import time
import threading
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import requests
from bs4 import BeautifulSoup
import urllib3
from playwright.async_api import async_playwright
import os
from datetime import datetime
import sys

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration - Use exact same settings as working URL extraction file
CONFIG = {
    'url_extraction': {
        'max_workers': 15,     # Same as working file
        'batch_size': 25,      # Same as working file
        'delay': 0.05,         # Same as working file
        'timeout': 500,        # Same as working file (was 30 in combined)
        'max_urls': 10000
    },
    'content_scraping': {
        'headless': True,
        'nav_timeout': 60000,
        'max_concurrent': 3,
        'delay_between_pages': 2.0,
        'retry_attempts': 3
    },
    'output': {
        'incremental_save': True,
        'incremental_file': 'scraped_content_incremental.json',
        'final_file': 'scraped_content_final.json',
        'failed_urls_file': 'failed_urls.json'
    }
}

# Logging setup - Windows-compatible (no emoji)
import sys

# Create custom logger to avoid encoding issues
logger = logging.getLogger("combined_scraper")
logger.setLevel(logging.INFO)

# File handler with UTF-8 encoding
file_handler = logging.FileHandler('scraping.log', encoding='utf-8')
file_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
file_handler.setFormatter(file_formatter)

# Console handler with default encoding
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class URLExtractor:
    """Extracts all internal URLs from a domain"""
    
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.verify = False
        self.all_urls = set()
        self.processed = set()
        self.lock = threading.Lock()
        
        # Use exact same headers as working URL extraction file
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
        }
    
    def get_internal_urls(self, url, base_domain):
        """Extract all internal URLs from a given URL - Same logic as working separate file"""
        try:
            print("before request", url)
            
            # Use exact same approach as working file
            response = self.session.get(url, headers=self.headers, timeout=500)
            response.raise_for_status()
            print("at line 30", response)
            
            soup = BeautifulSoup(response.content, 'html.parser')
            internal_urls = set()
            
            # Find all links - exact same logic as working file
            for link in soup.find_all('a', href=True):
                href = link['href']
                # Convert relative URLs to absolute
                full_url = urljoin(url, href)
                
                # Check if URL belongs to the same domain
                parsed_url = urlparse(full_url)
                if parsed_url.netloc == base_domain:
                    if full_url.lower().endswith(('.pdf', '.ppt', '.pptx', '.doc', '.docx', 
                                                '.xls', '.xlsx', '.zip', '.rar', '.jpg', 
                                                '.jpeg', '.png', '.gif', '.mp4', '.avi', '.mp3')):
                        continue
                    # Clean URL (remove fragments)
                    clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                    if parsed_url.query:
                        clean_url += f"?{parsed_url.query}"
                    internal_urls.add(clean_url)
            
            # Small delay to be respectful
            if self.config['delay'] > 0:
                time.sleep(self.config['delay'])
            
            return internal_urls
            
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return set()
    
    def process_batch(self, urls_batch, base_domain):
        """Process a batch of URLs in parallel"""
        new_urls = set()
        
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            future_to_url = {
                executor.submit(self.get_internal_urls, url, base_domain): url 
                for url in urls_batch
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    urls_found = future.result()
                    
                    with self.lock:
                        self.processed.add(url)
                        truly_new = urls_found - self.all_urls
                        new_urls.update(truly_new)
                        self.all_urls.update(urls_found)
                    
                    print(f"✓ Processed: {url} ({len(urls_found)} URLs found)")
                    
                except Exception as e:
                    print(f"✗ Failed: {url} - {e}")
                    with self.lock:
                        self.processed.add(url)
        
        return new_urls
    
    def extract_all_urls(self, start_url):
        """Extract all URLs from the domain"""
        parsed_start = urlparse(start_url)
        base_domain = parsed_start.netloc
        
        to_process = deque([start_url])
        iteration = 0
        last_5_counts = []
        
        print(f"Starting parallel extraction from: {start_url}")
        print(f"Base domain: {base_domain}")
        print(f"Max workers: {self.config['max_workers']}, Batch size: {batch_size}")
        print("-" * 60)
        
        start_time = time.time()
        
        while to_process:
            iteration += 1
            
            current_batch = []
            batch_count = min(self.config['batch_size'], len(to_process))
            
            for _ in range(batch_count):
                if to_process:
                    url = to_process.popleft()
                    if url not in self.processed:
                        current_batch.append(url)
            
            if not current_batch:
                break
            
            print(f"\nIteration {iteration}: Processing batch of {len(current_batch)} URLs")
            iteration_start = time.time()
            
            new_urls = self.process_batch(current_batch, base_domain)
            
            for url in new_urls:
                if url not in self.processed:
                    to_process.append(url)
            
            new_count = len(new_urls)
            last_5_counts.append(new_count)
            
            iteration_time = time.time() - iteration_start
            
            print(f"Batch completed in {iteration_time:.2f}s")
            print(f"New URLs found: {new_count}")
            print(f"Total unique URLs: {len(self.all_urls)}")
            print(f"URLs in queue: {len(to_process)}")
            
            if len(last_5_counts) > 5:
                last_5_counts.pop(0)
            
            # Stopping conditions - same as working file
            if len(last_5_counts) >= 3 and sum(last_5_counts[-3:]) == 0:
                print("\nStopping: No new URLs found in last 3 iterations")
                break
            
            if len(self.all_urls) > self.config['max_urls']:
                print(f"\nSafety limit reached: {len(self.all_urls)} URLs found")
                break
        
        total_time = time.time() - start_time
        print(f"\nExtraction completed in {total_time:.2f}s")
        logger.info(f"URL extraction completed in {total_time:.2f}s")
        logger.info(f"Total URLs extracted: {len(self.all_urls)}")
        
        return sorted(self.all_urls)


class ContentScraper:
    """Scrapes content from URLs using Playwright - Same logic as working deep analysis file"""
    
    def __init__(self, config):
        self.config = config
        self.scraped_data = []
        self.failed_urls = []
        self.lock = asyncio.Lock()
        
        # Use exact same headers as working deep analysis file
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
        }
    
    def build_headers(self, target_url: str) -> dict:
        """Return realistic navigation headers including Referer and Client Hints - Same as working file"""
        try:
            parsed = urlparse(target_url)
            referer = "https://www.google.com/"
        except Exception:
            referer = "https://www.google.com/"

        merged = dict(self.headers)
        merged.setdefault('Referer', referer)
        merged.setdefault('sec-ch-ua', '"Chromium";v="123", "Not:A-Brand";v="99"')
        merged.setdefault('sec-ch-ua-mobile', '?0')
        merged.setdefault('sec-ch-ua-platform', '"Windows"')
        merged.setdefault('sec-fetch-dest', 'document')
        merged.setdefault('sec-fetch-mode', 'navigate')
        merged.setdefault('sec-fetch-site', 'none')
        merged.setdefault('sec-fetch-user', '?1')
        merged.setdefault('upgrade-insecure-requests', '1')
        return merged
    
    async def auto_scroll(self, page):
        """Slow scroll to trigger lazy loading - Same as working file"""
        try:
            await page.evaluate(
                """async () => {
                    const step = 300;
                    const delay = ms => new Promise(r => setTimeout(r, ms));
                    let pos = 0;
                    const height = document.body.scrollHeight;
                    while (pos < height) {
                        window.scrollBy(0, step);
                        pos += step;
                        await delay(200);
                    }
                    window.scrollTo(0,0);
                }"""
            )
        except Exception:
            logger.debug("auto_scroll: ignore scroll script failure", exc_info=True)
    
    async def extract_structured_content(self, page):
        """Extract structured content from the page - Same logic as working deep analysis file"""
        logger.debug("inside extract_structured_content")
        js = r"""
        () => {
          const containers = Array.from(document.querySelectorAll(
            "article.uol-c-card, main, section.uol-c-section, div.uol-c-section__inner"
          ));
          const results = [];
          const seen = new Set();

          function normalizeText(s){
            return (s || "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
          }

          for (const container of containers) {
            // First, handle article cards that might not have <h*> but use .uol-c-card__title
            const cardTitle = container.querySelector(".uol-c-card__title");
            const isCard = container.tagName && container.tagName.toLowerCase() === "article" && cardTitle;
            if (isCard) {
              const title = normalizeText(cardTitle.innerText);
              if (title && !seen.has(title)) {
                seen.add(title);
                const body = container.querySelector(".uol-c-card__body");
                const content = body ? normalizeText(body.innerText) : normalizeText(container.innerText.replace(cardTitle.innerText, ""));
                results.push({ heading: title, content: content });
              }
              // still continue to next container (do not attempt generic headings inside card)
              continue;
            }

            // For generic containers: gather headings in-order
            const headings = Array.from(container.querySelectorAll("h1, h2, h3, h4, h5, h6"));
            if (headings.length === 0) continue;

            for (let i = 0; i < headings.length; i++) {
              const h = headings[i];
              const headingText = normalizeText(h.innerText);
              if (!headingText || seen.has(headingText)) continue;
              // Build a Range: from immediately after this heading to before the next heading in this container
              const range = document.createRange();
              try {
                range.setStartAfter(h);
              } catch (e) {
                // fallback: if can't set start after, continue
                continue;
              }
              const nextHeading = (i + 1 < headings.length) ? headings[i + 1] : null;
              try {
                if (nextHeading) {
                  range.setEndBefore(nextHeading);
                } else {
                  // end at end of container
                  const last = container.lastChild || h;
                  range.setEndAfter(last);
                }
              } catch (e) {
                // ignore range errors
              }
              let content = normalizeText(range.toString());

              // If range gave no content, try useful fallbacks:
              if (!content) {
                // 1) If heading sits inside an article/card, try its .uol-c-card__body
                const cardAncestor = h.closest("article");
                if (cardAncestor) {
                  const cbody = cardAncestor.querySelector(".uol-c-card__body");
                  if (cbody) content = normalizeText(cbody.innerText);
                }
                // 2) If container has a prominent summary paragraph before cards, try capturing that
                if (!content) {
                  const p = h.nextElementSibling;
                  if (p && p.tagName && p.tagName.toLowerCase() === "p") {
                    content = normalizeText(p.innerText);
                  }
                }
              }

              // Final cleanup: strip any accidental heading repetition inside content
              if (content && content.startsWith(headingText)) {
                content = content.substring(headingText.length).trim();
              }

              seen.add(headingText);
              results.push({ heading: headingText, content: content });
            }
          }

          return results;
        }
        """
        
        try:
            result = await page.evaluate(js)
            logger.debug("JS extraction returned %d blocks", len(result) if isinstance(result, list) else -1)
            return result or []
        except Exception:
            logger.exception("JS extraction failed")
            return []
    
    async def scrape_single_page(self, browser, url, semaphore):
        """Scrape content from a single page - Same logic as working deep analysis file"""
        async with semaphore:
            context = None
            page = None
            try:
                req_headers = self.build_headers(url)
                context = await browser.new_context(
                    user_agent=req_headers.get('User-Agent'),
                    extra_http_headers=req_headers,
                    locale='en-US',
                    timezone_id='UTC'
                )
                page = await context.new_page()
                page.set_default_navigation_timeout(60000)  # Same as working file

                logger.info("Visiting %s", url)
                
                # Try navigation with brief retries to mitigate transient bot checks - Same as working file
                attempts = 3
                nav_response = None
                for attempt in range(1, attempts + 1):
                    try:
                        nav_response = await page.goto(
                            url, wait_until="domcontentloaded", timeout=60000
                        )
                    except Exception:
                        logger.exception("Navigation attempt %d failed", attempt)
                        nav_response = None

                    # Validate navigation response
                    ok = False
                    try:
                        if nav_response is not None:
                            status_code = nav_response.status
                            ok = isinstance(status_code, int) and status_code < 400
                    except Exception:
                        logger.debug("Unable to read navigation response/status", exc_info=True)

                    if ok:
                        break
                    if attempt < attempts:
                        wait_ms = 1000 * attempt
                        logger.warning(
                            "Nav not OK (resp=%s). Retrying in %d ms...",
                            getattr(nav_response, 'status', None), wait_ms
                        )
                        await page.wait_for_timeout(wait_ms)
                        
                # Final check
                if nav_response is None:
                    logger.error("No response received for URL: %s", url)
                    await self.add_failed_url(url, "No response received")
                    return None
                try:
                    status_code = nav_response.status
                    if isinstance(status_code, int) and status_code >= 400:
                        logger.error("HTTP %s received for URL: %s", status_code, url)
                        await self.add_failed_url(url, f"HTTP {status_code}")
                        return None
                except Exception:
                    logger.debug("Unable to read final navigation status", exc_info=True)

                # give lazy JS a short time then scroll - Same timing as working file
                await page.wait_for_timeout(800)
                await self.auto_scroll(page)
                # wait a bit for lazy content to load after scroll
                await page.wait_for_timeout(1200)

                # Extract content
                data = await self.extract_structured_content(page)
                
                # per-heading log summary - Same as working file
                logger.info("Extracted %d heading blocks", len(data))
                for i, item in enumerate(data, start=1):
                    heading_text = item.get("heading") or ""
                    content_text = item.get("content") or ""
                    logger.debug("[%d] %r (content length = %d)", i, heading_text, len(content_text))
                
                result = {
                    'url': url,
                    'title': await page.title(),
                    'scraped_at': datetime.now().isoformat(),
                    'content_blocks': data,
                    'total_blocks': len(data)
                }
                
                # Save incrementally
                if self.config.get('incremental_save', True):
                    await self.save_incremental(result)
                
                logger.info("Scraped %d blocks from %s", len(data), url)
                return result

            except Exception as e:
                logger.exception("Unhandled error during scrape_single_page for %s", url)
                await self.add_failed_url(url, str(e))
                return None
                
            finally:
                # best-effort cleanup - Same as working file
                try:
                    if page is not None:
                        await page.close()
                except Exception:
                    logger.debug("Ignoring page.close error", exc_info=True)
                try:
                    if context is not None:
                        await context.close()
                except Exception:
                    logger.debug("Ignoring context.close error", exc_info=True)
    
    async def save_incremental(self, result):
        """Save result incrementally"""
        async with self.lock:
            try:
                filename = CONFIG['output']['incremental_file']
                
                # Read existing data
                existing_data = []
                if os.path.exists(filename):
                    with open(filename, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                
                # Add new result
                existing_data.append(result)
                
                # Write back
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, ensure_ascii=False, indent=2)
                    
            except Exception as e:
                logger.error(f"Failed to save incremental data: {e}")
    
    async def add_failed_url(self, url, error):
        """Add failed URL to tracking"""
        async with self.lock:
            self.failed_urls.append({
                'url': url,
                'error': error,
                'timestamp': datetime.now().isoformat()
            })
    
    async def scrape_all_urls(self, urls):
        """Scrape content from all URLs"""
        logger.info(f"Starting content scraping for {len(urls)} URLs")
        
        # Limit concurrent browser instances
        semaphore = asyncio.Semaphore(self.config['max_concurrent'])
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.config['headless'],
                args=["--disable-blink-features=AutomationControlled"]
            )
            
            try:
                tasks = []
                for url in urls:
                    task = self.scrape_single_page(browser, url, semaphore)
                    tasks.append(task)
                    
                    # Add delay between task creation
                    if self.config['delay_between_pages'] > 0:
                        await asyncio.sleep(self.config['delay_between_pages'])
                
                # Execute all tasks
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Filter successful results
                successful_results = [r for r in results if r is not None and not isinstance(r, Exception)]
                
                logger.info(f"Successfully scraped {len(successful_results)} out of {len(urls)} URLs")
                
                return successful_results
                
            finally:
                await browser.close()


class CombinedScraper:
    """Main class that combines URL extraction and content scraping"""
    
    def __init__(self, config=None):
        self.config = config or CONFIG
        self.url_extractor = URLExtractor(self.config['url_extraction'])
        self.content_scraper = ContentScraper(self.config['content_scraping'])
    
    async def scrape_domain(self, start_url):
        """Main method to scrape entire domain"""
        logger.info("="*60)
        logger.info(">> COMBINED DOMAIN SCRAPER STARTED")
        logger.info("="*60)
        logger.info(f"Target URL: {start_url}")
        
        start_time = time.time()
        
        # Step 1: Extract all URLs
        logger.info("\n" + "="*40)
        logger.info("STEP 1: EXTRACTING URLs")
        logger.info("="*40)
        
        urls = self.url_extractor.extract_all_urls(start_url)
        
        if not urls:
            logger.error("No URLs found. Exiting.")
            return
        
        logger.info(f"Found {len(urls)} URLs to scrape")
        
        # Step 2: Scrape content from all URLs
        logger.info("\n" + "="*40)
        logger.info("STEP 2: SCRAPING CONTENT")
        logger.info("="*40)
        
        scraped_results = await self.content_scraper.scrape_all_urls(urls)
        
        # Step 3: Save final results
        logger.info("\n" + "="*40)
        logger.info("STEP 3: SAVING RESULTS")
        logger.info("="*40)
        
        final_data = {
            'domain': urlparse(start_url).netloc,
            'start_url': start_url,
            'scraping_started': datetime.now().isoformat(),
            'total_urls_found': len(urls),
            'successfully_scraped': len(scraped_results),
            'failed_urls': len(self.content_scraper.failed_urls),
            'results': scraped_results
        }
        
        # Save final consolidated data
        final_file = self.config['output']['final_file']
        with open(final_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)
        
        # Save failed URLs
        if self.content_scraper.failed_urls:
            failed_file = self.config['output']['failed_urls_file']
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(self.content_scraper.failed_urls, f, ensure_ascii=False, indent=2)
        
        # Final statistics
        total_time = time.time() - start_time
        total_content_blocks = sum(len(result.get('content_blocks', [])) for result in scraped_results)
        
        logger.info("="*60)
        logger.info(">> SCRAPING COMPLETE")
        logger.info("="*60)
        logger.info(f"Total time: {total_time:.2f}s")
        logger.info(f"URLs found: {len(urls)}")
        logger.info(f"Successfully scraped: {len(scraped_results)}")
        logger.info(f"Failed: {len(self.content_scraper.failed_urls)}")
        logger.info(f"Total content blocks: {total_content_blocks}")
        logger.info(f"Final results saved to: {final_file}")
        
        if self.content_scraper.failed_urls:
            logger.info(f"Failed URLs saved to: {failed_file}")
        
        return final_data


async def main():
    """Main function"""
    # Configure the target URL here
    START_URL = "https://www.tanyapepsodent.com/"
    
    # You can modify the configuration here if needed
    scraper = CombinedScraper(CONFIG)
    
    try:
        results = await scraper.scrape_domain(START_URL)
        return results
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())