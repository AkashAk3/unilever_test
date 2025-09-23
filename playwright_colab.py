
from flask import Blueprint, request, jsonify, current_app
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import time
import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import requests
import re
import json  # Add this import for JSON handling

executor = ThreadPoolExecutor(max_workers=2)


async def fetch_html(url):
    """
    Scrape the full HTML content from a given URL using Playwright Async API.
    Returns the HTML content as a string.
    """
    from playwright.async_api import async_playwright
    try:
        async with async_playwright() as p:
            # Launch a headless browser
            # logger.info(f"Launching browser and navigating to {url}")
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            # Create page and navigate
            page = await context.new_page()
            page.set_default_timeout(30000)

            # Navigate to URL and wait for content
            await page.goto(url, wait_until='networkidle', timeout=30000)

            # Wait for page to fully load
            time.sleep(3)

            # Get the full HTML content
            html_content = await page.content()

            # Close browser
            await browser.close()

            print(f"✅ HTML fetched successfully with Playwright ({len(html_content)} characters)")
            return html_content
    except Exception as e:
        # logger.error(f"Error scraping {url}: {str(e)}")
        print("error scrapping")
        return None

def extract_internal_links(html_content, base_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc

    internal_links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        full_url = urljoin(base_url, href)
        parsed_href = urlparse(full_url)
        if parsed_href.netloc == base_domain and parsed_href.scheme in ('http', 'https'):
            internal_links.add(full_url)
    print(f"Found {len(internal_links)} internal links")
    return list(internal_links)

def process_single_link_with_context(link, app):
    """Process a single link within Flask app context"""
    print("inside process_single_link_with_context")
    with app.app_context():
        return process_single_link(link)
    
from playwright.async_api import async_playwright  # Replace sync_playwright with async_playwright

# async def extract_structured_content(page):
#     """Extract structured headings and contents from the page"""
#     structured_content = []
#     try:
#         headings = await page.query_selector_all('h1, h2, h3, h4, h5, h6')
#         for heading in headings:
#             tag = await heading.evaluate("node => node.tagName.toLowerCase()")  # e.g., 'h1'
#             heading_text = (await heading.inner_text()).strip()

#             # Extract content after this heading until the next heading
#             content_script = """
#             (heading) => {
#                 let content = '';
#                 let next = heading.nextElementSibling;
#                 while (next && !next.tagName.match(/^H[1-6]$/)) {
#                     if (next.tagName === 'P' || next.tagName === 'DIV' || next.textContent.trim()) {
#                         content += next.textContent.trim() + ' ';
#                     }
#                     next = next.nextElementSibling;
#                 }
#                 return content.trim();
#             }
#             """
#             content = await page.evaluate(content_script, heading)

#             if heading_text or content:  # Only add if there's meaningful data
#                 structured_content.append({
#                     'heading': heading_text,
#                     'content': f"{content}".strip()  # Combine heading text with content if desired
#                 })
#     except Exception as e:
#         print(f"❌ Error extracting structured content: {e}")

# #     return structured_content


# async def extract_structured_content(page):
#     """Extract structured headings and contents from the main content area of the page"""
#     structured_content = []
#     try:
#         # Focus on the main content area by excluding common header and footer elements
#         main_content_selector = "main, article, div:not([class*='header']):not([class*='footer'])"
#         main_content = await page.query_selector(main_content_selector)

#         if not main_content:
#             print("❌ Main content area not found")
#             return structured_content

#         headings = await main_content.query_selector_all('h1, h2, h3, h4, h5, h6')
#         for heading in headings:
#             tag = await heading.evaluate("node => node.tagName.toLowerCase()")  # e.g., 'h1'
#             heading_text = (await heading.inner_text()).strip()

#             # Extract content after this heading until the next heading
#             content_script = """
#             (heading) => {
#                 let content = '';
#                 let next = heading.nextElementSibling;
#                 while (next && !next.tagName.match(/^H[1-6]$/)) {
#                     if (next.tagName === 'P' || next.tagName === 'DIV' || next.textContent.trim()) {
#                         content += next.textContent.trim() + ' ';
#                     }
#                     next = next.nextElementSibling;
#                 }
#                 return content.trim();
#             }
#             """
#             content = await page.evaluate(content_script, heading)

#             if heading_text or content:  # Only add if there's meaningful data
#                 structured_content.append({
#                     'heading': tag,
#                     'content': f"{heading_text} {content}".strip()  # Combine heading text with content if desired
#                 })
#     except Exception as e:
#         print(f"❌ Error extracting structured content: {e}")

#     return structured_content

# async def extract_structured_content(page):
#     """Extract structured headings and contents from the main content area of the page"""
#     structured_content = []
#     try:
#         # Select only meaningful containers (exclude nav/header/footer)
#         main_content_selector = """
#         main#main-content,
#         div.uol-c-module,
#         section.uol-c-module,
#         div.uol-content-wrapper,
#         div.uol-layout,
#         div#content
#         """
#         main_contents = await page.query_selector_all(main_content_selector)

#         if not main_contents:
#             print("❌ Main content area not found")
#             return structured_content

#         for main_content in main_contents:
#             headings = await main_content.query_selector_all("h1, h2, h3, h4, h5, h6")
#             for heading in headings:
#                 tag = await heading.evaluate("node => node.tagName.toLowerCase()")
#                 heading_text = (await heading.inner_text()).strip()

#                 # Extract content after this heading until the next heading
#                 content_script = """
#                 (heading) => {
#                     let content = '';
#                     let next = heading.nextElementSibling;
#                     while (next && !next.tagName.match(/^H[1-6]$/)) {
#                         if (next.tagName === 'P' || next.tagName === 'DIV' || next.textContent.trim()) {
#                             content += next.textContent.trim() + ' ';
#                         }
#                         next = next.nextElementSibling;
#                     }
#                     return content.trim();
#                 }
#                 """
#                 content = await page.evaluate(content_script, heading)

#                 if heading_text or content:
#                     structured_content.append({
#                         "heading": tag,
#                         "content": f"{heading_text} {content}".strip()
#                     })
#     except Exception as e:
#         print(f"❌ Error extracting structured content: {e}")

#     return structured_content
 
# async def extract_structured_content(page):
#     """Extract structured headings & content from Unilever pages, skipping company/news sections."""

#     structured_content = []
#     skip_keywords = ["news", "press release", "latest news", "about us", "our company", "careers", "legal", "contact", "cookies", "privacy", "sustainability", "investors", "suppliers"]

#     try:
#         # Potential main content wrappers to try
#         possible_wrappers = [
#             "main",                                  # generic main
#             "div[class*='content']",                 # class contains “content”
#             "section[class*='content']",
#             "div[id*='content']",
#             "section[id*='content']",
#             "div[class*='page']",
#             "section[class*='page']"
#         ]

#         main_contents = []
#         for sel in possible_wrappers:
#             found = await page.query_selector_all(sel)
#             if found:
#                 main_contents.extend(found)

#         # Optionally, filter out wrappers that seem too small or clearly footers
#         filtered_main_contents = []
#         for mc in main_contents:
#             text = (await mc.inner_text()).strip()
#             # Skip wrappers that have tiny content or are just nav/footer
#             if len(text) < 200:   # arbitrary threshold
#                 continue
#             # Skip wrappers whose text is mostly navigation or skip keywords
#             tlower = text.lower()
#             if any(kw in tlower for kw in skip_keywords) and len(text.split()) < 100:
#                 continue
#             filtered_main_contents.append(mc)

#         if not filtered_main_contents:
#             print("❌ Main content area not found (after fallback heuristics)")
#             return structured_content

#         for main_content in filtered_main_contents:
#             headings = await main_content.query_selector_all("h1, h2, h3, h4, h5, h6")
#             for heading in headings:
#                 heading_text_raw = (await heading.inner_text()).strip()
#                 heading_text = heading_text_raw.lower()

#                 # Skip if the heading is one of the sections we don't want
#                 if any(kw in heading_text for kw in skip_keywords):
#                     continue

#                 tag = await heading.evaluate("node => node.tagName.toLowerCase()")

#                 # Extract content after this heading until the next heading or until content wrapper ends
#                 content_script = """
#                 (heading, skipList) => {
#                     let content = '';
#                     let next = heading.nextElementSibling;
#                     while (next) {
#                         if (next.tagName && next.tagName.match(/^H[1-6]$/)) {
#                             break;
#                         }
#                         let text = (next.textContent || '').trim();
#                         if (text) {
#                             // skip if this block is itself a heading for skip sections
#                             let tl = text.toLowerCase();
#                             let skip = skipList.some(kw => tl.includes(kw));
#                             if (!skip) {
#                                 content += text + ' ';
#                             }
#                         }
#                         next = next.nextElementSibling;
#                     }
#                     return content.trim();
#                 }
#                 """
#                 content = await page.evaluate(content_script, heading, skip_keywords)

#                 # store only if there's meaningful content
#                 if heading_text_raw and content:
#                     structured_content.append({
#                         "heading_tag": tag,
#                         "heading_text": heading_text_raw,
#                         "content": content
#                     })

#     except Exception as e:
#         print(f"❌ Error extracting structured content: {e}")

#     return 


async def extract_structured_content(page):
    """
    Extract structured content from the page using BeautifulSoup.
    Falls back gracefully if main container is not found.
    """
    try:
        html_content = await page.content()
        soup = BeautifulSoup(html_content, "html.parser")

        # Try to find main content container
        container = (
            soup.select_one("main")
            or soup.select_one("article")
            or soup.select_one("div.uco-l-content")
            or soup.select_one("div[role='main']")
        )

        if not container:
            print("⚠️ No main container found, falling back to whole body")
            container = soup.body

        structured_content = []
        for heading in container.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            heading_text = heading.get_text(strip=True)
            content_parts = []
            # Collect paragraphs/divs until the next heading
            for sibling in heading.find_next_siblings():
                if sibling.name and sibling.name.startswith("h"):
                    break
                if sibling.name in ["p", "div"] and sibling.get_text(strip=True):
                    content_parts.append(sibling.get_text(strip=True))
            structured_content.append({
                "heading": heading_text,
                "content": " ".join(content_parts).strip()
            })

        return structured_content

    except Exception as e:
        print(f"❌ Error extracting structured content: {e}")
        return []





async def capture_screenshot_and_metadata(page, link):
    """Captures full-page screenshot and metadata (title, url)"""
    try:
        title = await page.title()

        # Capture screenshot as base64
        screenshot_bytes = await page.screenshot(full_page=True)
        screenshot_base64 = base64.b64encode(screenshot_bytes).decode("utf-8")

        screenshot_info = {
            "image_title": title,
            "screenshot_base64": screenshot_base64[:10],
            "image_url_link": link
        }

        return screenshot_info
    except Exception as e:
        print(f"❌ Failed to capture screenshot for {link}: {e}")
        return {
            "image_title": "",
            "screenshot_base64": "",
            "image_url_link": link
        }

async def process_single_link(link, page):
    """Process a single internal link with structured heading extraction and screenshot capture using Playwright Async API"""
    print(f"🔍 Processing internal link: {link}")

    try:
        # Navigate to the page and wait for it to load
        await page.goto(link, wait_until="networkidle", timeout=30000)  # Adjust timeout as needed

        # Extract structured content
        structured_content = await extract_structured_content(page)

        # Capture screenshot and metadata
        screenshot_info = await capture_screenshot_and_metadata(page, link)

        image_sections = {"datas": [screenshot_info]}

    except Exception as e:
        print(f"❌ Error processing link {link}: {e}")
        return None

    if not structured_content:
        print(f"❌ No structured content extracted for {link}")
        return None

    print(f"✅ Successfully processed: {link}")
    return {
        "processed_content": structured_content,
        "image_sections": image_sections
    }


async def scrape_and_process(url):
    """Main scraping endpoint"""
    try:
        if not url:
            return json.dumps({"error": "URL is required"})

        print(f"🚀 Starting scrape process for: {url}")

        # Fetch main page HTML
        print(f"📥 Fetching main URL: {url}")
        html = await fetch_html(url)
        if html is None:
            return json.dumps({"error": "Failed to fetch URL"})

        # Extract internal links
        internal_links = extract_internal_links(html, url)
        if not internal_links:
            return json.dumps({"error": "No internal links found"})

        print(f"📋 Processing {len(internal_links)} internal links")

        # Process each link in parallel with a shared browser context
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            async def process_link(link):
                try:
                    page = await context.new_page()
                    result = await process_single_link(link, page)
                    await page.close()
                    return result
                except Exception as e:
                    print(f"❌ Failed to process link {link}: {e}")
                    return None

            tasks = [process_link(link) for link in internal_links]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            await browser.close()
        print("type of results",type(results))
        # Filter out None results and handle exceptions
        processed_contents = results

        #screenshot
        # screenshot_info = await capture_screenshot_and_metadata(page, link)

        # Aggregate results
        result = {
            "processed_content": processed_contents,
            "stats": {
                "total_links_found": len(internal_links),
                "successfully_processed": len(processed_contents),
                "main_url": url
            }
        }

        print(f"✅ Scraping completed. Processed {len(processed_contents)} out of {len(internal_links)} links")
        # printting each link heading with content with link number
        print("All processed content:\n")
        for i, content in enumerate(processed_contents, start=1):
            print(f"Link {i} : {internal_links[i-1]}: {content}")
        return json.dumps(result)

    except Exception as e:
        print(f"💥 Error in /scrape: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"error": "Internal server error", "details": str(e)})

if __name__ == "__main__":
    url = "https://www.unilever.com/news/news-search/2025/unilevers-100-accelerator-partnership-unlocks-ai-innovation-across-supply-chain/"
    try:
        print("Starting the scraping process...")
        asyncio.run(scrape_and_process(url))
        print("Scraping process completed.")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
