from flask import Blueprint, request, jsonify, current_app
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import time
import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import requests
import re
import json

executor = ThreadPoolExecutor(max_workers=2)

# ------------------------------
# Fetch HTML (without Playwright)
# # ------------------------------
# async def fetch_html(url):
#     try:
#         headers = {
#             "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
#                           "AppleWebKit/537.36 (KHTML, like Gecko) "
#                           "Chrome/120.0.0.0 Safari/537.36"
#         }
#         resp = requests.get(url, headers=headers, timeout=30)
#         resp.raise_for_status()
#         print(f"✅ HTML fetched successfully with requests ({len(resp.text)} characters)")
#         return resp.text
#     except Exception as e:
#         print(f"❌ Error fetching {url}: {str(e)}")
#         return None
async def fetch_html(url):
    from playwright.async_api import async_playwright
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            page.set_default_timeout(30000)

            await page.goto(url, wait_until='networkidle', timeout=30000)
            html_content = await page.content()
            await browser.close()

            print(f"✅ HTML fetched successfully with Playwright ({len(html_content)} characters)")
            return html_content
    except Exception as e:
        print(f"❌ Error fetching {url}: {str(e)}")
        return None

# ------------------------------
# Extract internal links
# ------------------------------
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

# ------------------------------
# Extract structured content
# ------------------------------
def extract_structured_content(html):
    structured_content = []
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Look for main containers
        container = (
            soup.find("main")
            or soup.find("article")
            or soup.find("div", {"id": "main"})
            or soup.find("div", {"role": "main"})
        )

        if not container:
            print("⚠️ Falling back to full page")
            container = soup

        # Extract all headings
        headings = container.find_all(re.compile(r"^h[1-6]$", re.I))
        if not headings:
            print("⚠️ No headings found, extracting only paragraphs")
            for p in container.find_all("p"):
                text = p.get_text(strip=True)
                if text:
                    structured_content.append({"heading": "", "content": text})
            return structured_content

        # For each heading, capture following siblings until next heading
        for heading in headings:
            heading_text = heading.get_text(strip=True)
            content_parts = []
            for sibling in heading.find_next_siblings():
                if sibling.name and re.match(r"^h[1-6]$", sibling.name, re.I):
                    break
                if sibling.name in ["p", "div"]:
                    text = sibling.get_text(strip=True)
                    if text:
                        content_parts.append(text)
            content = " ".join(content_parts).strip()
            structured_content.append({
                "heading": heading_text,
                "content": content
            })

    except Exception as e:
        print(f"❌ Error extracting structured content: {e}")

    return structured_content

# ------------------------------
# Process a single link
# ------------------------------
def process_single_link(link):
    print(f"🔍 Processing internal link: {link}")
    try:
        html = requests.get(link, headers={"User-Agent": "Mozilla/5.0"}).text
        structured_content = extract_structured_content(html)
        if not structured_content:
            print(f"❌ No structured content extracted for {link}")
            return None

        return {
            "processed_content": structured_content,
            "image_sections": {
                "datas": [{
                    "image_title": soup_title(html),
                    "screenshot_base64": "",  # No screenshot with BeautifulSoup
                    "image_url_link": link
                }]
            }
        }
    except Exception as e:
        print(f"❌ Error processing link {link}: {e}")
        return None

# Helper to get <title>
def soup_title(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.title.string.strip() if soup.title else ""

# ------------------------------
# Main scraping flow
# ------------------------------
async def scrape_and_process(url):
    try:
        if not url:
            return json.dumps({"error": "URL is required"})

        print(f"🚀 Starting scrape process for: {url}")

        # Fetch main page
        html = await fetch_html(url)
        if html is None:
            return json.dumps({"error": "Failed to fetch URL"})

        # Extract internal links
        internal_links = extract_internal_links(html, url)
        if not internal_links:
            return json.dumps({"error": "No internal links found"})

        print(f"📋 Processing {len(internal_links)} internal links")

        # Process links in parallel
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, process_single_link, link) for link in internal_links]
        results = await asyncio.gather(*tasks)

        processed_contents = [r for r in results if r]

        result = {
            "processed_content": processed_contents,
            "stats": {
                "total_links_found": len(internal_links),
                "successfully_processed": len(processed_contents),
                "main_url": url
            }
        }

        print("All processed content:\n")
        for i, content in enumerate(processed_contents, start=1):
            print(f"Link {i} : {internal_links[i-1]}: {content}")
        # return json.dumps(result)

        print(f"✅ Scraping completed. Processed {len(processed_contents)} out of {len(internal_links)} links")
        return json.dumps(result, indent=2)

    except Exception as e:
        print(f"💥 Error in /scrape: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"error": "Internal server error", "details": str(e)})


if __name__ == "__main__":
    url = "https://www.unilever.com"
    try:
        print("Starting the scraping process...")
        asyncio.run(scrape_and_process(url))
        print("Scraping process completed.")
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
